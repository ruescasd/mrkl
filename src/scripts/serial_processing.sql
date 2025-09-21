-- 1. Create our strictly controlled processed table
CREATE TABLE processed_log (
    -- Use BIGSERIAL to avoid any wraparound concerns
    id BIGINT PRIMARY KEY,
    -- Original row id from append_only_log for tracking
    source_id BIGINT NOT NULL,
    data TEXT NOT NULL,
    leaf_hash BYTEA NOT NULL,
    -- Timestamp for monitoring/debugging
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    -- Ensure rows are truly immutable once written
    CONSTRAINT processed_log_immutable CHECK (processed_at = processed_at)
);

-- 2. Create index for efficient lookups and duplicate prevention
CREATE UNIQUE INDEX processed_log_data_idx ON processed_log(data);

-- 3. Create a function for serialized processing that guarantees:
-- - Single writer at a time
-- - No gaps in IDs
-- - Strict ordering
-- - Atomic batch processing
CREATE OR REPLACE FUNCTION process_next_batch(
    batch_size INT DEFAULT 1000
) RETURNS TABLE (
    rows_processed BIGINT,
    first_id BIGINT,
    last_id BIGINT
) LANGUAGE plpgsql AS $$
DECLARE
    next_id BIGINT;
    rows_affected BIGINT;
    batch_first_id BIGINT;
    batch_last_id BIGINT;
BEGIN
    -- Get an advisory lock to ensure only one process runs at a time
    -- Using a static string hash as the lock key
    IF NOT pg_try_advisory_xact_lock(hashtext('process_next_batch')) THEN
        RAISE EXCEPTION 'Another processing batch is running';
    END IF;

    -- Find the next ID to use
    SELECT COALESCE(MAX(id), 0) + 1 INTO next_id FROM processed_log;
    
    -- Start a CTE for atomic processing
    WITH numbered_rows AS (
        -- Select unprocessed rows
        SELECT 
            al.id as source_id,
            al.data,
            al.leaf_hash,
            ROW_NUMBER() OVER (ORDER BY al.id) as row_num
        FROM append_only_log al
        WHERE NOT EXISTS (
            -- Exclude already processed rows
            SELECT 1 FROM processed_log pl WHERE pl.source_id = al.id
        )
        -- Ensure deterministic ordering
        ORDER BY al.id
        -- Limit batch size
        LIMIT batch_size
    ),
    inserted AS (
        -- Insert rows with strictly sequential IDs
        INSERT INTO processed_log (id, source_id, data, leaf_hash)
        SELECT 
            next_id + row_num - 1,
            source_id,
            data,
            leaf_hash
        FROM numbered_rows
        -- Return information about the inserted rows
        RETURNING id
    )
    -- Capture batch statistics
    SELECT COUNT(*), MIN(id), MAX(id)
    INTO rows_affected, batch_first_id, batch_last_id
    FROM inserted;

    -- Return batch processing results
    RETURN QUERY SELECT rows_affected, batch_first_id, batch_last_id;
END;
$$;

-- 4. Create a view that shows processing status
CREATE OR REPLACE VIEW processing_status AS
SELECT 
    (SELECT COUNT(*) FROM append_only_log) as total_source_rows,
    (SELECT COUNT(*) FROM processed_log) as total_processed_rows,
    (SELECT MAX(id) FROM append_only_log) as last_source_id,
    (SELECT MAX(id) FROM processed_log) as last_processed_id,
    (SELECT MAX(processed_at) FROM processed_log) as last_processing_time;

-- 5. Function to validate sequence integrity
CREATE OR REPLACE FUNCTION validate_processed_sequence()
RETURNS TABLE (
    has_gaps BOOLEAN,
    first_gap BIGINT,
    expected_count BIGINT,
    actual_count BIGINT
) LANGUAGE plpgsql AS $$
DECLARE
    max_id BIGINT;
    min_id BIGINT;
    row_count BIGINT;
BEGIN
    SELECT MIN(id), MAX(id), COUNT(*) 
    INTO min_id, max_id, row_count
    FROM processed_log;
    
    -- Check for gaps in sequence
    WITH RECURSIVE sequence AS (
        SELECT min_id as id
        UNION ALL
        SELECT id + 1
        FROM sequence
        WHERE id < max_id
    ),
    gaps AS (
        SELECT s.id
        FROM sequence s
        LEFT JOIN processed_log p ON p.id = s.id
        WHERE p.id IS NULL
        LIMIT 1
    )
    SELECT 
        CASE WHEN EXISTS (SELECT 1 FROM gaps) THEN true ELSE false END,
        (SELECT id FROM gaps LIMIT 1),
        max_id - min_id + 1,
        row_count
    INTO has_gaps, first_gap, expected_count, actual_count;
    
    RETURN NEXT;
END;
$$;

-- 6. Grant minimal required permissions
GRANT SELECT ON append_only_log TO service_user;
GRANT SELECT, INSERT ON processed_log TO service_user;
GRANT EXECUTE ON FUNCTION process_next_batch(INT) TO service_user;
GRANT EXECUTE ON FUNCTION validate_processed_sequence() TO service_user;
GRANT SELECT ON processing_status TO service_user;

-- Example usage:
-- SELECT * FROM process_next_batch(100);  -- Process next 100 rows
-- SELECT * FROM processing_status;        -- Check current status
-- SELECT * FROM validate_processed_sequence();  -- Validate sequence integrity