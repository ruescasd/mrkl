/// cargo run --bin setup -- --reset
use anyhow::Result;
use tokio_postgres::{Client, NoTls};
use std::env;

/// Connects to the database and returns a client.
async fn connect() -> Result<Client> {
    let db_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set in .env file");
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(client)
}

/// Sets up the database schema, including the table, function, and trigger.
/// If reset is true, it will drop and recreate all objects.
async fn setup_database(client: &Client, reset: bool) -> Result<()> {
    if reset {
        println!("🗑️ Dropping existing objects...");
        client.batch_execute(r#"
            DROP TRIGGER IF EXISTS on_new_row_trigger ON append_only_log;
            DROP FUNCTION IF EXISTS notify_new_row();
            DROP FUNCTION IF EXISTS process_next_batch(INT);
            DROP FUNCTION IF EXISTS validate_processed_sequence();
            DROP VIEW IF EXISTS processing_status;
            DROP TABLE IF EXISTS processed_log;
            DROP TABLE IF EXISTS append_only_log;
        "#).await?;
        println!("✅ Existing objects dropped.");
    }

    // 1. Create the append-only source table
    client.batch_execute(r#"
        CREATE TABLE IF NOT EXISTS append_only_log (
            id          SERIAL PRIMARY KEY,
            data        TEXT NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            leaf_hash   BYTEA NOT NULL
        );
    "#).await?;
    println!("✅ Table 'append_only_log' is ready.");

    // 2. Create the processed log table with strict controls
    client.batch_execute(r#"
        CREATE TABLE IF NOT EXISTS processed_log (
            id BIGINT PRIMARY KEY,
            source_id BIGINT NOT NULL,
            data TEXT NOT NULL,
            leaf_hash BYTEA NOT NULL,
            processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT processed_log_immutable CHECK (processed_at = processed_at)
        );
        
        CREATE UNIQUE INDEX processed_log_data_idx ON processed_log(data);
    "#).await?;
    println!("✅ Table 'processed_log' is ready.");

    // 3. Create the processing function
    client.batch_execute(r#"
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
    "#).await?;
    println!("✅ Function 'process_next_batch' is ready.");

    // 4. Create status view
    client.batch_execute(r#"
        CREATE OR REPLACE VIEW processing_status AS
        SELECT 
            (SELECT COUNT(*) FROM append_only_log) as total_source_rows,
            (SELECT COUNT(*) FROM processed_log) as total_processed_rows,
            (SELECT MAX(id) FROM append_only_log) as last_source_id,
            (SELECT MAX(id) FROM processed_log) as last_processed_id,
            (SELECT MAX(processed_at) FROM processed_log) as last_processing_time;
    "#).await?;
    println!("✅ View 'processing_status' is ready.");

    // 5. Create validation function
    client.batch_execute(r#"
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
    "#).await?;
    println!("✅ Function 'validate_processed_sequence' is ready.");

    // 6. Add some test data if specified
    if reset {
        println!("📝 Adding test data...");
        client.batch_execute(r#"
            -- Function to calculate SHA256 hash
            CREATE OR REPLACE FUNCTION sha256(text) RETURNS bytea AS $$
            BEGIN
                RETURN decode(
                    substring(
                        encode(
                            digest($1, 'sha256'::text)::bytea, 
                            'hex'
                        ), 
                        1
                    ), 
                    'hex'
                );
            END;
            $$ LANGUAGE plpgsql IMMUTABLE STRICT;

            -- Insert test data
            INSERT INTO append_only_log (data, leaf_hash)
            SELECT 
                'test_data_' || i::text as data,
                sha256('test_data_' || i::text) as leaf_hash
            FROM generate_series(1, 5) i;
        "#).await?;
        println!("✅ Test data added (5 rows).");
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env file
    dotenv::dotenv().ok();

    // Check if --reset flag is provided
    let reset = env::args().any(|arg| arg == "--reset");

    let client = connect().await?;

    println!("--- Setting up database schema ---");
    setup_database(&client, reset).await?;
    println!("----------------------------------");

    Ok(())
}