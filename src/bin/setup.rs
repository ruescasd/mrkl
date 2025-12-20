/// cargo run --bin setup -- --reset
use anyhow::Result;
use std::env;
use tokio_postgres::{Client, NoTls};

/// Connects to the database and returns a client.
async fn connect() -> Result<Client> {
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env file");
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
        client
            .batch_execute(
                r#"
            DROP FUNCTION IF EXISTS process_next_batch(INT);
            DROP TABLE IF EXISTS merkle_log;
            DROP TABLE IF EXISTS source_log;
        "#,
            )
            .await?;
        println!("✅ Existing objects dropped.");
    }

    // 1. Create the append-only source table
    client
        .batch_execute(
            r#"
        CREATE TABLE IF NOT EXISTS source_log (
            id          SERIAL PRIMARY KEY,
            data        TEXT NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            leaf_hash   BYTEA NOT NULL
        );
    "#,
        )
        .await?;
    println!("✅ Table 'source_log' is ready.");

    // 2. Create the merkle log table with strict controls - Now without data column
    client
        .batch_execute(
            r#"
        CREATE TABLE IF NOT EXISTS merkle_log (
            id BIGINT PRIMARY KEY,
            source_id BIGINT NOT NULL,
            leaf_hash BYTEA NOT NULL,
            processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT merkle_log_immutable CHECK (processed_at = processed_at)
        );

        -- Index on source_id for lookups and duplicate prevention
        CREATE UNIQUE INDEX merkle_log_source_idx ON merkle_log(source_id);
        -- Index on leaf_hash for proof generation
        CREATE INDEX merkle_log_hash_idx ON merkle_log(leaf_hash);
    "#,
        )
        .await?;
    println!("✅ Table 'merkle_log' is ready.");

    // 3. Create the processing function
    client
        .batch_execute(
            r#"
        CREATE OR REPLACE FUNCTION process_next_batch(
            batch_size INT DEFAULT 10000
        ) RETURNS TABLE (
            rows_processed BIGINT,
            first_id BIGINT,
            last_id BIGINT
        ) LANGUAGE plpgsql AS $$
        DECLARE
            next_id BIGINT;
            last_processed_source_id BIGINT;
            rows_affected BIGINT;
            batch_first_id BIGINT;
            batch_last_id BIGINT;
        BEGIN
            -- Get an advisory lock to ensure only one process runs at a time
            IF NOT pg_try_advisory_xact_lock(hashtext('process_next_batch')) THEN
                RAISE EXCEPTION 'Another processing batch is running';
            END IF;

            -- Find our starting points
            SELECT COALESCE(MAX(id), 0) + 1 INTO next_id FROM merkle_log;
            SELECT COALESCE(MAX(source_id), 0) INTO last_processed_source_id FROM merkle_log;

            -- Start a CTE for atomic processing
            WITH source_rows AS (
                -- Select unprocessed rows using index scan
                SELECT
                    al.id as source_id,
                    al.leaf_hash,
                    al.id - last_processed_source_id as row_offset
                FROM source_log al
                WHERE al.id > last_processed_source_id
                ORDER BY al.id
                LIMIT batch_size
            ),
            inserted AS (
                -- Insert rows with sequential IDs based on row_offset (no data column)
                INSERT INTO merkle_log (id, source_id, leaf_hash)
                SELECT
                    next_id + row_offset - 1,
                    source_id,
                    leaf_hash
                FROM source_rows
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
    "#,
        )
        .await?;
    println!("✅ Function 'process_next_batch' is ready.");

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
