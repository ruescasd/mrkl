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
            DROP TABLE IF EXISTS append_only_log;
        "#).await?;
        println!("✅ Existing objects dropped.");
    }

    // 1. Create the append-only table.
    client.batch_execute(r#"
        CREATE TABLE IF NOT EXISTS append_only_log (
            id          SERIAL PRIMARY KEY,
            data        TEXT NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            leaf_hash   BYTEA NOT NULL
        );
    "#).await?;
    println!("✅ Table 'append_only_log' is ready.");

    // 2. Create the function that sends a notification.
    client.batch_execute(r#"
        CREATE OR REPLACE FUNCTION notify_new_row()
        RETURNS TRIGGER AS $$
        BEGIN
            -- Payload is the ID of the new row
            PERFORM pg_notify('new_row_channel', NEW.id::text);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    "#).await?;
    println!("✅ Notification function 'notify_new_row' is ready.");
    
    // 3. Create the trigger that executes the function on INSERT.
    client.batch_execute(r#"
        -- Drop the trigger first to ensure it's not duplicated
        DROP TRIGGER IF EXISTS on_new_row_trigger ON append_only_log;

        CREATE TRIGGER on_new_row_trigger
        AFTER INSERT ON append_only_log
        FOR EACH ROW
        EXECUTE FUNCTION notify_new_row();
    "#).await?;
    println!("✅ Trigger 'on_new_row_trigger' is attached.");

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