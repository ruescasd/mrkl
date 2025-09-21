use anyhow::Result;
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, NoTls};

/// Connects to the database and returns a client.
async fn connect() -> Result<Client> {
    let db_url = std::env::var("DATABASE_URL")
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
async fn setup_database(client: &Client) -> Result<()> {
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

    let client = connect().await?;

    println!("--- Setting up database schema ---");
    setup_database(&client).await?;
    println!("----------------------------------");

    println!("\n--- Inserting 5 new log entries ---");
    let statement = client.prepare(
        "INSERT INTO append_only_log (data, leaf_hash) VALUES ($1, $2)"
    ).await?;

    for i in 1..=5 {
        let data = format!("This is log entry number {}", i);
        
        // In a real system, you would hash a canonical representation of the row's data.
        let mut hasher = Sha256::new();
        hasher.update(data.as_bytes());
        let hash_result = hasher.finalize();

        client.execute(&statement, &[&data, &hash_result.as_slice()]).await?;
        println!("Inserted entry '{}'", data);
        
        // Add a small delay to simulate real-world usage
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
    println!("------------------------------------");

    Ok(())
}