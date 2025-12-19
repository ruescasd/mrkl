/// cargo run --bin main
use anyhow::Result;
use mrkl::service;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    // Initialize application state (database connection + merkle state)
    let app_state = service::initialize_app_state().await?;

    // Run the complete server with batch processing
    service::run_server(app_state).await?;

    Ok(())
}
