pub mod client;
pub mod processor;
pub mod routes;
pub mod server;
pub mod state;
pub mod validation;

pub use client::Client;
pub use processor::rebuild_all_logs;
pub use routes::{
    ConsistencyQuery, InclusionQuery, get_consistency_proof, get_inclusion_proof, get_log_size,
    get_merkle_root,
};
pub use server::{create_server, initialize_app_state, run_server};
pub use state::MerkleState;
pub use validation::{validate_all_logs, print_validation_report, LogValidation};