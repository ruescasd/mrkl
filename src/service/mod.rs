pub mod client;
pub mod metrics;
pub mod processor;
pub mod responses;
pub mod routes;
pub mod server;
pub mod state;
pub mod validation;

pub use client::Client;
pub use processor::rebuild_all_logs;
pub use responses::{
    ApiError, ApiResponse, ConsistencyProofResponse, HasLeafResponse, HasRootResponse,
    InclusionProofResponse, RootResponse, SizeResponse, MetricsResponse,
    LogMetricsResponse, GlobalMetricsResponse,
};
pub use routes::{
    ConsistencyQuery, HasLeafQuery, HasRootQuery, InclusionQuery, get_consistency_proof,
    get_inclusion_proof, get_log_size, get_merkle_root, has_leaf, has_root,
    admin_pause, admin_resume, admin_stop, admin_status, AdminControlResponse,
};
pub use server::{create_server, initialize_app_state, run_server};
pub use state::MerkleState;
pub use validation::{validate_all_logs, print_validation_report, LogValidation};