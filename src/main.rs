use crate::config::load_config;
use crate::server::run_server;

mod api;
mod auth;
mod config;
mod connection;
mod fs_opt;
mod jwt;
mod server;
mod session;
mod shell;

/// Main entry point
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    // Load configuration
    let config = load_config();

    run_server(config).await
}
