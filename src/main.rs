use log::{error, info};
use std::collections::HashMap;
use std::env;
use std::fs::{self};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::auth::create_authenticator;
use crate::config::load_config;
use crate::connection::handle_client;
use crate::session::SessionManager;

mod auth;
mod config;
mod connection;
mod fs_opt;
mod api;
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

    let host = config.server.host.as_str();
    let port = config.server.port;

    // Create authenticator
    let authenticator = create_authenticator(&config).expect("Failed to create authenticator");

    info!("Starting TTYD server on {}:{}", host, port);
    info!(
        "Authentication: {}",
        if config.auth.enabled {
            "enabled"
        } else {
            "disabled"
        }
    );
    info!("Open http://localhost:{} in your browser", port);

    let listener = TcpListener::bind((host, port)).await?;

    // Shared current working directory state
    let initial_dir = if let Some(ref custom_dir) = config.server.cur_dir {
        // Use custom directory from config
        PathBuf::from(custom_dir)
    } else {
        // Use current working directory
        env::current_dir()?
    };

    // Validate and create the directory if it doesn't exist
    if !initial_dir.exists() {
        info!("Creating working directory: {}", initial_dir.display());
        fs::create_dir_all(&initial_dir)?;
    }

    info!("Working directory: {}", initial_dir.display());

    // Create session manager to track multiple WebSocket sessions
    let session_manager: SessionManager = Arc::new(RwLock::new(HashMap::new()));

    // Wrap authenticator in Arc for sharing across connections
    let authenticator = Arc::new(authenticator);

    // main loop
    // 1. create tcp connection
    // 2. create a new session state
    // 3. start session worker
    loop {
        let (stream, addr) = listener.accept().await?;
        info!("New connection from {}", addr);

        let initial_dir_clone = initial_dir.clone();
        let session_manager_clone = session_manager.clone();
        let authenticator_clone = authenticator.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(
                stream,
                initial_dir_clone,
                session_manager_clone,
                authenticator_clone,
            )
            .await
            {
                error!("Error handling client: {}", e);
            }
        });
    }
}
