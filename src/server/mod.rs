use axum::{
    extract::{State, WebSocketUpgrade},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use log::{error, info};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::ServiceBuilder;
use tower_http::cors::{Any, CorsLayer};

use crate::api::{index_handler, download_handler, list_handler, upload_handler};
use crate::auth::create_authenticator;
use crate::config::Config;
use crate::connection::websocket::handle_websocket_connection;
use crate::session::SessionManager;

/// Application state shared across all handlers
#[derive(Clone)]
pub struct AppState {
    pub sessions: SessionManager,
    pub initial_dir: PathBuf,
    pub authenticator: Arc<Box<dyn crate::auth::Authenticator>>,
}

/// WebSocket handler using Axum
async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    info!("WebSocket upgrade request received");

    ws.on_upgrade(|socket| async move {
        if let Err(e) = handle_websocket_connection(
            socket,
            state.initial_dir,
            state.sessions,
            state.authenticator,
        )
        .await
        {
            error!("WebSocket connection error: {}", e);
        }
    })
}


pub async fn run_server(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let host = config.server.host.as_str();
    let port = config.server.port;

    // Create authenticator
    let authenticator = Arc::new(
        create_authenticator(&config).expect("Failed to create authenticator")
    );

    // Create session manager
    let sessions: SessionManager = Arc::new(RwLock::new(HashMap::new()));

    // Determine initial working directory
    let initial_dir = if let Some(ref custom_dir) = config.server.cur_dir {
        PathBuf::from(custom_dir)
    } else {
        env::current_dir()?
    };

    // Create directory if it doesn't exist
    if !initial_dir.exists() {
        info!("Creating working directory: {}", initial_dir.display());
        fs::create_dir_all(&initial_dir)?;
    }

    // Build axum application state
    let app_state = AppState {
        sessions: sessions.clone(),
        initial_dir: initial_dir.clone(),
        authenticator: authenticator.clone(),
    };

    info!("Starting TTYD server on {}:{}", host, port);
    info!(
        "Authentication: {}",
        if config.auth.enabled {
            "enabled"
        } else {
            "disabled"
        }
    );
    info!("Working directory: {}", initial_dir.display());
    info!("Open http://localhost:{} in your browser", port);

    // Build axum router
    let app = Router::new()
        .route("/", get(index_handler))
        .route("/download", get(download_handler))
        .route("/ls", get(list_handler))
        .route("/upload", post(upload_handler))
        .route("/ws", get(websocket_handler))
        .with_state(app_state)
        .layer(
            ServiceBuilder::new()
                .layer(CorsLayer::new().allow_origin(Any).allow_methods(Any))
        );

    // Start axum server
    let listener = tokio::net::TcpListener::bind((host, port)).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
