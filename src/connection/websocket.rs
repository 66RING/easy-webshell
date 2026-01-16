use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};

// Axum WebSocket types
use axum::extract::ws::{Message, WebSocket};

use crate::auth::{AuthMethod, Authenticator, Credentials};
use crate::jwt::generate_token;
use crate::session::{
    add_authenticated_session, remove_authenticated_session, AuthenticatedSessions, SessionManager,
};
use crate::shell::pty::sync_current_directory;
use crate::shell::PtySession;

use crate::api::types::WsClientMessage;

/// Handle WebSocket connection
/// 3 worker
/// 1. forward pty to websocket
/// 2. forward websocket to pty
/// 3. periodically sync current directory
pub async fn handle_websocket_connection(
    socket: WebSocket,
    initial_config_dir: PathBuf,
    session_manager: SessionManager,
    authenticated_sessions: AuthenticatedSessions,
    jwt_keys: Arc<crate::jwt::JwtKeys>,
    authenticator: Arc<Box<dyn Authenticator>>,
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::sync::mpsc;

    // Generate unique session ID
    let session_id = format!(
        "session_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros()
    );

    // Split WebSocket into sender and receiver
    let (mut ws_sender, mut ws_receiver) = socket.split();

    // Create session-local current directory (not shared with other connections)
    let session_current_dir = Arc::new(Mutex::new(initial_config_dir.clone()));

    info!(
        "New WebSocket connection established. session id {}, dir {}",
        session_id,
        session_current_dir.lock().await.display()
    );

    // Register session
    {
        let mut sessions = session_manager.write().await;
        sessions.insert(session_id.clone(), session_current_dir.clone());
    }

    let initial_dir = Some(initial_config_dir.clone());

    // Create a new pty session
    let pty_session = Arc::new(Mutex::new(PtySession::with_size_and_dir(
        80,
        24,
        initial_dir.as_ref(),
    )?));
    // Clone for different tasks
    let pty_session_clone = pty_session.clone();
    #[allow(unused_variables)]
    let pty_session_for_sync = pty_session.clone();

    // Create channel for control messages (auth responses, etc.)
    let (ctrl_tx, mut ctrl_rx) = mpsc::channel::<String>(32);

    // Authentication state - shared between tasks
    let auth_enabled = !matches!(authenticator.method(), AuthMethod::None);

    // Clone session_id for use in first task
    let session_id_for_pty2ws = session_id.clone();

    // Task 1: Forward PTY output to WebSocket
    let pty_to_ws_task = tokio::spawn(async move {
        // Send session_id immediately after connection
        let session_msg = if auth_enabled {
            serde_json::json!({"auth": "required", "session_id": session_id_for_pty2ws}).to_string()
        } else {
            serde_json::json!({"auth": "success", "session_id": session_id_for_pty2ws}).to_string()
        };

        if let Err(e) = ws_sender.send(Message::Text(session_msg)).await {
            error!("Failed to send session_id message: {}", e);
            return;
        }
        info!("Sent session_id {} to client", session_id_for_pty2ws);

        // periodically forward data to frontend
        // 1. forward pty data
        let mut interval = interval(Duration::from_millis(10));
        loop {
            interval.tick().await;

            // forward pty data to frontend
            let mut session = pty_session.lock().await;
            let data = session.read(4096);
            drop(session);

            if !data.is_empty() {
                debug!("PTY -> WS: {} bytes", data.len());
                let text = String::from_utf8_lossy(&data).to_string();
                if ws_sender.send(Message::Text(text)).await.is_err() {
                    break;
                }
            }

            // Check for control messages
            // and forward to frontend
            if let Ok(msg) = ctrl_rx.try_recv() {
                if ws_sender.send(Message::Text(msg)).await.is_err() {
                    break;
                }
            }
        }
    });

    // Task 2: Forward WebSocket input to PTY
    // clone a new Arc to do |async move|
    let pty_session_for_sync = pty_session_clone.clone();
    let session_id_for_ws2pty = session_id.clone();
    let authenticated_sessions_for_ws = authenticated_sessions.clone();
    let jwt_keys_for_ws = jwt_keys.clone();
    let ws_to_pty_task = tokio::spawn(async move {
        let mut authenticated = !auth_enabled;

        // TODO: review receiver是什么时候触发的? (在前端)
        // loop:
        // 1. receive control command TODO: review
        //  - authenticator
        //  - pty input data (user input)
        //  - pty control data (resize, ...)
        // 2. receive pty binary data and update pty
        // 3. close
        while let Some(result) = ws_receiver.next().await {
            match result {
                Ok(Message::Text(text)) => {
                    // Try to parse as structured message first
                    if let Ok(msg) = serde_json::from_str::<WsClientMessage>(&text) {
                        match msg {
                            // Terminal resize (only when authenticated)
                            WsClientMessage::Resize { cols, rows } => {
                                if authenticated {
                                    debug!("Terminal resized to {}x{}", cols, rows);
                                    let mut session = pty_session_clone.lock().await;
                                    let _ = session.set_winsize(cols, rows);
                                } else {
                                    debug!("Ignoring resize while not authenticated");
                                }
                            }

                            // Text input (only when authenticated)
                            WsClientMessage::Input { data } => {
                                if authenticated {
                                    debug!("WS -> PTY: {} bytes", data.len());
                                    let mut session = pty_session_clone.lock().await;
                                    session.write(data.as_bytes());
                                } else {
                                    debug!("Ignoring input while not authenticated");
                                }
                            }

                            // Authentication (only when not authenticated)
                            WsClientMessage::Auth { username, password } => {
                                if authenticated {
                                    debug!("Ignoring auth message while already authenticated");
                                    continue;
                                }

                                let credentials = Credentials {
                                    username,
                                    password: Some(password),
                                    ssh_key: None,
                                };

                                if authenticator.authenticate(&credentials) {
                                    authenticated = true;
                                    add_authenticated_session(
                                        &authenticated_sessions_for_ws,
                                        session_id_for_ws2pty.clone(),
                                    )
                                    .await;

                                    let token = generate_token(
                                        &session_id_for_ws2pty,
                                        &jwt_keys_for_ws,
                                        24,
                                    )
                                    .unwrap_or_else(|e| {
                                        log::error!("Failed to generate JWT token: {}", e);
                                        String::new()
                                    });

                                    let _ = ctrl_tx
                                        .send(
                                            serde_json::json!({
                                                "auth": "success",
                                                "session_id": session_id_for_ws2pty,
                                                "token": token
                                            })
                                            .to_string(),
                                        )
                                        .await;
                                    info!("Authentication successful");
                                    continue;
                                } else {
                                    let _ = ctrl_tx
                                        .send(serde_json::json!({"auth": "failed"}).to_string())
                                        .await;
                                    info!("Authentication failed");
                                    continue;
                                }
                            }

                            // Ping/Pong (always allowed)
                            WsClientMessage::Ping => {
                                debug!("Received ping from client");
                            }
                        }
                    } else {
                        // Legacy: treat plain text as input (for backwards compatibility)
                        if authenticated {
                            debug!("WS -> PTY (legacy): {} bytes", text.len());
                            let mut session = pty_session_clone.lock().await;
                            session.write(text.as_bytes());
                        } else {
                            debug!("Ignoring legacy message while not authenticated");
                        }
                    }
                }
                Ok(Message::Binary(data)) => {
                    if authenticated {
                        debug!("WS -> PTY: {} bytes (binary)", data.len());
                        let mut session = pty_session_clone.lock().await;
                        session.write(&data);
                    } else {
                        debug!("Ignoring binary message while not authenticated");
                    }
                }
                Ok(Message::Close(_)) => {
                    info!("WebSocket close frame received");
                    break;
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }
    });

    // Task 3: Periodically sync current directory
    let session_dir_for_sync = session_current_dir.clone();
    let session_manager_for_cleanup = session_manager.clone();
    let authenticated_sessions_for_cleanup = authenticated_sessions.clone();
    let session_id_for_cleanup = session_id.clone();
    let dir_sync_task = tokio::spawn(async move {
        let mut sync_interval = interval(Duration::from_millis(100));
        // First tick completes immediately, so we skip it
        sync_interval.tick().await;
        loop {
            sync_interval.tick().await;
            sync_current_directory(pty_session_for_sync.clone(), session_dir_for_sync.clone())
                .await;
        }
    });

    // Wait for either task to complete
    tokio::select! {
        _ = pty_to_ws_task => {
            debug!("PTY to WS task completed");
        }
        _ = ws_to_pty_task => {
            debug!("WS to PTY task completed");
        }
        _ = dir_sync_task => {
            debug!("Directory sync task completed");
        }
    }

    // Clean up session
    {
        let mut sessions = session_manager_for_cleanup.write().await;
        sessions.remove(&session_id_for_cleanup);
    }
    // Remove from authenticated sessions
    remove_authenticated_session(&authenticated_sessions_for_cleanup, &session_id_for_cleanup)
        .await;
    info!(
        "WebSocket connection closed, session {} removed",
        session_id_for_cleanup
    );
    Ok(())
}
