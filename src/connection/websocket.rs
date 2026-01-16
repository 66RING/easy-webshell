use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};

// Axum WebSocket types
use axum::extract::ws::{Message, WebSocket};

use serde::{Deserialize, Serialize};

use crate::auth::{AuthMethod, Authenticator, Credentials};
use crate::session::SessionManager;
use crate::shell::PtySession;
use crate::shell::pty::sync_current_directory;

/// Resize message from client
#[derive(Debug, Serialize, Deserialize)]
struct ResizeMessage {
    cols: u16,
    rows: u16,
}


/// Handle WebSocket connection
/// 3 worker
/// 1. forward pty to websocket
/// 2. forward websocket to pty
/// 3. periodically sync current directory
pub async fn handle_websocket_connection(
    socket: WebSocket,
    initial_config_dir: PathBuf,
    session_manager: SessionManager,
    authenticator: Arc<Box<dyn Authenticator>>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("New WebSocket connection established");

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
    // TODO: review. initial_config_dir.clone is useless?
    // 为什么需要session dir?
    // session dir从initial_config_dir中初始化 => clone出一个新的
    let session_current_dir = Arc::new(Mutex::new(initial_config_dir.clone()));

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
    // TODO: review why clone?
    let pty_session_clone = pty_session.clone();

    // Create channel for control messages (auth responses, etc.)
    // TODO: review usage
    let (ctrl_tx, mut ctrl_rx) = mpsc::channel::<String>(32);

    // Authentication state - shared between tasks
    let auth_enabled = !matches!(authenticator.method(), AuthMethod::None);

    // Clone session_id for use in first task
    // TODO: reivew why clone?
    let session_id_for_first_task = session_id.clone();

    // Task 1: Forward PTY output to WebSocket
    let pty_to_ws_task = tokio::spawn(async move {
        // Send session_id immediately after connection
        // TODO: 登录后再send?
        // 这里发送信号给前端: 需要认证还是不需要
        let session_msg = if auth_enabled {
            serde_json::json!({"auth": "required", "session_id": session_id_for_first_task})
                .to_string()
        } else {
            serde_json::json!({"auth": "success", "session_id": session_id_for_first_task})
                .to_string()
        };

        if let Err(e) = ws_sender.send(Message::Text(session_msg)).await {
            error!("Failed to send session_id message: {}", e);
            return;
        }
        info!("Sent session_id {} to client", session_id_for_first_task);

        // periodically forward data to frontend
        // 1. forward pty data
        // 2. TODO: review
        let mut interval = interval(Duration::from_millis(10));
        loop {
            interval.tick().await;

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
            // TODO: review这里接收到的是什么
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
    let session_id_for_ws = session_id.clone();
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
                    // Check if it's an authentication message
                    if !authenticated {
                        // Try to parse as JSON authentication message
                        if let Ok(auth_msg) = serde_json::from_str::<serde_json::Value>(&text) {
                            if auth_msg.get("auth").and_then(|v| v.as_str()) == Some("login") {
                                if let (Some(username), Some(password)) = (
                                    auth_msg.get("username").and_then(|v| v.as_str()),
                                    auth_msg.get("password").and_then(|v| v.as_str()),
                                ) {
                                    let credentials = Credentials {
                                        username: username.to_string(),
                                        password: Some(password.to_string()),
                                        ssh_key: None,
                                    };

                                    if authenticator.authenticate(&credentials) {
                                        authenticated = true;
                                        let _ = ctrl_tx.send(
                                            serde_json::json!({"auth": "success", "session_id": session_id_for_ws}).to_string()
                                        ).await;
                                        info!("Authentication successful for user: {}", username);
                                        continue;
                                    } else {
                                        let _ = ctrl_tx
                                            .send(serde_json::json!({"auth": "failed"}).to_string())
                                            .await;
                                        info!("Authentication failed for user: {}", username);
                                        // Continue to allow retry, don't break the connection
                                        continue;
                                    }
                                }
                            }
                        }

                        // Not authenticated and not a valid auth message, ignore it
                        debug!("Ignoring non-auth message while not authenticated");
                        continue;
                    }

                    // Check if it's a resize message (JSON)
                    // TODO: 还有更好的方式处理不同类型的指令吗
                    // 这里是强制尝试序列化
                    // 所有指令都通过json格式传递?
                    if text.starts_with('{') {
                        // TODO: 如果以后还有resize以为的控制指令, 这里应该怎么设计?
                        if let Ok(msg) = serde_json::from_str::<ResizeMessage>(&text) {
                            debug!("Terminal resized to {}x{}", msg.cols, msg.rows);
                            let mut session = pty_session_clone.lock().await;
                            let _ = session.set_winsize(msg.cols, msg.rows);
                        } else {
                            // Not JSON, treat as regular input
                            debug!("WS -> PTY: {} bytes", text.len());
                            let mut session = pty_session_clone.lock().await;
                            session.write(text.as_bytes());
                        }
                    } else {
                        // Regular input to PTY
                        debug!("WS -> PTY: {} bytes", text.len());
                        let mut session = pty_session_clone.lock().await;
                        // TODO: review binary和text都是write
                        // pty session是有什么协议来识别这里数据类型吗?
                        session.write(text.as_bytes());
                    }
                }
                Ok(Message::Binary(data)) => {
                    debug!("WS -> PTY: {} bytes (binary)", data.len());
                    let mut session = pty_session_clone.lock().await;
                    session.write(&data);
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
    let session_id_for_cleanup = session_id.clone();
    let dir_sync_task = tokio::spawn(async move {
        let mut sync_interval = interval(Duration::from_secs(2));
        // Initial sync
        tokio::time::sleep(Duration::from_millis(100)).await;
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
    info!(
        "WebSocket connection closed, session {} removed",
        session_id_for_cleanup
    );
    Ok(())
}
