use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs::{self};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{interval, Duration};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::WebSocketStream;

use crate::auth::create_authenticator;
use crate::auth::{AuthMethod, Authenticator, Credentials};
use crate::config::load_config;
use crate::connection::http::handle_http_connection;
use crate::session::SessionManager;
use crate::shell::PtySession;

mod auth;
mod config;
mod connection;
mod fs_opt;
mod handler;
mod session;
mod shell;

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
async fn handle_websocket_connection(
    ws_stream: WebSocketStream<TcpStream>,
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

    // TODO: review为什么sink是sender， stream是receiver
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

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
    let session_dir_clone = session_current_dir.clone();

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

/// Sync current working directory from PTY
async fn sync_current_directory(
    pty_session: Arc<Mutex<PtySession>>,
    current_dir: Arc<Mutex<PathBuf>>,
) {
    // Get the PTS name from the session
    let pts_name = {
        let session = pty_session.lock().await;
        session.pts_name.clone()
    };

    if let Some(pts) = pts_name {
        // Find the process that has this PTY as its controlling terminal
        // by looking at /proc/[pid]/fd/0 (stdin) or /proc/[pid]/fd/1 (stdout)
        let cwd = find_shell_cwd(&pts);

        // Handle the result before any await
        let cwd_opt = cwd.ok();

        if let Some(cwd) = cwd_opt {
            let mut dir = current_dir.lock().await;
            let old_dir = dir.clone();
            *dir = cwd.clone();
            if old_dir != *dir {
                info!(
                    "Current directory updated: {} -> {}",
                    old_dir.display(),
                    dir.display()
                );
            }
        }
    }
}

/// Find the shell's current working directory by PTY device
/// by looking at /proc/[pid]/fd/0 (stdin) or /proc/[pid]/fd/1 (stdout)
/// if the fd_id was link to the target pts device. the process found.
///
/// Linux fs tips: every thing is a file.
/// process state: /proc/[pid]
///     opened file: /proc/[pid]/fd (a link to target file/device)
///     cwd: /proc/[pid]/cwd (a link)
///     ...
fn find_shell_cwd(pts_name: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let proc_path = PathBuf::from("/proc");

    // Iterate through all process directories in /proc
    for entry in fs::read_dir(&proc_path)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
    {
        let pid_str = entry.file_name();
        // Skip if not a numeric PID
        if pid_str
            .to_string_lossy()
            .chars()
            .all(|c| c.is_ascii_digit())
        {
            let pid: u32 = pid_str.to_string_lossy().parse().unwrap_or(0);
            if pid == 0 || pid == std::process::id() {
                continue;
            }

            // Check if this process has the PTY as its stdin/stdout/stderr
            // TODO: 新增一些pty的debug信息, 比如pts name
            let fds_path = entry.path().join("fd");
            if let Ok(fds) = fs::read_dir(&fds_path) {
                for fd_entry in fds.filter_map(|e| e.ok()) {
                    if let Ok(target) = fs::read_link(&fd_entry.path()) {
                        let target_str = target.to_string_lossy();
                        // Check if this fd points to our PTY
                        if target_str.contains(pts_name) || target_str == pts_name {
                            // Found the process! Now get its cwd
                            let cwd_path = entry.path().join("cwd");
                            if let Ok(cwd) = fs::read_link(&cwd_path) {
                                debug!("Found shell PID {} with cwd: {}", pid, cwd.display());
                                return Ok(cwd);
                            }
                        }
                    }
                }
            }
        }
    }

    // Fallback: return current directory
    debug!("Could not find shell process, using current directory");
    Ok(env::current_dir()?)
}

/// Handle client connection
///     handle ws or http
async fn handle_client(
    stream: TcpStream,
    initial_config_dir: PathBuf,
    session_manager: SessionManager,
    authenticator: Arc<Box<dyn Authenticator>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Peek at the first bytes to determine the request type without consuming them
    let mut peek_buffer = [0u8; 512];
    // TODO: review why need a peek?
    match stream.peek(&mut peek_buffer).await {
        Ok(0) => return Ok(()),
        Ok(_) => {}
        Err(_) => {
            // Connection might be closed, try HTTP anyway
            // TODO: review why?
            // also as a http server?
            handle_http_connection(
                stream,
                initial_config_dir.clone(),
                session_manager.clone(),
                authenticator,
            )
            .await?;
            return Ok(());
        }
    }

    // TODO: review websocket的固定格式吗
    // 描述以下这个协议?
    // 为什么是upgrade: 和websocket字符串
    let request_peek = String::from_utf8_lossy(&peek_buffer).to_lowercase();

    // Check if it's a WebSocket upgrade request
    if request_peek.contains("upgrade:") && request_peek.contains("websocket") {
        // Use accept_async for WebSocket - it will handle the handshake
        match tokio_tungstenite::accept_async(stream).await {
            Ok(ws_stream) => {
                handle_websocket_connection(
                    ws_stream,
                    initial_config_dir,
                    session_manager,
                    authenticator,
                )
                .await?;
            }
            Err(e) => {
                error!("WebSocket handshake failed: {}", e);
            }
        }
    } else {
        // Handle as regular HTTP
        handle_http_connection(stream, initial_config_dir, session_manager, authenticator).await?;
    }

    Ok(())
}

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
