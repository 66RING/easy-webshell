use futures_util::{SinkExt, StreamExt};
use libc::{c_ushort, fcntl, F_GETFL, F_SETFL, ioctl, O_NONBLOCK, TIOCSWINSZ};
use log::{debug, error, info};
use pty::fork::{Fork, Master};
use serde::{Deserialize, Serialize};
use std::env;
use std::ffi::CStr;
use std::fs::{self, File};
use std::io::{Read, Write, Cursor};
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::WebSocketStream;
use tokio::time::{interval, Duration};

use zip::{ZipWriter};
use zip::write::FileOptions;

const MAX_UPLOAD_SIZE: usize = 100 * 1024 * 1024; // 100MB max file size

// ============================================================
// Authentication System - Extensible Design
// ============================================================

/// Authentication method configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum AuthMethod {
    Password,
    #[serde(skip)]
    SSHKey, // Reserved for future implementation
    None,   // No authentication
}

/// User credentials provided during authentication
#[derive(Debug, Clone)]
pub struct Credentials {
    pub username: String,
    pub password: Option<String>,
    #[allow(dead_code)]
    pub ssh_key: Option<String>, // Reserved for future SSH authentication
}

/// Trait for authentication strategies - allows easy extension
pub trait Authenticator: Send + Sync {
    /// Authenticate the provided credentials
    fn authenticate(&self, credentials: &Credentials) -> bool;

    /// Get the authentication method type
    fn method(&self) -> AuthMethod;
}

/// Password-based authenticator implementation
pub struct PasswordAuthenticator {
    pub username: String,
    pub password: String,
}

impl Authenticator for PasswordAuthenticator {
    fn authenticate(&self, credentials: &Credentials) -> bool {
        credentials.username == self.username
            && credentials.password.as_ref().map_or(false, |p| p == &self.password)
    }

    fn method(&self) -> AuthMethod {
        AuthMethod::Password
    }
}

/// No-op authenticator for when authentication is disabled
pub struct NoAuthenticator;

impl Authenticator for NoAuthenticator {
    fn authenticate(&self, _credentials: &Credentials) -> bool {
        true
    }

    fn method(&self) -> AuthMethod {
        AuthMethod::None
    }
}

/// Configuration file structure
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub auth: AuthConfig,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    #[serde(default = "default_cur_dir")]
    pub cur_dir: Option<String>,
}

fn default_cur_dir() -> Option<String> {
    None
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthConfig {
    pub enabled: bool,
    pub method: String, // "password", "ssh_key", "none"
    pub username: Option<String>,
    pub password: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                host: "0.0.0.0".to_string(),
                port: 7681,  // Changed from 8080 to match config.toml.example
                cur_dir: None,
            },
            auth: AuthConfig {
                enabled: false,
                method: "none".to_string(),
                username: None,
                password: None,
            },
        }
    }
}

/// Load configuration from file, or return default config
fn load_config() -> Config {
    // Try multiple possible config file locations
    let config_paths = vec![
        "config.toml",                              // Current directory
        "/etc/ttyd/config.toml",                    // System-wide config
    ];

    for config_path in config_paths {
        match fs::read_to_string(config_path) {
            Ok(content) => {
                info!("Found config file: {}", config_path);
                match toml::from_str::<Config>(&content) {
                    Ok(config) => {
                        info!("Successfully loaded configuration from {}", config_path);
                        info!("Server: {}:{}", config.server.host, config.server.port);
                        info!("Auth: {} (method: {})",
                            if config.auth.enabled { "enabled" } else { "disabled" },
                            config.auth.method
                        );
                        if let Some(ref dir) = config.server.cur_dir {
                            info!("Working directory: {}", dir);
                        }
                        return config;
                    }
                    Err(e) => {
                        error!("Failed to parse config file '{}': {}", config_path, e);
                        error!("Please check the file format. Using default configuration.");
                        continue;
                    }
                }
            }
            Err(_) => {
                // File not found, try next path
                continue;
            }
        }
    }

    info!("No config file found, using default configuration");
    info!("Default: server on 0.0.0.0:7681, authentication disabled");
    Config::default()
}

/// Create authenticator based on configuration
fn create_authenticator(config: &Config) -> Option<Box<dyn Authenticator>> {
    if !config.auth.enabled {
        info!("Authentication disabled");
        return Some(Box::new(NoAuthenticator));
    }

    match config.auth.method.to_lowercase().as_str() {
        "password" => {
            // Require username and password to be explicitly set
            let username = config.auth.username.as_ref()?;
            let password = config.auth.password.as_ref()?;

            info!("Password authentication enabled for user: {} {}", username, password);
            Some(Box::new(PasswordAuthenticator {
                username: username.clone(),
                password: password.clone(),
            }))
        }
        "ssh_key" => {
            // Reserved for future implementation
            error!("SSH key authentication not yet implemented");
            None
        }
        "none" => {
            info!("No authentication configured");
            Some(Box::new(NoAuthenticator))
        }
        _ => {
            error!("Unknown authentication method: {}", config.auth.method);
            None
        }
    }
}

// ============================================================
// End of Authentication System
// ============================================================

/// File information for directory listing
#[derive(Debug, Serialize, Deserialize)]
struct FileInfo {
    name: String,
    path: String,
    is_dir: bool,
    size: Option<u64>,
    modified: Option<u64>,
}

/// Directory listing response
#[derive(Debug, Serialize)]
struct DirectoryListing {
    current_path: String,
    files: Vec<FileInfo>,
}

/// Terminal window size structure
#[repr(C)]
struct Winsize {
    ws_row: c_ushort,
    ws_col: c_ushort,
    ws_xpixel: c_ushort,
    ws_ypixel: c_ushort,
}

/// Resize message from client
#[derive(Debug, Serialize, Deserialize)]
struct ResizeMessage {
    cols: u16,
    rows: u16,
}

/// PTY session manager
struct PtySession {
    _fork: Fork,
    master: Option<Master>,
    pts_name: Option<String>,
}

impl PtySession {
    /// Create a new PTY session with specified size and initial directory
    fn with_size_and_dir(cols: u16, rows: u16, initial_dir: Option<&PathBuf>) -> Result<Self, Box<dyn std::error::Error>> {
        let shell = env::var("SHELL").unwrap_or_else(|_| "/bin/bash".to_string());

        let fork = Fork::from_ptmx()?;

        if let Ok(_child) = fork.is_child() {
            // Child process - set working directory if specified
            if let Some(dir) = initial_dir {
                if let Err(e) = env::set_current_dir(dir) {
                    eprintln!("Failed to set working directory {}: {}", dir.display(), e);
                }
            }

            // Spawn shell
            let _ = Command::new(&shell).exec();
            // If exec returns, there was an error
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to exec shell",
            )
            .into())
        } else {
            // Parent process
            let master = fork.is_parent()?;

            // Get the PTS slave name
            let pts_name = unsafe {
                master
                    .ptsname()
                    .ok()
                    .and_then(|s| CStr::from_ptr(s).to_str().ok())
                    .map(|s| s.to_string())
            };

            let mut session = PtySession {
                _fork: fork,
                master: Some(master.clone()),
                pts_name,
            };

            // Set master to non-blocking mode
            unsafe {
                let flags = fcntl(master.as_raw_fd(), F_GETFL, 0);
                if flags < 0 {
                    return Err(std::io::Error::last_os_error().into());
                }
                if fcntl(master.as_raw_fd(), F_SETFL, flags | O_NONBLOCK) < 0 {
                    return Err(std::io::Error::last_os_error().into());
                }
            }

            session.set_winsize(cols, rows)?;

            Ok(session)
        }
    }

    /// Set terminal window size
    fn set_winsize(&mut self, cols: u16, rows: u16) -> Result<(), std::io::Error> {
        if let Some(ref master) = self.master {
            let winsize = Winsize {
                ws_row: rows,
                ws_col: cols,
                ws_xpixel: 0,
                ws_ypixel: 0,
            };

            unsafe {
                if ioctl(master.as_raw_fd(), TIOCSWINSZ as u64, &winsize) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
        }
        Ok(())
    }

    /// Read from PTY (non-blocking)
    fn read(&mut self, size: usize) -> Vec<u8> {
        if let Some(ref mut master) = self.master {
            let mut buffer = vec![0u8; size];
            match master.read(&mut buffer) {
                Ok(n) if n > 0 => {
                    buffer.truncate(n);
                    buffer
                }
                _ => Vec::new(),
            }
        } else {
            Vec::new()
        }
    }

    /// Write to PTY
    fn write(&mut self, data: &[u8]) {
        if let Some(ref mut master) = self.master {
            let _ = master.write_all(data);
        }
    }

    /// Close the PTY session
    fn close(&mut self) {
        if let Some(mut master) = self.master.take() {
            let _ = master.flush();
        }
    }
}

impl Drop for PtySession {
    fn drop(&mut self) {
        self.close();
    }
}

/// HTML content for the terminal interface
fn get_html_content() -> String {
    std::fs::read_to_string("index.html")
        .unwrap_or_else(|e| {
            error!("Failed to read index.html: {}", e);
            "<html><body><h1>Error loading page</h1></body></html>".to_string()
        })
}

/// Handle WebSocket connection
async fn handle_websocket_connection(
    ws_stream: WebSocketStream<TcpStream>,
    current_dir: Arc<Mutex<PathBuf>>,
    authenticator: Arc<Box<dyn Authenticator>>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("New WebSocket connection established");

    use tokio::sync::mpsc;

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    // Get initial directory for PTY session
    let initial_dir = {
        let dir = current_dir.lock().await;
        Some(dir.clone())
    };

    let pty_session = Arc::new(Mutex::new(PtySession::with_size_and_dir(80, 24, initial_dir.as_ref())?));
    let pty_session_clone = pty_session.clone();
    let current_dir_clone = current_dir.clone();

    // Create channel for control messages (auth responses, etc.)
    let (ctrl_tx, mut ctrl_rx) = mpsc::channel::<String>(32);

    // Authentication state - shared between tasks
    let auth_enabled = !matches!(authenticator.method(), AuthMethod::None);

    // Task 1: Forward PTY output to WebSocket
    let pty_to_ws_task = tokio::spawn(async move {
        // Send auth required message immediately if authentication is enabled
        if auth_enabled {
            let auth_msg = serde_json::json!({"auth": "required"}).to_string();
            if let Err(e) = ws_sender.send(Message::Text(auth_msg)).await {
                error!("Failed to send auth required message: {}", e);
                return;
            }
            info!("Sent auth required message to client");
        }

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
            if let Ok(msg) = ctrl_rx.try_recv() {
                if ws_sender.send(Message::Text(msg)).await.is_err() {
                    break;
                }
            }
        }
    });

    // Task 2: Forward WebSocket input to PTY
    let pty_session_for_sync = pty_session_clone.clone();
    let pty_session_for_sync2 = pty_session_clone.clone();
    let current_dir_for_ws = current_dir.clone();
    let ws_to_pty_task = tokio::spawn(async move {
        let mut authenticated = !auth_enabled;
        let mut last_cmd = String::new();

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
                                    auth_msg.get("password").and_then(|v| v.as_str())
                                ) {
                                    let credentials = Credentials {
                                        username: username.to_string(),
                                        password: Some(password.to_string()),
                                        ssh_key: None,
                                    };

                                    if authenticator.authenticate(&credentials) {
                                        authenticated = true;
                                        let _ = ctrl_tx.send(
                                            serde_json::json!({"auth": "success"}).to_string()
                                        ).await;
                                        info!("Authentication successful for user: {}", username);
                                        continue;
                                    } else {
                                        let _ = ctrl_tx.send(
                                            serde_json::json!({"auth": "failed"}).to_string()
                                        ).await;
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
                    if text.starts_with('{') {
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
                        session.write(text.as_bytes());

                        // Track commands to detect cd
                        if text == "\r" || text == "\n" {
                            // Check if last command was 'cd'
                            let cmd = last_cmd.trim().to_string();
                            if cmd.starts_with("cd ") || cmd == "cd" {
                                // Sync current directory after cd command
                                let pty = pty_session_for_sync.clone();
                                let dir = current_dir_for_ws.clone();
                                tokio::spawn(async move {
                                    // Wait a bit for cd to complete
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                    sync_current_directory(pty, dir).await;
                                });
                            }
                            last_cmd.clear();
                        } else if text == "\u{7f}" || text == "\x08" {
                            // Backspace
                            last_cmd.pop();
                        } else if text.len() == 1 && text.chars().next().map(|c| c.is_ascii_graphic()).unwrap_or(false) {
                            // Regular character
                            last_cmd.push(text.chars().next().unwrap());
                        }
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
    let dir_sync_task = tokio::spawn(async move {
        let mut sync_interval = interval(Duration::from_secs(2));
        // Initial sync
        tokio::time::sleep(Duration::from_millis(500)).await;
        loop {
            sync_interval.tick().await;
            sync_current_directory(pty_session_for_sync2.clone(), current_dir_clone.clone()).await;
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

    info!("WebSocket connection closed");
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
                info!("Current directory updated: {} -> {}", old_dir.display(), dir.display());
            }
        }
    }
}

/// Find the shell's current working directory by PTY device
fn find_shell_cwd(pts_name: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let proc_path = PathBuf::from("/proc");

    // Iterate through all process directories in /proc
    for entry in fs::read_dir(&proc_path)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
    {
        let pid_str = entry.file_name();
        // Skip if not a numeric PID
        if pid_str.to_string_lossy().chars().all(|c| c.is_ascii_digit()) {
            let pid: u32 = pid_str.to_string_lossy().parse().unwrap_or(0);
            if pid == 0 || pid == std::process::id() {
                continue;
            }

            // Check if this process has the PTY as its stdin/stdout/stderr
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
async fn handle_client(
    stream: TcpStream,
    current_dir: Arc<Mutex<std::path::PathBuf>>,
    authenticator: Arc<Box<dyn Authenticator>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Peek at the first bytes to determine the request type without consuming them
    let mut peek_buffer = [0u8; 512];
    match stream.peek(&mut peek_buffer).await {
        Ok(0) => return Ok(()),
        Ok(_) => {}
        Err(_) => {
            // Connection might be closed, try HTTP anyway
            let dir = current_dir.lock().await;
            handle_http_connection(stream, dir.clone(), authenticator).await?;
            return Ok(());
        }
    }

    let request_peek = String::from_utf8_lossy(&peek_buffer).to_lowercase();

    // Check if it's a WebSocket upgrade request
    if request_peek.contains("upgrade:") && request_peek.contains("websocket") {
        // Use accept_async for WebSocket - it will handle the handshake
        match tokio_tungstenite::accept_async(stream).await {
            Ok(ws_stream) => {
                handle_websocket_connection(ws_stream, current_dir, authenticator).await?;
            }
            Err(e) => {
                error!("WebSocket handshake failed: {}", e);
            }
        }
    } else {
        // Handle as regular HTTP
        let dir = current_dir.lock().await;
        handle_http_connection(stream, dir.clone(), authenticator).await?;
    }

    Ok(())
}

/// Parse multipart/form-data and extract file
fn parse_multipart_upload(
    body: &[u8],
    boundary: &str,
) -> Result<(String, Vec<u8>), Box<dyn std::error::Error>> {
    let boundary_str = format!("--{}", boundary);
    let boundary_bytes = boundary_str.as_bytes();

    let mut start = 0;

    // Find each part
    let mut filename = String::new();
    let mut file_data = Vec::new();

    while start < body.len() {
        // Find boundary
        let boundary_pos = body[start..]
            .windows(boundary_bytes.len())
            .position(|w| w == boundary_bytes);

        let boundary_pos = match boundary_pos {
            Some(pos) => pos + start,
            None => break,
        };

        // Check if this is the end boundary
        let end_marker_start = boundary_pos + boundary_bytes.len();
        if end_marker_start + 2 <= body.len()
            && &body[end_marker_start..end_marker_start + 2] == b"--"
        {
            break;
        }

        // Find end of headers (double newline)
        let headers_end = body[boundary_pos + boundary_bytes.len() + 2..]
            .windows(4)
            .position(|w| w == b"\r\n\r\n");

        let headers_end = match headers_end {
            Some(pos) => pos + boundary_pos + boundary_bytes.len() + 2,
            None => break,
        };

        // Parse headers to find filename (only headers are text, data is binary)
        let headers_section =
            String::from_utf8_lossy(&body[boundary_pos + boundary_bytes.len() + 2..headers_end]);
        let data_start = headers_end + 4;

        // Extract filename from Content-Disposition header
        for line in headers_section.lines() {
            if line.contains("filename=") {
                let start = line.find("filename=\"").unwrap() + 10;
                let end = line[start..].find('"').unwrap();
                filename = line[start..start + end].to_string();
                // Normalize path separators to forward slash
                filename = filename.replace('\\', "/");
                break;
            }
        }

        // Find next boundary
        let next_boundary = body[data_start..]
            .windows(boundary_bytes.len())
            .position(|w| w == boundary_bytes);

        let data_end = match next_boundary {
            Some(pos) => pos + data_start - 2, // -2 for \r\n before boundary
            None => body.len(),
        };

        if !filename.is_empty() {
            // Copy raw bytes for binary data
            file_data = body[data_start..data_end].to_vec();
            break;
        }

        start = boundary_pos + 1;
    }

    if filename.is_empty() {
        return Err("No file found in upload".into());
    }

    Ok((filename, file_data))
}

/// Handle file upload request
async fn handle_file_upload(
    content_type: &str,
    body: &[u8],
    current_dir: &std::path::PathBuf,
) -> Result<String, Box<dyn std::error::Error>> {
    // Extract boundary from Content-Type
    let boundary = content_type
        .strip_prefix("multipart/form-data; boundary=")
        .ok_or("Invalid content type")?;

    let (filename, file_data) = parse_multipart_upload(body, boundary)?;

    let file_path = current_dir.join(&filename);

    // Create parent directories if they don't exist
    if let Some(parent) = file_path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)?;
        }
    }

    // Write file to disk (binary mode)
    let mut file = File::create(&file_path)?;
    file.write_all(&file_data)?;
    file.flush()?;

    info!(
        "File uploaded: {} ({} bytes)",
        file_path.display(),
        file_data.len()
    );

    Ok(format!("File uploaded: {}", filename))
}

/// Handle file download request
async fn handle_file_download(
    query: &str,
    current_dir: &std::path::PathBuf,
) -> Result<(Vec<u8>, String), Box<dyn std::error::Error>> {
    // Parse query parameter: ?path=/filename or ?path=relative/path/file.txt
    let path_param = query
        .strip_prefix("?path=")
        .or_else(|| query.strip_prefix("path="))
        .unwrap_or("");

    if path_param.is_empty() {
        return Err("No file path specified".into());
    }

    // Decode URL encoding
    let decoded_path = url_decoding(path_param);

    // If path is absolute, use it directly; otherwise, join with current directory
    let file_path = if decoded_path.starts_with('/') {
        PathBuf::from(&decoded_path)
    } else {
        current_dir.join(&decoded_path)
    };

    if !file_path.exists() {
        return Err(format!("File not found: {}", file_path.display()).into());
    }

    // Check if it's a directory - if so, create a zip file
    if file_path.is_dir() {
        info!("Zipping directory: {}", file_path.display());
        let zip_data = create_zip_from_directory(&file_path)?;

        let dir_name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("archive");

        let filename = format!("{}.zip", dir_name);

        // Determine content type for zip
        let content_type = "application/zip".to_string();

        info!("Directory zipped: {} -> {} ({} bytes)", file_path.display(), filename, zip_data.len());

        // Return zip with special filename header
        return Ok((zip_data, content_type));
    }

    // Read file content
    let file_data = fs::read(&file_path)?;

    // Determine content type
    let content_type = mime_guess::from_path(&file_path)
        .first_or_octet_stream()
        .to_string();

    info!("File downloaded: {} ({} bytes)", file_path.display(), file_data.len());

    Ok((file_data, content_type))
}

/// Simple URL decoding (percent decoding)
fn url_decoding(input: &str) -> String {
    let mut result = String::new();
    let mut chars = input.chars();

    while let Some(c) = chars.next() {
        if c == '%' {
            let hex1 = chars.next();
            let hex2 = chars.next();

            if let (Some(h1), Some(h2)) = (hex1, hex2) {
                if let (Some(d1), Some(d2)) = (h1.to_digit(16), h2.to_digit(16)) {
                    let byte = (d1 * 16 + d2) as u8;
                    result.push(byte as char);
                } else {
                    result.push(c);
                    result.push(h1);
                    result.push(h2);
                }
            } else {
                result.push(c);
            }
        } else if c == '+' {
            result.push(' ');
        } else {
            result.push(c);
        }
    }

    result
}

/// List files in a directory
async fn handle_list_directory(
    query: &str,
    current_dir: &std::path::PathBuf,
) -> Result<DirectoryListing, Box<dyn std::error::Error>> {
    // Parse query parameter: ?path=/folder or ?path=relative/path
    let path_param = query
        .strip_prefix("?path=")
        .or_else(|| query.strip_prefix("path="))
        .unwrap_or("");

    let target_path = if path_param.is_empty() || path_param == "." {
        current_dir.clone()
    } else {
        let decoded_path = url_decoding(path_param);
        if decoded_path.starts_with('/') {
            PathBuf::from(&decoded_path)
        } else {
            current_dir.join(&decoded_path)
        }
    };

    if !target_path.exists() {
        return Err("Directory not found".into());
    }

    let mut files = Vec::new();

    if target_path.is_dir() {
        // List directory contents
        let entries = fs::read_dir(&target_path)?;
        for entry in entries {
            let entry = entry?;
            let metadata = entry.metadata().ok();
            let name = entry.file_name().to_string_lossy().to_string();
            let is_dir = metadata.as_ref().map(|m| m.is_dir()).unwrap_or(false);
            let size = if !is_dir {
                metadata.as_ref().map(|m| m.len())
            } else {
                None
            };
            let modified = metadata.as_ref().and_then(|m| m.modified().ok()).map(|t| {
                t.duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            });

            // Skip hidden files
            if !name.starts_with('.') {
                files.push(FileInfo {
                    path: entry.path().to_string_lossy().to_string(),
                    name,
                    is_dir,
                    size,
                    modified,
                });
            }
        }
    }

    // Sort: directories first, then files
    files.sort_by(|a, b| {
        if a.is_dir && !b.is_dir {
            return std::cmp::Ordering::Less;
        } else if !a.is_dir && b.is_dir {
            return std::cmp::Ordering::Greater;
        }
        a.name.cmp(&b.name)
    });

    Ok(DirectoryListing {
        current_path: target_path.to_string_lossy().to_string(),
        files,
    })
}

/// Create a zip file from a directory
fn create_zip_from_directory(dir_path: &Path) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut buffer = Vec::new();
    let mut zip = ZipWriter::new(Cursor::new(&mut buffer));

    let dir_name = dir_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("archive");

    fn add_to_zip(zip: &mut ZipWriter<Cursor<&mut Vec<u8>>>, dir: &Path, base: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let options: FileOptions<'_, ()> = FileOptions::default();

        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = path.strip_prefix(base)?.to_string_lossy().to_string();

            if path.is_dir() {
                zip.add_directory(name.clone(), options)?;
                add_to_zip(zip, &path, base)?;
            } else {
                zip.start_file(name, options)?;
                let mut file = File::open(&path)?;
                let mut file_buffer = Vec::new();
                file.read_to_end(&mut file_buffer)?;
                zip.write_all(&file_buffer)?;
            }
        }
        Ok(())
    }

    let base = dir_path.parent().unwrap_or_else(|| Path::new("."));
    add_to_zip(&mut zip, dir_path, base)?;
    zip.finish()?;

    Ok(buffer)
}

/// Handle plain HTTP connection
async fn handle_http_connection(
    mut stream: TcpStream,
    current_dir: std::path::PathBuf,
    _authenticator: Arc<Box<dyn Authenticator>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Read HTTP headers first
    let mut header_buffer = vec![0u8; 8192];
    let total_read;
    let header_end;

    // Read until we find the end of headers (\r\n\r\n)
    let mut bytes_read = 0;
    loop {
        let n = stream.read(&mut header_buffer[bytes_read..]).await?;
        if n == 0 {
            return Ok(());
        }
        bytes_read += n;

        // Look for end of headers marker
        if let Some(pos) = header_buffer[..bytes_read].windows(4).position(|w| w == b"\r\n\r\n") {
            total_read = bytes_read;
            header_end = pos + 4;
            break;
        }

        if bytes_read >= header_buffer.len() {
            return Err("Headers too large".into());
        }
    }

    let header_data = String::from_utf8_lossy(&header_buffer[..header_end]);
    let request_lines: Vec<&str> = header_data.lines().collect();

    if request_lines.is_empty() {
        return Ok(());
    }

    let first_line = request_lines[0];
    let parts: Vec<&str> = first_line.split_whitespace().collect();

    if parts.len() < 2 {
        return Ok(());
    }

    let method = parts[0];
    let full_path = parts[1];

    // Split path and query string
    let (path, query) = if let Some(pos) = full_path.find('?') {
        (&full_path[..pos], Some(&full_path[pos..]))
    } else {
        (full_path, None)
    };

    let (status, content_type, body) = if method == "GET" && path == "/download" {
        // Handle file download
        let query_str = query.unwrap_or("?path=");

        // Process the download before any await
        let download_result = handle_file_download(query_str, &current_dir).await;

        // Extract file data and content type before any await
        let result = download_result.map_err(|e| e.to_string());

        match result {
            Ok((file_data, content_type_header)) => {
                // Extract filename from path for Content-Disposition header
                let path_param = query_str
                    .strip_prefix("?path=")
                    .or_else(|| query_str.strip_prefix("path="))
                    .unwrap_or("download");

                // Decode URL encoding first, then extract filename
                let decoded_path = url_decoding(path_param);
                let filename = decoded_path
                    .split('/')
                    .last()
                    .unwrap_or("download");

                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nContent-Disposition: attachment; filename=\"{}\"\r\nAccess-Control-Allow-Origin: *\r\n\r\n",
                    content_type_header,
                    file_data.len(),
                    filename
                );

                stream.write_all(response.as_bytes()).await?;
                stream.write_all(&file_data).await?;
                stream.flush().await?;
                return Ok(());
            }
            Err(e) => {
                let error_msg = format!("Download failed: {}", e);
                ("404 Not Found", "text/plain", error_msg)
            }
        }
    } else if method == "GET" && path == "/ls" {
        // Handle directory listing
        let query_str = query.unwrap_or("?path=.");
        let list_result = handle_list_directory(query_str, &current_dir).await;

        match list_result {
            Ok(listing) => {
                let json_body = serde_json::to_string(&listing)?;
                ("200 OK", "application/json", json_body)
            }
            Err(e) => {
                let error_msg = format!("List failed: {}", e);
                ("500 Internal Server Error", "application/json", format!(r#"{{"error":"{}"}}"#, error_msg))
            }
        }
    } else if method == "POST" && path == "/upload" {
        // Extract Content-Length
        let content_length = request_lines
            .iter()
            .find(|line| line.to_lowercase().starts_with("content-length:"))
            .and_then(|line| line.split(':').nth(1))
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(0);

        if content_length > MAX_UPLOAD_SIZE {
            (
                "413 Payload Too Large",
                "text/plain",
                format!("File too large. Maximum size: {} MB", MAX_UPLOAD_SIZE / 1024 / 1024),
            )
        } else {
            // Calculate body size already read
            let body_already_read = total_read - header_end;

            // Allocate buffer for entire request
            let mut request_buffer = vec![0u8; header_end + content_length];
            request_buffer[..header_end].copy_from_slice(&header_buffer[..header_end]);

            // Copy body data already read
            request_buffer[header_end..header_end + body_already_read]
                .copy_from_slice(&header_buffer[header_end..total_read]);

            // Read remaining body data
            if body_already_read < content_length {
                stream
                    .read_exact(&mut request_buffer[header_end + body_already_read..])
                    .await?;
            }

            let content_type_header = request_lines
                .iter()
                .find(|line| line.to_lowercase().starts_with("content-type:"))
                .and_then(|line| line.split(':').nth(1))
                .map(|s| s.trim())
                .unwrap_or("");

            match handle_file_upload(content_type_header, &request_buffer, &current_dir).await {
                Ok(msg) => ("200 OK", "text/plain", msg),
                Err(e) => ("400 Bad Request", "text/plain", format!("Upload failed: {}", e)),
            }
        }
    } else if path == "/" || path == "/index.html" {
        ("200 OK", "text/html", get_html_content())
    } else {
        ("404 Not Found", "text/plain", "Not Found".to_string())
    };

    let response = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\n\r\n{}",
        status,
        content_type,
        body.len(),
        body
    );

    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;

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
    let authenticator = create_authenticator(&config)
        .expect("Failed to create authenticator");

    info!("Starting TTYD server on {}:{}", host, port);
    info!("Authentication: {}", if config.auth.enabled { "enabled" } else { "disabled" });
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

    let current_dir = Arc::new(Mutex::new(initial_dir));

    // Wrap authenticator in Arc for sharing across connections
    let authenticator = Arc::new(authenticator);

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("New connection from {}", addr);

        let current_dir_clone = current_dir.clone();
        let authenticator_clone = authenticator.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, current_dir_clone, authenticator_clone).await {
                error!("Error handling client: {}", e);
            }
        });
    }
}
