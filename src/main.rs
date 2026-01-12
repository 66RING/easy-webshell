use futures_util::{SinkExt, StreamExt};
use libc::{c_ushort, fcntl, F_GETFL, F_SETFL, ioctl, O_NONBLOCK, TIOCSWINSZ};
use log::{debug, error, info};
use pty::fork::{Fork, Master};
use serde::{Deserialize, Serialize};
use std::env;
use std::ffi::CStr;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::WebSocketStream;
use tokio::time::{interval, Duration};

const MAX_UPLOAD_SIZE: usize = 100 * 1024 * 1024; // 100MB max file size

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
    /// Create a new PTY session with default size
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Self::with_size(80, 24)
    }

    /// Create a new PTY session with specified size
    fn with_size(cols: u16, rows: u16) -> Result<Self, Box<dyn std::error::Error>> {
        let shell = env::var("SHELL").unwrap_or_else(|_| "/bin/bash".to_string());

        let fork = Fork::from_ptmx()?;

        if let Ok(_child) = fork.is_child() {
            // Child process - spawn shell
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
) -> Result<(), Box<dyn std::error::Error>> {
    info!("New WebSocket connection established");

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let pty_session = Arc::new(Mutex::new(PtySession::new()?));
    let pty_session_clone = pty_session.clone();
    let current_dir_clone = current_dir.clone();

    // Task 1: Forward PTY output to WebSocket
    let pty_to_ws_task = tokio::spawn(async move {
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
        }
    });

    // Task 2: Forward WebSocket input to PTY
    let pty_session_for_sync = pty_session_clone.clone();
    let pty_session_for_sync2 = pty_session_clone.clone();
    let current_dir_for_ws = current_dir.clone();
    let ws_to_pty_task = tokio::spawn(async move {
        let mut last_cmd = String::new();
        while let Some(result) = ws_receiver.next().await {
            match result {
                Ok(Message::Text(text)) => {
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
) -> Result<(), Box<dyn std::error::Error>> {
    // Peek at the first bytes to determine the request type without consuming them
    let mut peek_buffer = [0u8; 512];
    match stream.peek(&mut peek_buffer).await {
        Ok(0) => return Ok(()),
        Ok(_) => {}
        Err(_) => {
            // Connection might be closed, try HTTP anyway
            let dir = current_dir.lock().await;
            handle_http_connection(stream, dir.clone()).await?;
            return Ok(());
        }
    }

    let request_peek = String::from_utf8_lossy(&peek_buffer).to_lowercase();

    // Check if it's a WebSocket upgrade request
    if request_peek.contains("upgrade:") && request_peek.contains("websocket") {
        // Use accept_async for WebSocket - it will handle the handshake
        match tokio_tungstenite::accept_async(stream).await {
            Ok(ws_stream) => {
                handle_websocket_connection(ws_stream, current_dir).await?;
            }
            Err(e) => {
                error!("WebSocket handshake failed: {}", e);
            }
        }
    } else {
        // Handle as regular HTTP
        let dir = current_dir.lock().await;
        handle_http_connection(stream, dir.clone()).await?;
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

/// Handle plain HTTP connection
async fn handle_http_connection(
    mut stream: TcpStream,
    current_dir: std::path::PathBuf,
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
    let path = parts[1];

    let (status, content_type, body) = if method == "POST" && path == "/upload" {
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

    let host = "0.0.0.0";
    let port = 7681;

    info!("Starting TTYD server on {}:{}", host, port);
    info!("Open http://localhost:{} in your browser", port);

    let listener = TcpListener::bind((host, port)).await?;

    // Shared current working directory state
    let current_dir = Arc::new(Mutex::new(env::current_dir()?));

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("New connection from {}", addr);

        let current_dir_clone = current_dir.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, current_dir_clone).await {
                error!("Error handling client: {}", e);
            }
        });
    }
}
