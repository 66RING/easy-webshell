use futures_util::{SinkExt, StreamExt};
use libc::{c_ushort, fcntl, F_GETFL, F_SETFL, ioctl, O_NONBLOCK, TIOCSWINSZ};
use log::{debug, error, info};
use pty::fork::{Fork, Master};
use serde::{Deserialize, Serialize};
use std::env;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::process::Command;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::WebSocketStream;

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

            let mut session = PtySession {
                _fork: fork,
                master: Some(master.clone()),
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
) -> Result<(), Box<dyn std::error::Error>> {
    info!("New WebSocket connection established");

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let pty_session = Arc::new(Mutex::new(PtySession::new()?));
    let pty_session_clone = pty_session.clone();

    // Task 1: Forward PTY output to WebSocket
    let pty_to_ws_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(10));
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
    let ws_to_pty_task = tokio::spawn(async move {
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

    // Wait for either task to complete
    tokio::select! {
        _ = pty_to_ws_task => {
            debug!("PTY to WS task completed");
        }
        _ = ws_to_pty_task => {
            debug!("WS to PTY task completed");
        }
    }

    info!("WebSocket connection closed");
    Ok(())
}

/// Handle client connection
async fn handle_client(stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    // Peek at the first bytes to determine the request type without consuming them
    let mut peek_buffer = [0u8; 512];
    match stream.peek(&mut peek_buffer).await {
        Ok(0) => return Ok(()),
        Ok(_) => {}
        Err(_) => {
            // Connection might be closed, try HTTP anyway
            handle_http_connection(stream).await?;
            return Ok(());
        }
    }

    let request_peek = String::from_utf8_lossy(&peek_buffer).to_lowercase();

    // Check if it's a WebSocket upgrade request
    if request_peek.contains("upgrade:") && request_peek.contains("websocket") {
        // Use accept_async for WebSocket - it will handle the handshake
        match tokio_tungstenite::accept_async(stream).await {
            Ok(ws_stream) => {
                handle_websocket_connection(ws_stream).await?;
            }
            Err(e) => {
                error!("WebSocket handshake failed: {}", e);
            }
        }
    } else {
        // Handle as regular HTTP
        handle_http_connection(stream).await?;
    }

    Ok(())
}

/// Handle plain HTTP connection
async fn handle_http_connection(
    mut stream: TcpStream,
) -> Result<(), Box<dyn std::error::Error>> {
    // Read the HTTP request
    let mut request_buffer = [0u8; 4096];
    let n = stream.read(&mut request_buffer).await?;

    if n == 0 {
        return Ok(());
    }

    let request = String::from_utf8_lossy(&request_buffer[..n]).to_string();
    let request_lines: Vec<&str> = request.lines().collect();

    if request_lines.is_empty() {
        return Ok(());
    }

    let first_line = request_lines[0];
    let parts: Vec<&str> = first_line.split_whitespace().collect();

    if parts.len() >= 2 {
        let path = parts[1];

        let (status, content_type, body) = if path == "/" || path == "/index.html" {
            ("200 OK", "text/html", get_html_content())
        } else {
            ("404 Not Found", "text/plain", "Not Found".to_string())
        };

        let response = format!(
            "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\n\r\n{}",
            status,
            content_type,
            body.len(),
            body
        );

        stream.write_all(response.as_bytes()).await?;
        stream.flush().await?;
    }

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

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("New connection from {}", addr);

        tokio::spawn(async move {
            if let Err(e) = handle_client(stream).await {
                error!("Error handling client: {}", e);
            }
        });
    }
}
