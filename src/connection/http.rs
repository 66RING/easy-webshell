use crate::handler::http_handlers::{
    get_html_content, handle_file_download, handle_file_upload, handle_list_directory, url_decoding
};
use crate::session::{SessionManager, extract_session_id_from_query};
use crate::auth::Authenticator;
use futures_util::{SinkExt, StreamExt};
use log::debug;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;


const MAX_UPLOAD_SIZE: usize = 100 * 1024 * 1024; // 100MB max file size

/// Handle plain HTTP connection
pub async fn handle_http_connection(
    mut stream: TcpStream,
    initial_config_dir: PathBuf,
    session_manager: SessionManager,
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
        if let Some(pos) = header_buffer[..bytes_read]
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
        {
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

        // Extract session_id from query parameters or use default directory
        let session_id = extract_session_id_from_query(query_str);
        let dir = if let Some(sid) = session_id {
            let sessions = session_manager.read().await;
            sessions.get(&sid).map(|d| d.clone())
        } else {
            None
        };

        // Use session directory or fall back to initial config directory
        let dir = if let Some(d) = dir {
            d.lock().await.clone()
        } else {
            initial_config_dir.clone()
        };

        // Process the download before any await
        let download_result = handle_file_download(query_str, &dir).await;

        // Extract file data and content type before any await
        let result = download_result.map_err(|e| e.to_string());

        match result {
            Ok((file_data, content_type_header)) => {
                // Extract filename from path for Content-Disposition header
                // Parse query parameter to extract only the path part (ignore session_id)
                let path_param = query_str
                    .strip_prefix("?")
                    .unwrap_or(query_str)
                    .split('&')
                    .find_map(|p| {
                        let p = p.trim_start_matches("path=");
                        if p.contains('=') {
                            None
                        } else {
                            Some(p)
                        }
                    })
                    .unwrap_or("download");

                // Decode URL encoding first, then extract filename
                let decoded_path = url_decoding(path_param);
                let filename = decoded_path.split('/').last().unwrap_or("download");

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
        let session_id = extract_session_id_from_query(query_str);
        debug!(
            "LS request: query={}, session_id={:?}",
            query_str, session_id
        );

        let dir = if let Some(sid) = session_id {
            let sessions = session_manager.read().await;
            sessions.get(&sid).map(|d| d.clone())
        } else {
            debug!("No session_id found, using initial_config_dir");
            None
        };

        let dir = if let Some(d) = dir {
            let d = d.lock().await.clone();
            debug!("Using session directory: {}", d.display());
            d
        } else {
            debug!(
                "Using initial config directory: {}",
                initial_config_dir.display()
            );
            initial_config_dir.clone()
        };

        let list_result = handle_list_directory(query_str, &dir).await;

        match list_result {
            Ok(listing) => {
                let json_body = serde_json::to_string(&listing)?;
                ("200 OK", "application/json", json_body)
            }
            Err(e) => {
                let error_msg = format!("List failed: {}", e);
                (
                    "500 Internal Server Error",
                    "application/json",
                    format!(r#"{{"error":"{}"}}"#, error_msg),
                )
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
                format!(
                    "File too large. Maximum size: {} MB",
                    MAX_UPLOAD_SIZE / 1024 / 1024
                ),
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

            // Extract session_id from headers or query parameters
            let session_id = request_lines
                .iter()
                .find(|line| line.to_lowercase().starts_with("x-session-id:"))
                .and_then(|line| line.split(':').nth(1))
                .map(|s| s.trim().to_string())
                .or_else(|| extract_session_id_from_query(query.unwrap_or("?path=")));

            let dir = if let Some(sid) = session_id {
                let sessions = session_manager.read().await;
                sessions.get(&sid).map(|d| d.clone())
            } else {
                None
            };
            let dir = if let Some(d) = dir {
                d.lock().await.clone()
            } else {
                initial_config_dir.clone()
            };
            match handle_file_upload(content_type_header, &request_buffer, &dir).await {
                Ok(msg) => ("200 OK", "text/plain", msg),
                Err(e) => (
                    "400 Bad Request",
                    "text/plain",
                    format!("Upload failed: {}", e),
                ),
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
