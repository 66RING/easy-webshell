use log::error;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpStream;

use crate::auth::Authenticator;
use crate::connection::http::handle_http_connection;
use crate::connection::websocket::handle_websocket_connection;
use crate::session::SessionManager;

pub mod http;
pub mod websocket;

/// Handle client connection
///     handle ws or http
pub async fn handle_client(
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
