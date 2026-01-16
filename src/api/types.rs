//! API request and response types
//!
//! This module contains all the data structures used for
//! HTTP request parsing and response serialization.

#![allow(dead_code)]  // Allow unused types - they're defined for future use

use serde::{Deserialize, Serialize};

/// Query parameters for file operations (download, list)
#[derive(Debug, Deserialize, Clone)]
pub struct FileQuery {
    /// File or directory path
    pub path: Option<String>,
    /// Session ID for directory tracking
    pub session_id: Option<String>,
    /// Filename for download (optional)
    pub filename: Option<String>,
}

/// Response for file upload
#[derive(Debug, Serialize)]
pub struct UploadResponse {
    /// Success message
    pub message: String,
    /// Path where file was uploaded
    pub path: String,
}

/// Error response structure
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    /// Error message
    pub error: String,
}

/// WebSocket authentication message from client
#[derive(Debug, Deserialize)]
pub struct WsAuthMessage {
    /// Message type, must be "login"
    pub auth: String,
    /// Username
    pub username: String,
    /// Password
    pub password: String,
}

/// WebSocket session info message
#[derive(Debug, Serialize)]
pub struct WsSessionInfo {
    /// Authentication status
    pub auth: String,
    /// Session ID
    pub session_id: String,
}

/// Generic JSON response wrapper
#[derive(Debug, Serialize)]
pub struct JsonResponse<T> {
    /// Response data
    pub data: T,
    /// Optional message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}
