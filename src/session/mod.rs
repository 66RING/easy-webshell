use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

// Session manager to track multiple WebSocket sessions
// TODO: a wrapper class
pub type SessionManager = Arc<RwLock<HashMap<String, Arc<Mutex<PathBuf>>>>>;

/// Extract session_id from query string
pub fn extract_session_id_from_query(query: &str) -> Option<String> {
    query.split('&')
        .find_map(|p| {
            let p = p.trim_start_matches('?');
            if p.starts_with("session_id=") {
                Some(p["session_id=".len()..].to_string())
            } else {
                None
            }
        })
}

