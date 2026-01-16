use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

// Session manager to track multiple WebSocket sessions
// TODO: a wrapper class
pub type SessionManager = Arc<RwLock<HashMap<String, Arc<Mutex<PathBuf>>>>>;

// Authenticated sessions manager - tracks sessions that have successfully authenticated
pub type AuthenticatedSessions = Arc<RwLock<HashSet<String>>>;

/// Create a new authenticated sessions manager
pub fn create_authenticated_sessions() -> AuthenticatedSessions {
    Arc::new(RwLock::new(HashSet::new()))
}

/// Add a session to the authenticated set
pub async fn add_authenticated_session(sessions: &AuthenticatedSessions, session_id: String) {
    let mut auth_sessions = sessions.write().await;
    auth_sessions.insert(session_id);
    log::debug!("Session added to authenticated set");
}

/// Remove a session from the authenticated set
pub async fn remove_authenticated_session(sessions: &AuthenticatedSessions, session_id: &str) {
    let mut auth_sessions = sessions.write().await;
    auth_sessions.remove(session_id);
    log::debug!("Session removed from authenticated set");
}

/// Check if a session is authenticated
pub async fn is_authenticated(sessions: &AuthenticatedSessions, session_id: &str) -> bool {
    let auth_sessions = sessions.read().await;
    auth_sessions.contains(session_id)
}

