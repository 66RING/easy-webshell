use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

// Session manager to track multiple WebSocket sessions
// TODO: a wrapper class
pub type SessionManager = Arc<RwLock<HashMap<String, Arc<Mutex<PathBuf>>>>>;

