use axum::{
    extract::State,
    http::StatusCode,
    middleware::Next,
    response::Response,
    Json,
};
use crate::session::is_authenticated;
use crate::server::AppState;
use serde_json::json;

/// Authentication middleware
/// Validates session_id from query parameters before allowing request to proceed
pub async fn auth_middleware(
    State(state): State<AppState>,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    // Extract session_id from query string
    let uri = request.uri();
    let query = uri.query().unwrap_or("");
    let session_id = query
        .split('&')
        .find_map(|pair| {
            let mut kv = pair.split('=');
            if kv.next() == Some("session_id") {
                kv.next()
            } else {
                None
            }
        });

    let sid = session_id.ok_or((
        StatusCode::UNAUTHORIZED,
        Json(json!({"error": "No session ID provided"})),
    ))?;

    // Verify session is authenticated
    if is_authenticated(&state.authenticated_sessions, sid).await {
        Ok(next.run(request).await)
    } else {
        Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Session not authenticated"})),
        ))
    }
}
