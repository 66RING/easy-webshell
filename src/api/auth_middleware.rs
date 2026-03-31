use axum::{
    extract::State,
    http::StatusCode,
    middleware::Next,
    response::Response,
    Json,
};
use crate::server::AppState;
use crate::jwt::validate_token;
use crate::session::is_authenticated;
use serde_json::json;
use log::debug;

/// Authentication middleware
/// Validates JWT token from query parameters before allowing request to proceed
pub async fn auth_middleware(
    State(state): State<AppState>,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    // Extract token from query string
    let uri = request.uri();
    let query = uri.query().unwrap_or("");
    // TODO: split is no a good idea?
    // way too ugly
    let token = query
        .split('&')
        .find_map(|pair| {
            let mut kv = pair.split('=');
            // TODO: get k
            if kv.next() == Some("token") {
                // TODO: return value
                kv.next()
            } else {
                None
            }
        });

    let token_str = token.ok_or((
        StatusCode::UNAUTHORIZED,
        Json(json!({"error": "No authentication token provided"})),
    ))?;

    // Validate JWT token and extract session_id
    let session_id = validate_token(token_str, &state.jwt_keys)
        .map_err(|e| {
            debug!("JWT validation failed: {}", e);
            (StatusCode::UNAUTHORIZED, Json(json!({"error": format!("Invalid token: {}", e)})))
        })?;

    // Also verify session is still active (double-check)
    if is_authenticated(&state.authenticated_sessions, &session_id).await {
        Ok(next.run(request).await)
    } else {
        Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Session not active or has expired"})),
        ))
    }
}
