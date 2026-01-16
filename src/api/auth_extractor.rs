use axum::{
    async_trait,
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
};
use crate::session::is_authenticated;
use crate::server::AppState;

/// Authenticated session ID - guaranteed to be valid
pub struct AuthenticatedSession(pub String);

#[async_trait]
impl FromRequestParts<AppState> for AuthenticatedSession {
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        // Extract query parameters
        let uri = &parts.uri;
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
            })
            .map(|s| s.to_string());

        // Verify session is authenticated
        let sid = session_id.ok_or((StatusCode::UNAUTHORIZED, "No session ID provided"))?;

        if is_authenticated(&state.authenticated_sessions, &sid).await {
            Ok(AuthenticatedSession(sid))
        } else {
            Err((StatusCode::UNAUTHORIZED, "Session not authenticated"))
        }
    }
}
