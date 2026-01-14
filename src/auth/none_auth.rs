use crate::auth::{AuthMethod, Authenticator, Credentials};

/// No-op authenticator for when authentication is disabled
pub struct NoAuthenticator;

impl Authenticator for NoAuthenticator {
    fn authenticate(&self, _credentials: &Credentials) -> bool {
        true
    }

    fn method(&self) -> AuthMethod {
        AuthMethod::None
    }
}
