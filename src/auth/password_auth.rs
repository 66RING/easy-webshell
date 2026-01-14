use crate::auth::{AuthMethod, Authenticator, Credentials};

/// Password-based authenticator implementation
pub struct PasswordAuthenticator {
    pub username: String,
    pub password: String,
}

impl Authenticator for PasswordAuthenticator {
    fn authenticate(&self, credentials: &Credentials) -> bool {
        credentials.username == self.username
            && credentials
                .password
                .as_ref()
                .map_or(false, |p| p == &self.password)
    }

    fn method(&self) -> AuthMethod {
        AuthMethod::Password
    }
}
