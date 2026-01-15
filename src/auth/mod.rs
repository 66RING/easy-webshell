use self::none_auth::NoAuthenticator;
use self::password_auth::PasswordAuthenticator;
use crate::config::Config;
use log::{error, info};
use serde::{Deserialize, Serialize};

mod none_auth;
mod password_auth;

/// Authentication method configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum AuthMethod {
    Password,
    #[serde(skip)]
    SSHKey, // Reserved for future implementation
    None, // No authentication
}

/// User credentials provided during authentication
#[derive(Debug, Clone)]
pub struct Credentials {
    pub username: String,
    pub password: Option<String>,
    #[allow(dead_code)]
    pub ssh_key: Option<String>, // Reserved for future SSH authentication
}

/// Trait for authentication strategies - allows easy extension
pub trait Authenticator: Send + Sync {
    /// Authenticate the provided credentials
    fn authenticate(&self, credentials: &Credentials) -> bool;

    /// Get the authentication method type
    fn method(&self) -> AuthMethod;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthConfig {
    pub enabled: bool,
    pub method: String, // "password", "ssh_key", "none"
    pub username: Option<String>,
    pub password: Option<String>,
}

/// Create authenticator based on configuration
pub fn create_authenticator(config: &Config) -> Option<Box<dyn Authenticator>> {
    if !config.auth.enabled {
        info!("Authentication disabled");
        return Some(Box::new(NoAuthenticator));
    }

    match config.auth.method.to_lowercase().as_str() {
        "password" => {
            // Require username and password to be explicitly set
            let username = config.auth.username.as_ref()?;
            let password = config.auth.password.as_ref()?;

            info!(
                "Password authentication enabled for user: {} {}",
                username, password
            );
            Some(Box::new(PasswordAuthenticator {
                username: username.clone(),
                password: password.clone(),
            }))
        }
        "ssh_key" => {
            // Reserved for future implementation
            error!("SSH key authentication not yet implemented");
            None
        }
        "none" => {
            info!("No authentication configured");
            Some(Box::new(NoAuthenticator))
        }
        _ => {
            error!("Unknown authentication method: {}", config.auth.method);
            None
        }
    }
}
