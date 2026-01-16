use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use log::{error, info};

/// JWT claims for session authentication
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    /// Session ID
    pub sub: String,
    /// Issued at (timestamp)
    pub iat: i64,
    /// Expiration time (timestamp)
    pub exp: i64,
    /// Issuer
    pub iss: String,
}

/// JWT encoding/decoding keys
pub struct JwtKeys {
    pub encoding: EncodingKey,
    pub decoding: DecodingKey,
}

impl JwtKeys {
    /// Create new JWT keys from a secret
    pub fn from_secret(secret: &[u8]) -> Self {
        Self {
            encoding: EncodingKey::from_secret(secret),
            decoding: DecodingKey::from_secret(secret),
        }
    }

    /// Generate a random secret key
    pub fn generate_random() -> Vec<u8> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        // Use a simple hash of time + process ID as seed
        let seed = format!("{}-{}-{}", nonce, std::process::id(), std::env::var("HOSTNAME").unwrap_or_default());
        let mut key = [0u8; 32];
        for (i, byte) in seed.bytes().enumerate() {
            key[i % 32] ^= byte;
        }
        key.to_vec()
    }
}

/// Generate a JWT token for a session
pub fn generate_token(session_id: &str, keys: &JwtKeys, expiration_hours: i64) -> Result<String, String> {
    let now = Utc::now();
    let expiration = now + Duration::hours(expiration_hours);

    let claims = Claims {
        sub: session_id.to_owned(),
        iat: now.timestamp(),
        exp: expiration.timestamp(),
        iss: "ttyd".to_owned(),
    };

    encode(&Header::default(), &claims, &keys.encoding)
        .map_err(|e| {
            error!("Failed to encode JWT token: {}", e);
            format!("Token generation failed: {}", e)
        })
}

/// Validate a JWT token and return the session ID if valid
pub fn validate_token(token: &str, keys: &JwtKeys) -> Result<String, String> {
    let token_data = decode::<Claims>(
        token,
        &keys.decoding,
        &Validation::default()
    )
    .map_err(|e| {
        error!("Failed to decode JWT token: {}", e);
        format!("Invalid token: {}", e)
    })?;

    // Check expiration
    let now = Utc::now().timestamp();
    if token_data.claims.exp < now {
        return Err("Token has expired".to_string());
    }

    info!("Validated token for session: {}", token_data.claims.sub);
    Ok(token_data.claims.sub)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_generation_and_validation() {
        let secret = b"test_secret_key_for_testing";
        let keys = JwtKeys::from_secret(secret);

        let session_id = "test_session_123";
        let token = generate_token(session_id, &keys, 1).unwrap();

        let extracted_session_id = validate_token(&token, &keys).unwrap();
        assert_eq!(extracted_session_id, session_id);
    }

    #[test]
    fn test_invalid_token() {
        let secret = b"test_secret_key_for_testing";
        let keys = JwtKeys::from_secret(secret);

        let result = validate_token("invalid_token", &keys);
        assert!(result.is_err());
    }
}
