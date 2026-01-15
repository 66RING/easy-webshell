use crate::auth::AuthConfig;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::fs::{self};

/// Configuration file structure
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub auth: AuthConfig,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    #[serde(default = "default_cur_dir")]
    pub cur_dir: Option<String>,
}

fn default_cur_dir() -> Option<String> {
    None
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                host: "0.0.0.0".to_string(),
                port: 7681,
                cur_dir: None,
            },
            auth: AuthConfig {
                enabled: false,
                method: "none".to_string(),
                username: None,
                password: None,
            },
        }
    }
}

/// Load configuration from file, or return default config
pub fn load_config() -> Config {
    // Try multiple possible config file locations
    let config_paths = vec![
        "config.toml",           // Current directory
        "/etc/ttyd/config.toml", // System-wide config
    ];

    for config_path in config_paths {
        match fs::read_to_string(config_path) {
            Ok(content) => {
                info!("Found config file: {}", config_path);
                match toml::from_str::<Config>(&content) {
                    Ok(config) => {
                        info!("Successfully loaded configuration from {}", config_path);
                        info!("Server: {}:{}", config.server.host, config.server.port);
                        info!(
                            "Auth: {} (method: {})",
                            if config.auth.enabled {
                                "enabled"
                            } else {
                                "disabled"
                            },
                            config.auth.method
                        );
                        if let Some(ref dir) = config.server.cur_dir {
                            info!("Working directory: {}", dir);
                        }
                        return config;
                    }
                    Err(e) => {
                        error!("Failed to parse config file '{}': {}", config_path, e);
                        error!("Please check the file format. Using default configuration.");
                        continue;
                    }
                }
            }
            Err(_) => {
                // File not found, try next path
                continue;
            }
        }
    }

    info!("No config file found, using default configuration");
    info!("Default: server on 0.0.0.0:7681, authentication disabled");
    Config::default()
}
