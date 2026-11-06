use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf};
use thiserror::Error;

const MIN_AUTHENTICATION_KEY_BYTES: usize = 32;
const MAX_STDIN_CONFIG_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyConfig {
    pub connection: ConnectionConfig,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ConnectionConfig {
    Unix {
        socket: PathBuf,
    },
    WebSocket {
        address: SocketAddr,
        authentication_key: String,
        #[serde(default)]
        allowed_origins: Vec<String>,
    },
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("could not read AgencyProxy config: {0}")]
    Read(#[source] std::io::Error),
    #[error("could not parse AgencyProxy config: {0}")]
    Parse(#[source] serde_json::Error),
    #[error("AgencyProxy stdin config exceeds the {MAX_STDIN_CONFIG_BYTES}-byte limit")]
    StdinTooLarge,
    #[error("WebSocket address must be loopback, got {0}")]
    NonLoopback(SocketAddr),
    #[error(
        "WebSocket authentication key must contain at least {MIN_AUTHENTICATION_KEY_BYTES} URL-safe characters"
    )]
    WeakAuthenticationKey,
    #[error("allowed WebSocket origins must begin with http:// or https://, got {0}")]
    InvalidOrigin(String),
}

impl ProxyConfig {
    /// Read and validate the config file, or stdin when `path` is `-`.
    ///
    /// This is one small blocking read once at startup, before any socket
    /// exists, so there is nothing for it to stall.
    /// It stays `async` so callers do not change.
    pub async fn read(path: &std::path::Path) -> Result<Self, ConfigError> {
        let bytes = if path == std::path::Path::new("-") {
            let stdin = std::io::stdin();
            read_stdin(stdin.lock())?
        } else {
            // Explicit file configs keep their existing full-file read behavior.
            std::fs::read(path).map_err(ConfigError::Read)?
        };
        let config: Self = serde_json::from_slice(&bytes).map_err(ConfigError::Parse)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let ConnectionConfig::WebSocket {
            address,
            authentication_key,
            allowed_origins,
        } = &self.connection
        else {
            return Ok(());
        };
        if !address.ip().is_loopback() {
            return Err(ConfigError::NonLoopback(*address));
        }
        if authentication_key.len() < MIN_AUTHENTICATION_KEY_BYTES
            || !authentication_key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(ConfigError::WeakAuthenticationKey);
        }
        if let Some(origin) = allowed_origins
            .iter()
            .find(|origin| !(origin.starts_with("https://") || origin.starts_with("http://")))
        {
            return Err(ConfigError::InvalidOrigin(origin.clone()));
        }
        Ok(())
    }
}

fn read_stdin(mut input: impl std::io::Read) -> Result<Vec<u8>, ConfigError> {
    use std::io::Read as _;

    let mut bytes = Vec::new();
    input
        .by_ref()
        .take((MAX_STDIN_CONFIG_BYTES as u64) + 1)
        .read_to_end(&mut bytes)
        .map_err(ConfigError::Read)?;
    if bytes.len() > MAX_STDIN_CONFIG_BYTES {
        return Err(ConfigError::StdinTooLarge);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn parses_each_connection_type() {
        let unix: ProxyConfig = serde_json::from_str(
            r#"{"connection":{"type":"unix","socket":"/tmp/agency-proxy.sock"}}"#,
        )
        .expect("Unix config should parse");
        assert!(matches!(unix.connection, ConnectionConfig::Unix { .. }));

        let web: ProxyConfig = serde_json::from_str(&format!(
            r#"{{"connection":{{"type":"web_socket","address":"127.0.0.1:17820","authenticationKey":"{KEY}","allowedOrigins":["https://agencyzero.example"]}}}}"#
        ))
        .expect("WebSocket config should parse");
        web.validate().expect("loopback WebSocket should validate");
    }

    #[test]
    fn rejects_unsupported_tls_fields_in_websocket_config() {
        let config = format!(
            r#"{{"connection":{{"type":"web_socket","address":"127.0.0.1:17820","authenticationKey":"{KEY}","tls":{{"certificates":["/path/cert.pem"],"privateKey":"/path/key.pem"}}}}}}"#
        );
        assert!(serde_json::from_str::<ProxyConfig>(&config).is_err());
    }

    #[test]
    fn bounds_stdin_config_without_truncating_at_the_limit() {
        let exact_limit = std::io::Cursor::new(vec![b' '; MAX_STDIN_CONFIG_BYTES]);
        assert_eq!(
            read_stdin(exact_limit)
                .expect("stdin at the limit should be accepted")
                .len(),
            MAX_STDIN_CONFIG_BYTES
        );

        let oversized = std::io::Cursor::new(vec![b' '; MAX_STDIN_CONFIG_BYTES + 1]);
        assert!(matches!(
            read_stdin(oversized),
            Err(ConfigError::StdinTooLarge)
        ));
    }

    #[test]
    fn rejects_public_or_unauthenticated_websocket_listeners() {
        let public = ProxyConfig {
            connection: ConnectionConfig::WebSocket {
                address: "0.0.0.0:17820".parse().expect("address should parse"),
                authentication_key: KEY.into(),
                allowed_origins: Vec::new(),
            },
        };
        assert!(matches!(
            public.validate(),
            Err(ConfigError::NonLoopback(_))
        ));

        let weak = ProxyConfig {
            connection: ConnectionConfig::WebSocket {
                address: "127.0.0.1:17820".parse().expect("address should parse"),
                authentication_key: "short".into(),
                allowed_origins: Vec::new(),
            },
        };
        assert!(matches!(
            weak.validate(),
            Err(ConfigError::WeakAuthenticationKey)
        ));
    }
}
