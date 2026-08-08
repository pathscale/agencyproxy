use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf};
use thiserror::Error;

const MIN_AUTHENTICATION_KEY_BYTES: usize = 32;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyConfig {
    pub connection: ConnectionConfig,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
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
        #[serde(default)]
        tls: Option<TlsConfig>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    pub certificates: Vec<PathBuf>,
    pub private_key: PathBuf,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("could not read AgencyProxy config: {0}")]
    Read(#[source] std::io::Error),
    #[error("could not parse AgencyProxy config: {0}")]
    Parse(#[source] serde_json::Error),
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
    pub async fn read(path: &std::path::Path) -> Result<Self, ConfigError> {
        let bytes = tokio::fs::read(path).await.map_err(ConfigError::Read)?;
        let config: Self = serde_json::from_slice(&bytes).map_err(ConfigError::Parse)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let ConnectionConfig::WebSocket {
            address,
            authentication_key,
            allowed_origins,
            tls: _,
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
    fn rejects_public_or_unauthenticated_websocket_listeners() {
        let public = ProxyConfig {
            connection: ConnectionConfig::WebSocket {
                address: "0.0.0.0:17820".parse().expect("address should parse"),
                authentication_key: KEY.into(),
                allowed_origins: Vec::new(),
                tls: None,
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
                tls: None,
            },
        };
        assert!(matches!(
            weak.validate(),
            Err(ConfigError::WeakAuthenticationKey)
        ));
    }
}
