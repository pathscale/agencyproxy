use agency_proxy::{
    ConnectionConfig, ProxyConfig, ProxyServer, RuntimeRegistry, WebSocketConfig,
    WebSocketTlsConfig, serve_websocket,
};
use clap::Parser;
use std::{path::PathBuf, process::ExitCode};

#[derive(Debug, Parser)]
#[command(name = "agency-proxy", version, about)]
struct Args {
    /// JSON configuration selecting the proxy connection transport.
    #[arg(long, value_name = "PATH", conflicts_with = "socket")]
    config: Option<PathBuf>,
    /// Compatibility shortcut for a permission-restricted Unix endpoint.
    #[arg(long, value_name = "PATH", required_unless_present = "config")]
    socket: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let config = match args.config {
        Some(path) => match ProxyConfig::read(&path).await {
            Ok(config) => config,
            Err(error) => {
                eprintln!("could not start AgencyProxy: {error}");
                return ExitCode::FAILURE;
            }
        },
        None => ProxyConfig {
            connection: ConnectionConfig::Unix {
                socket: args
                    .socket
                    .expect("clap requires either --config or --socket"),
            },
        },
    };
    let registry = RuntimeRegistry::default();
    let result = match config.connection {
        ConnectionConfig::Unix { socket } => serve_unix(socket, registry).await,
        ConnectionConfig::WebSocket {
            address,
            authentication_key,
            allowed_origins,
            tls,
        } => {
            eprintln!("AgencyProxy MCP/WebSocket listening on {address}");
            serve_websocket(
                registry,
                WebSocketConfig {
                    address,
                    authentication_key,
                    allowed_origins,
                    tls: tls.map(|tls| WebSocketTlsConfig {
                        certificates: tls.certificates,
                        private_key: tls.private_key,
                    }),
                },
            )
            .await
            .map_err(|error| error.to_string())
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("AgencyProxy stopped: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn serve_unix(socket: PathBuf, registry: RuntimeRegistry) -> Result<(), String> {
    let server = ProxyServer::bind_with_registry(&socket, registry)
        .await
        .map_err(|error| error.to_string())?;
    eprintln!(
        "AgencyProxy listening on {}",
        server.socket_path().display()
    );
    server.serve().await.map_err(|error| error.to_string())
}
