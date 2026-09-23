use agency_proxy::{
    ConnectionConfig, ProxyConfig, ProxyServer, RuntimeRegistry, WebSocketConfig, serve_websocket,
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
        } => match termination() {
            Err(error) => Err(format!("could not register SIGTERM and SIGINT: {error}")),
            Ok(terminated) => {
                eprintln!("AgencyProxy MCP/WebSocket listening on {address}");
                // The process owns its signals and tells the server when to stop.
                // The server polls `stop` on its own thread, where tokio's signal
                // driver does not reach, so the signal is waited for here and
                // forwarded through a oneshot, which needs no runtime to await.
                let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
                tokio::spawn(async move {
                    terminated.await;
                    let _ = stop.send(());
                });
                serve_websocket(
                    registry,
                    WebSocketConfig {
                        address,
                        authentication_key,
                        allowed_origins,
                    },
                    async move {
                        let _ = stopped.await;
                    },
                )
                .await
                .map_err(|error| error.to_string())
            }
        },
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("AgencyProxy stopped: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Resolves on the first SIGTERM or SIGINT.
///
/// Both are registered before this returns, so a signal that arrives while the
/// server is still binding is not lost to the default action.
fn termination() -> std::io::Result<impl std::future::Future<Output = ()>> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    Ok(async move {
        tokio::select! {
            _ = terminate.recv() => {}
            _ = interrupt.recv() => {}
        }
    })
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
