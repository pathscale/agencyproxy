use agency_proxy::{
    ConnectionConfig, ProxyConfig, ProxyServer, RuntimeRegistry, WebSocketConfig, serve_websocket,
};
use clap::Parser;
use futures::{channel::oneshot, future::select};
use nagoya::reactor::{Handle, Reactor, block_on_with};
use nagoya::signal::{Signal, SignalKind};
use std::{path::PathBuf, pin::pin, process::ExitCode};

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

/// The composition root: the one place that owns the process's signals and
/// its reactor, and hands the reactor's handle to everything that registers a
/// descriptor.
///
/// The order is load-bearing. Signals first, before any thread exists (see
/// `termination`); then the reactor, whose own thread therefore inherits the
/// signal block; then serving. `nagoya::block_on` drives each future on this
/// thread and starts no thread of its own, and reading the config is a plain
/// file read, so nothing before `termination` creates one either.
fn main() -> ExitCode {
    let args = Args::parse();
    let config = match args.config {
        Some(path) => match nagoya::block_on(ProxyConfig::read(&path)) {
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
    let stopped = match &config.connection {
        ConnectionConfig::Unix { .. } => None,
        ConnectionConfig::WebSocket { .. } => match termination() {
            Ok(stopped) => Some(stopped),
            Err(error) => {
                eprintln!("AgencyProxy stopped: could not register SIGTERM and SIGINT: {error}");
                return ExitCode::FAILURE;
            }
        },
    };
    // Owned here until `main` returns. Every socket and provider process is
    // registered on it, and a dropped reactor stops its thread and hangs them.
    let reactor = match Reactor::start() {
        Ok(reactor) => reactor,
        Err(error) => {
            eprintln!("AgencyProxy stopped: could not start the I/O reactor: {error}");
            return ExitCode::FAILURE;
        }
    };
    let result = nagoya::block_on(serve(config.connection, &reactor.handle(), stopped));
    drop(reactor);
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
///
/// Called before anything in the process has started a thread, and that order
/// is load-bearing on Linux. There a nagoya `Signal` is a `signalfd`, which
/// only receives a signal that is blocked, and the block it installs covers the
/// calling thread and the threads it creates afterwards but not threads that
/// already exist. A SIGTERM delivered to an older thread would take the
/// default action and kill the process without a drain. So the waiters are
/// made here on the main thread, first, and every thread after them (the
/// reactor, the pool, the WebSocket server) inherits the block. On the BSDs
/// and macOS it is a process-wide handler and the order does not matter.
///
/// The reactor is a local one, with no thread of its own, for the same reason:
/// starting a threaded reactor to register on would create a thread before the
/// block. A dedicated thread, started after the waiters exist, polls it.
fn termination() -> std::io::Result<oneshot::Receiver<()>> {
    let reactor = Reactor::local()?;
    let handle = reactor.handle();
    let mut terminate = Signal::new(SignalKind::terminate(), &handle)?;
    let mut interrupt = Signal::new(SignalKind::interrupt(), &handle)?;
    let (stop, stopped) = oneshot::channel();
    std::thread::Builder::new()
        .name("agency-proxy-signals".into())
        .spawn(move || {
            block_on_with(&reactor, async move {
                // Either outcome is a stop, including a failed wait: signals
                // can then no longer be observed, and draining now is safer
                // than running on unable to hear the operator.
                let _ = select(pin!(terminate.recv()), pin!(interrupt.recv())).await;
                let _ = stop.send(());
                // Never returns, so the waiters are never dropped. Dropping a
                // nagoya `Signal` restores the default disposition, and a
                // second SIGTERM during the drain would then kill the process.
                // tokio never restored it and later signals were no-ops;
                // keeping the waiters alive for the life of the process
                // preserves that. Nothing reads them again, so nothing spins.
                // The thread does not hold the process open: it ends when
                // `main` returns.
                std::future::pending::<()>().await;
                drop((terminate, interrupt));
            });
        })?;
    Ok(stopped)
}

async fn serve(
    connection: ConnectionConfig,
    reactor: &Handle,
    stopped: Option<oneshot::Receiver<()>>,
) -> Result<(), String> {
    let registry = RuntimeRegistry::new(reactor.clone());
    match connection {
        ConnectionConfig::Unix { socket } => serve_unix(socket, registry, reactor).await,
        ConnectionConfig::WebSocket {
            address,
            authentication_key,
            allowed_origins,
        } => {
            eprintln!("AgencyProxy MCP/WebSocket listening on {address}");
            // The process owns its signals and tells the server when to stop.
            // The server polls `stop` on its own thread under its own reactor,
            // so the signal is waited for elsewhere and forwarded through a
            // oneshot, which needs no runtime to await.
            serve_websocket(
                registry,
                WebSocketConfig {
                    address,
                    authentication_key,
                    allowed_origins,
                },
                async move {
                    if let Some(stopped) = stopped {
                        let _ = stopped.await;
                    } else {
                        // `main` registers the signals for every WebSocket
                        // configuration, so this is not reached; without them
                        // the server runs until the process is killed.
                        std::future::pending::<()>().await;
                    }
                },
            )
            .await
            .map_err(|error| error.to_string())
        }
    }
}

async fn serve_unix(
    socket: PathBuf,
    registry: RuntimeRegistry,
    reactor: &Handle,
) -> Result<(), String> {
    let server = ProxyServer::bind_with_registry(&socket, registry, reactor)
        .await
        .map_err(|error| error.to_string())?;
    eprintln!(
        "AgencyProxy listening on {}",
        server.socket_path().display()
    );
    server.serve().await.map_err(|error| error.to_string())
}
