//! Local AgencyProxy server and provider-runtime boundary.

pub mod broadcast;
mod config;
mod flag;
mod runtime;
mod web;

use agency_proxy_protocol::{
    Capability, ClientFrame, ClientMessage, ErrorCode, MAX_FRAME_BYTES, PROTOCOL_VERSION, RunId,
    ServerFrame, ServerResponse, ShutdownMode,
};
// The neutral framing takes a `futures_io` stream, and `NagoyaStream` presents
// a nagoya socket as one directly, so there is no compat layer in between.
use endpoint_libs::libs::ws::{
    WireMessage,
    transport::{framed::framed_json_neutral_with_max_frame, nagoya::NagoyaStream},
};
use flag::Flag;
use futures::{
    Sink, SinkExt, Stream, StreamExt,
    channel::mpsc,
    future::{AbortHandle, Either, abortable, select},
};
use nagoya::net::{TcpListener, TcpStream};
use nagoya::reactor::{Addr, Handle};
use std::{
    collections::BTreeMap,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    pin::pin,
    sync::Arc,
};
use thiserror::Error;

pub use config::{ConfigError, ConnectionConfig, ProxyConfig};
pub use runtime::{Attachment, RuntimeError, RuntimeRegistry, SequencedEvent};
pub use web::{WebSocketConfig, serve_websocket};

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid JSON frame: {0}")]
    Json(#[from] serde_json::Error),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("another AgencyProxy is already listening at {0}")]
    AlreadyRunning(PathBuf),
    #[error("refusing to replace a non-socket path at {0}")]
    UnsafeSocketPath(PathBuf),
}

#[derive(Debug)]
pub struct ProxyServer {
    listener: TcpListener,
    registry: RuntimeRegistry,
    socket_path: PathBuf,
    // Was a `tokio::sync::watch::Sender<bool>`; see `flag`.
    shutdown: Arc<Flag>,
    // An async lock because the Shutdown and StartRun admission check holds it
    // across the acknowledgement write. nagoya's `sync` has no `Mutex`; its
    // `RwLock` taken only for writing is one.
    lifecycle: Arc<nagoya::sync::RwLock<Lifecycle>>,
}

#[derive(Debug, Default)]
struct Lifecycle {
    stopping: bool,
}

impl ProxyServer {
    /// Bind with a fresh registry whose provider runs use the same reactor.
    ///
    /// The listener and every accepted connection are registered on
    /// `reactor`, which the caller owns and must keep running for as long as
    /// the server is in use.
    pub async fn bind(socket_path: impl AsRef<Path>, reactor: &Handle) -> Result<Self, Error> {
        Self::bind_with_registry(socket_path, RuntimeRegistry::new(reactor.clone()), reactor).await
    }

    /// Bind with an existing registry. `reactor` carries the listener and its
    /// connections; the registry keeps the handle it was built with for
    /// provider processes.
    pub async fn bind_with_registry(
        socket_path: impl AsRef<Path>,
        registry: RuntimeRegistry,
        reactor: &Handle,
    ) -> Result<Self, Error> {
        let socket_path = socket_path.as_ref().to_path_buf();
        prepare_socket_path(&socket_path).await?;
        // nagoya's socket type is `TcpListener` for every address family; a
        // path address makes it a Unix-domain listener.
        let listener = TcpListener::bind(unix_address(&socket_path)?, reactor)
            .map_err(std::io::Error::from)?;
        set_permissions(&socket_path, 0o600)?;
        Ok(Self {
            listener,
            registry,
            socket_path,
            shutdown: Arc::default(),
            lifecycle: Arc::default(),
        })
    }

    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub async fn serve(self) -> Result<(), Error> {
        loop {
            // The shutdown request is polled first so a steady stream of
            // connections cannot keep a stopping daemon alive; tokio's
            // `select!` picked a random branch, which had the same property on
            // average. Accept is cancel safe: losing leaves any pending
            // connection in the kernel's backlog, closed with the listener.
            let accepted = match select(pin!(self.shutdown.wait()), self.listener.accept()).await {
                Either::Left(((), _)) => return Ok(()),
                Either::Right((accepted, _)) => accepted,
            };
            let (stream, _) = accepted.map_err(std::io::Error::from)?;
            let registry = self.registry.clone();
            let request_shutdown = Arc::clone(&self.shutdown);
            let lifecycle = self.lifecycle.clone();
            // Detached, as tokio's spawn was: a connection ends on its own.
            drop(nagoya::spawn(async move {
                if let Err(error) =
                    handle_connection(stream, registry, request_shutdown, lifecycle).await
                {
                    eprintln!("AgencyProxy connection failed: {error}");
                }
            }));
        }
    }
}

fn unix_address(socket_path: &Path) -> Result<Addr, Error> {
    Addr::path(socket_path.as_os_str().as_bytes())
        .map_err(|error| Error::Io(std::io::Error::from(error)))
}

impl Drop for ProxyServer {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

/// Make the socket's directory private and clear a stale socket from a
/// previous daemon.
///
/// Blocking `std` calls rather than tokio's thread-pool wrappers: these are
/// three metadata operations and one Unix-domain connect, run once at bind
/// time, and a Unix-domain connect completes or fails inside the call.
async fn prepare_socket_path(socket_path: &Path) -> Result<(), Error> {
    let parent = socket_path
        .parent()
        .ok_or_else(|| Error::UnsafeSocketPath(socket_path.to_path_buf()))?;
    std::fs::create_dir_all(parent)?;
    set_permissions(parent, 0o700)?;
    let metadata = match std::fs::symlink_metadata(socket_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !std::os::unix::fs::FileTypeExt::is_socket(&metadata.file_type()) {
        return Err(Error::UnsafeSocketPath(socket_path.to_path_buf()));
    }
    if std::os::unix::net::UnixStream::connect(socket_path).is_ok() {
        return Err(Error::AlreadyRunning(socket_path.to_path_buf()));
    }
    std::fs::remove_file(socket_path)?;
    Ok(())
}

fn set_permissions(path: &Path, mode: u32) -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    Ok(())
}

async fn handle_connection(
    stream: TcpStream,
    registry: RuntimeRegistry,
    request_shutdown: Arc<Flag>,
    lifecycle: Arc<nagoya::sync::RwLock<Lifecycle>>,
) -> Result<(), Error> {
    let mut transport =
        framed_json_neutral_with_max_frame(NagoyaStream::new(stream), MAX_FRAME_BYTES + 1);
    let Some(first) = receive_client(&mut transport).await? else {
        return Ok(());
    };
    let ClientMessage::Hello { version, .. } = first.message else {
        send_error(
            &mut transport,
            first.request_id,
            ErrorCode::ProtocolViolation,
            "the first frame must be hello",
        )
        .await?;
        return Ok(());
    };
    let Some(version) = PROTOCOL_VERSION.negotiate(version) else {
        send_error(
            &mut transport,
            first.request_id,
            ErrorCode::IncompatibleVersion,
            "the protocol major version is incompatible",
        )
        .await?;
        return Ok(());
    };
    send_response(
        &mut transport,
        first.request_id,
        ServerResponse::Hello {
            server_name: "agency-proxy".into(),
            version,
            capabilities: vec![
                Capability::EventReplay,
                Capability::LiveInjection,
                Capability::Approvals,
                Capability::Cancellation,
                Capability::SessionInterruption,
                Capability::ProviderDetection,
                Capability::LifecycleControl,
            ],
        },
    )
    .await?;

    let (outbound, mut outgoing) = mpsc::unbounded::<ServerFrame>();
    // Abort handles rather than join handles: nagoya's `JoinHandle::cancel`
    // consumes the handle, and these are aborted from a map. `abortable` stops
    // the task at its next suspension point, as tokio's `abort` did.
    let mut attachments = BTreeMap::<RunId, AbortHandle>::new();
    loop {
        // The outgoing queue is never exhausted, because this function holds
        // `outbound`, so its branch never disables itself the way a failed
        // `Some(frame)` pattern did under `tokio::select!`. Both halves are
        // cancel safe: the framed stream keeps a partial frame in its own
        // buffer. The losing future is dropped at the end of this statement,
        // which releases `transport` for the writes below.
        let step = match select(outgoing.next(), pin!(receive_client(&mut transport))).await {
            Either::Left((frame, _)) => Either::Left(frame),
            Either::Right((incoming, _)) => Either::Right(incoming),
        };
        match step {
            Either::Left(frame) => {
                if let Some(frame) = frame {
                    send_server(&mut transport, &frame).await?;
                }
            }
            Either::Right(incoming) => {
                let Some(frame) = incoming? else { break };
                match frame.message {
                    ClientMessage::ListRuns => {
                        send_response(
                            &mut transport,
                            frame.request_id,
                            ServerResponse::Runs {
                                runs: registry.list().await,
                            },
                        )
                        .await?;
                    }
                    ClientMessage::ProbeProviders => {
                        send_response(
                            &mut transport,
                            frame.request_id,
                            ServerResponse::Providers {
                                providers: registry.probe_providers().await,
                            },
                        )
                        .await?;
                    }
                    ClientMessage::ReadAccountUsage => {
                        send_response(
                            &mut transport,
                            frame.request_id,
                            ServerResponse::AccountUsage {
                                providers: registry.account_usage().await,
                            },
                        )
                        .await?;
                    }
                    ClientMessage::ShutdownIfIdle => {
                        let mut lifecycle = lifecycle.write().await;
                        let active = registry.active_count().await;
                        if active > 0 {
                            send_error(
                                &mut transport,
                                frame.request_id,
                                ErrorCode::Conflict,
                                &format!("AgencyProxy has {active} active run(s)"),
                            )
                            .await?;
                            continue;
                        }
                        lifecycle.stopping = true;
                        send_response(&mut transport, frame.request_id, ServerResponse::Accepted)
                            .await?;
                        request_shutdown.set();
                        return Ok(());
                    }
                    ClientMessage::Shutdown { mode } => {
                        let mut lifecycle = lifecycle.write().await;
                        if lifecycle.stopping {
                            send_error(
                                &mut transport,
                                frame.request_id,
                                ErrorCode::Conflict,
                                "AgencyProxy is already stopping",
                            )
                            .await?;
                            continue;
                        }
                        // This flag and StartRun share the same lock. Once the
                        // acknowledgement is visible, no new provider can pass
                        // the admission gate while existing runs drain.
                        lifecycle.stopping = true;
                        send_response(&mut transport, frame.request_id, ServerResponse::Accepted)
                            .await?;
                        drop(lifecycle);

                        // Detached, as tokio's spawn was: the drain outlives
                        // this connection, which returns now.
                        drop(nagoya::spawn(async move {
                            if mode == ShutdownMode::Terminate {
                                registry.cancel_all().await;
                            }
                            registry.wait_until_idle().await;
                            request_shutdown.set();
                        }));
                        return Ok(());
                    }
                    ClientMessage::StartRun {
                        run_id, request, ..
                    } => {
                        let lifecycle = lifecycle.write().await;
                        if lifecycle.stopping {
                            send_error(
                                &mut transport,
                                frame.request_id,
                                ErrorCode::Conflict,
                                "AgencyProxy is stopping",
                            )
                            .await?;
                            continue;
                        }
                        match registry.start(run_id, *request).await {
                            Ok(()) => {
                                send_response(
                                    &mut transport,
                                    frame.request_id,
                                    ServerResponse::Accepted,
                                )
                                .await?
                            }
                            Err(error) => {
                                send_runtime_error(&mut transport, frame.request_id, error).await?
                            }
                        }
                    }
                    ClientMessage::AttachRun {
                        run_id,
                        after_sequence,
                    } => {
                        match registry.attach(&run_id, after_sequence).await {
                            Ok(attachment) => {
                                send_response(
                                    &mut transport,
                                    frame.request_id,
                                    ServerResponse::Run {
                                        run: attachment.snapshot,
                                    },
                                )
                                .await?;
                                for event in &attachment.replay {
                                    send_server(&mut transport, &event_frame(event)).await?;
                                }
                                if let Some(previous) = attachments.remove(&run_id) {
                                    previous.abort();
                                }
                                let event_sender = outbound.clone();
                                let attached_registry = registry.clone();
                                let attached_run = run_id.clone();
                                let mut events = attachment.events;
                                let mut last_sequence = attachment
                                    .replay
                                    .last()
                                    .map_or(after_sequence, |event| event.sequence);
                                let (forward, abort) = abortable(async move {
                                    loop {
                                        match events.recv().await {
                                            Ok(event) => {
                                                if event.sequence <= last_sequence {
                                                    continue;
                                                }
                                                last_sequence = event.sequence;
                                                if event_sender
                                                    .unbounded_send(event_frame(&event))
                                                    .is_err()
                                                {
                                                    return;
                                                }
                                            }
                                            Err(broadcast::RecvError::Lagged(_)) => {
                                                let Ok(replay) = attached_registry
                                                    .attach(&attached_run, last_sequence)
                                                    .await
                                                else {
                                                    return;
                                                };
                                                for event in &replay.replay {
                                                    last_sequence = event.sequence;
                                                    if event_sender
                                                        .unbounded_send(event_frame(event))
                                                        .is_err()
                                                    {
                                                        return;
                                                    }
                                                }
                                                events = replay.events;
                                            }
                                            Err(broadcast::RecvError::Closed) => return,
                                        }
                                    }
                                });
                                // Detached; `abort` is how it is stopped.
                                drop(nagoya::spawn(forward));
                                attachments.insert(run_id, abort);
                            }
                            Err(error) => {
                                send_runtime_error(&mut transport, frame.request_id, error).await?
                            }
                        }
                    }
                    ClientMessage::DetachRun { run_id } => {
                        if let Some(task) = attachments.remove(&run_id) {
                            task.abort();
                        }
                        send_response(&mut transport, frame.request_id, ServerResponse::Accepted)
                            .await?;
                    }
                    ClientMessage::InjectMessage { run_id, body, .. } => {
                        match registry.inject(&run_id, &body).await {
                            Ok(()) => {
                                send_response(
                                    &mut transport,
                                    frame.request_id,
                                    ServerResponse::Accepted,
                                )
                                .await?
                            }
                            Err(error) => {
                                send_runtime_error(&mut transport, frame.request_id, error).await?
                            }
                        }
                    }
                    ClientMessage::CancelRun { run_id, .. } => {
                        match registry.cancel(&run_id).await {
                            Ok(()) => {
                                send_response(
                                    &mut transport,
                                    frame.request_id,
                                    ServerResponse::Accepted,
                                )
                                .await?
                            }
                            Err(error) => {
                                send_runtime_error(&mut transport, frame.request_id, error).await?
                            }
                        }
                    }
                    ClientMessage::InterruptSession {
                        provider,
                        session_id,
                        binary,
                        ..
                    } => {
                        match registry
                            .interrupt_session(&provider, &session_id, binary)
                            .await
                        {
                            Ok(interrupted) => {
                                send_response(
                                    &mut transport,
                                    frame.request_id,
                                    ServerResponse::SessionInterrupted { interrupted },
                                )
                                .await?
                            }
                            Err(error) => {
                                send_runtime_error(&mut transport, frame.request_id, error).await?
                            }
                        }
                    }
                    ClientMessage::DecideApproval {
                        run_id,
                        approval_id,
                        decision,
                        ..
                    } => match registry.decide(&run_id, &approval_id, decision).await {
                        Ok(()) => {
                            send_response(
                                &mut transport,
                                frame.request_id,
                                ServerResponse::Accepted,
                            )
                            .await?
                        }
                        Err(error) => {
                            send_runtime_error(&mut transport, frame.request_id, error).await?
                        }
                    },
                    ClientMessage::AckEvents {
                        run_id,
                        through_sequence,
                    } => match registry.acknowledge(&run_id, through_sequence).await {
                        Ok(()) => {
                            send_response(
                                &mut transport,
                                frame.request_id,
                                ServerResponse::Accepted,
                            )
                            .await?
                        }
                        Err(error) => {
                            send_runtime_error(&mut transport, frame.request_id, error).await?
                        }
                    },
                    ClientMessage::Hello { .. } => {
                        send_error(
                            &mut transport,
                            frame.request_id,
                            ErrorCode::ProtocolViolation,
                            "hello may only be sent once",
                        )
                        .await?;
                    }
                }
            }
        }
    }
    for (_, task) in attachments {
        task.abort();
    }
    Ok(())
}

fn event_frame(event: &SequencedEvent) -> ServerFrame {
    ServerFrame::Event {
        run_id: event.run_id.clone(),
        sequence: event.sequence,
        event: event.event.clone(),
    }
}

async fn receive_client<T, E>(transport: &mut T) -> Result<Option<ClientFrame>, Error>
where
    T: Stream<Item = Result<WireMessage, E>> + Unpin,
    E: std::error::Error,
{
    match transport.next().await {
        None | Some(Ok(WireMessage::Close(_))) => Ok(None),
        Some(Ok(message)) => message
            .as_text()
            .ok_or_else(|| Error::Transport("expected a JSON text frame".into()))
            .and_then(|text| serde_json::from_str(text).map_err(Error::from))
            .map(Some),
        Some(Err(error)) => Err(Error::Transport(error.to_string())),
    }
}

async fn send_server<T, E>(transport: &mut T, frame: &ServerFrame) -> Result<(), Error>
where
    T: Sink<WireMessage, Error = E> + Unpin,
    E: std::error::Error,
{
    transport
        .send(WireMessage::Text(serde_json::to_string(frame)?.into()))
        .await
        .map_err(|error| Error::Transport(error.to_string()))
}

async fn send_response<T, E>(
    transport: &mut T,
    request_id: u64,
    response: ServerResponse,
) -> Result<(), Error>
where
    T: Sink<WireMessage, Error = E> + Unpin,
    E: std::error::Error,
{
    send_server(
        transport,
        &ServerFrame::Response {
            request_id,
            response,
        },
    )
    .await
}

async fn send_error<T, E>(
    transport: &mut T,
    request_id: u64,
    code: ErrorCode,
    message: &str,
) -> Result<(), Error>
where
    T: Sink<WireMessage, Error = E> + Unpin,
    E: std::error::Error,
{
    send_response(
        transport,
        request_id,
        ServerResponse::Error {
            code,
            message: message.into(),
        },
    )
    .await
}

async fn send_runtime_error<T, E>(
    transport: &mut T,
    request_id: u64,
    error: RuntimeError,
) -> Result<(), Error>
where
    T: Sink<WireMessage, Error = E> + Unpin,
    E: std::error::Error,
{
    let code = match &error {
        RuntimeError::NotFound => ErrorCode::NotFound,
        RuntimeError::Conflict => ErrorCode::Conflict,
        RuntimeError::ReplayExpired { .. } => ErrorCode::Conflict,
        RuntimeError::Provider(_) | RuntimeError::Permission(_) => ErrorCode::ProtocolViolation,
        RuntimeError::Start(_) | RuntimeError::Control(_) => ErrorCode::Internal,
    };
    send_error(transport, request_id, code, &error.to_string()).await
}
