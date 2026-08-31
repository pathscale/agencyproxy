//! Local AgencyProxy server and provider-runtime boundary.

mod config;
mod runtime;
mod web;

use agency_proxy_protocol::{
    Capability, ClientFrame, ClientMessage, ErrorCode, MAX_FRAME_BYTES, PROTOCOL_VERSION, RunId,
    ServerFrame, ServerResponse, ShutdownMode,
};
use endpoint_libs::libs::ws::{WireMessage, transport::framed::framed_json_with_max_frame};
use futures::{Sink, SinkExt, Stream, StreamExt};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;
use tokio::net::{UnixListener, UnixStream};

pub use config::{ConfigError, ConnectionConfig, ProxyConfig, TlsConfig};
pub use runtime::{Attachment, RuntimeError, RuntimeRegistry, SequencedEvent};
pub use web::{WebSocketConfig, WebSocketTlsConfig, serve_websocket};

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
    listener: UnixListener,
    registry: RuntimeRegistry,
    socket_path: PathBuf,
    shutdown: tokio::sync::watch::Sender<bool>,
    lifecycle: Arc<tokio::sync::Mutex<Lifecycle>>,
}

#[derive(Debug, Default)]
struct Lifecycle {
    stopping: bool,
}

impl ProxyServer {
    pub async fn bind(socket_path: impl AsRef<Path>) -> Result<Self, Error> {
        Self::bind_with_registry(socket_path, RuntimeRegistry::default()).await
    }

    pub async fn bind_with_registry(
        socket_path: impl AsRef<Path>,
        registry: RuntimeRegistry,
    ) -> Result<Self, Error> {
        let socket_path = socket_path.as_ref().to_path_buf();
        prepare_socket_path(&socket_path).await?;
        let listener = UnixListener::bind(&socket_path)?;
        set_permissions(&socket_path, 0o600)?;
        let (shutdown, _) = tokio::sync::watch::channel(false);
        Ok(Self {
            listener,
            registry,
            socket_path,
            shutdown,
            lifecycle: Arc::default(),
        })
    }

    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub async fn serve(self) -> Result<(), Error> {
        let mut shutdown = self.shutdown.subscribe();
        loop {
            let accepted = tokio::select! {
                accepted = self.listener.accept() => accepted,
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        return Ok(());
                    }
                    continue;
                }
            };
            let (stream, _) = accepted?;
            let registry = self.registry.clone();
            let request_shutdown = self.shutdown.clone();
            let lifecycle = self.lifecycle.clone();
            tokio::spawn(async move {
                if let Err(error) =
                    handle_connection(stream, registry, request_shutdown, lifecycle).await
                {
                    eprintln!("AgencyProxy connection failed: {error}");
                }
            });
        }
    }
}

impl Drop for ProxyServer {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

async fn prepare_socket_path(socket_path: &Path) -> Result<(), Error> {
    let parent = socket_path
        .parent()
        .ok_or_else(|| Error::UnsafeSocketPath(socket_path.to_path_buf()))?;
    tokio::fs::create_dir_all(parent).await?;
    set_permissions(parent, 0o700)?;
    let metadata = match tokio::fs::symlink_metadata(socket_path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !std::os::unix::fs::FileTypeExt::is_socket(&metadata.file_type()) {
        return Err(Error::UnsafeSocketPath(socket_path.to_path_buf()));
    }
    if UnixStream::connect(socket_path).await.is_ok() {
        return Err(Error::AlreadyRunning(socket_path.to_path_buf()));
    }
    tokio::fs::remove_file(socket_path).await?;
    Ok(())
}

fn set_permissions(path: &Path, mode: u32) -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    Ok(())
}

async fn handle_connection(
    stream: UnixStream,
    registry: RuntimeRegistry,
    request_shutdown: tokio::sync::watch::Sender<bool>,
    lifecycle: Arc<tokio::sync::Mutex<Lifecycle>>,
) -> Result<(), Error> {
    let mut transport = framed_json_with_max_frame(stream, MAX_FRAME_BYTES + 1);
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

    let (outbound, mut outgoing) = tokio::sync::mpsc::unbounded_channel::<ServerFrame>();
    let mut attachments = BTreeMap::<RunId, tokio::task::JoinHandle<()>>::new();
    loop {
        tokio::select! {
            Some(frame) = outgoing.recv() => send_server(&mut transport, &frame).await?,
            incoming = receive_client(&mut transport) => {
                let Some(frame) = incoming? else { break };
                match frame.message {
                    ClientMessage::ListRuns => {
                        send_response(&mut transport, frame.request_id, ServerResponse::Runs {
                            runs: registry.list().await,
                        }).await?;
                    }
                    ClientMessage::ProbeProviders => {
                        send_response(&mut transport, frame.request_id, ServerResponse::Providers {
                            providers: registry.probe_providers().await,
                        }).await?;
                    }
                    ClientMessage::ReadAccountUsage => {
                        send_response(&mut transport, frame.request_id, ServerResponse::AccountUsage {
                            providers: registry.account_usage().await,
                        }).await?;
                    }
                    ClientMessage::ShutdownIfIdle => {
                        let mut lifecycle = lifecycle.lock().await;
                        let active = registry.active_count().await;
                        if active > 0 {
                            send_error(
                                &mut transport,
                                frame.request_id,
                                ErrorCode::Conflict,
                                &format!("AgencyProxy has {active} active run(s)"),
                            ).await?;
                            continue;
                        }
                        lifecycle.stopping = true;
                        send_response(
                            &mut transport,
                            frame.request_id,
                            ServerResponse::Accepted,
                        ).await?;
                        let _ = request_shutdown.send(true);
                        return Ok(());
                    }
                    ClientMessage::Shutdown { mode } => {
                        let mut lifecycle = lifecycle.lock().await;
                        if lifecycle.stopping {
                            send_error(
                                &mut transport,
                                frame.request_id,
                                ErrorCode::Conflict,
                                "AgencyProxy is already stopping",
                            ).await?;
                            continue;
                        }
                        // This flag and StartRun share the same lock. Once the
                        // acknowledgement is visible, no new provider can pass
                        // the admission gate while existing runs drain.
                        lifecycle.stopping = true;
                        send_response(
                            &mut transport,
                            frame.request_id,
                            ServerResponse::Accepted,
                        ).await?;
                        drop(lifecycle);

                        tokio::spawn(async move {
                            if mode == ShutdownMode::Terminate {
                                registry.cancel_all().await;
                            }
                            registry.wait_until_idle().await;
                            let _ = request_shutdown.send(true);
                        });
                        return Ok(());
                    }
                    ClientMessage::StartRun { run_id, request, .. } => {
                        let lifecycle = lifecycle.lock().await;
                        if lifecycle.stopping {
                            send_error(
                                &mut transport,
                                frame.request_id,
                                ErrorCode::Conflict,
                                "AgencyProxy is stopping",
                            ).await?;
                            continue;
                        }
                        match registry.start(run_id, *request).await {
                            Ok(()) => send_response(&mut transport, frame.request_id, ServerResponse::Accepted).await?,
                            Err(error) => send_runtime_error(&mut transport, frame.request_id, error).await?,
                        }
                    }
                    ClientMessage::AttachRun { run_id, after_sequence } => {
                        match registry.attach(&run_id, after_sequence).await {
                            Ok(attachment) => {
                                send_response(&mut transport, frame.request_id, ServerResponse::Run {
                                    run: attachment.snapshot,
                                }).await?;
                                for event in &attachment.replay {
                                    send_server(&mut transport, &event_frame(event)).await?;
                                }
                                if let Some(previous) = attachments.remove(&run_id) { previous.abort(); }
                                let event_sender = outbound.clone();
                                let attached_registry = registry.clone();
                                let attached_run = run_id.clone();
                                let mut events = attachment.events;
                                let mut last_sequence = attachment.replay.last()
                                    .map_or(after_sequence, |event| event.sequence);
                                attachments.insert(run_id, tokio::spawn(async move {
                                    loop {
                                        match events.recv().await {
                                            Ok(event) => {
                                                if event.sequence <= last_sequence { continue; }
                                                last_sequence = event.sequence;
                                                if event_sender.send(event_frame(&event)).is_err() { return; }
                                            }
                                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                                let Ok(replay) = attached_registry.attach(&attached_run, last_sequence).await else { return; };
                                                for event in &replay.replay {
                                                    last_sequence = event.sequence;
                                                    if event_sender.send(event_frame(event)).is_err() { return; }
                                                }
                                                events = replay.events;
                                            }
                                            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                                        }
                                    }
                                }));
                            }
                            Err(error) => send_runtime_error(&mut transport, frame.request_id, error).await?,
                        }
                    }
                    ClientMessage::DetachRun { run_id } => {
                        if let Some(task) = attachments.remove(&run_id) { task.abort(); }
                        send_response(&mut transport, frame.request_id, ServerResponse::Accepted).await?;
                    }
                    ClientMessage::InjectMessage { run_id, body, .. } => {
                        match registry.inject(&run_id, &body).await {
                            Ok(()) => send_response(&mut transport, frame.request_id, ServerResponse::Accepted).await?,
                            Err(error) => send_runtime_error(&mut transport, frame.request_id, error).await?,
                        }
                    }
                    ClientMessage::CancelRun { run_id, .. } => {
                        match registry.cancel(&run_id).await {
                            Ok(()) => send_response(&mut transport, frame.request_id, ServerResponse::Accepted).await?,
                            Err(error) => send_runtime_error(&mut transport, frame.request_id, error).await?,
                        }
                    }
                    ClientMessage::InterruptSession { provider, session_id, binary, .. } => {
                        match registry.interrupt_session(&provider, &session_id, binary).await {
                            Ok(interrupted) => send_response(
                                &mut transport,
                                frame.request_id,
                                ServerResponse::SessionInterrupted { interrupted },
                            ).await?,
                            Err(error) => send_runtime_error(&mut transport, frame.request_id, error).await?,
                        }
                    }
                    ClientMessage::DecideApproval { run_id, approval_id, decision, .. } => {
                        match registry.decide(&run_id, &approval_id, decision).await {
                            Ok(()) => send_response(&mut transport, frame.request_id, ServerResponse::Accepted).await?,
                            Err(error) => send_runtime_error(&mut transport, frame.request_id, error).await?,
                        }
                    }
                    ClientMessage::AckEvents { run_id, through_sequence } => {
                        match registry.acknowledge(&run_id, through_sequence).await {
                            Ok(()) => send_response(&mut transport, frame.request_id, ServerResponse::Accepted).await?,
                            Err(error) => send_runtime_error(&mut transport, frame.request_id, error).await?,
                        }
                    }
                    ClientMessage::Hello { .. } => {
                        send_error(&mut transport, frame.request_id, ErrorCode::ProtocolViolation, "hello may only be sent once").await?;
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
