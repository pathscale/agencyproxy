//! Local AgencyProxy server foundation.

mod framing;

use agency_proxy_protocol::{
    Capability, ClientFrame, ClientMessage, ErrorCode, PROTOCOL_VERSION, RunId, RunSnapshot,
    ServerFrame, ServerResponse,
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;
use tokio::{
    io::BufReader,
    net::{UnixListener, UnixStream},
    sync::RwLock,
};

pub use framing::{read_frame, write_frame};

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid JSON frame: {0}")]
    Json(#[from] serde_json::Error),
    #[error("frame exceeds the protocol size limit")]
    FrameTooLarge,
    #[error("connection ended in the middle of a frame")]
    TruncatedFrame,
    #[error("another AgencyProxy is already listening at {0}")]
    AlreadyRunning(PathBuf),
    #[error("refusing to replace a non-socket path at {0}")]
    UnsafeSocketPath(PathBuf),
}

#[derive(Clone, Debug, Default)]
struct Registry(Arc<RwLock<BTreeMap<RunId, RunSnapshot>>>);

#[derive(Debug)]
pub struct ProxyServer {
    listener: UnixListener,
    registry: Registry,
    socket_path: PathBuf,
}

impl ProxyServer {
    pub async fn bind(socket_path: impl AsRef<Path>) -> Result<Self, Error> {
        let socket_path = socket_path.as_ref().to_path_buf();
        prepare_socket_path(&socket_path).await?;
        let listener = UnixListener::bind(&socket_path)?;
        set_permissions(&socket_path, 0o600)?;
        Ok(Self {
            listener,
            registry: Registry::default(),
            socket_path,
        })
    }

    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub async fn serve(self) -> Result<(), Error> {
        loop {
            let (stream, _) = self.listener.accept().await?;
            let registry = self.registry.clone();
            tokio::spawn(async move {
                if let Err(error) = handle_connection(stream, registry).await {
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

async fn handle_connection(stream: UnixStream, registry: Registry) -> Result<(), Error> {
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    let Some(first) = read_frame::<ClientFrame>(&mut read).await? else {
        return Ok(());
    };
    let ClientMessage::Hello {
        client_name: _,
        version,
    } = first.message
    else {
        return send_error(
            &mut write,
            first.request_id,
            ErrorCode::ProtocolViolation,
            "the first frame must be hello",
        )
        .await;
    };
    let Some(version) = PROTOCOL_VERSION.negotiate(version) else {
        return send_error(
            &mut write,
            first.request_id,
            ErrorCode::IncompatibleVersion,
            "the protocol major version is incompatible",
        )
        .await;
    };
    send_response(
        &mut write,
        first.request_id,
        ServerResponse::Hello {
            server_name: "agency-proxy".into(),
            version,
            capabilities: vec![
                Capability::EventReplay,
                Capability::LiveInjection,
                Capability::Approvals,
                Capability::Cancellation,
            ],
        },
    )
    .await?;

    while let Some(frame) = read_frame::<ClientFrame>(&mut read).await? {
        match frame.message {
            ClientMessage::ListRuns => {
                let runs = registry.0.read().await.values().cloned().collect();
                send_response(&mut write, frame.request_id, ServerResponse::Runs { runs }).await?;
            }
            ClientMessage::AttachRun { run_id, .. } => {
                let run = registry.0.read().await.get(&run_id).cloned();
                match run {
                    Some(run) => {
                        send_response(&mut write, frame.request_id, ServerResponse::Run { run })
                            .await?;
                    }
                    None => {
                        send_error(
                            &mut write,
                            frame.request_id,
                            ErrorCode::NotFound,
                            "run does not exist",
                        )
                        .await?;
                    }
                }
            }
            ClientMessage::Hello { .. } => {
                send_error(
                    &mut write,
                    frame.request_id,
                    ErrorCode::ProtocolViolation,
                    "hello may only be sent once",
                )
                .await?;
            }
            _ => {
                send_error(
                    &mut write,
                    frame.request_id,
                    ErrorCode::NotImplemented,
                    "command is part of the protocol but not implemented in this foundation",
                )
                .await?;
            }
        }
    }
    Ok(())
}

async fn send_response(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    request_id: u64,
    response: ServerResponse,
) -> Result<(), Error> {
    write_frame(
        writer,
        &ServerFrame::Response {
            request_id,
            response,
        },
    )
    .await
}

async fn send_error(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    request_id: u64,
    code: ErrorCode,
    message: &str,
) -> Result<(), Error> {
    send_response(
        writer,
        request_id,
        ServerResponse::Error {
            code,
            message: message.into(),
        },
    )
    .await
}
