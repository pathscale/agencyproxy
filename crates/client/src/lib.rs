use agency_proxy_protocol::{
    ClientFrame, ClientMessage, MAX_FRAME_BYTES, PROTOCOL_VERSION, ProtocolVersion, ServerFrame,
    ServerResponse,
};
use endpoint_libs::libs::ws::{WireMessage, transport::framed::framed_json_with_max_frame};
use futures::{SinkExt, StreamExt};
use std::{collections::BTreeMap, path::Path};
use thiserror::Error;
use tokio::{
    net::UnixStream,
    sync::{broadcast, mpsc, oneshot},
};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("proxy transport failed: {0}")]
    Transport(String),
    #[error("proxy protocol failed: {0}")]
    Protocol(String),
    #[error("proxy client task stopped")]
    Closed,
}

struct PendingRequest {
    message: ClientMessage,
    response: oneshot::Sender<Result<ServerResponse, ClientError>>,
}

#[derive(Clone, Debug)]
pub struct Client {
    requests: mpsc::Sender<PendingRequest>,
    events: broadcast::Sender<ServerFrame>,
    version: ProtocolVersion,
}

impl Client {
    pub async fn connect(socket_path: impl AsRef<Path>) -> Result<Self, ClientError> {
        let stream = UnixStream::connect(socket_path).await?;
        let transport = framed_json_with_max_frame(stream, MAX_FRAME_BYTES + 1);
        let (requests, request_rx) = mpsc::channel(64);
        let (events, _) = broadcast::channel(512);
        tokio::spawn(drive(transport, request_rx, events.clone()));
        let mut client = Self {
            requests,
            events,
            version: PROTOCOL_VERSION,
        };
        match client
            .request(ClientMessage::Hello {
                client_name: "agency-proxy-client".into(),
                version: PROTOCOL_VERSION,
            })
            .await?
        {
            ServerResponse::Hello { version, .. } if version.major == PROTOCOL_VERSION.major => {
                client.version = version;
                Ok(client)
            }
            ServerResponse::Error { message, .. } => Err(ClientError::Protocol(message)),
            response => Err(ClientError::Protocol(format!(
                "unexpected hello response: {response:?}"
            ))),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ServerFrame> {
        self.events.subscribe()
    }

    #[must_use]
    pub const fn version(&self) -> ProtocolVersion {
        self.version
    }

    pub async fn request(&self, message: ClientMessage) -> Result<ServerResponse, ClientError> {
        let (response, received) = oneshot::channel();
        self.requests
            .send(PendingRequest { message, response })
            .await
            .map_err(|_| ClientError::Closed)?;
        received.await.map_err(|_| ClientError::Closed)?
    }
}

async fn drive<T, E>(
    mut transport: T,
    mut requests: mpsc::Receiver<PendingRequest>,
    events: broadcast::Sender<ServerFrame>,
) where
    T: futures::Sink<WireMessage, Error = E>
        + futures::Stream<Item = Result<WireMessage, E>>
        + Unpin,
    E: std::error::Error,
{
    let mut next_request_id = 1u64;
    let mut pending = BTreeMap::<u64, oneshot::Sender<Result<ServerResponse, ClientError>>>::new();
    let stopped = loop {
        tokio::select! {
            request = requests.recv() => {
                let Some(request) = request else { break "request channel closed".to_string() };
                let request_id = next_request_id;
                next_request_id = next_request_id.wrapping_add(1).max(1);
                let frame = ClientFrame { request_id, message: request.message };
                let encoded = match serde_json::to_string(&frame) {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        let _ = request.response.send(Err(ClientError::Protocol(error.to_string())));
                        continue;
                    }
                };
                if let Err(error) = transport.send(WireMessage::Text(encoded.into())).await {
                    let detail = error.to_string();
                    let _ = request.response.send(Err(ClientError::Transport(detail.clone())));
                    break detail;
                }
                pending.insert(request_id, request.response);
            }
            incoming = transport.next() => {
                let Some(incoming) = incoming else { break "proxy closed the connection".into() };
                let message = match incoming {
                    Ok(message) => message,
                    Err(error) => break error.to_string(),
                };
                if message.is_close() { break "proxy closed the connection".into(); }
                let Some(text) = message.as_text() else { continue };
                let frame: ServerFrame = match serde_json::from_str(text) {
                    Ok(frame) => frame,
                    Err(error) => break error.to_string(),
                };
                match frame {
                    ServerFrame::Response { request_id, response } => {
                        if let Some(waiter) = pending.remove(&request_id) {
                            let _ = waiter.send(Ok(response));
                        }
                    }
                    event @ ServerFrame::Event { .. } => {
                        let _ = events.send(event);
                    }
                }
            }
        }
    };
    for (_, waiter) in pending {
        let _ = waiter.send(Err(ClientError::Transport(stopped.clone())));
    }
}
