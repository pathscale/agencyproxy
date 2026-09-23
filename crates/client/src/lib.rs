pub mod broadcast;

use agency_proxy_protocol::{
    ClientFrame, ClientMessage, MAX_FRAME_BYTES, PROTOCOL_VERSION, ProtocolVersion, ServerFrame,
    ServerResponse,
};
// The neutral framing takes a `futures_io` stream, and `NagoyaStream` presents
// a nagoya socket as one directly, so there is no compat layer in between.
use endpoint_libs::libs::ws::{
    WireMessage,
    transport::{framed::framed_json_neutral_with_max_frame, nagoya::NagoyaStream},
};
use futures::{
    SinkExt, StreamExt,
    channel::{mpsc, oneshot},
    future::{Either, select},
};
use nagoya::net::TcpStream;
use nagoya::reactor::{Addr, Handle};
use std::{collections::BTreeMap, os::unix::ffi::OsStrExt, path::Path};
use thiserror::Error;

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
    // Unbounded where tokio's was bounded at 64. `futures`' bounded sender
    // needs `&mut self` to send, and cloning one per request would give every
    // clone its own guaranteed slot, which is no bound either. The drive task
    // is the only consumer and every request waits for its answer, so the
    // queue is bounded by the number of callers awaiting `request`.
    requests: mpsc::UnboundedSender<PendingRequest>,
    events: broadcast::Sender<ServerFrame>,
    version: ProtocolVersion,
}

impl Client {
    /// Connect to the proxy's Unix socket and negotiate the protocol.
    ///
    /// The connection is registered on `reactor`, and makes progress only
    /// while that reactor is polled: the caller owns it and must keep it
    /// running for as long as the client is in use. A threaded reactor
    /// (`Reactor::start`) is the usual choice, since nothing else here polls.
    pub async fn connect(
        socket_path: impl AsRef<Path>,
        reactor: &Handle,
    ) -> Result<Self, ClientError> {
        let address = Addr::path(socket_path.as_ref().as_os_str().as_bytes())
            .map_err(std::io::Error::from)?;
        let stream = TcpStream::connect(address, reactor)
            .await
            .map_err(std::io::Error::from)?;
        let transport =
            framed_json_neutral_with_max_frame(NagoyaStream::new(stream), MAX_FRAME_BYTES + 1);
        let (requests, request_rx) = mpsc::unbounded();
        let (events, _) = broadcast::channel(512);
        // Detached, as tokio's spawn was: the task ends when every `Client`
        // clone is gone and the request channel closes, or when the proxy does.
        drop(nagoya::spawn(drive(transport, request_rx, events.clone())));
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
            .unbounded_send(PendingRequest { message, response })
            .map_err(|_| ClientError::Closed)?;
        received.await.map_err(|_| ClientError::Closed)?
    }
}

async fn drive<T, E>(
    mut transport: T,
    mut requests: mpsc::UnboundedReceiver<PendingRequest>,
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
        // Both halves are cancel safe: the framed stream keeps a partial frame
        // in its own buffer, and an unreceived request stays in the channel.
        // The losing future is dropped at the end of this statement, which
        // releases `transport` for the send below.
        let step = match select(requests.next(), transport.next()).await {
            Either::Left((request, _)) => Either::Left(request),
            Either::Right((incoming, _)) => Either::Right(incoming),
        };
        match step {
            Either::Left(request) => {
                let Some(request) = request else {
                    break "request channel closed".to_string();
                };
                let request_id = next_request_id;
                next_request_id = next_request_id.wrapping_add(1).max(1);
                let frame = ClientFrame {
                    request_id,
                    message: request.message,
                };
                let encoded = match serde_json::to_string(&frame) {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        let _ = request
                            .response
                            .send(Err(ClientError::Protocol(error.to_string())));
                        continue;
                    }
                };
                if let Err(error) = transport.send(WireMessage::Text(encoded.into())).await {
                    let detail = error.to_string();
                    let _ = request
                        .response
                        .send(Err(ClientError::Transport(detail.clone())));
                    break detail;
                }
                pending.insert(request_id, request.response);
            }
            Either::Right(incoming) => {
                let Some(incoming) = incoming else {
                    break "proxy closed the connection".into();
                };
                let message = match incoming {
                    Ok(message) => message,
                    Err(error) => break error.to_string(),
                };
                if message.is_close() {
                    break "proxy closed the connection".into();
                }
                let Some(text) = message.as_text() else {
                    continue;
                };
                let frame: ServerFrame = match serde_json::from_str(text) {
                    Ok(frame) => frame,
                    Err(error) => break error.to_string(),
                };
                match frame {
                    ServerFrame::Response {
                        request_id,
                        response,
                    } => {
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
