use agency_proxy::{ProxyServer, read_frame, write_frame};
use agency_proxy_protocol::{
    ClientFrame, ClientMessage, ErrorCode, PROTOCOL_VERSION, ServerFrame, ServerResponse,
};
use std::os::unix::fs::PermissionsExt;
use tempfile::tempdir;
use tokio::{io::BufReader, net::UnixStream};

#[tokio::test]
async fn hello_then_list_runs_uses_the_versioned_local_protocol() {
    let dir = tempdir().expect("temp dir should exist");
    let socket = dir.path().join("runtime/agent.sock");
    let server = ProxyServer::bind(&socket)
        .await
        .expect("server should bind");
    assert_eq!(
        std::fs::metadata(socket.parent().expect("socket has parent"))
            .expect("runtime dir should exist")
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(&socket)
            .expect("socket should exist")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let task = tokio::spawn(server.serve());
    let stream = UnixStream::connect(&socket)
        .await
        .expect("client should connect");
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);

    write_frame(
        &mut write,
        &ClientFrame {
            request_id: 1,
            message: ClientMessage::Hello {
                client_name: "test-client".into(),
                version: PROTOCOL_VERSION,
            },
        },
    )
    .await
    .expect("hello should write");
    let hello: ServerFrame = read_frame(&mut read)
        .await
        .expect("hello should read")
        .expect("hello response should exist");
    assert!(matches!(
        hello,
        ServerFrame::Response {
            request_id: 1,
            response: ServerResponse::Hello {
                version: PROTOCOL_VERSION,
                ..
            }
        }
    ));

    write_frame(
        &mut write,
        &ClientFrame {
            request_id: 2,
            message: ClientMessage::ListRuns,
        },
    )
    .await
    .expect("list should write");
    let runs: ServerFrame = read_frame(&mut read)
        .await
        .expect("runs should read")
        .expect("runs response should exist");
    assert_eq!(
        runs,
        ServerFrame::Response {
            request_id: 2,
            response: ServerResponse::Runs { runs: Vec::new() },
        }
    );
    task.abort();
}

#[tokio::test]
async fn commands_before_hello_are_rejected() {
    let dir = tempdir().expect("temp dir should exist");
    let socket = dir.path().join("agent.sock");
    let server = ProxyServer::bind(&socket)
        .await
        .expect("server should bind");
    let task = tokio::spawn(server.serve());
    let stream = UnixStream::connect(&socket)
        .await
        .expect("client should connect");
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    write_frame(
        &mut write,
        &ClientFrame {
            request_id: 9,
            message: ClientMessage::ListRuns,
        },
    )
    .await
    .expect("frame should write");
    let response: ServerFrame = read_frame(&mut read)
        .await
        .expect("response should read")
        .expect("response should exist");
    assert!(matches!(
        response,
        ServerFrame::Response {
            request_id: 9,
            response: ServerResponse::Error {
                code: ErrorCode::ProtocolViolation,
                ..
            }
        }
    ));
    task.abort();
}

#[tokio::test]
async fn refuses_to_replace_a_regular_file_at_the_socket_path() {
    let dir = tempdir().expect("temp dir should exist");
    let socket = dir.path().join("agent.sock");
    std::fs::write(&socket, b"owner data").expect("fixture should write");
    let error = ProxyServer::bind(&socket)
        .await
        .expect_err("regular file must be preserved");
    assert!(matches!(error, agency_proxy::Error::UnsafeSocketPath(path) if path == socket));
    assert_eq!(
        std::fs::read(&socket).expect("fixture should remain"),
        b"owner data"
    );
}
