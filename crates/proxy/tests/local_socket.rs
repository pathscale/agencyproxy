use agency_proxy::ProxyServer;
use agency_proxy_client::Client;
use agency_proxy_protocol::{
    ClientFrame, ClientMessage, ErrorCode, MAX_FRAME_BYTES, RunEvent, RunId, RunRequest,
    ServerFrame, ServerResponse, ShutdownMode,
};
use endpoint_libs::libs::ws::{WireMessage, transport::framed::framed_json_with_max_frame};
use futures::{SinkExt, StreamExt};
use std::{collections::BTreeMap, os::unix::fs::PermissionsExt, time::Duration};
use tempfile::tempdir;
use tokio::net::UnixStream;

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
    let client = Client::connect(&socket)
        .await
        .expect("client should connect and negotiate");
    assert_eq!(
        client
            .request(ClientMessage::ListRuns)
            .await
            .expect("list should succeed"),
        ServerResponse::Runs { runs: Vec::new() }
    );
    task.abort();
}

#[tokio::test]
async fn idle_daemon_accepts_a_graceful_shutdown() {
    let dir = tempdir().expect("temp dir should exist");
    let socket = dir.path().join("agent.sock");
    let server = ProxyServer::bind(&socket)
        .await
        .expect("server should bind");
    let task = tokio::spawn(server.serve());
    let client = Client::connect(&socket)
        .await
        .expect("client should connect");

    assert_eq!(
        client
            .request(ClientMessage::ShutdownIfIdle)
            .await
            .expect("shutdown should answer"),
        ServerResponse::Accepted
    );
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("server should stop promptly")
        .expect("server task should join")
        .expect("server should stop cleanly");
    assert!(!socket.exists(), "graceful shutdown removes its socket");
}

#[tokio::test]
async fn shutdown_refuses_to_interrupt_an_active_run() {
    let dir = tempdir().expect("temp dir should exist");
    let binary = dir.path().join("slow-claude");
    std::fs::write(
        &binary,
        r#"#!/bin/sh
sleep 2
printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"proxy-session","usage":{"input_tokens":1,"output_tokens":1}}'
"#,
    )
    .expect("fake provider should write");
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700))
        .expect("fake provider should be executable");

    let socket = dir.path().join("agent.sock");
    let server = ProxyServer::bind(&socket)
        .await
        .expect("server should bind");
    let task = tokio::spawn(server.serve());
    let client = Client::connect(&socket)
        .await
        .expect("client should connect");
    let run_id = RunId("active".into());
    assert_eq!(
        client
            .request(ClientMessage::StartRun {
                run_id: run_id.clone(),
                request: Box::new(RunRequest {
                    provider: "claude".into(),
                    model: String::new(),
                    prompt: "test".into(),
                    is_command: false,
                    system: None,
                    permission: "read_only".into(),
                    effort: None,
                    extra_thinking: None,
                    approvals: false,
                    interactive: false,
                    workspace_roots: vec![dir.path().to_string_lossy().into_owned()],
                    resume_session_id: None,
                    binary: Some(binary.to_string_lossy().into_owned()),
                    environment: BTreeMap::new(),
                    unchecked_args: Vec::new(),
                    metadata: BTreeMap::new(),
                }),
                idempotency_key: "start-active".into(),
            })
            .await
            .expect("start should answer"),
        ServerResponse::Accepted
    );

    assert!(matches!(
        client
            .request(ClientMessage::ShutdownIfIdle)
            .await
            .expect("shutdown refusal should answer"),
        ServerResponse::Error {
            code: ErrorCode::Conflict,
            message,
        } if message.contains("1 active run")
    ));
    assert!(socket.exists(), "refused shutdown keeps serving");

    task.abort();
}

#[tokio::test]
async fn cancel_stops_an_uncooperative_provider_and_publishes_a_terminal_event() {
    let dir = tempdir().expect("temp dir should exist");
    let binary = dir.path().join("stubborn-claude");
    std::fs::write(&binary, "#!/bin/sh\ntrap '' TERM\nsleep 30\n")
        .expect("fake provider should write");
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700))
        .expect("fake provider should be executable");

    let socket = dir.path().join("agent.sock");
    let server = ProxyServer::bind(&socket)
        .await
        .expect("server should bind");
    let task = tokio::spawn(server.serve());
    let client = Client::connect(&socket)
        .await
        .expect("client should connect");
    let mut events = client.subscribe();
    let run_id = RunId("stubborn".into());
    assert_eq!(
        client
            .request(ClientMessage::StartRun {
                run_id: run_id.clone(),
                request: Box::new(RunRequest {
                    provider: "claude".into(),
                    model: String::new(),
                    prompt: "test".into(),
                    is_command: false,
                    system: None,
                    permission: "read_only".into(),
                    effort: None,
                    extra_thinking: None,
                    approvals: false,
                    interactive: false,
                    workspace_roots: vec![dir.path().to_string_lossy().into_owned()],
                    resume_session_id: None,
                    binary: Some(binary.to_string_lossy().into_owned()),
                    environment: BTreeMap::new(),
                    unchecked_args: Vec::new(),
                    metadata: BTreeMap::new(),
                }),
                idempotency_key: "start-stubborn".into(),
            })
            .await
            .expect("start should answer"),
        ServerResponse::Accepted
    );
    assert!(matches!(
        client
            .request(ClientMessage::AttachRun {
                run_id: run_id.clone(),
                after_sequence: 0,
            })
            .await
            .expect("attach should answer"),
        ServerResponse::Run { .. }
    ));

    assert_eq!(
        client
            .request(ClientMessage::CancelRun {
                run_id: run_id.clone(),
                idempotency_key: "cancel-stubborn".into(),
            })
            .await
            .expect("cancel should answer"),
        ServerResponse::Accepted
    );

    let terminal = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let ServerFrame::Event {
                run_id: event_run_id,
                event: RunEvent::Failed(message),
                ..
            } = events.recv().await.expect("event stream should stay open")
                && event_run_id == run_id
            {
                break message;
            }
        }
    })
    .await
    .expect("cancel should publish a terminal event promptly");
    assert_eq!(terminal, "the run was canceled");

    assert!(matches!(
        client
            .request(ClientMessage::ListRuns)
            .await
            .expect("list should answer"),
        ServerResponse::Runs { runs }
            if runs.iter().any(|run| run.run_id == run_id && run.state == agency_proxy_protocol::RunState::Canceled)
    ));
    task.abort();
}

#[tokio::test]
async fn draining_shutdown_rejects_new_runs_and_waits_for_the_active_run() {
    let dir = tempdir().expect("temp dir should exist");
    let binary = dir.path().join("slow-claude");
    let release = dir.path().join("release-provider");
    std::fs::write(
        &binary,
        r#"#!/bin/sh
while [ ! -f "$AGENCY_PROXY_TEST_RELEASE" ]; do sleep 0.01; done
printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"proxy-session","usage":{"input_tokens":1,"output_tokens":1}}'
"#,
    )
    .expect("fake provider should write");
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700))
        .expect("fake provider should be executable");

    let socket = dir.path().join("agent.sock");
    let server = ProxyServer::bind(&socket)
        .await
        .expect("server should bind");
    let task = tokio::spawn(server.serve());
    let client = Client::connect(&socket)
        .await
        .expect("client should connect");
    let request = RunRequest {
        provider: "claude".into(),
        model: String::new(),
        prompt: "test".into(),
        is_command: false,
        system: None,
        permission: "read_only".into(),
        effort: None,
        extra_thinking: None,
        approvals: false,
        interactive: false,
        workspace_roots: vec![dir.path().to_string_lossy().into_owned()],
        resume_session_id: None,
        binary: Some(binary.to_string_lossy().into_owned()),
        environment: BTreeMap::from([(
            "AGENCY_PROXY_TEST_RELEASE".into(),
            release.to_string_lossy().into_owned(),
        )]),
        unchecked_args: Vec::new(),
        metadata: BTreeMap::new(),
    };
    assert_eq!(
        client
            .request(ClientMessage::StartRun {
                run_id: RunId("active".into()),
                request: Box::new(request.clone()),
                idempotency_key: "start-active".into(),
            })
            .await
            .expect("start should answer"),
        ServerResponse::Accepted
    );
    assert_eq!(
        client
            .request(ClientMessage::Shutdown {
                mode: ShutdownMode::Drain,
            })
            .await
            .expect("drain should answer"),
        ServerResponse::Accepted
    );
    let late_client = Client::connect(&socket)
        .await
        .expect("daemon should keep serving while runs drain");
    assert!(
        matches!(
            late_client
                .request(ClientMessage::StartRun {
                    run_id: RunId("too-late".into()),
                    request: Box::new(request),
                    idempotency_key: "start-too-late".into(),
                })
                .await
                .expect("rejected start should answer"),
            ServerResponse::Error {
                code: ErrorCode::Conflict,
                message,
            } if message.contains("stopping")
        ),
        "draining closes admission before acknowledging"
    );
    std::fs::write(&release, "release").expect("provider release should write");
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("server should stop after the run drains")
        .expect("server task should join")
        .expect("server should stop cleanly");
}

#[tokio::test]
async fn terminating_shutdown_stops_active_runs_before_exit() {
    let dir = tempdir().expect("temp dir should exist");
    let binary = dir.path().join("slow-claude");
    std::fs::write(&binary, "#!/bin/sh\nsleep 30\n").expect("fake provider should write");
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700))
        .expect("fake provider should be executable");

    let socket = dir.path().join("agent.sock");
    let server = ProxyServer::bind(&socket)
        .await
        .expect("server should bind");
    let task = tokio::spawn(server.serve());
    let client = Client::connect(&socket)
        .await
        .expect("client should connect");
    assert_eq!(
        client
            .request(ClientMessage::StartRun {
                run_id: RunId("active".into()),
                request: Box::new(RunRequest {
                    provider: "claude".into(),
                    model: String::new(),
                    prompt: "test".into(),
                    is_command: false,
                    system: None,
                    permission: "read_only".into(),
                    effort: None,
                    extra_thinking: None,
                    approvals: false,
                    interactive: false,
                    workspace_roots: vec![dir.path().to_string_lossy().into_owned()],
                    resume_session_id: None,
                    binary: Some(binary.to_string_lossy().into_owned()),
                    environment: BTreeMap::new(),
                    unchecked_args: Vec::new(),
                    metadata: BTreeMap::new(),
                }),
                idempotency_key: "start-active".into(),
            })
            .await
            .expect("start should answer"),
        ServerResponse::Accepted
    );
    assert_eq!(
        client
            .request(ClientMessage::Shutdown {
                mode: ShutdownMode::Terminate,
            })
            .await
            .expect("terminate should answer"),
        ServerResponse::Accepted
    );
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("termination should not wait for natural completion")
        .expect("server task should join")
        .expect("server should stop cleanly");
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
    let mut transport = framed_json_with_max_frame(stream, MAX_FRAME_BYTES + 1);
    let frame = ClientFrame {
        request_id: 9,
        message: ClientMessage::ListRuns,
    };
    transport
        .send(WireMessage::Text(
            serde_json::to_string(&frame)
                .expect("frame should encode")
                .into(),
        ))
        .await
        .expect("frame should write");
    let response = transport
        .next()
        .await
        .expect("response should exist")
        .expect("response should read");
    let response: ServerFrame = serde_json::from_str(
        response
            .as_text()
            .expect("response should be a JSON text frame"),
    )
    .expect("response should decode");
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

#[tokio::test]
async fn provider_run_survives_a_client_disconnect_and_replays_on_attach() {
    let dir = tempdir().expect("temp dir should exist");
    let binary = dir.path().join("fake-claude");
    std::fs::write(
        &binary,
        r#"#!/bin/sh
printf '%s\n' '{"type":"system","subtype":"init","session_id":"proxy-session","model":"fake-model"}'
sleep 1
printf '%s\n' '{"type":"assistant","session_id":"proxy-session","message":{"content":[{"type":"text","text":"survived restart"}]}}'
printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"survived restart","session_id":"proxy-session","usage":{"input_tokens":1,"output_tokens":2}}'
"#,
    )
    .expect("fake provider should write");
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700))
        .expect("fake provider should be executable");

    let socket = dir.path().join("agent.sock");
    let server = ProxyServer::bind(&socket)
        .await
        .expect("server should bind");
    let task = tokio::spawn(server.serve());

    let first = Client::connect(&socket)
        .await
        .expect("first client should connect");
    let run_id = RunId("survivor".into());
    assert!(matches!(
        first
            .request(ClientMessage::StartRun {
                run_id: run_id.clone(),
                request: Box::new(RunRequest {
                    provider: "claude".into(),
                    model: String::new(),
                    prompt: "test".into(),
                    is_command: false,
                    system: None,
                    permission: "read_only".into(),
                    effort: None,
                    extra_thinking: None,
                    approvals: false,
                    interactive: false,
                    workspace_roots: vec![dir.path().to_string_lossy().into_owned()],
                    resume_session_id: None,
                    binary: Some(binary.to_string_lossy().into_owned()),
                    environment: BTreeMap::new(),
                    unchecked_args: Vec::new(),
                    metadata: BTreeMap::new(),
                }),
                idempotency_key: "start-once".into(),
            })
            .await
            .expect("start should succeed"),
        ServerResponse::Accepted
    ));
    drop(first);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let second = Client::connect(&socket)
        .await
        .expect("replacement client should connect");
    let mut events = second.subscribe();
    assert!(matches!(
        second
            .request(ClientMessage::AttachRun {
                run_id: run_id.clone(),
                after_sequence: 0,
            })
            .await
            .expect("attach should succeed"),
        ServerResponse::Run { .. }
    ));

    let mut saw_text = false;
    let mut saw_finished = false;
    let mut latest = 0;
    for _ in 0..8 {
        let frame = tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("proxy should replay promptly")
            .expect("replay frame should exist");
        if let ServerFrame::Event {
            sequence, event, ..
        } = frame
        {
            latest = sequence;
            match event {
                RunEvent::Text(text) if text == "survived restart" => saw_text = true,
                RunEvent::Finished(_) => {
                    saw_finished = true;
                    break;
                }
                _ => {}
            }
        }
    }
    assert!(saw_text, "replacement client receives the missed text");
    assert!(
        saw_finished,
        "replacement client receives the terminal outcome"
    );
    assert!(latest > 0);
    task.abort();
}
