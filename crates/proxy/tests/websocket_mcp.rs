use agency_proxy::{RuntimeRegistry, WebSocketConfig, serve_websocket};
// endpoint-libs' own WebSocket client: nago-wss over a nagoya socket, the same
// stack the server speaks. It replaced tokio-tungstenite, which pulled tokio
// back into the test graph.
use endpoint_libs::libs::ws::{WsClient, WsClientBuilder};
use nagoya::reactor::{Handle, Reactor};
use serde_json::{Value, json};
use std::{
    net::TcpListener,
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use tempfile::tempdir;

const KEY: &str = "0123456789abcdef0123456789abcdef";
const ORIGIN: &str = "https://agencyzero.example";

fn unused_loopback_address() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral listener should bind");
    listener
        .local_addr()
        .expect("ephemeral listener should have an address")
}

// Every test starts its own reactor and keeps it for the whole test: the
// WebSocket client and the registry's provider processes are registered on it.
// `nagoya::block_on` polls no reactor, so a threaded one is what keeps those
// sockets moving while the test awaits. The server under test runs its own
// reactor on its own thread.

/// Open a WebSocket to the proxy, offering the authentication subprotocol when
/// `authenticated`. Always sends the allowed `Origin`.
async fn connect(
    address: std::net::SocketAddr,
    authenticated: bool,
    reactor: &Handle,
) -> eyre::Result<WsClient> {
    let mut builder = WsClientBuilder::new().header("Origin", ORIGIN);
    if authenticated {
        builder = builder.protocol_header(format!("agency-proxy.{KEY}"));
    }
    let (client, _) = builder.build(&format!("ws://{address}"), reactor).await?;
    Ok(client)
}

/// Connect, retrying while the server is still binding.
async fn connect_when_ready(
    address: std::net::SocketAddr,
    authenticated: bool,
    reactor: &Handle,
) -> WsClient {
    loop {
        match connect(address, authenticated, reactor).await {
            Ok(client) => break client,
            Err(_) => nagoya::sleep(Duration::from_millis(20)).await,
        }
    }
}

async fn send_json(socket: &mut WsClient, value: Value) -> eyre::Result<()> {
    socket.send_raw(value.to_string().as_bytes()).await
}

async fn receive_json(socket: &mut WsClient) -> Value {
    loop {
        // Full workspace runs can contend with several process-backed transport
        // tests on CI. Keep the assertion bounded while allowing scheduler
        // headroom beyond the fake provider's intentional one-second delay.
        let frame = nagoya::timeout(Duration::from_secs(5), socket.recv_raw())
            .await
            .expect("server should answer promptly");
        match frame {
            Ok(value) => return value,
            // `recv_raw` has already answered a ping by the time it reports
            // one, and it reports any non-text frame as an error. Skipping
            // control frames is what the tungstenite loop did explicitly.
            Err(error)
                if error.to_string().ends_with("got ping")
                    || error.to_string().ends_with("got pong") => {}
            Err(error) => panic!("server should send a JSON text frame: {error:#}"),
        }
    }
}

#[test]
fn authenticated_websocket_serves_the_minimal_mcp_surface() {
    let reactor = Reactor::start().expect("test reactor should start");
    let handle = reactor.handle();
    nagoya::block_on(async {
        let address = unused_loopback_address();
        let task = nagoya::spawn(serve_websocket(
            RuntimeRegistry::new(handle.clone()),
            WebSocketConfig {
                address,
                authentication_key: KEY.into(),
                allowed_origins: vec![ORIGIN.into()],
            },
            // Each test's server lives until the test process exits. None of them
            // takes a signal, which is what lets several run in one process.
            std::future::pending(),
        ));

        let mut socket = connect_when_ready(address, true, &handle).await;
        send_json(&mut socket, json!({"method":1,"seq":1,"params":{}}))
            .await
            .expect("legacy frame should send");
        let rejected = receive_json(&mut socket).await;
        assert_eq!(rejected["error"]["code"], -32600);

        send_json(
            &mut socket,
            json!({
                "jsonrpc":"2.0",
                "id":1,
                "method":"initialize",
                "params":{
                    "protocolVersion":"2025-06-18",
                    "capabilities":{},
                    "clientInfo":{"name":"agencyzero-test","version":"0"}
                }
            }),
        )
        .await
        .expect("initialize should send");
        let initialized = receive_json(&mut socket).await;
        assert_eq!(initialized["result"]["serverInfo"]["name"], "agency-proxy");

        send_json(
            &mut socket,
            json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        )
        .await
        .expect("initialized notification should send");
        send_json(
            &mut socket,
            json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
        )
        .await
        .expect("tools/list should send");
        let tools = receive_json(&mut socket).await;
        let names = tools["result"]["tools"]
            .as_array()
            .expect("tools should be an array")
            .iter()
            .map(|tool| tool["name"].as_str().expect("tool should have a name"))
            .collect::<Vec<_>>();
        assert_eq!(names.len(), 11);
        assert!(names.contains(&"list_runs"));
        assert!(names.contains(&"start_run"));
        assert!(names.contains(&"subscribe_run"));
        assert!(names.contains(&"unsubscribe_run"));
        assert!(!names.iter().any(|name| name.contains("shutdown")));

        send_json(
            &mut socket,
            json!({
                "jsonrpc":"2.0",
                "id":3,
                "method":"tools/call",
                "params":{"name":"list_runs","arguments":{}}
            }),
        )
        .await
        .expect("list_runs should send");
        let runs = receive_json(&mut socket).await;
        assert_eq!(runs["result"]["isError"], false);
        assert_eq!(runs["result"]["structuredContent"]["runs"], json!([]));

        task.cancel();
    });
}

#[test]
fn websocket_rejects_clients_without_the_configured_key() {
    let reactor = Reactor::start().expect("test reactor should start");
    let handle = reactor.handle();
    nagoya::block_on(async {
        let address = unused_loopback_address();
        let task = nagoya::spawn(serve_websocket(
            RuntimeRegistry::new(handle.clone()),
            WebSocketConfig {
                address,
                authentication_key: KEY.into(),
                allowed_origins: vec![ORIGIN.into()],
            },
            // Each test's server lives until the test process exits. None of them
            // takes a signal, which is what lets several run in one process.
            std::future::pending(),
        ));
        let mut socket = connect_when_ready(address, false, &handle).await;
        send_json(
            &mut socket,
            json!({
                "jsonrpc":"2.0",
                "id":1,
                "method":"initialize",
                "params":{
                    "protocolVersion":"2025-06-18",
                    "capabilities":{},
                    "clientInfo":{"name":"unauthenticated","version":"0"}
                }
            }),
        )
        .await
        .ok();
        // `recv_raw` succeeds only for a JSON text frame, which is exactly the
        // MCP response an unauthenticated client must not get. A close, a
        // dropped connection or a control frame is an error here.
        let closed = nagoya::timeout(Duration::from_secs(2), socket.recv_raw())
            .await
            .expect("unauthenticated connection should close promptly");
        assert!(
            closed.is_err(),
            "unauthenticated client must not receive an MCP response"
        );
        task.cancel();
    });
}

#[test]
fn release_cli_starts_the_configured_websocket_transport() {
    let reactor = Reactor::start().expect("test reactor should start");
    let handle = reactor.handle();
    nagoya::block_on(async {
        let address = unused_loopback_address();
        let dir = tempdir().expect("temporary config directory should exist");
        let config_path = dir.path().join("agency-proxy.json");
        std::fs::write(
            &config_path,
            json!({
                "connection": {
                    "type": "web_socket",
                    "address": address.to_string(),
                    "authenticationKey": KEY,
                    "allowedOrigins": [ORIGIN]
                }
            })
            .to_string(),
        )
        .expect("config should write");
        let mut child = Command::new(env!("CARGO_BIN_EXE_agency-proxy"))
            .arg("--config")
            .arg(&config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("AgencyProxy should launch");

        let mut socket = None;
        for _ in 0..100 {
            if let Ok(connected) = connect(address, true, &handle).await {
                socket = Some(connected);
                break;
            }
            assert!(
                child
                    .try_wait()
                    .expect("child status should be readable")
                    .is_none(),
                "configured AgencyProxy exited before accepting WebSocket connections"
            );
            nagoya::sleep(Duration::from_millis(20)).await;
        }
        let mut socket = socket.expect("configured WebSocket should become reachable");
        send_json(
            &mut socket,
            json!({
                "jsonrpc":"2.0",
                "id":1,
                "method":"initialize",
                "params":{
                    "protocolVersion":"2025-06-18",
                    "capabilities":{},
                    "clientInfo":{"name":"agencyzero-cli-test","version":"0"}
                }
            }),
        )
        .await
        .expect("initialize should send");
        let initialized = receive_json(&mut socket).await;
        assert_eq!(initialized["result"]["serverInfo"]["name"], "agency-proxy");

        child.kill().expect("test proxy should stop");
        child.wait().expect("test proxy should be reaped");
    });
}

#[cfg(unix)]
#[test]
fn term_stops_websocket_admission_then_drains_owned_runs() {
    use std::os::unix::fs::PermissionsExt;

    let reactor = Reactor::start().expect("test reactor should start");
    let handle = reactor.handle();
    nagoya::block_on(async {
        let address = unused_loopback_address();
        let dir = tempdir().expect("temporary config directory should exist");
        let provider = dir.path().join("slow-claude");
        std::fs::write(
            &provider,
            r#"#!/bin/sh
sleep 2
printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"proxy-session","usage":{"input_tokens":1,"output_tokens":1}}'
"#,
        )
        .expect("fake provider should write");
        std::fs::set_permissions(&provider, std::fs::Permissions::from_mode(0o700))
            .expect("fake provider should be executable");
        let config_path = dir.path().join("agency-proxy.json");
        std::fs::write(
            &config_path,
            json!({
                "connection": {
                    "type": "web_socket",
                    "address": address.to_string(),
                    "authenticationKey": KEY,
                    "allowedOrigins": [ORIGIN]
                }
            })
            .to_string(),
        )
        .expect("config should write");
        let mut child = Command::new(env!("CARGO_BIN_EXE_agency-proxy"))
            .arg("--config")
            .arg(&config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("AgencyProxy should launch");

        let mut socket = None;
        for _ in 0..100 {
            if let Ok(connected) = connect(address, true, &handle).await {
                socket = Some(connected);
                break;
            }
            nagoya::sleep(Duration::from_millis(20)).await;
        }
        let mut socket = socket.expect("configured WebSocket should become reachable");
        send_json(
            &mut socket,
            json!({
                "jsonrpc":"2.0",
                "id":1,
                "method":"initialize",
                "params":{
                    "protocolVersion":"2025-06-18",
                    "capabilities":{},
                    "clientInfo":{"name":"agencyzero-drain-test","version":"0"}
                }
            }),
        )
        .await
        .expect("initialize should send");
        let _ = receive_json(&mut socket).await;
        send_json(
            &mut socket,
            json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        )
        .await
        .expect("initialized notification should send");
        send_json(
            &mut socket,
            json!({
                "jsonrpc":"2.0",
                "id":2,
                "method":"tools/call",
                "params":{
                    "name":"start_run",
                    "arguments":{
                        "runId":"drain-me",
                        "request":{
                            "provider":"claude",
                            "model":"",
                            "prompt":"test",
                            "isCommand":false,
                            "system":null,
                            "permission":"read_only",
                            "effort":null,
                            "extraThinking":null,
                            "approvals":false,
                            "interactive":false,
                            "workspaceRoots":[dir.path().to_string_lossy()],
                            "resumeSessionId":null,
                            "binary":provider.to_string_lossy(),
                            "environment":{},
                            "uncheckedArgs":[],
                            "metadata":{}
                        }
                    }
                }
            }),
        )
        .await
        .expect("start_run should send");
        let started = receive_json(&mut socket).await;
        assert_eq!(started["result"]["isError"], false);

        // SAFETY: `child.id()` is the dedicated AgencyProxy process created by
        // this test, and SIGTERM is the behavior under test.
        let sent = unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
        assert_eq!(sent, 0, "SIGTERM should reach the test proxy");
        nagoya::sleep(Duration::from_millis(100)).await;
        assert!(
            child
                .try_wait()
                .expect("child status should be readable")
                .is_none(),
            "proxy must remain alive while the accepted provider run drains"
        );

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if child
                .try_wait()
                .expect("child status should be readable")
                .is_some()
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "proxy should exit after the provider run settles"
            );
            nagoya::sleep(Duration::from_millis(25)).await;
        }
    });
}

#[cfg(unix)]
#[test]
fn subscribe_replays_then_pushes_sequence_numbered_mcp_notifications() {
    use std::os::unix::fs::PermissionsExt;

    let reactor = Reactor::start().expect("test reactor should start");
    let handle = reactor.handle();
    nagoya::block_on(async {
        let address = unused_loopback_address();
        let dir = tempdir().expect("temporary provider directory should exist");
        let provider = dir.path().join("eventful-claude");
        std::fs::write(
            &provider,
            r#"#!/bin/sh
sleep 1
printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"proxy-session","usage":{"input_tokens":1,"output_tokens":1}}'
"#,
        )
        .expect("fake provider should write");
        std::fs::set_permissions(&provider, std::fs::Permissions::from_mode(0o700))
            .expect("fake provider should be executable");

        let task = nagoya::spawn(serve_websocket(
            RuntimeRegistry::new(handle.clone()),
            WebSocketConfig {
                address,
                authentication_key: KEY.into(),
                allowed_origins: vec![ORIGIN.into()],
            },
            // Each test's server lives until the test process exits. None of them
            // takes a signal, which is what lets several run in one process.
            std::future::pending(),
        ));
        let mut socket = connect_when_ready(address, true, &handle).await;
        send_json(
            &mut socket,
            json!({
                "jsonrpc":"2.0","id":1,"method":"initialize",
                "params":{
                    "protocolVersion":"2025-06-18","capabilities":{},
                    "clientInfo":{"name":"subscription-test","version":"0"}
                }
            }),
        )
        .await
        .expect("initialize should send");
        let _ = receive_json(&mut socket).await;
        send_json(
            &mut socket,
            json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        )
        .await
        .expect("initialized notification should send");
        send_json(
            &mut socket,
            json!({
                "jsonrpc":"2.0","id":2,"method":"tools/call",
                "params":{
                    "name":"start_run",
                    "arguments":{
                        "runId":"subscribed",
                        "request":{
                            "provider":"claude","model":"","prompt":"test",
                            "isCommand":false,"system":null,"permission":"read_only",
                            "effort":null,"extraThinking":null,"approvals":false,
                            "interactive":false,
                            "workspaceRoots":[dir.path().to_string_lossy()],
                            "resumeSessionId":null,"binary":provider.to_string_lossy(),
                            "environment":{},"uncheckedArgs":[],"metadata":{}
                        }
                    }
                }
            }),
        )
        .await
        .expect("start_run should send");
        let started = receive_json(&mut socket).await;
        assert_eq!(started["result"]["isError"], false);

        send_json(
            &mut socket,
            json!({
                "jsonrpc":"2.0","id":3,"method":"tools/call",
                "params":{
                    "name":"subscribe_run",
                    "arguments":{"runId":"subscribed","afterSequence":0}
                }
            }),
        )
        .await
        .expect("subscribe_run should send");
        let subscribed = receive_json(&mut socket).await;
        assert_eq!(subscribed["id"], 3);
        assert_eq!(subscribed["result"]["isError"], false);
        let replayed_through = subscribed["result"]["structuredContent"]["events"]
            .as_array()
            .expect("subscribe result should contain replay")
            .last()
            .and_then(|event| event["sequence"].as_u64())
            .unwrap_or(0);

        let notification = loop {
            let frame = receive_json(&mut socket).await;
            if frame["method"] == "notifications/run_event" {
                break frame;
            }
        };
        assert_eq!(notification["params"]["runId"], "subscribed");
        assert!(
            notification["params"]["sequence"]
                .as_u64()
                .expect("notification should carry a sequence")
                > replayed_through
        );

        task.cancel();
    });
}
