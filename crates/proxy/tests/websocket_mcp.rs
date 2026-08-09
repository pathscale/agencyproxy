use agency_proxy::{RuntimeRegistry, WebSocketConfig, serve_websocket};
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    net::TcpListener,
    process::{Command, Stdio},
    time::Duration,
};
use tempfile::tempdir;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest, http::HeaderValue},
};

const KEY: &str = "0123456789abcdef0123456789abcdef";

fn unused_loopback_address() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral listener should bind");
    listener
        .local_addr()
        .expect("ephemeral listener should have an address")
}

async fn receive_json<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        // Full workspace runs can contend with several process-backed transport
        // tests on CI. Keep the assertion bounded while allowing scheduler
        // headroom beyond the fake provider's intentional one-second delay.
        let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("server should answer promptly")
            .expect("server should keep the connection open")
            .expect("server frame should be valid");
        match frame {
            Message::Text(text) => {
                return serde_json::from_str(&text).expect("server should send JSON");
            }
            Message::Binary(bytes) => {
                return serde_json::from_slice(&bytes).expect("server should send JSON");
            }
            Message::Ping(_) | Message::Pong(_) => {}
            other => panic!("unexpected server frame: {other:?}"),
        }
    }
}

#[tokio::test]
async fn authenticated_websocket_serves_the_minimal_mcp_surface() {
    let address = unused_loopback_address();
    let task = tokio::spawn(serve_websocket(
        RuntimeRegistry::default(),
        WebSocketConfig {
            address,
            authentication_key: KEY.into(),
            allowed_origins: vec!["https://agencyzero.example".into()],
            tls: None,
        },
    ));

    let mut request = format!("ws://{address}")
        .into_client_request()
        .expect("WebSocket request should build");
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        HeaderValue::from_str(&format!("agency-proxy.{KEY}"))
            .expect("authentication protocol should be valid"),
    );
    request.headers_mut().insert(
        "Origin",
        HeaderValue::from_static("https://agencyzero.example"),
    );

    let (mut socket, _) = loop {
        match connect_async(request.clone()).await {
            Ok(connected) => break connected,
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    };
    socket
        .send(Message::Text(
            json!({"method":1,"seq":1,"params":{}}).to_string().into(),
        ))
        .await
        .expect("legacy frame should send");
    let rejected = receive_json(&mut socket).await;
    assert_eq!(rejected["error"]["code"], -32600);

    socket
        .send(Message::Text(
            json!({
                "jsonrpc":"2.0",
                "id":1,
                "method":"initialize",
                "params":{
                    "protocolVersion":"2025-06-18",
                    "capabilities":{},
                    "clientInfo":{"name":"agencyzero-test","version":"0"}
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("initialize should send");
    let initialized = receive_json(&mut socket).await;
    assert_eq!(initialized["result"]["serverInfo"]["name"], "agency-proxy");

    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0","method":"notifications/initialized"})
                .to_string()
                .into(),
        ))
        .await
        .expect("initialized notification should send");
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0","id":2,"method":"tools/list"})
                .to_string()
                .into(),
        ))
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

    socket
        .send(Message::Text(
            json!({
                "jsonrpc":"2.0",
                "id":3,
                "method":"tools/call",
                "params":{"name":"list_runs","arguments":{}}
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("list_runs should send");
    let runs = receive_json(&mut socket).await;
    assert_eq!(runs["result"]["isError"], false);
    assert_eq!(runs["result"]["structuredContent"]["runs"], json!([]));

    task.abort();
}

#[tokio::test]
async fn websocket_rejects_clients_without_the_configured_key() {
    let address = unused_loopback_address();
    let task = tokio::spawn(serve_websocket(
        RuntimeRegistry::default(),
        WebSocketConfig {
            address,
            authentication_key: KEY.into(),
            allowed_origins: vec!["https://agencyzero.example".into()],
            tls: None,
        },
    ));
    let mut request = format!("ws://{address}")
        .into_client_request()
        .expect("WebSocket request should build");
    request.headers_mut().insert(
        "Origin",
        HeaderValue::from_static("https://agencyzero.example"),
    );
    let (mut socket, _) = loop {
        match connect_async(request.clone()).await {
            Ok(connected) => break connected,
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    };
    socket
        .send(Message::Text(
            json!({
                "jsonrpc":"2.0",
                "id":1,
                "method":"initialize",
                "params":{
                    "protocolVersion":"2025-06-18",
                    "capabilities":{},
                    "clientInfo":{"name":"unauthenticated","version":"0"}
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .ok();
    let closed = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("unauthenticated connection should close promptly");
    assert!(
        !matches!(closed, Some(Ok(Message::Text(_)))),
        "unauthenticated client must not receive an MCP response"
    );
    task.abort();
}

#[tokio::test]
async fn release_cli_starts_the_configured_websocket_transport() {
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
                "allowedOrigins": ["https://agencyzero.example"]
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

    let mut request = format!("ws://{address}")
        .into_client_request()
        .expect("WebSocket request should build");
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        HeaderValue::from_str(&format!("agency-proxy.{KEY}"))
            .expect("authentication protocol should be valid"),
    );
    request.headers_mut().insert(
        "Origin",
        HeaderValue::from_static("https://agencyzero.example"),
    );
    let mut socket = None;
    for _ in 0..100 {
        if let Ok((connected, _)) = connect_async(request.clone()).await {
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
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut socket = socket.expect("configured WebSocket should become reachable");
    socket
        .send(Message::Text(
            json!({
                "jsonrpc":"2.0",
                "id":1,
                "method":"initialize",
                "params":{
                    "protocolVersion":"2025-06-18",
                    "capabilities":{},
                    "clientInfo":{"name":"agencyzero-cli-test","version":"0"}
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("initialize should send");
    let initialized = receive_json(&mut socket).await;
    assert_eq!(initialized["result"]["serverInfo"]["name"], "agency-proxy");

    child.kill().expect("test proxy should stop");
    child.wait().expect("test proxy should be reaped");
}

#[cfg(unix)]
#[tokio::test]
async fn term_stops_websocket_admission_then_drains_owned_runs() {
    use std::os::unix::fs::PermissionsExt;

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
                "allowedOrigins": ["https://agencyzero.example"]
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

    let mut request = format!("ws://{address}")
        .into_client_request()
        .expect("WebSocket request should build");
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        HeaderValue::from_str(&format!("agency-proxy.{KEY}"))
            .expect("authentication protocol should be valid"),
    );
    request.headers_mut().insert(
        "Origin",
        HeaderValue::from_static("https://agencyzero.example"),
    );
    let mut socket = None;
    for _ in 0..100 {
        if let Ok((connected, _)) = connect_async(request.clone()).await {
            socket = Some(connected);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut socket = socket.expect("configured WebSocket should become reachable");
    socket
        .send(Message::Text(
            json!({
                "jsonrpc":"2.0",
                "id":1,
                "method":"initialize",
                "params":{
                    "protocolVersion":"2025-06-18",
                    "capabilities":{},
                    "clientInfo":{"name":"agencyzero-drain-test","version":"0"}
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("initialize should send");
    let _ = receive_json(&mut socket).await;
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0","method":"notifications/initialized"})
                .to_string()
                .into(),
        ))
        .await
        .expect("initialized notification should send");
    socket
        .send(Message::Text(
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
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("start_run should send");
    let started = receive_json(&mut socket).await;
    assert_eq!(started["result"]["isError"], false);

    // SAFETY: `child.id()` is the dedicated AgencyProxy process created by
    // this test, and SIGTERM is the behavior under test.
    let sent = unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    assert_eq!(sent, 0, "SIGTERM should reach the test proxy");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        child
            .try_wait()
            .expect("child status should be readable")
            .is_none(),
        "proxy must remain alive while the accepted provider run drains"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if child
            .try_wait()
            .expect("child status should be readable")
            .is_some()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "proxy should exit after the provider run settles"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn subscribe_replays_then_pushes_sequence_numbered_mcp_notifications() {
    use std::os::unix::fs::PermissionsExt;

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

    let task = tokio::spawn(serve_websocket(
        RuntimeRegistry::default(),
        WebSocketConfig {
            address,
            authentication_key: KEY.into(),
            allowed_origins: vec!["https://agencyzero.example".into()],
            tls: None,
        },
    ));
    let mut request = format!("ws://{address}")
        .into_client_request()
        .expect("WebSocket request should build");
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        HeaderValue::from_str(&format!("agency-proxy.{KEY}"))
            .expect("authentication protocol should be valid"),
    );
    request.headers_mut().insert(
        "Origin",
        HeaderValue::from_static("https://agencyzero.example"),
    );
    let (mut socket, _) = loop {
        match connect_async(request.clone()).await {
            Ok(connected) => break connected,
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    };
    socket
        .send(Message::Text(
            json!({
                "jsonrpc":"2.0","id":1,"method":"initialize",
                "params":{
                    "protocolVersion":"2025-06-18","capabilities":{},
                    "clientInfo":{"name":"subscription-test","version":"0"}
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("initialize should send");
    let _ = receive_json(&mut socket).await;
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0","method":"notifications/initialized"})
                .to_string()
                .into(),
        ))
        .await
        .expect("initialized notification should send");
    socket
        .send(Message::Text(
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
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("start_run should send");
    let started = receive_json(&mut socket).await;
    assert_eq!(started["result"]["isError"], false);

    socket
        .send(Message::Text(
            json!({
                "jsonrpc":"2.0","id":3,"method":"tools/call",
                "params":{
                    "name":"subscribe_run",
                    "arguments":{"runId":"subscribed","afterSequence":0}
                }
            })
            .to_string()
            .into(),
        ))
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

    task.abort();
}
