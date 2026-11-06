use agency_proxy::{RuntimeRegistry, WebSocketConfig, serve_websocket};
// endpoint-libs' own WebSocket client: nago-wss over a nagoya socket, the same
// stack the server speaks. It replaced tokio-tungstenite, which pulled tokio
// back into the test graph.
use endpoint_libs::libs::ws::{WsClient, WsClientBuilder};
use nagoya::reactor::{Handle, Reactor};
use serde_json::{Value, json};
use std::{
    io::Write,
    net::TcpListener,
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};
use tempfile::tempdir;

const KEY: &str = "0123456789abcdef0123456789abcdef";
const ORIGIN: &str = "https://agencyzero.example";

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn wait_for_exit(&mut self, timeout: Duration) -> std::io::Result<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "AgencyProxy child did not exit before the deadline",
                ));
            }
            thread::sleep(Duration::from_millis(10).min(remaining));
        }
    }
}

impl ChildGuard {
    fn terminate_and_reap(&mut self) -> std::io::Result<()> {
        match self.child.try_wait() {
            Ok(Some(_)) => return Ok(()),
            Ok(None) | Err(_) => {}
        }

        if let Err(kill_error) = self.child.kill() {
            // `kill` can lose a race with natural exit. Recheck before
            // reporting its error; otherwise leave cleanup bounded.
            if matches!(self.child.try_wait(), Ok(Some(_))) {
                return Ok(());
            }
            return Err(kill_error);
        }

        self.wait_for_exit(Duration::from_secs(1)).map(|_| ())
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Err(error) = self.terminate_and_reap() {
            // Drop also runs while unwinding. Write best-effort so cleanup
            // failures are visible without risking a second panic.
            let _ = writeln!(
                std::io::stderr().lock(),
                "failed to terminate/reap AgencyProxy child during cleanup: {error}"
            );
        }
    }
}

fn spawn_proxy_with_config(config: String) -> ChildGuard {
    let mut child = ChildGuard {
        child: Command::new(env!("CARGO_BIN_EXE_agency-proxy"))
            .arg("--config")
            .arg("-")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to start agency-proxy"),
    };
    let mut stdin = child.child.stdin.take().expect("stdin was not piped");
    stdin
        .write_all(config.as_bytes())
        .expect("failed to send configuration");
    drop(stdin);
    child
}

fn spawn_proxy_with_file_config(path: &Path, stderr: Stdio) -> ChildGuard {
    ChildGuard {
        child: Command::new(env!("CARGO_BIN_EXE_agency-proxy"))
            .arg("--config")
            .arg(path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(stderr)
            .spawn()
            .expect("failed to start agency-proxy"),
    }
}

fn spawn_proxy_with_stdin_config_and_stderr(config: &str, stderr: Stdio) -> ChildGuard {
    let mut child = ChildGuard {
        child: Command::new(env!("CARGO_BIN_EXE_agency-proxy"))
            .arg("--config")
            .arg("-")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(stderr)
            .spawn()
            .expect("failed to start agency-proxy"),
    };
    let mut stdin = child.child.stdin.take().expect("stdin was not piped");
    stdin
        .write_all(config.as_bytes())
        .expect("failed to send configuration");
    drop(stdin);
    child
}

fn assert_cli_config_error_is_redacted(
    config: &str,
    marker: &str,
    expected_diagnostic: &str,
    explicit_file: bool,
) {
    let directory = tempdir().expect("temporary test scratch should exist");
    let stderr_path = directory.path().join("stderr.log");
    let stderr = Stdio::from(
        std::fs::File::create(&stderr_path).expect("stderr capture file should be created"),
    );
    let mut child = if explicit_file {
        let path = directory.path().join("synthetic-config.json");
        std::fs::write(&path, config).expect("synthetic config should be written");
        spawn_proxy_with_file_config(&path, stderr)
    } else {
        spawn_proxy_with_stdin_config_and_stderr(config, stderr)
    };

    let status = child
        .wait_for_exit(Duration::from_secs(3))
        .expect("invalid config should fail promptly");
    assert!(!status.success(), "invalid config must fail startup");
    let stderr = std::fs::read_to_string(&stderr_path)
        .expect("stderr capture file should be readable after child exit");
    assert!(
        stderr.contains("could not start AgencyProxy:"),
        "startup diagnostic prefix should be preserved: {stderr}"
    );
    assert!(stderr.contains(expected_diagnostic), "stderr: {stderr}");
    assert!(
        !stderr.contains(marker),
        "input marker leaked to stderr: {stderr}"
    );
}

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
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut rejection_received = false;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "unauthenticated connection should reject and close promptly"
            );
            match nagoya::timeout(remaining, socket.recv_raw()).await {
                Err(_) => panic!("unauthenticated connection should reject and close promptly"),
                Ok(Ok(rejection)) => {
                    assert!(
                        !rejection_received,
                        "server sent a duplicate authentication rejection"
                    );
                    let fields = rejection
                        .as_object()
                        .expect("rejection must be a JSON object");
                    assert_eq!(
                        fields.len(),
                        6,
                        "unexpected authentication rejection envelope"
                    );
                    assert_eq!(rejection["type"], "Error");
                    assert_eq!(rejection["method"], 0);
                    assert_eq!(rejection["seq"], 0);
                    assert_eq!(rejection["code"], 100400);
                    assert!(rejection["log_id"].is_string());
                    let params = rejection["params"]
                        .as_object()
                        .expect("authentication rejection params must be an object");
                    assert_eq!(params.len(), 2);
                    assert_eq!(rejection["params"]["kind"], "BadRequest");
                    assert_eq!(
                        rejection["params"]["message"],
                        "AgencyProxy authentication failed"
                    );
                    assert!(rejection.get("jsonrpc").is_none());
                    assert!(rejection.get("id").is_none());
                    rejection_received = true;
                }
                Ok(Err(error)) => {
                    let diagnostic = error.to_string();
                    if diagnostic.ends_with("got ping") || diagnostic.ends_with("got pong") {
                        // `recv_raw` reports control frames as errors after the
                        // client has handled them. Keep waiting under one deadline.
                        continue;
                    }
                    // The current client flattens transport errors into text.
                    // Classify only OS connection teardown errors, not arbitrary
                    // parse/protocol failures, after the rejection envelope.
                    let reset = diagnostic
                        .strip_prefix("websocket read failed: io: errno ")
                        .and_then(|code| code.parse::<i32>().ok())
                        .is_some_and(|code| {
                            matches!(
                                std::io::Error::from_raw_os_error(code).kind(),
                                std::io::ErrorKind::ConnectionReset
                                    | std::io::ErrorKind::ConnectionAborted
                            )
                        });
                    let terminal = reset
                        || diagnostic == "Connection closed"
                        || diagnostic.ends_with("got close")
                        || diagnostic == "websocket read failed: closed without a handshake";
                    assert!(
                        terminal,
                        "unexpected WebSocket receive failure before termination"
                    );
                    assert!(
                        rejection_received,
                        "connection terminated before the authentication rejection"
                    );
                    break;
                }
            }
        }
        task.cancel();
    });
}

#[test]
fn release_cli_starts_the_configured_websocket_transport() {
    let reactor = Reactor::start().expect("test reactor should start");
    let handle = reactor.handle();
    nagoya::block_on(async {
        let address = unused_loopback_address();
        let config = json!({
            "connection": {
                "type": "web_socket",
                "address": address.to_string(),
                "authenticationKey": KEY,
                "allowedOrigins": [ORIGIN]
            }
        })
        .to_string();
        let dir = tempdir().expect("temporary config directory should exist");
        let config_path = dir.path().join("websocket-config.json");
        std::fs::write(&config_path, config).expect("explicit config should be written");
        let mut child = spawn_proxy_with_file_config(&config_path, Stdio::null());

        let mut socket = None;
        let startup_deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < startup_deadline {
            if let Ok(Ok(connected)) =
                nagoya::timeout(Duration::from_millis(250), connect(address, true, &handle)).await
            {
                socket = Some(connected);
                break;
            }
            assert!(
                child
                    .child
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

        child.child.kill().expect("test proxy should stop");
        child
            .wait_for_exit(Duration::from_secs(3))
            .expect("test proxy should be reaped promptly");
    });
}

#[test]
fn cli_config_errors_redact_input() {
    let type_marker = "synthetic-unknown-type-marker";
    let unknown_type = json!({"connection": {"type": type_marker}}).to_string();
    assert_cli_config_error_is_redacted(
        &unknown_type,
        type_marker,
        "invalid config JSON (data error at line",
        false,
    );

    let field_marker = "synthetic-unknown-field-marker";
    let mut connection = json!({"type": "unix", "socket": "synthetic-socket"});
    connection
        .as_object_mut()
        .expect("connection should be an object")
        .insert(field_marker.into(), json!("synthetic-field-value-marker"));
    let unknown_field = json!({"connection": connection}).to_string();
    assert_cli_config_error_is_redacted(
        &unknown_field,
        field_marker,
        "invalid config JSON (data error at line",
        true,
    );

    let origin_marker = "synthetic-invalid-origin-marker";
    let invalid_origin = json!({
        "connection": {
            "type": "web_socket",
            "address": "127.0.0.1:0",
            "authenticationKey": KEY,
            "allowedOrigins": [origin_marker]
        }
    })
    .to_string();
    assert_cli_config_error_is_redacted(
        &invalid_origin,
        origin_marker,
        "allowed WebSocket origins must begin with http:// or https://",
        false,
    );
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
        let config = json!({
            "connection": {
                "type": "web_socket",
                "address": address.to_string(),
                "authenticationKey": KEY,
                "allowedOrigins": [ORIGIN]
            }
        })
        .to_string();
        let mut child = spawn_proxy_with_config(config);

        let mut socket = None;
        let startup_deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < startup_deadline {
            if let Ok(Ok(connected)) =
                nagoya::timeout(Duration::from_millis(250), connect(address, true, &handle)).await
            {
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

        // SAFETY: `child.child.id()` is the dedicated AgencyProxy process created by
        // this test, and SIGTERM is the behavior under test.
        let sent = unsafe { libc::kill(child.child.id() as i32, libc::SIGTERM) };
        assert_eq!(sent, 0, "SIGTERM should reach the test proxy");
        nagoya::sleep(Duration::from_millis(100)).await;
        assert!(
            child
                .child
                .try_wait()
                .expect("child status should be readable")
                .is_none(),
            "proxy must remain alive while the accepted provider run drains"
        );

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if child
                .child
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
