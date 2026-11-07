# AgencyProxy

AgencyProxy is the small, headless owner of live coding-agent processes and
sessions. AgencyZero connects to it instead of launching providers directly,
so the desktop UI can restart without interrupting active work.

This repository intentionally does not depend on AgencyZero. Headless installs
ship only the proxy executable. AgencyZero desktop releases will bundle a
pinned AgencyProxy binary and consume the versioned protocol crate.

## Boundary

- AgencyProxy owns provider processes, live sessions, cancellation, injection,
  approvals, and replay state. It does not own AgencyZero projects, settings,
  messages, or the product database.
- Unix IPC uses owner-only runtime-directory and socket permissions.
- The optional WebSocket listener binds only to a loopback address, requires a
  URL-safe authentication key, and exposes the same narrow run-control surface
  as nine MCP tools. Proxy lifecycle control is intentionally not exposed.
- Versioned JSON frames with request IDs, stable run IDs, event sequence
  numbers, and explicit acknowledgement cursors.

## Local development

```sh
cargo test --workspace
cargo run -p agency-proxy -- --socket /tmp/agencyproxy-dev/agent.sock
```

`--socket` remains the compatibility/default Unix transport. A JSON config can
select the transport instead:

```json
{
  "connection": {
    "type": "web_socket",
    "address": "127.0.0.1:17820",
    "authenticationKey": "replace-with-at-least-32-url-safe-characters",
    "allowedOrigins": ["https://agencyzero.example"]
  }
}
```

Run it with `agency-proxy --config /path/to/agency-proxy.json`. The WebSocket
listener serves plain `ws://`; terminate TLS at a trusted edge proxy when an
HTTPS-hosted frontend needs `wss://`. Browser clients authenticate with the
WebSocket subprotocol `agency-proxy.<authenticationKey>`.

For a credential-bearing config, send its JSON over stdin instead of saving it
in a file:

```sh
your-secret-provider render-config | agency-proxy --config -
```

`--config -` reads JSON to EOF from stdin and validates it before opening a
socket. The secret provider command above is a placeholder; use the approved
provider for the environment. Do not put config values in command arguments.

## Publishing

The three crates share the workspace version in the root `Cargo.toml`; bump it
before publishing changed crate behavior. Run formatting, Clippy, tests, and
`cargo package --workspace` locally on the reviewed source. After rebase merge,
explicitly dispatch **Publish** from `master` to publish missing workspace
crates in dependency order. Publication does not run a separate remote CI gate.
All workflows are manual; merging to `master` starts nothing.
The repository must provide an Actions secret named
`CARGO_REGISTRY_TOKEN` with permission to publish all three crates.
