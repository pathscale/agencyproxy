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
    "allowedOrigins": ["https://agencyzero.example"],
    "tls": {
      "certificates": ["/path/to/localhost-cert.pem"],
      "privateKey": "/path/to/localhost-key.pem"
    }
  }
}
```

Run it with `agency-proxy --config /path/to/agency-proxy.json`. TLS is optional
for local development, but an HTTPS-hosted frontend should use the configured
certificate and connect with `wss://`. Browser clients authenticate with the
WebSocket subprotocol `agency-proxy.<authenticationKey>`.
