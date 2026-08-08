# AgencyProxy

AgencyProxy is the small, headless owner of live coding-agent processes and
sessions. AgencyZero connects to it instead of launching providers directly,
so the desktop UI can restart without interrupting active work.

This repository intentionally does not depend on AgencyZero. Headless installs
ship only the proxy executable. AgencyZero desktop releases will bundle a
pinned AgencyProxy binary and consume the versioned protocol crate.

## v0 boundary

- Local Unix-domain socket only; no TCP listener.
- No application-layer authentication. Owner-only runtime-directory and socket
  permissions are the local boundary.
- Versioned JSON frames with request IDs, stable run IDs, event sequence
  numbers, and explicit acknowledgement cursors.
- The proxy owns provider processes, live sessions, cancellation, injection,
  approvals, and replay state. AgencyZero owns its product database and UI.

The later remote connector will use the same protocol over an authenticated
outbound server connection.

## Local development

```sh
cargo test --workspace
cargo run -p agency-proxy -- --socket /tmp/agencyproxy-dev/agent.sock
```

