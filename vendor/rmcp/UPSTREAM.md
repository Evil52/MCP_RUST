# Vendored rmcp

This directory contains the source of `rmcp` 3.1.1 from the official
Model Context Protocol Rust SDK:

- upstream: https://github.com/modelcontextprotocol/rust-sdk
- crate: https://crates.io/crates/rmcp/3.1.1
- crate checksum: `094c075f6698deef5a657cf4df6b684dff65157d255978b92b552ec22503f17a`
- upstream commit: `baac607e52b9788ec20902e2c7143ba4f4786f4b`
- license: Apache-2.0 (see `LICENSE`)

Local modification: `Tool` exposes the OpenAI plugin authentication extension
`securitySchemes` as a list of JSON objects, preserving future scheme types.
The same value is mirrored into `_meta.securitySchemes` by the application for
compatibility.

Local modification: `LocalSessionManager` enforces a configurable hard session
cap (256 by default) and prunes terminated handles before capacity checks. This
bounds memory and task growth at the transport layer. The parent application
additionally authenticates every `/mcp` HTTP request before session lookup or
allocation; the vendored transport itself remains authentication-agnostic. `SessionManager`
also exposes a backward-compatible typed error classifier: local capacity
exhaustion maps to HTTP 503 with `Retry-After: 1`, while unclassified manager
errors retain HTTP 500. The manager also exposes a configurable idle lifetime,
treats closed worker handles as absent, and suspends idle expiry while a request
is in flight; the application sets a bounded 120-second default so abandoned
handshakes release their slots without terminating long-running calls.

Local modification: a malformed body or invalid JSON-RPC envelope received with
`Content-Type: application/json` maps to a fixed, sanitized HTTP 400 response.
Unsupported media types remain HTTP 415 and are rejected before reading the
body.

Local modification: structured tool results produced through `Json<T>` are
serialized into a byte buffer capped at 2 MiB plus 64 KiB of bounded
wrapper-metadata headroom. Oversized serialization stops at the cap; accepted
input is dropped before the bounded bytes are parsed into `Value`, and the byte
buffer is dropped before the fallback text is built. The compatibility-required
fallback text and `structuredContent` are both retained, then an allocation-free
counting pass caps the serialized `CallToolResult` at 6 MiB plus 64 KiB. Size and
serialization failures return payload-free protocol errors.

Local modification: Streamable HTTP legacy session workers inherit a child of
the server configuration's cancellation token. Cancelling the root token now
terminates both sessions that are still handshaking and fully initialized
sessions, allowing the application to enforce a bounded graceful shutdown.

The upstream `build.rs` is intentionally omitted: it manages Git hook settings
for the SDK workspace and must not mutate the parent application's repository.

Keep this patch small and remove the vendored dependency after upstream `rmcp`
ships an equivalent typed field.
