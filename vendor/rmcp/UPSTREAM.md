# Vendored rmcp

This directory contains the source of `rmcp` 3.3.0 from the official
Model Context Protocol Rust SDK:

- upstream: https://github.com/modelcontextprotocol/rust-sdk
- crate: https://crates.io/crates/rmcp/3.3.0
- crate checksum: `b88db56b8ae316560e9e868b6b978ea940f27cb883323fc90e07435e44158f5c`
- upstream commit: `3e636cab26c013eca5131103c03d20237f12c4df`
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

Local modification: Streamable HTTP schema lookup never caches unknown tool
names. Positive entries are capped at 256, with a 128-byte name and 64 KiB
serialized-schema budget per entry. Oversized definitions and cache overflow
still undergo the same header validation without retaining their schemas.
The size check uses an allocation-free bounded writer; concurrent insertions
recheck capacity under the write lock.

The upstream `build.rs` is intentionally omitted: it manages Git hook settings
for the SDK workspace and must not mutate the parent application's repository.

Keep this patch small. The patch inventory, upgrade checklist, upstream tracking
gap and removal conditions are recorded in
[ADR 0003](../../docs/adr/0003-vendored-rmcp.md).
Removal requires equivalent behavior for every patch above (or a verified
application-level replacement), not only the typed field.

## 3.3.0 maintenance review (2026-09-13)

The published archive checksum and `.cargo_vcs_info.json` were verified against
the [official release](https://github.com/modelcontextprotocol/rust-sdk/releases/tag/rmcp-v3.3.0).
The 3.1.4-to-3.3.0 source diff was imported with a three-way comparison against
the verified 3.1.4 archive. No upstream Git-hook build script is imported.

Upstream now keeps `initialize` on legacy protocol versions and exposes
`ServerHandler::negotiate_initialize` (PRs
[#1228](https://github.com/modelcontextprotocol/rust-sdk/pull/1228) and
[#1247](https://github.com/modelcontextprotocol/rust-sdk/pull/1247)). HTTP errors
use a boxed internal wrapper; the application-facing early header classifier
still returns the existing sanitized `BoxResponse` contract. Session admission,
restore error classification, cancellation propagation and the bounded positive
schema cache remain local, with no relaxation of their budgets.

There are no verified upstream acceptance/issue IDs for our local patches.
They remain application-local for these reasons: `securitySchemes` is an OpenAI
plugin compatibility extension; session and result/schema budgets are this
service's resource policy; early HTTP admission and sanitized errors are its
auth/body-read boundary; root cancellation is required by its bounded shutdown;
omitting `build.rs` prevents an SDK import from modifying repository Git hooks.
Upstream feature additions are not evidence that these contracts can be removed.

Local validation: the OAuth wire, HTTP router, session cap/expiry, bounded JSON,
schema-cache and Nexus CLI suites passed, together with the negotiation tests
in `tests/rmcp_upgrade.rs`. The full all-feature/all-target workspace run passed
1,213 tests (50 database-fixture-only tests remain explicitly ignored there);
four PostgreSQL session-hardening tests also passed against a disposable database.
Clippy with denied warnings, rustdoc with denied warnings, formatting and the
first-party Rust structure budget passed. These are local build proofs, not
production deployment or campaign-control evidence.
