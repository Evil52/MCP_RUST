# ADR 0003: The rmcp fork is a tracked compatibility and safety obligation

- Status: accepted; fork retained
- Date: 2026-09-09
- Owner: MCP transport maintainers / reviewer of each SDK upgrade

## Decision and patch inventory

Keep the exact version, upstream commit and archive checksum in
[`vendor/rmcp/UPSTREAM.md`](../../vendor/rmcp/UPSTREAM.md). Do not add domain
behavior to the SDK. The current local obligations are:

| Patch | Required behavior | Application regression evidence |
| --- | --- | --- |
| Tool `securitySchemes` | Preserve typed top-level scheme objects and compatibility metadata | `tests/oauth_wire.rs` |
| Local session admission/lifetime | Hard cap, prune closed sessions, bounded idle lifetime, in-flight protection; classified overload becomes 503 + Retry-After | `tests/session_limit.rs`, `tests/http_router.rs` |
| HTTP input errors | Sanitized malformed JSON/envelope 400; unsupported media type 415 before body read | `tests/http_router.rs` |
| Structured result budget | Bounded serialization and bounded complete result, payload-free failures; retain both content representations | `tests/structured_result_limit.rs` |
| Session cancellation | Root shutdown reaches pending and initialized session workers | `tests/http_router.rs` bounded-shutdown tests |
| Packaging | Omit upstream Git-hook-mutating build script | Review vendored manifest and build-script diff on every import |

The tests document required behavior, not a claim that every fork line has
independent security coverage. New/changed patch paths require focused review.

## Upstream tracking gap

Repository provenance points to `modelcontextprotocol/rust-sdk`, but there are
no recorded verified upstream issue/PR IDs for these local patches. Do not
invent IDs or infer acceptance from a matching version. On the next SDK
maintenance PR, link each patch to an existing verified issue/PR or document
why it will stay application-local. Opening issues is external coordination
and is not performed by this local refactor.

CI already compares application/vendor/documented versions and the published
archive checksum. The latest upstream version is a hard gate on scheduled and
manual runs and a warning on unrelated PR runs. That checksum identifies the
source archive; it does not verify the locally modified directory byte-for-byte.

## Upgrade checklist

1. Inspect the official release and diff from the recorded upstream commit.
2. Verify the new source archive/checksum, then reapply only still-needed patches.
3. Review session admission, cancellation, serialization allocations and error
   disclosure independently of normal API migration. Record changed assumptions.
4. Run the focused tests above, full workspace gates and coverage. Review
   `tools/list` security metadata, input/output schemas and tool names.
5. Update the version, checksum, commit and per-patch upstream references in
   the same PR. Use the existing canary/release evidence procedure before rollout.

## Removal conditions

Remove `[patch.crates-io]` and the vendor directory only after **all** listed
contracts are supplied by upstream or an application-owned replacement with
equivalent bounds. Test that unpatched candidate directly, including hostile
inputs, overload and shutdown. A typed `securitySchemes` field alone is not a
sufficient removal condition. No SDK update or fork removal occurs in this ADR.
