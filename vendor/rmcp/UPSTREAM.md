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

The upstream `build.rs` is intentionally omitted: it manages Git hook settings
for the SDK workspace and must not mutate the parent application's repository.

Keep this patch small and remove the vendored dependency after upstream `rmcp`
ships an equivalent typed field.
