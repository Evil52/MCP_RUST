# ADR 0004: PostgreSQL transport trust boundary

- Status: accepted operational constraint; remote TLS implementation pending
- Date: 2026-09-09

## Current contract

`mcp-storage` currently connects with `tokio_postgres::NoTls`. That is plaintext
PostgreSQL transport, not implicit encryption, even when role authentication
uses SCRAM. Least privilege, statement/transaction limits and connection
supervision solve different problems and do not provide confidentiality.

The supported plaintext topology is the isolated database network on the same
trusted Docker host, or loopback access for disposable tests. Do not expose the
database port to a public/LAN listener. A compromised host or an unauthorized
container attached to that network is outside the protection provided by
`NoTls`; network membership and host access must be controlled.

This is an operational constraint, not new URL validation: the current API
accepts a PostgreSQL `Config` and does not prove that a hostname is local or
that a supplied route is encrypted. This refactor changes neither credentials
nor the deployed database topology.

## Before using a remote database

Direct plaintext connections to another VM or an untrusted network are not
supported by this contract. Choose and verify one of these designs first:

- Implement a TLS connector with required encryption, trusted CA, hostname
  verification and explicit certificate rotation; require client certificates
  when the deployment policy calls for mutual TLS. Do not allow a plaintext
  fallback or disable peer verification.
- Keep the application's destination bound to local loopback behind a managed
  authenticated encrypted tunnel. Restrict its destination, verify the remote
  peer, deny a direct plaintext bypass and fail closed when the tunnel is down.

The tunnel option is a future topology decision, not a claim that an existing
MCP tunnel protects database traffic. No database TLS connector is added here.

## Migration acceptance checks

Prove invalid CA/name (and invalid client certificate if applicable) is refused;
packet capture/configuration review proves no unprotected remote hop; a stopped
tunnel cannot fall back to direct access. Re-run role/grant checks, session
timeout validation, backend-termination recovery and interrupted-transaction
tests. Restore from a protected backup in a disposable environment before the
separate production cutover. Never use a successful `SELECT 1` alone as proof
of transport security.
