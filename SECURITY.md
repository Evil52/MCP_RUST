# Security policy

## Supported versions

The current default branch is supported. Older snapshots and unmerged branches are not supported.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability and do not include Ozon, Wildberries,
SonarQube, Tunnel, or other credentials in reports, screenshots, logs, commits, or pull requests.

Use GitHub private vulnerability reporting for the repository. If that feature is unavailable,
contact the repository owner through an approved private corporate channel. Include affected
versions, impact, reproduction steps with sanitized data, and a suggested remediation when known.

Revoke and rotate any credential that may have been exposed before continuing the investigation.
Production credentials must never be used in CI; tests use local mocks only.

## Security invariants

The following properties are treated as release gates:

1. Marketplace egress is read-only. `OzonClient::post`, `PerformanceClient::get`, and
   `WbClient::request` enforce exact allowlists before credentials are selected and before any
   socket is opened. Chat input cannot supply a host, HTTP method, or path. Redirects and ambient
   HTTP proxies are disabled.
2. Production Ozon Seller egress uses 16 stable reporting/list/info paths. Three finance-accrual
   preview paths require an explicit client capability and remain disabled in the production
   Compose file. Ozon Performance business egress is fixed to exactly
   `GET /api/client/campaign`, `GET /api/client/statistics/daily/json`, and
   `GET /api/client/statistics/expense/json`; its internal `POST /api/client/token` is not
   model-callable. WB Promotion egress is fixed to `GET /adv/v1/promotion/count`,
   `GET /api/advert/v2/adverts`, and `GET /adv/v3/fullstats`. WB egress likewise requires an
   exact allowlisted method, path, host, and quota.
   A safe HTTP verb is never sufficient by itself.
3. Every MCP tool rejects unknown fields and applies bounded runtime validation in addition to its
   JSON Schema. A denied role, account, endpoint, malformed input, missing credential, or exhausted
   resource budget fails before marketplace network access.
4. Account RBAC is enforced by the Rust server. Ozon Seller finance and all Ozon Performance
   methods require `finance` or `admin`; actors with other roles are rejected before credential
   selection and cannot reach those endpoints even if the underlying API credential is broad.
   A Performance Client ID may belong to only one configured store; startup rejects reuse across
   stores because an organization-wide advertising response cannot be safely partitioned by ACL.
5. Marketplace responses are bounded after decompression, obvious PII fields are redacted, and the
   result is labelled `untrusted_external_marketplace_data`. Marketplace text must never be treated
   as model instructions or forwarded to another tool without a new explicit user request.
6. Credentials, request bodies, response diagnostics, JWTs, and marketplace payloads are not written
   to application logs. Structured upstream errors contain only safe kind/status/account/endpoint and
   a strictly sanitized request ID.
7. Store clients, JWT/JWKS verification, OAuth wire behavior, session limits, schemas, request bodies,
   negative write-paths, retries, compression limits, and RBAC are covered by local mock tests. No
   live marketplace request is part of CI.

## Resource and availability controls

- Ozon: per-Client-Id pacing, per-client concurrency 16, global concurrency 32, at most three
  attempts for explicitly transient failures, and a logical deadline covering rate waits/retries.
- Ozon Performance: fixed official host, one-second per-Client-Id pacing, per-client concurrency 2,
  global concurrency 8, bounded cached OAuth tokens, and at most one replay after an HTTP 401 token
  refresh. There is no generic retry that can create or duplicate an advertising operation.
- Wildberries: quota is shared by token rather than account alias, funnel pacing is 20 seconds,
  ping pacing is 10 seconds, per-token concurrency is 4, global concurrency is 8, and the complete
  operation has a 60-second deadline. Promotion campaign reads share a 200 ms quota bucket and
  full statistics has a separate 20-second bucket. Vendor retry headers are parsed conservatively.
- MCP HTTP: the local session registry defaults to a hard maximum of 256 entries and atomically
  rejects N+1/concurrent overflow. `MCP_MAX_SESSIONS` can lower this value.
- Responses are streamed and capped at 8 MiB after decompression; error diagnostics are capped at
  4 KiB. JWKS is capped at 1 MiB, 64 keys, and 16 KiB per JSON string/field name.
- Compose runs the MCP process as non-root with a read-only filesystem, no Linux capabilities,
  `no-new-privileges`, loopback-only published ports, memory/CPU/PID limits, and bounded log files.

## Authentication and deployment modes

`MCP_AUTH_MODE=dev` trusts one server-side `MCP_ACTOR_ID`. It is intended only for a single-user
loopback development instance. A shared or public Tunnel to a dev instance grants every reachable
client that actor's read permissions; never expose an admin dev instance to multiple users.

Shared or externally reachable deployments must use `MCP_AUTH_MODE=jwt`. Tool discovery remains
public for the ChatGPT OAuth flow, while every `tools/call` validates RS256 signature, issuer,
resource audience, required scope, time claims, and the provisioned actor on that individual HTTP
request. Identity is not cached in the MCP session. The current Keycloak Compose stack uses
localhost, HTTP, and `start-dev`; it is an integration-test stack, not a production deployment.

For production, use public HTTPS issuer/resource URLs, Keycloak production mode behind a correctly
configured reverse proxy, exact redirect URIs, PKCE S256, disabled Direct Access Grants, immutable
OIDC `sub` bindings, network ACLs, and credential rotation. Prefer vendor-side least-privilege API
tokens where the marketplace supports them; the Rust allowlist remains mandatory even when a token
has broader vendor permissions.

## Known residual risks

- The standard Compose file imports `.env` into the container. Keep a dedicated runtime `.env`
  containing only MCP settings and credentials explicitly referenced by the access registry,
  including Ozon Seller, Ozon Performance, and Wildberries credentials. The registry stores only
  environment-variable names, never secret values. Do not place Sonar, SSH, or other unrelated
  secrets in this file. A production orchestrator should use managed secrets instead of plain
  container environment variables.
- The bounded session manager currently returns a generic HTTP 500 when capacity is exhausted.
  Allocation is safely rejected, but a future transport update should map this condition to 429/503.
- Application logs provide safe transport/error telemetry but are not an append-only corporate audit
  ledger. A production rollout that requires non-repudiation must add an external protected audit sink
  for actor/tool/account/outcome without payloads or credentials.
- PII redaction is field-name based and deliberately conservative, not a full DLP system. Regulatory
  deployments require documented data classification, retention, and an independent DLP/privacy
  review.
- Vendor API semantics and quotas can change. Endpoint allowlist additions and preview promotion must
  be reviewed against current official documentation and merged through protected CI; never enable a
  generic proxy or dynamic marketplace path.

## Release checklist

- Run `./scripts/local-ci.sh`, require 100% line coverage, Clippy/rustdoc warnings as errors,
  RustSec/cargo-deny, CodeQL, dependency review, secret scanning, and the hardened-container job.
- Verify the production tool list contains no preview tools and every tool advertises
  `readOnlyHint=true`, `destructiveHint=false`, and the expected OAuth/noauth policy.
- Verify the Ozon Performance namespace contains exactly `ozon_performance_campaigns`,
  `ozon_performance_daily`, and `ozon_performance_expenses`. Assert that the mutating GET paths
  `/api/client/campaign/all_sku_promo/activate`,
  `/api/client/campaign/all_sku_promo/deactivate`, and
  `/api/client/campaign/all_sku_promo/set_bid` fail locally without credentials or network access.
- Verify the WB Promotion namespace contains exactly `wb_promotion_campaigns`,
  `wb_promotion_campaign_details`, and `wb_promotion_stats`. Assert that campaign start, pause,
  stop, delete, budget deposit, bid changes, SKU-promo activation and deactivation fail locally
  without credentials or network access, including the mutating operations that use HTTP `GET`.
- Confirm the runtime registry and `.env` are ignored regular files with mode `600`; never copy their
  contents into logs, screenshots, CI artifacts, or Git.
- Run OAuth/Keycloak smoke tests only against disposable test identities. Run marketplace canaries
  sequentially, read-only, on isolated configuration, and never as part of routine CI.
