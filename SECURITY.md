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

1. Marketplace egress is read-only. `OzonClient::post`, `PerformanceClient::request`, and
   `WbClient::request` enforce exact allowlists before credentials are selected and before any
   socket is opened. Chat input cannot supply a host, HTTP method, or path. Redirects and ambient
   HTTP proxies are disabled.
2. Production Ozon Seller egress uses 34 stable reporting/list/info paths, including the three
   finance-accrual reads promoted after canary validation. Posting lists use only
   `POST /v3/posting/fbo/list` and `POST /v4/posting/fbs/list`; the superseded
   `/v2/posting/fbo/list` and `/v3/posting/fbs/list` are denied. Reviews use only
   `POST /v2/review/list`; the superseded `/v1/review/list` is denied. Warehouse discovery uses
   `POST /v2/warehouse/list` with a bounded limit and optional cursor/warehouse-ID filters.
   Ozon Performance business
   egress is fixed to exactly `GET /api/client/campaign`,
   `GET /api/client/statistics/daily`, `GET /api/client/statistics/expense`,
   `GET /api/client/limits/list`, `GET /api/client/campaign/{campaignId}/objects`,
   `GET /api/client/campaign/{campaignId}/v2/products`, and
   `POST /api/client/statistics/products/sku`. Dynamic routes require one canonical positive
   numeric campaign ID and an exact suffix; they are not prefix allowlists. The internal
   `POST /api/client/token` is not model-callable. WB egress contains exactly 22 fixed read
   operations. Its Search Report subset is
   `POST /api/v2/search-report/product/search-texts` and
   `POST /api/v2/search-report/product/orders`. Its Promotion subset is exactly
   `GET /adv/v1/promotion/count`, `GET /api/advert/v2/adverts`, `GET /adv/v3/fullstats`,
   `POST /api/advert/v1/bids/min`, `GET /api/advert/v0/bids/recommendations`, and
   `POST /adv/v0/normquery/get-bids`; the other 14 existing listing/reporting records remain
   enumerated and regression-tested in `src/wb.rs`. The read-only `POST` methods are allowed by
   exact host/method/path records, never by a generic verb or path prefix. Every WB record also
   fixes its host and quota class.
   A safe HTTP verb is never sufficient by itself.
3. Every MCP tool rejects unknown fields and applies bounded runtime validation in addition to its
   JSON Schema. A denied role, account, endpoint, malformed input, missing credential, or exhausted
   resource budget fails before marketplace network access.
4. Account RBAC is enforced by the Rust server. Ozon Seller finance and all Ozon Performance
   methods require `finance` or `admin`; actors with other roles are rejected before credential
   selection and cannot reach those endpoints even if the underlying API credential is broad.
   A Performance Client ID may belong to only one configured store; startup rejects reuse across
   stores because an organization-wide advertising response cannot be safely partitioned by ACL.
   Vendor subscription is not inferred from configured credentials: realization data may require
   Ozon Plus/Pro and reviews may require paid access. Missing entitlement is returned as a bounded
   upstream failure and never triggers a fallback to another account or endpoint.
5. Marketplace responses are bounded after decompression, obvious PII fields are redacted, and the
   result is labelled `untrusted_external_marketplace_data`. Marketplace text must never be treated
   as model instructions or forwarded to another tool without a new explicit user request.
6. Ozon Seller Analytics departures are paced at one request per 60 seconds per shared Client-Id.
   The wait happens before global and per-client network permits are acquired; report pagination is
   capped at ten pages so the quota cannot turn a daily run into an unbounded backfill.
7. Credentials, request bodies, response diagnostics, JWTs, and marketplace payloads are not written
   to application logs. Structured upstream errors contain only safe kind/status/account/endpoint and
   a strictly sanitized request ID.
8. Store clients, JWT/JWKS verification, OAuth wire behavior, session limits, schemas, request bodies,
   negative write-paths, retries, compression limits, and RBAC are covered by local mock tests. No
   live marketplace request is part of CI.
9. The separate `mcp-ozon-control` scaffold is disabled and credentialless. It loads only
   `CONTROL_MCP_*`, has no marketplace client or endpoint, exposes only local read-only status/scope,
   and runs on an internal Docker network without Internet egress. No analytics admin receives an
   implicit Control scope. Adding a key, egress, plan/apply tool, or marketplace write path requires
   a separate threat-model and release-gate review.

## Resource and availability controls

- Ozon: ordinary per-Client-Id pacing plus a dedicated 60-second Analytics gate, per-client
  concurrency 16, global concurrency 32, at most three attempts for explicitly transient failures,
  and a logical deadline covering rate waits/retries.
  Pacing and retry backoff hold no network permit, and retry permit races preserve the preceding
  causal upstream error rather than replacing it with a local overload.
- Ozon Performance: fixed official host, one-second per-Client-Id pacing, per-client concurrency 2,
  global concurrency 8, bounded cached OAuth tokens, and at most one replay after an HTTP 401 token
  refresh. OAuth refresh and pacing hold no business-request permits. There is no generic retry
  that can create or duplicate an advertising operation.
- Wildberries: quota is shared by token rather than account alias, funnel pacing is 20 seconds,
  ping pacing is 10 seconds, per-token concurrency is 4, global concurrency is 8, and the complete
  operation has a 60-second deadline. Promotion campaign reads share a 200 ms quota bucket and
  full statistics has a separate 20-second bucket. Search Report reads have a separate 20-second
  bucket and do not retry automatically; minimum bids, recommended bids, and search-cluster bids
  use separate 3-second, 12-second, and 200-millisecond buckets. Vendor retry headers are parsed
  conservatively. Search Reports are updated roughly once per hour, which is refresh cadence rather
  than row granularity. Product orders requests are bounded to seven days and their `dateItems` are
  stored as individual daily rows; period-level frequency and an unproven daily median are not copied
  into them. Search-text reports remain one aggregate over the explicitly requested period of at most
  31 inclusive days. Neither source has a region or organic/advertising split, and neither may be
  represented as a live search-result position.
  Startup accepts only a syntactically valid WB Personal JWT with numeric `acc=3`; Base, Test,
  Service, missing, malformed, and unknown token types fail closed before egress without exposing
  the token or decoded payload. This local decode selects the capability/quota policy and does not
  claim to verify the JWT signature; WB verifies authenticity on each request. Service/Base flows
  remain unsupported because they require an explicit `X-Client-Secret` and matching `asid` design.
- MCP HTTP: at most 32 non-GET requests parse and execute inside the MCP service at once; the 33rd
  fails fast with HTTP 503 while `/health` remains available. That ingress permit is released as
  soon as the handler constructs a response. Result-bearing POST responses have a separate hard
  cap of 16, acquired before dispatch and held through the response body until EOF or drop. Valid
  id-less JSON-RPC notifications (including `notifications/initialized` and
  `notifications/cancelled`) and valid client responses/errors bypass only the result-body cap, so
  unread control responses cannot starve cancellation ingress. Long-lived GET/SSE connections have
  an independent hard cap of 64, so stream shadows cannot consume POST execution capacity;
  overflow returns HTTP 503 and `Retry-After: 1`. POST bodies are fully received under one
  10-second deadline and a fixed 256 KiB streaming limit before JSON deserialization (the rmcp
  transport keeps the same 256 KiB limit as defense in depth). A syntactically malformed body or
  invalid JSON-RPC envelope with the correct media type receives a fixed, sanitized HTTP 400;
  a non-JSON `Content-Type` remains HTTP 415 and is rejected before the body is read.
  Browser requests carrying `Origin` are restricted to the protected resource's exact
  scheme/host/effective port in JWT mode, or to loopback hosts in development mode. Requests
  without `Origin` remain available to non-browser MCP clients. In JWT mode, the `Host` hostname
  must match `MCP_PUBLIC_URL`; its port is intentionally ignored because a reverse proxy may use a
  different internal listener port, while browser ports remain constrained by the exact Origin
  policy. A proxy must preserve that public hostname (or deliberately rewrite it to the same
  configured policy). Dev HTTP refuses a non-loopback bind unless the
  deployment explicitly sets `MCP_DEV_ALLOW_NON_LOOPBACK=true`; this opt-in is intended only for
  an isolated container whose published host port is separately restricted to loopback.
  The in-process plaintext listener accepts HTTP/1.1 only, caps accepted TCP connections at 128,
  and enforces a 10-second deadline for the initial and every keep-alive request header. Production
  TLS and HTTP/2 must terminate at a hardened reverse proxy with its own bounded connection,
  stream, header, and idle timeouts; the proxy-to-application hop uses HTTP/1.1. Keeping HTTP/2 off
  the bounded application listener prevents an idle multiplexed connection from retaining one of
  its 128 connection slots indefinitely.
  The local session registry separately defaults to a hard maximum of 256 entries and atomically
  rejects N+1/concurrent overflow with HTTP 503 and `Retry-After: 1`;
  `MCP_MAX_SESSIONS` can lower that value. Abandoned legacy sessions are reclaimed after 120
  seconds without protocol activity (`MCP_SESSION_IDLE_TIMEOUT_SECONDS`, constrained to 90–300).
  In-flight requests suspend the idle countdown, which restarts only after their final response;
  dropping the initialize response does not pin its slot beyond the idle lifetime.
- A shared 16-slot `tools/call` admission gate applies across every MCP session. Overflow fails
  locally before marketplace dispatch, while JSON-RPC cancellation drops the local outbound future
  and releases its permits. An already delivered read-only vendor request may still finish remotely.
- On SIGTERM/Ctrl-C the HTTP listener stops accepting immediately, drains naturally for up to
  55 seconds, then cancels MCP sessions, streams, and calls. Remaining connection futures are
  dropped by 65 seconds; Compose allows 70 seconds, leaving a 5-second termination margin.
- Successful marketplace responses are streamed and capped at 2 MiB after decompression; larger
  pages or periods must be split into bounded requests. The inner JSON for a structured MCP result
  has the same 2 MiB data budget plus 64 KiB of metadata headroom. MCP compatibility carries it in
  both `structuredContent` and a text JSON fallback; the serialized `CallToolResult` is separately
  capped at 6 MiB plus 64 KiB. Error diagnostics are capped at 4 KiB. JWKS is capped at 1 MiB,
  64 keys, and 16 KiB per JSON string/field name.
- Compose runs the MCP process as non-root with a read-only filesystem, no Linux capabilities,
  `no-new-privileges`, loopback-only published ports, memory/CPU/PID limits, and bounded log files.

## Authentication and deployment modes

`MCP_AUTH_MODE=dev` trusts one server-side `MCP_ACTOR_ID`. It is intended only for a single-user
loopback development instance. A shared or public Tunnel to a dev instance grants every reachable
client that actor's read permissions; never expose an admin dev instance to multiple users.

Shared or externally reachable deployments must use `MCP_AUTH_MODE=jwt`. The OAuth protected-resource
metadata and `/health` remain public, while every HTTP request to `/mcp` — including initialize,
notifications, tool discovery/calls, session GET, and session DELETE — validates the RS256 signature,
issuer, resource audience, required scope, time claims, and provisioned actor after bounded transport
admission but before body polling or session lookup/allocation. The registry snapshot used to map the
OIDC identity is reused for that request's tool RBAC, and identity is never cached in the MCP session.
The repository does not bundle an identity provider. For production, use public HTTPS
issuer/resource URLs, a correctly configured reverse proxy, exact redirect URIs, PKCE S256,
disabled Direct Access Grants, immutable
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
- Verify the production tool list contains exactly 67 stable tools, no preview tools, and every
  tool advertises `readOnlyHint=true`, `destructiveHint=false`, and the expected OAuth/noauth policy.
- Verify the Ozon Seller allowlist contains exactly 34 stable paths. Assert that
  `POST /v3/posting/fbo/list` and `POST /v4/posting/fbs/list` are admitted while their superseded
  list versions fail locally without credentials or network access.
- Verify the Ozon Performance namespace contains exactly `ozon_performance_campaigns`,
  `ozon_performance_daily`, `ozon_performance_expenses`, `ozon_performance_limits`,
  `ozon_performance_campaign_objects`, `ozon_performance_campaign_products`, and
  `ozon_performance_sku_statistics`. Assert that zero, signed, padded, encoded, extra-segment and
  otherwise non-canonical campaign IDs fail locally. Assert also that the mutating GET paths
  `/api/client/campaign/all_sku_promo/activate`,
  `/api/client/campaign/all_sku_promo/deactivate`, and
  `/api/client/campaign/all_sku_promo/set_bid` fail locally without credentials or network access.
- Verify the WB Search namespace contains exactly `wb_search_product_queries` and
  `wb_search_orders_positions`. Verify the WB Promotion namespace contains exactly
  `wb_promotion_campaigns`, `wb_promotion_campaign_details`, `wb_promotion_stats`,
  `wb_promotion_minimum_bids`, `wb_promotion_recommended_bids`, and
  `wb_promotion_search_cluster_bids`. Assert that campaign start, pause, stop, delete, budget
  deposit, `PATCH /api/advert/v1/bids`, `POST`/`DELETE /adv/v0/normquery/bids`, minus-phrase
  changes, SKU-promo activation and deactivation fail locally without credentials or network
  access, including the mutating operations that use HTTP `GET`.
- Confirm the runtime registry and `.env` are ignored regular files with mode `600`; never copy their
  contents into logs, screenshots, CI artifacts, or Git.
- Generate a scheduled-report credential directory only with the policy-scoped
  `report-collector bootstrap-credentials` command. Confirm the directory is mode `700`, each file
  is mode `600`, file names exactly match the enabled policy's marketplace bindings, and neither the
  source `.env` nor credential values appear in Compose environment, command output or artifacts.
- Provision Gmail OAuth separately from marketplace credentials. The private directory must contain
  exactly `client_id`, `client_secret`, and `refresh_token`, be mode `700`, and contain only regular
  mode-`600` files. Initial consent must request only
  `https://www.googleapis.com/auth/gmail.send`; do not use a Gmail password, a broader mail scope, a
  testing refresh token with an unsuitable lifetime, a symlink, or an address/token in Git, Compose
  environment, logs, screenshots, or report artifacts. OAuth refresh and Gmail delivery must use
  their fixed Google endpoints through the dedicated allowlisted mail-egress proxy.
- Keep the address routing document in a separate private regular file. Its symbolic names must
  match the enabled policy exactly; missing, extra, duplicate, malformed, or prompt-supplied routes
  fail closed. Do not combine email addresses with OAuth credentials or marketplace secrets, and do
  not expose the routing file through Compose environment, report artifacts, logs, or screenshots.
- Validate routing, report scope and the immutable artifact before OAuth. Perform at most one token
  refresh and one Gmail send per claimed attempt. Only failures that occur before send, plus an
  explicit Gmail rate limit, may be scheduled for a later bounded attempt. A timeout, transport
  failure, 5xx response, redirect or malformed receipt after send begins has an ambiguous outcome:
  keep the outbox row `sending`, never resend it automatically, and require operator reconciliation.
  A database failure while recording any post-claim outcome follows the same rule: leave the claim
  `sending` and never reinterpret it as a retryable pre-send failure.
  Reconcile an ambiguous attempt only through the operator commands `reconcile-sent` or
  `reconcile-suppress` in `dry_run` mode with the current enabled policy. Confirm `sent` only after
  Gmail independently provides the exact provider message ID. If the outcome cannot be proven,
  suppress it permanently as `operator_reconciled_unknown`; never return it to `ready`, retry it,
  or edit the batch directly. The reconciliation record must remain append-only, scoped to the
  exact audience, policy version, batch and active attempt, and an identical replay must be the
  only idempotent outcome.
  Persist local artifact/routing failures and explicit provider rejection under separate permanent
  error classes; never relabel them as a transient transport failure merely to make them retryable.
  Before any automatic loop exists, exercise only the explicit `delivery_canary` `deliver-one`
  command. It must claim at most one ready batch, finish within the outer 60-second budget, and
  leave any timed-out claimed row `sending`. The default Compose service must remain `disabled`
  and must not mount routing, OAuth credentials, or the mail-egress network. Use only the
  `reporting-mail-canary` profile and `scripts/run-report-mail-canary.sh`: profile startup alone
  must run `healthcheck`, the wrapper must wait for the deny-by-default proxy, invoke exactly one
  `deliver-one`, and stop the proxy on exit. The worker may attach only to the internal database
  and mail-proxy networks; only the credentialless proxy may attach to the outbound bridge, and
  its allowlist must remain exactly `oauth2.googleapis.com` plus `gmail.googleapis.com` on TLS 443.
  Do not deploy `REPORT_WORKER_MODE=scheduled_delivery` until that canary is reconciled. The
  scheduled process must retain the one-minute missed-tick-skip cadence, a maximum of 16 attempts
  per pass, the 60-second bound per attempt and process exit after five consecutive failed ticks.
  Restart catch-up may claim only `ready` work still inside its immutable delivery deadline;
  `sending`, `sent`, expired and permanent-failure rows must never be reclaimed automatically.
  Missed morning and evening occurrences must remain separate deliveries; a morning occurrence
  first recovered at the 17:00 boundary receives the bounded 23:00 deadline, while legacy mixed
  rows remain non-renderable and non-deliverable.
  Scheduled delivery must be activated only through the explicit `reporting-mail-live` profile
  and `scripts/start-report-mail-scheduler.sh --confirm-canary-sent-and-reconciled`. Its worker
  must retain the canary's exact read-only private mounts and internal-only database/mail-proxy
  networks; only the credentialless mail proxy may attach to the outbound bridge. Before service
  startup, `mail-preflight` must prove a successful canary for the selected audience and current
  policy version within 24 hours and reject any unresolved `sending` row for that audience.
- Before enabling `compose.reporting-live.yaml`, run the Ozon and WB pilot accounts sequentially
  through `scripts/run-report-canary.sh`. Require the disabled pilot policy, the explicit
  `reporting-canary` profile, the read-only credential-directory mount, an acquired account lease,
  an atomic complete snapshot set and zero credential values in environment, arguments or logs.
  Then start automatic collection only through
  `scripts/start-report-collector-scheduler.sh --confirm-canaries-published-and-reconciled`.
  Its database-backed `collection-preflight` must prove that every enabled-policy target shares
  one successful, fully paginated four-source cutoff from the previous 24 hours; it performs no
  marketplace request.
- Run OAuth provider integration tests only against disposable test identities. Run marketplace canaries
  sequentially, read-only, on isolated configuration, and never as part of routine CI.
