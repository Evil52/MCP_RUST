# OzonOFK Control MCP: Ozon campaigns and WB promotion bids

`mcp-ozon-control` is a separate fail-closed process for marketplace writes.
It does not extend the analytics server or its read-only allowlists. Without an
explicit policy, JWT auth, restricted PostgreSQL session, fixed egress and
dedicated marketplace credentials, it starts with all writes disabled.

## Implemented scope

The WB workflow changes product-card bids through `PATCH /api/advert/v1/bids`.
The Ozon workflow creates one CPC SKU campaign, adds its one product and
activates it through the fixed Performance API origin. Ozon write calls are
single-attempt and cannot be sent by the Analytics MCP.

Available tools:

- `ozon_ads_control_status` — reports static policy/runtime prerequisites and
  states explicitly that per-target runtime gates are still required;
- `ozon_ads_control_scope` — shows the actor's exact local Ozon/WB policy;
- `ozon_performance_prepare_campaign_launch` — runs a live duplicate-SKU
  preflight and persists an immutable single-SKU plan;
- `ozon_performance_approve_campaign_launch` — records a distinct authorized
  actor's approval of the exact plan digest;
- `ozon_performance_apply_campaign_launch` — performs one guarded
  create → add product → activate sequence;
- `ozon_performance_reconcile_campaign_launch` — read-back only after an
  ambiguous result; it never repeats a write;
- `wb_promotion_prepare_bid_update` — first reserves a bounded preparation
  attempt, then reads current campaign bids and creates an immutable
  five-minute PostgreSQL plan;
- `wb_promotion_approve_bid_plan` — records a distinct authorized actor's
  append-only approval from `plan_id + plan_digest + approval_reference`;
- `wb_promotion_apply_bid_plan` — claims an approved plan once, reserves its
  bounded action quota using `plan_id + plan_digest`, re-reads its precondition,
  performs one PATCH attempt, and reads the campaign back;
- `wb_promotion_bid_plan_status` — reads durable plan state without WB egress;
- `wb_promotion_reconcile_bid_plan` — read-back only; it never repeats a write.

This is a separate twelve-tool Control registry. It is not part of the Analytics
MCP release contract, which contains exactly 79 tools. Seventy-eight are
read-only; `ofk_request_ozon_sales_refresh` only mutates the internal deduplicated
snapshot-refresh queue and has no marketplace egress. In Control,
`prepare`, `approve`, `apply`, and `reconcile` intentionally advertise
non-read-only annotations because they change durable Control state; only
`apply` sends a marketplace mutation. `approve` is also marked destructive
because it grants execution authority, even though it has no WB egress.

Prices, stocks, product cards, arbitrary campaign deletion and search-cluster
bids are not enabled. Ozon creation is limited to exact policy-bound SKU,
budget, DRR and approver tuples.

These seven tools are a supervised execution kernel, not an autonomous store
manager. There is no scheduled decision worker, bid optimizer, forecast model
or policy-learning loop in this workflow. ChatGPT can analyze data and prepare
a bounded proposal, but production execution still requires the independent
approval identity and short-lived operator leases described below.

WB currently documents campaign bid changes for statuses `4`, `9`, and `11`,
with `combined` placement for a unified bid and `search`/`recommendations` for
a manual bid. The request is bounded to 50 distinct `nm_id + placement` pairs.
See the [official WB Promotion API](https://dev.wildberries.ru/docs/openapi/promotion).

## Ozon launch contract for Diana's store

The reviewed Furnitura policy contains five independent one-SKU targets. Each
campaign has a weekly budget of 2,000 RUB, a local total-spend stop at 2,000
RUB and `TARGET_BIDS` with an initial 7 RUB CPC and an immutable 12 RUB policy
ceiling. DRR 15% is a local fail-closed boundary and TOP-30 is an observation
goal, not an API placement guarantee.

The optional static-guard pacing controller consumes only a complete,
non-partial Ozon search-position snapshot published for the exact account, SKU
and region. A snapshot older than 45 minutes, a missing or ambiguous monitor,
or a position read error blocks upward changes. When attributed DRR is at most
15% and a fresh position is worse than TOP-30, the controller raises CPC by
exactly 1 RUB after a 30-minute cooldown, capped at 12 RUB. TOP-30 holds the
bid. Attributed DRR above 15% lowers CPC by exactly 1 RUB after the cooldown,
floored at 7 RUB; a breach already at 7 RUB pauses the campaign immediately.
The 2,000 RUB spend cap also pauses immediately.

Before a bid request, the intended transition is saved in the guard state.
After the single write attempt, the product bid is read back through the
Performance API. Only an exact match completes the transition and starts the
cooldown. An unavailable or mismatching readback creates a durable incident
and blocks further automatic actions for that campaign. A process restart
reconciles the saved pending transition by readback and never blindly repeats
the write.

This controller is not a position source. Position-based raises remain
operationally disabled until a reviewed live provider is deployed and exactly
one active Moscow monitor with a search phrase exists for each guarded SKU.
Competitive-bid values must not be presented as measured search positions.

Confirmation is an authenticated MCP action, not a message in chat. The plan
author calls `ozon_performance_prepare_campaign_launch` and gives Diana the
returned `plan_id` and `plan_digest`. Diana signs in through the configured
OIDC provider as the registry principal `diana_serafimovich` and calls:

```json
{
  "name": "ozon_performance_approve_campaign_launch",
  "arguments": {
    "plan_id": "<64 hex characters>",
    "plan_digest": "<64 hex characters>",
    "approval_reference": "diana_2026_09_02"
  }
}
```

The approval lasts at most three minutes and cannot be created by the plan
author. Apply additionally requires enabled policy, the write-capability flag,
and active database leases for `global`, `account/furnitura_dlya_doma` and the
exact `sku/furnitura_dlya_doma/<sku>` key. After successful activation, a
separate guard polls Performance statistics every minute. It deactivates the
campaign once spend reaches 2,000 RUB or attributed DRR exceeds 15%; an
unreadable statistics response requests a fail-closed stop. A timeout or
uncertain stop becomes an incident and is never retried blindly.

Use `config/control-policy.ozon-furnitura.live.example.json` only with the
reviewed five SKUs. The plan-only rehearsal file has a different revision, so
plans from it are intentionally invalid after switching to live policy.

## Safety contract

Every bid change must pass all of these gates:

1. The authenticated plan actor has basic access to the WB account and an exact
   `account_id + advert_id + nm_id + placement` policy binding. The same target
   lists one or more distinct `approver_actor_ids`, each with account access.
2. The requested bid is inside server-side kopeck limits and its change is no
   larger than `max_delta_percent`. A zero current bid is rejected for manual
   review. The target also fixes hourly/daily action ceilings, cooldown and a
   daily cumulative absolute bid-delta ceiling.
3. Before its WB read, `prepare` atomically reserves a two-minute append-only
   attempt. PostgreSQL caps an actor at 60 attempts/hour, a campaign at its
   policy hourly action limit, and active plans/unconsumed reservations at
   three. A matching reservation can create at most one plan. The plan stores
   the current state, exact requested/normalized changes, policy schema
   version, revision and digest, expires after five minutes, and is immutable.
4. `approve` accepts only an authenticated actor explicitly listed for that
   exact target. Self-approval is rejected. PostgreSQL stores an append-only
   approval bound to `plan_id + plan_digest`, and the approval cannot outlive
   the plan or two minutes, whichever comes first.
5. `apply` accepts only `approved` state. Its transaction requires active
   operator leases at global, account and campaign scope, enforces the plan's
   hourly/daily/cooldown/cumulative-delta limits, inserts one consumed-attempt
   reservation, then atomically changes `approved -> applying`. A partial
   unique index permits only one applying plan per account/campaign, and an
   unresolved incident blocks another plan for the same campaign.
6. Immediately before PATCH, the campaign is read again. Any changed
   precondition rejects the plan without writing. Control then revalidates the
   exact plan/policy digests, approval, reservation and all runtime leases once
   more after its writer queue wait.
7. The dedicated writer makes exactly one HTTP attempt. Timeout, connection
   loss, any HTTP error after send, or an invalid success body is ambiguous and
   is never retried; HTTP 4xx is not assumed to prove an atomic no-op.
8. After HTTP success, Control reads the campaign again. Only a matching state
   becomes `applied`; otherwise it becomes `reconciliation_required`.
9. `reconcile` can confirm the expected state, but cannot send PATCH. If a
   Control process stopped while a plan was `applying`, reconciliation is
   refused for three minutes, then atomically marks the abandoned attempt
   `ambiguous` before read-back. It still never repeats PATCH.

Plan and audit rows are durable and append-only from the application workflow.
The restricted `control_writer` role has no access to reporting or position
schemas and cannot delete plans or audit events.

Preparation reservations, plans, approvals, write reservations and audit rows
are deliberately retained for forensic history. Before live activation,
configure PostgreSQL size alerts and an operator-owned archive/retention job;
the application role cannot prune this evidence. The DB-backed preparation
caps bound growth and WB read pressure, but do not replace storage monitoring.

WB documents bid propagation at roughly 30-second intervals, so an immediate
read-back may legitimately produce `reconciliation_required`; wait for
propagation and call the read-back reconciliation tool. It may update the local
plan status but can never repeat the marketplace mutation.

Approval is a persisted two-person boundary, not a client confirmation dialog:
the author of a plan cannot approve it, and `apply` cannot substitute for
approval. ChatGPT may prepare and explain a plan under the plan actor, but a
separately authenticated listed approver must approve its exact digest. Do not
give one autonomous agent both identities or let it obtain either actor's JWT
through chat. The operator-owned runtime leases remain an independent kill
switch even after approval.

The gate is fail-closed authorization for a dispatch, not cancellation of a
request already authorized in flight. Revoking a lease stops a write that has
not completed its final database revalidation, but there is an unavoidable
small interval between that transaction commit and the HTTP send. For the
supervised singleton pilot, disable gates first and then wait at least the
configured WB request timeout plus the 250 ms pacing interval before treating
the executor as quiescent. An instantaneous/HA kill switch requires a
database-backed executor protocol that holds an operator-visible dispatch lock
through the bounded send.

`approval_reference` is a short restricted ASCII audit identifier, not a
free-form instruction. The MCP result returns approval identity/timestamps but
does not echo this field into the model context.

## Policy for ИП Домнышев

The access registry already binds account `ip_domnyshev_wb` to actor `wb6`
(`Вахрушева Наталья / Торсунова Вероника`). Campaign and product IDs still need
to be copied from the actual cabinet; do not use example numbers in production.

`wb6` currently represents two people behind one OIDC username. That is enough
for a plan-only technical rehearsal, but it is not individual accountability
for live writes. Before enabling the executor, create separate reviewed actors
and OIDC principals for Natalia Vakhrusheva and Veronika Torsunova, bind the
chosen plan author explicitly, and keep every approver/operator identity
separate. Do not invent those identifiers in this repository before IAM has
provided their real immutable subjects.

Start with `plan_only`:

```json
{
  "version": 1,
  "revision": 1,
  "mode": "plan_only",
  "actors": [
    {
      "actor_id": "wb6",
      "targets": [],
      "wb_promotion_bid_targets": [
        {
          "account_id": "ip_domnyshev_wb",
          "seller_sid": "00000000-0000-0000-0000-000000000000",
          "advert_id": 12345,
          "nm_ids": [13335157],
          "placements": ["search", "recommendations"],
          "bid_limits_kopecks": {
            "min_minor": 250,
            "max_minor": 5000,
            "max_delta_percent": 10
          },
          "approver_actor_ids": ["rustam_magasumov"],
          "action_limits": {
            "max_actions_per_hour": 4,
            "max_actions_per_day": 12,
            "cooldown_seconds": 900,
            "max_cumulative_abs_delta_kopecks_per_day": 5000
          }
        }
      ]
    }
  ]
}
```

`plan_only` permits preparation, persisted approval and inspection but refuses
`apply`. After a successful rehearsal, change the policy to `enabled` and
restart Control.
Increment `revision` for every effective policy change. `version` identifies
the JSON schema; `revision` identifies one operator-reviewed policy document,
and missing or zero revisions fail startup. The exact policy bytes are hashed
into each plan, so a revision or document change invalidates old execution
authority. `approver_actor_ids` must contain only distinct actors other than the
plan author; listing an Analytics admin here is explicit Control authority, not
an implicit consequence of the admin role. Replace all campaign, product and
limit numbers in this example with reviewed production values. Replace the
zero `seller_sid` too: the exact non-zero registry value is required in the
target and becomes part of the policy/plan digest.

When `CONTROL_MCP_DATABASE_URL` is present in JWT mode, startup registers the
current policy revision/digest even if `mode` is `disabled`; no WB token is read
in that mode. This persists a rollback-prevention tombstone, so an older
previously enabled revision cannot be restored later. For an emergency stop,
first disable the operator-owned global DB gate, then restart with a higher
disabled revision using the planner overlay only (never the live writer
overlay).

## Dedicated WB tokens

Do not replace or widen `IP_DOMNYSHEV_WB_API_TOKEN`. That existing token remains
read-only and belongs to analytics/reporting.

Create two separate Personal production tokens in the WB cabinet:

- the Control reader token: category **Продвижение** only, read-only access;
- the Control writer token: category **Продвижение** only, read/write access;
- no other API categories on either token.

Control locally decodes capability metadata and requires the exact capability
bits for each purpose. It refuses a non-Personal/test/expired token, a writer
token in the read-only slot, a read-only token in the writer slot, or either
token with another API category. WB still verifies the signature on every
request. The bit definitions and least-privilege guidance are in the
[official WB token documentation](https://dev.wildberries.ru/ru/openapi/api-information).

For a temporary local migration, an existing Personal production read-only
token may be accepted when it includes Promotion plus other read categories by
setting `CONTROL_MCP_ALLOW_BROAD_READ_TOKEN=true`. The default remains `false`,
the writer token is always Promotion-only, and the egress proxy still permits
only the Promotion API host. Replace the broad reader with a dedicated token
before production rollout.

Before either Control token is accepted, add the reviewed WB seller UUID from
the documented `sid` claim to the `ip_domnyshev_wb` registry binding:

```json
"wildberries": {
  "api_token_env": "IP_DOMNYSHEV_WB_API_TOKEN",
  "seller_sid": "00000000-0000-0000-0000-000000000000"
}
```

Replace the zero UUID with the actual canonical UUID. It is not the numeric
`seller_client_id=4389764`. Decode the token only locally or confirm it with
WB's official `GET https://common-api.wildberries.ru/api/v1/seller-info`.
Control requires the READ-token `sid`, WRITE-token `sid`, and reviewed registry
`seller_sid` to be identical; missing, malformed, or cross-cabinet identity
fails startup. A non-empty `seller_sid` must also be unique across account
bindings, so aliases cannot split one cabinet's incident locks or action quotas.
The SID is stamped into the immutable preflight snapshot and exact read-back,
so an old ambiguous plan cannot be reconciled against a replacement cabinet.
Never rebind an existing `account_id` to another cabinet: create a new reviewed
account binding and higher policy revision. Any unresolved old-SID incident is
intentionally left locked as forensic evidence; do not edit append-only rows to
make it disappear.

Save each token as one line in its own file; never paste either value into
policy JSON, chat, git, Compose environment or the analytics `.env`:

```bash
sudo install -o 10001 -g 10001 -m 0400 /dev/null \
  /absolute/protected/path/ip-domnyshev-wb-promotion-read.token
sudo install -o 10001 -g 10001 -m 0400 /dev/null \
  /absolute/protected/path/ip-domnyshev-wb-promotion-write.token
# Edit without changing owner/mode; Control runs as uid/gid 10001.
```

Each token may have one trailing LF/CRLF. Other whitespace is rejected. The
planning container described below never mounts the writer file. On the target
Linux host, verify that uid 10001 can read each bind-mounted file; do not relax
permissions to group/other because startup deliberately rejects those modes.

## PostgreSQL activation

Add a new random `CONTROL_WRITER_DB_PASSWORD` to the ignored `.position.env`.
For a new database volume, migrations `020_wb_control_plans.sql` and
`024_ozon_control_campaign_plans.sql` run during normal initialization.
Existing volumes do not rerun init scripts. After a
verified backup, rebuild the database image and use the ledger-backed
migrator. The one-time baseline is accepted only when the complete structural
healthcheck of the existing schema succeeds:

```bash
docker compose --env-file .position.env -f compose.position.yaml up -d position-db
docker compose --env-file .position.env -f compose.position.yaml \
  exec -T position-db migrate-position-db --baseline-current
docker compose --env-file .position.env -f compose.position.yaml \
  exec -T position-db migrate-position-db
```

Use the repository bootstrap script when creating a fresh position environment:

```bash
./scripts/bootstrap-position-env.sh .position.env.new
```

It refuses to overwrite an existing secret file.

Runtime write gates live in PostgreSQL and are fail-closed. Migration creates a
disabled global gate; `control_writer` can only read gates, never enable them.
Before an executor can claim a plan, a separate database operator must issue
short-lived enabled leases (at most 15 minutes) for all three scopes: `global`,
the exact account and the exact campaign. Missing, disabled, expired or
temporarily suspended scope refuses the write. Never grant gate mutation to the
application role; use a reviewed operator procedure with an identified
`updated_by`, increasing revision and bounded lease.

## Planner and executor container wiring

The base `compose.control.yaml` remains local-only, credentialless and disabled.
Use two additional layers with different secret boundaries:

- `compose.control-wb-plan.yaml` creates the JWT/DB/proxy runtime, forces
  marketplace writes off and mounts only the dedicated READ token. The
  container has no path to the WRITE secret;
- `compose.control-wb-live.yaml` is a minimal, executor-capability overlay used
  only after the plan overlay. It adds the WRITE-token path/mount. Even in this
  layer `CONTROL_MCP_MARKETPLACE_WRITES_ENABLED` defaults to `false`; an
  operator must explicitly set it to `true` in addition to enabling policy and
  short-lived database gates.

Run exactly one live executor for each dedicated WRITE token. The 250 ms WB
write pacer is shared by all clones inside one process, but it is not a
distributed lock between replicas or separately launched projects. Do not use
Compose scaling, active/active failover, or reuse this token in another worker.
The database still prevents concurrent application for one campaign, but a
multi-replica autonomous deployment needs a database-backed token-wide pacer
before it can be considered safe.

Both runtimes connect Control only to PostgreSQL and two private proxy
networks. Control itself is never attached to `outbound`:

- `control-write-egress` is a credentialless CONNECT proxy that permits only
  TLS 443 to `advert-api.wildberries.ru`. Both WB clients are explicitly forced
  through it, while the writer independently permits only
  `PATCH /api/advert/v1/bids`;
- `control-auth-egress` is an exact-path TLS-verifying reverse proxy for the
  configured JWKS document. `JwtAuthenticator` deliberately ignores ambient
  proxy variables, so Control fetches public keys from the proxy's internal
  `http://control-auth-egress:8080/jwks` endpoint. Only the proxy containers are
  attached to `outbound`.

The binary rejects plaintext issuer/public URLs and every plaintext JWKS URL
except that exact internal proxy origin; lookalike hosts, ports, paths, query
strings and credentials fail startup.

The auth proxy accepts a DNS hostname and absolute path separately, permits no
port, IP literal, query, fragment or redirect-following, sends no caller headers
upstream, and verifies the upstream certificate/SNI. Set these values from the
reviewed OIDC discovery metadata; they are public routing metadata, never a
token or client secret.

Set the JWT variables and protected READ-token path, point
`CONTROL_MCP_POLICY_HOST` to the reviewed `plan_only` policy, then start the
planner-capable runtime:

```bash
export CONTROL_MCP_WB_PROMOTION_READ_TOKEN_FILE_HOST=/absolute/protected/path/ip-domnyshev-wb-promotion-read.token
export CONTROL_MCP_POLICY_HOST=/absolute/protected/path/control-policy.json
export CONTROL_MCP_ACCESS_CONFIG_HOST=/absolute/protected/path/access.json
export CONTROL_MCP_JWT_ISSUER=https://auth.example/realms/ofk
export CONTROL_MCP_JWT_JWKS_HOST=auth.example
export CONTROL_MCP_JWT_JWKS_PATH=/realms/ofk/protocol/openid-connect/certs
export CONTROL_MCP_PUBLIC_URL=https://control.example/mcp

release_record="$(
  ./scripts/verify-release-images.sh \
    control control-ingress control-auth-egress control-write-egress
)"
export MCP_RELEASE_GIT_SHA="$(jq -r '.git_sha' <<<"$release_record")"
export MCP_CONTROL_IMAGE="$(jq -r '.images.control' <<<"$release_record")"
export MCP_CONTROL_INGRESS_IMAGE="$(jq -r '.images["control-ingress"]' <<<"$release_record")"
export MCP_CONTROL_AUTH_EGRESS_IMAGE="$(jq -r '.images["control-auth-egress"]' <<<"$release_record")"
export MCP_CONTROL_WRITE_EGRESS_IMAGE="$(jq -r '.images["control-write-egress"]' <<<"$release_record")"

docker compose --env-file .position.env \
  -f compose.control.yaml -f compose.control-wb-plan.yaml \
  up -d --no-build --wait --wait-timeout 300
```

Only after the policy, approval flow, action limits and database-gate procedure
have passed release review, create the executor-capable runtime by layering the
live overlay explicitly:

```bash
export CONTROL_MCP_WB_PROMOTION_WRITE_TOKEN_FILE_HOST=/absolute/protected/path/ip-domnyshev-wb-promotion-write.token
export CONTROL_MCP_MARKETPLACE_WRITES_ENABLED=true

docker compose --env-file .position.env \
  -f compose.control.yaml -f compose.control-wb-plan.yaml \
  -f compose.control-wb-live.yaml up -d --no-build --wait --wait-timeout 300
```

Never use the live overlay without the plan layer, never mount the writer token
in the planner container, and never expose the base dev/no-auth service through
a public tunnel. Executor mode requires the exact JWT audience and
`mcp:ads-control` scope; mounting a writer token is capability provisioning, not
proof that policy, approval or runtime gates permit a write.

## Operational rule for ambiguous writes

If `apply` returns `ambiguous` or `reconciliation_required`, do not create or
apply a replacement plan. Call `wb_promotion_reconcile_bid_plan`. The same call
can safely recover a plan left in `applying` after a process interruption; it
returns `CONTROL_PLAN_APPLY_IN_PROGRESS` during the three-minute safety window.
If the state still does not match, stop and compare the WB cabinet/audit trail
manually.
