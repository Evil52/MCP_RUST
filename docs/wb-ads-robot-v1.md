# WB ADS ROBOT v1

This document records how the user-supplied `WB ADS ROBOT v1` prompt is
translated into the deterministic Rust automation runtime. The runtime does
not execute natural-language system prompts. A rule is active only after it is
represented in typed policy, enforced in code, and covered by tests.

## Fixed scope

- Policy version: `wb_ads_robot.v1`
- Account: `ip_domnyshev_wb`
- Campaign: `39682633` (`Робот`)
- Payment and placement: `cpc`, search only
- Allowed products: `449627598`, `449627015`, `497424314`
- Advertising timezone: `Europe/Moscow`
- Target DRR / hard DRR: 15% / 25%
- Maximum bid: 6 RUB
- Maximum ordinary bid step: 15%
- Cooldown: 6 hours
- Protective threshold / daily ceiling: 250 RUB / 300 RUB
- Daily impression target: 5,000, subordinate to financial guards
- Automatic budget top-up: forbidden

## P0 enforcement in this change

The policy starts with `write_enabled=false`. It is a shadow policy until the
remaining mandatory data and execution guards are implemented and replayed.

- Policy scope, CPC/search contract, timezone, target and limits are typed and
  fail-closed.
- Campaign-level delivery never selects a SKU or increases bids. Aggregate
  statistics produce `attribution_incomplete`; genuine per-SKU stock guards
  remain observable.
- A normal decision contains at most one SKU change. The executor independently
  refuses a multi-SKU decision.
- Missing current-day spend is explicit and blocks actions instead of becoming
  a trusted zero.
- A protective pause is not automatically resumed after midnight or restart.
- The business date is calculated at the Moscow UTC+3 boundary.
- A shadow-policy digest migration preserves pending, cooldown, pause and
  incident state instead of resetting the robot.

## P1 durable shadow state

The next runtime is intentionally separate from the legacy host executor and
still cannot write to WB:

- `wb_automation_writer` owns no schema and receives only the exact
  `wb_automation` table/sequence privileges needed for durable state.
- Immutable cycles, action attempts and audit events are append-only; campaign
  execution state has monotonic revision and safety-state triggers.
- The automation and manual Control paths use the same PostgreSQL advisory key
  (`wb/<account>/<campaign>`), so they cannot act on one campaign concurrently.
- A permanent idempotency key and a partial unique index allow at most one
  unresolved action for a campaign.
- `shadow-once-pg` accepts only a shadow policy, read token, reviewed registry,
  legacy state and the restricted PostgreSQL role. It has no writer-token
  argument and persists the observation/decision atomically.
- Legacy import is content-addressed and idempotent. It preserves action count,
  cooldown, protective pause and incident state, and refuses migration while a
  legacy marketplace write is unresolved.
- The one-shot Compose service joins only the internal database network and the
  credentialless read-egress network. It has no writer secret, writer egress,
  host port or writable persistent volume.

This P1 path remains shadow-only. It does not replace or enable the production
executor until its rollout SHA passes CI and the old timer is explicitly
stopped during a guarded migration.

### Guarded local shadow rollout

On the production Mac, install the five-minute PostgreSQL shadow only after the
release SHA and local gates have passed:

```bash
./scripts/install-wb-automation-shadow-agent.sh \
  --confirm-stop-legacy-writer-and-start-shadow
```

The installer fails unless the legacy state has no pending write. It stops the
legacy safe-auto LaunchAgent first, stops the WB write-egress container, creates
the restricted database role and schema on the existing position volume, then
starts `com.ofk.mcp-ozon-wb-automation-shadow`. The shadow LaunchAgent mounts no
writer token and invokes only `shadow-once-pg` every five minutes. The legacy
plist is retained with the `.shadow-disabled` suffix, so a router reset, host
restart or login cannot accidentally load both state owners.

This command deliberately suspends automatic WB mutations. Do not bootstrap the
legacy agent while the PostgreSQL shadow is active: both runtimes must never own
campaign state concurrently. Enabling writes again requires a separate reviewed
PostgreSQL executor and an explicit cutover; changing `write_enabled` alone is
not a cutover.

## CPC compatibility corrections

The supplied prompt names recommended-bid and search-cluster statistics as
mandatory CPC inputs. Current WB documentation limits recommended bids and
`normquery/stats` to CPM campaigns, so those signals are not applicable to this
CPC campaign and must never be fabricated:

- <https://dev.wildberries.ru/en/docs/openapi/promotion?locale=ru%2F>
- <https://dev.wildberries.ru/en/news/302>

The current minimum CPC bid endpoint does support CPC and remains a required
future read-before-write guard.

## Required before write enablement

- Fresh per-SKU minimum bids and complete current spend/revenue attribution.
- PostgreSQL snapshots, distributed campaign lock, durable pending,
  idempotency key and append-only audit.
- Fresh read/compare immediately before one write and immediate read-back.
- Persistent spend-without-mature-order accounting across Moscow midnight.
- Approved unit economics or a separately bounded exploration envelope.
- Typed completeness/freshness for funnel, price, search, position and stock
  coverage inputs.
- Deterministic target pacing, cost forecast and safe/hard CPC formulas backed
  by replay data.

Until these conditions are met, `policy_shadow_only` is the expected safe
result and no new WB write is authorized by v1.
