# Reporting + Suite readiness

## What is implemented

The existing `report-collector` is the only scheduled Ozon/WB collector.
`ofk_weekly_marketplace_ranking` reads published PostgreSQL sales snapshots and
withholds ranking/leader/outsider unless all registry accounts have complete
coverage for the same completed seven-day interval. For the current inventory
that is 14/14. It measures operational GMV, not reconciled financial revenue.

`REPORT_COLLECTION_POLICY` now accepts a mail-independent policy:

```json
{
  "version": 1,
  "enabled": false,
  "timezone": "Asia/Yekaterinburg",
  "account_ids": ["furnitura_dlya_doma", "ip_domnyshev_wb"]
}
```

The example is a two-account pilot, not the full production scope. Select real
account IDs from the authoritative registry; do not infer WB ownership from
Ozon manager names. The parser rejects empty/duplicate/unknown accounts,
unsupported versions/timezones, oversized input and mixed mail/collection
fields. Marketplace source bindings are validated by the collection planner.
Startup resolves no marketplace secrets. Credentials remain lease-scoped.

Legacy `DAILY_REPORT_POLICY` and strict delivery policy JSON remain supported
for existing deployments. At process level select only one policy environment
variable; configuring both nonempty fails closed. `report-worker` still uses
the original delivery policy and has not gained collection-only routing.
`bootstrap-credentials` supports either format and copies only the chosen
accounts' vendor credentials, never SMTP credentials or addresses.

## Rollout gates, deliberately not bypassed

1. Verify an immutable release containing these changes; run the isolated
   container smoke/canary before installing it. This work does not authorize
   an account migration or a marketplace write-runtime change.
2. Prepare the explicit collection policy and private credential directory.
   No sender/recipient email is required for snapshot collection. For manual
   canaries, use the same account list with `enabled=false` and pass it using
   `DAILY_REPORT_CANARY_POLICY_HOST` to `scripts/run-report-canary.sh`.
3. Publish and reconcile Ozon and WB canaries in the supported observation
   window. Every target must share a complete cutoff within the preceding
   24 hours. Disabled policy, fixed windows, required sources, leases and
   fenced staging/atomic publication are unchanged.
4. For scheduled collection set `enabled=true`,
   `REPORT_COLLECTION_POLICY_HOST`, `MCP_ACCESS_CONFIG_HOST` and
   `REPORT_COLLECTOR_CREDENTIAL_DIR_HOST`. The legacy
   `DAILY_REPORT_POLICY_HOST` alias remains supported; do not select conflicting
   paths. Run `scripts/start-report-collector-scheduler.sh
   --confirm-canaries-published-and-reconciled`. It validates DB evidence before
   starting the collector. It does not start mail delivery.
5. Verify the actual scheduler, successful collection claims, published source
   cutoffs, refresh outcomes and the real MCP weekly result. Container health
   alone is not proof of 14/14 data. A single complete daily canary does not
   fill seven historical days. The current refresh queue collects today's
   business date; do not use it as an imaginary historical backfill API.

Keep `snapshot=STALE` and `last_refresh=FAILED` as independent facts when both
are supported. Collection status/completeness and refresh status are separate
MCP reads. An unrelated current-day failure does not invalidate an already
published historical week. History is not cached forever: reconciliation of
returns/corrections needs a separately bounded historical collection policy;
do not silently substitute another day.

## Local verification

Read-only baseline observed on 2026-09-06: the main MCP returned `ready`, no
`report-collector` container was running, and `source_snapshots` contained two
Ozon accounts (latest cutoff 2026-08-20 12:00 UTC), with no WB accounts. This is
an observation, not a permanent status claim. The code/package work did not
enable production collection or modify any account connection.

```sh
bash scripts/build-ozonofk-suite.sh --check
cargo test --locked --lib reporting:: -- --test-threads=1
cargo test --locked --lib server::tests::weekly -- --test-threads=1
./scripts/with-position-test-db.sh cargo test --locked --test reporting_mcp_read -- --test-threads=1
```

The PostgreSQL ranking test publishes seven days for fourteen synthetic
accounts, checks that the first seven do not produce a ranking, then checks
the complete Ozon/WB result, including a genuinely zero-valued account.

Suite source validation is not behavioral evaluation. Run the nine cases in
`plugins/ozonofk-suite/evals/scenarios.json` against an isolated authenticated
test connection before claiming model acceptance. Include manager/admin RBAC,
14/14 and 7/14 coverage, unknown tools, source-text injection and failed refresh.
Account/tunnel/marketplace installation remains deferred.
