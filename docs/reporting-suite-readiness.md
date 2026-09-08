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
3. Apply migration `029_independent_source_collection.sql` through the existing
   ledger-controlled database migration runner. The collector and MCP reader
   require its fenced jobs, normalized page journal and metadata view.
4. For scheduled collection set `enabled=true`, `REPORT_COLLECTION_POLICY_HOST`,
   `MCP_ACCESS_CONFIG_HOST` and `REPORT_COLLECTOR_CREDENTIAL_DIR_HOST`. Run
   `scripts/start-report-collector-scheduler.sh --confirm-independent-source-collection`.
   It verifies immutable images and runs `sources-preflight` before starting
   `run-sources`. It does not start mail delivery. Base Compose remains disabled.
5. Check real per-source publications, page progress, errors, timestamps and
   full-report completeness for all policy accounts. Local fixtures do not
   establish live capacity or 14/14 coverage. A single complete daily snapshot
   also does not fill seven historical days.

### Independent sources (local implementation, not deployed)

Each account/source/cutoff has a durable job. `run-sources` reconstructs morning
and evening occurrences in the existing 24-hour observation window, then runs
one source page at a time under a two-minute fenced lease and a 100-second
quantum deadline. A source yields after a newly fetched page; the next quantum
replays normalized pages from PostgreSQL and requests the first missing page.
The global source worker admits only one active quantum. Per-account/source
pacing and `Retry-After` survive process restarts. WB sales and stocks share
one 20-second Analytics bucket; WB advertising has a separate 20-second bucket,
and Ozon sales uses 65 seconds. The Performance client reuses its OAuth token
cache and limiter between page quanta. Restart the collector after credential
rotation; an in-progress job assumes the same marketplace account identity. Existing API client guards
and read-only egress still apply; quotas used by other processes need separate
operational coordination.

Normalized checkpoints contain curated facts and pagination metadata, not raw
vendor bodies. Limits: 4 MiB/page, 32 MiB/job, 4,096 pages; source-specific fact
and pagination bounds also apply. Completed or terminal jobs discard temporary
page data, while published history remains immutable. Eight consecutive errors
without page progress stop that source. Stock/price runs expire after a
30-minute observation span; resumed pages are never labelled as new live data.

WB resumable advertising accepts up to 5,000 eligible campaigns and retains
50-ID chunks; it does not truncate inventories. The legacy manual collector
still has its original 500-campaign bound. An advertising or credential failure
stops only its source job: ready sales, stock, price and finance snapshots publish
independently, atomically with their own job completion.

`ofk_source_snapshot(account, source, limit, offset, snapshot_id)` reads only
published PostgreSQL facts. It returns `source_as_of`, `observed_from`, business
period, `available/stale/missing`, and the latest collection status separately.
A stale snapshot stays readable. Subsequent pages require the returned stable
snapshot ID. Account access is enforced; advertising/finance details require
finance/admin access. Full-report tools still use the existing required-source
manifest and never treat partial coverage as a complete report.

Manager refresh requests use the same jobs and keep their existing deduplication,
current-day restriction and four-hour queue lifetime. Their public status stays
`queued` while sources collect; `ofk_source_snapshot.latest_collection` shows
individual progress. The request becomes `succeeded` only after all required
sources pass the existing completion check. One failed source marks the request
failed without removing successful source snapshots.

Keep `snapshot=STALE` and `last_refresh=FAILED` as independent facts when both
are supported. Collection status/completeness and refresh status are separate
MCP reads. An unrelated current-day failure does not invalidate an already
published historical week. History is not cached forever: reconciliation of
returns/corrections needs a separately bounded historical collection policy;
do not silently substitute another day.

## Production collection findings, 2026-09-08

The explicit fourteen-account rollout exposed capacity limits that prevent
enabling the existing scheduler as a guarantee of fresh data. The verified
`612597ea1df90220492bcc62f83973c8de6cb558` collector encountered Ozon
Performance timeouts/429s, Seller Analytics 429s, a warehouse-stock parse
failure, and WB advertising timeouts. Existing complete snapshots remained
unchanged after failed account runs. In that production release, an unhealthy required source
prevents publication of every source in the same account's report batch.

Two WB campaign-list requests succeeded with 519 and 2,282 eligible campaigns
(`ip_usovik_wb` and `ofk_komplekt_wb`). These exceed the collector's fixed
500-campaign capacity. This is distinct from malformed upstream data; the
source now reports `campaign_inventory_limit` for that case. The legacy limit remains in force in that production image. The local resumable
implementation described above has a separate bounded inventory contract. Truncating to the
first 500 would create an incomplete report.
This diagnostic change is a local patch; the production collector image above
has not been replaced by it.

WB documents at most 50 campaign IDs per fullstats request and one request
every 20 seconds per seller account. Thus 2,282 campaigns require 46 requests
and at least 15 minutes between the first and last departure, excluding
response time, retries and the other sources. That alone exceeds the current
12-minute account deadline. Increasing the campaign-count constant does not
solve collection capacity. See the [WB fullstats contract](https://dev.wildberries.ru/openapi//promotion).

These findings motivated the local implementation above: durable progress per account, source,
business interval and page/chunk, with retry times that survive restarts.
Successful source data must remain available with its actual observation time,
while an aggregate report must still require its complete source manifest.
Failure of advertising must not erase or conceal an independently complete
sales or inventory result. Point-in-time stocks/prices must never be relabelled
as observations of an earlier cutoff. Freshness checks should cover all policy
accounts, including those that have never published any data.

### Storage measurements

`scripts/reporting-storage.sql` produces a JSON observation under a read-only
transaction and a ten-second statement deadline. Save dated outputs before
and after a complete collection. It includes database size, heap/TOAST and
index allocation by table, and fact-row counts by cutoff. Represented accounts
and `all_present_sources_complete` describe only rows that exist; neither
proves coverage of the fourteen-account registry. Use the reporting-health
scope or the activation gate to check completeness.

The 2026-09-08 04:55 UTC sample contained 25,310,899 database bytes,
3,686,400 reporting-table/index bytes and 17 historical source snapshots.
The bounded fourteen-account pass subsequently published one complete account:
`ip_domnyshev_wb`, four sources and 4,220 fact rows, observed at 05:05:57 UTC.
Sales/advertising cover September 7; stocks/prices retain the actual September
8 observation time. Reporting allocation increased by 655,360 bytes, to
4,341,760 bytes; total database size reached 25,974,451 bytes. The scheduler
remains disabled because thirteen accounts did not publish the required set.
The existing local health schedule now checks reporting coverage for all
fourteen accounts at minutes 00, 15, 30 and 45. External notification delivery
is not configured.
This small historical sample does not establish daily growth at fourteen
accounts. Failed API attempts are not a successful collection load benchmark.
Measure complete daily growth, backup growth and restore time before choosing
retention. Current immutable facts do not have an automatic retention purge;
do not describe a proposed retention period as already enforced.

## Independent-source verification, 2026-09-08

The local Rust run passed 924 library tests (two existing optional tests remain
ignored), plus the binary/wire suites. All ten PostgreSQL suites ran against an
isolated database: 28 tests passed, including source restart/fencing, independent
sales/stock/price publication with failed advertising, stable snapshot pagination,
shared WB Analytics pacing, durable backoff, checkpoint cleanup, expiry and refresh
completion. The schema upgrade/ACL test passed on both a new and an existing schema.
The final Performance-client change was also checked for reuse across pages,
rejection of expired leases and independent Seller credential resolution.

The final pre-release coverage run executed 1,007 tests across 27 suites with
no failures or ignored tests, including 929 library tests. It also verifies the
actual source scheduler replay for all nine marketplace/source combinations,
corrupt checkpoint and missing-credential isolation, persisted vendor delays,
and fenced publication. A loopback HTTP fixture proves one guarded Seller
attempt returns `Retry-After` without an internal retry. Coverage passed the
unchanged gates: 95.50% functions and 95.87% lines. The PostgreSQL fixture uses
microsecond cutoff precision consistently on macOS and Linux.

Clippy with denied warnings, formatting, shell checks, reporting-health tests,
Suite synchronization and documentation generation are part of local validation.
This is code/test evidence; production publication, all-account freshness and
model acceptance require their own release verification. Collection has not been
activated by this implementation work.

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
