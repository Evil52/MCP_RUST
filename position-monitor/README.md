# Marketplace position and bid history storage

This directory contains the PostgreSQL storage boundary for two deliberately
different datasets:

- the original Ozon exact search measurements (`monitors`, `measurements` and
  `alerts`); and
- additive Wildberries official Search Analytics and promotion-bid history
  (`wb_*` tables).

The WB schema does **not** claim to measure a live search result. Official WB
Search Analytics is updated roughly once per hour, but that is refresh cadence,
not row granularity. `search_product_orders` accepts a request window of at
most seven days and returns `dateItems`; the future collector must store each
`dt` item as a separate `data_granularity = 'daily'` row. It must not copy the
period-level frequency into those rows, and the endpoint does not prove a
daily median. `search_product_texts` is one aggregate over its explicitly
requested period of at most 31 inclusive days and is stored as
`data_granularity = 'period_aggregate'`. Neither source has region, live rank,
or an organic-versus-sponsored placement split. The reader view exposes these
limitations explicitly through `is_live_position = false`, a null `region`,
and `placement_split_available = false`.

This directory deliberately contains no store credential, API token, cookie,
authorization header, or browser profile. A provider-independent Ozon
collector core now exists in `src/position_collector`, but it has only a
`DisabledSource`. It also has a pure validated persistence payload and a
`DisabledRepository`. A transactional `PostgresRepository` is implemented and
verified against an ephemeral least-privilege database. A separate hardened
`position-collector` runtime now verifies that database contract, but its only
accepted mode is `disabled`: it neither schedules runs nor calls a source. There
is still no browser/provider adapter or deployed live collection job. The
`canary-plan <UTC-slot>` command only loads and validates exactly one active
30-minute/top-100 monitor and reports aggregate plan counts; it performs no
marketplace request and writes no collection result. The
database schema and least-privilege roles remain the storage boundary. The
additive Ozon collector migration now persists an overall position with an honest
`placement = unknown`, exact half-hour slots and terminal-only publication.
The disabled runtime invokes only the repository health contract and never
persists a business run.

The current core circuit breaker is in-memory only. The database migration adds
a durable circuit row and fail-closed per-region daily-budget claim function,
and the persistence adapter opens that circuit atomically for protective batch
results. A future provider runner must claim the durable budget before every
request and refuse collection while the circuit is open.

## Daily reporting persistence layer

The database includes a separate `daily_reporting` boundary for the planned
08:00/17:00 EKB reports. It stores immutable report occurrences, separate
period-preserving catch-up coverage, bounded delivery state, append-only provider attempts and normalized
sales, advertising, stock and price snapshots. A dedicated `report_collector`
can append and finalize snapshots; `report_worker` can read only terminal
published projections and operate the outbox. It does not store email bodies,
credentials or marketplace payloads. Rendered HTML/XLSX bytes live outside
PostgreSQL in an immutable artifact store; the outbox keeps only the stable
XLSX object key and SHA-256.
The collector also exposes an opt-in one-shot `collect-due` command and a
non-overlapping `run-scheduler` minute loop. Scheduled report work considers
only the open 08:00/17:00 EKB thirty-minute completion windows, skips already
published targets, claims each remaining account before resolving only that
account's read credentials, and publishes every mandatory source atomically.
Away from those windows the same single loop may claim at most one deduplicated
manager-requested Ozon refresh and publishes its five-source snapshot together
with queue completion. A refresh cannot start during the preceding 12-minute
deadline reserve or the following 65-second Seller API pacing tail. Queued
current-day work expires after four hours, which covers the fourteen-account
sequential worst case. Missed timer ticks are skipped; bounded sustained
failures exit for supervisor restart, and shutdown attempts to release the
active claim. The shipped Compose mode and
policy are still disabled and mount no credential directory, so neither path is
active in the default deployment. Opt-in scheduled mode accepts only an
operator-owned read-only directory named by `REPORT_COLLECTOR_CREDENTIAL_DIR`.
Each access-registry credential name maps to one bounded regular file directly
inside it; symlinks and unexpected names fail closed. Values are read only after
the exact account/cutoff claim succeeds and are never placed in Compose
environment variables, command arguments, images or logs.
The opt-in `compose.reporting-live.yaml` overlay requires explicit absolute
host paths for the access registry, enabled policy and credential directory,
and is guarded by the `reporting-live` Compose profile. It starts only the
collector scheduler; report generation and email remain disabled. Always render
the merged Compose model with `config --quiet` before starting that profile.
There is still no `Поисковая видимость за сутки` Dashboard, task registry or
email job. A manual deterministic
HTML/XLSX preview and a policy-scoped `report-worker generate <batch-id>` path
are implemented. Generation accepts no recipient/account/cutoff input, writes
only to the immutable local store, and can mark one pre-existing single-section
outbox batch ready while delivery remains disabled. Mixed morning+evening
batches remain rejected. The worker has an isolated persistent artifact volume
whose write access is included in its health check. An opt-in, non-delivery
`dry_run` mode plans due occurrences once per minute and recovers at most 16
single-section artifact batches per tick; the shipped Compose configuration
keeps the worker `disabled`. The planned compact
view contains collection status and completeness,
visibility rate, comparisons of complete reporting days, critical products, at
most five priority problems, manager tasks, and a link to the bounded detailed
report.

`found` and `not_found` are valid visibility observations. Blocked, failed and
missing slots reduce completeness and cannot be treated as product invisibility.
Responsibility, priority, deadlines and result checks come from versioned
application rules and server-side mappings, not from model inference. Existing
collector alerts are data-quality and position signals; they are not a manager
task workflow.

Detailed Excel workbooks are generated on demand from the same frozen report
run. They are exports rather than the system of record and are never stored in
PostgreSQL. Existing artifact bytes are never overwritten: a repeated stable
identity must have the same XLSX and HTML content hashes. The dedicated
`mcp-ozon-report-artifacts` volume is writable only by `report-worker`; no MCP,
collector or database service mounts it. See
`docs/search-position-monitoring.md` for the complete reporting contract.

The architecture has six security principals:

- `position_admin` owns the database and manages Ozon monitors and WB targets;
- `position_collector` may read target definitions and append runs and
  snapshots. It cannot change target identity or update/delete WB snapshots;
- `position_reader` is forced into read-only transactions and is the only role
  that the Rust MCP server may use for analytics reads;
- `report_refresh_requester` lets the Rust MCP server execute only the bounded
  refresh request/status functions. It cannot read the queue or snapshots,
  claim work, or access marketplace credentials;
- `report_worker` can use only the reporting outbox and cannot read raw Ozon or
  WB position history. It can read only published reporting snapshot views;
- `report_collector` can append normalized report facts and finalize their
  source snapshots. It cannot use the outbox or modify published facts.

For WB, target identity fields are immutable at the database layer. On update,
an admin may only pause/resume a target via `active`; a trigger owns
`updated_at`. A
collection run is idempotent per `(account_id, store_id, source,
scheduled_for)`, where `scheduled_for` must be an exact UTC hour. The collector
may finalize only the bounded status/diagnostic columns of a run.

The additive objects are:

- `wb_search_targets` and `wb_bid_targets`: admin-managed identities;
- `wb_collection_runs`: endpoint provenance and hourly collection outcome;
- `wb_search_snapshots`: source-aware official search statistics for an
  `nm_id` and phrase: daily order-position rows or a search-text period
  aggregate;
- `wb_bid_snapshots`: campaign, `nm_id`, normalized query/placement and bid
  observations;
- `latest_wb_search_snapshots` and `latest_wb_bid_snapshots`: bounded read
  projections for `position_reader`. Only snapshots belonging to `succeeded`
  or `partial` runs are publishable; each projection includes `run_status`,
  `is_partial`, bounded counts, error class and HTTP status. Running, failed
  and blocked attempts remain auditable to the admin in base tables but cannot
  replace the last published result. The reader cannot query raw runs or raw
  snapshots and therefore cannot bypass this publication boundary.

When a collector is implemented, it must fan out every
`search_product_orders.dateItems[].dt` response item into one daily row; the
request itself remains bounded to seven days. A `search_product_texts` snapshot
retains the exact requested period, bounded to 31 inclusive days. Bid
observations may be taken once per hour. Cluster-bid responses do not carry
payment type or placement, so both remain null; recommendation snapshots have
the endpoint's fixed `cpm` contract and a null placement; minimum-bid snapshots
retain the payment type and placement supplied to and returned for that method.
History is append-only for the collector; no automatic retention or deletion
job is installed yet. Encrypted backups are covered below; a reviewed retention
period for the history itself still has to be defined before scheduled
collection is enabled.

The database is not published on a host port. Future collector and MCP services
must join the internal Docker network `mcp-ozon-position-internal` explicitly.
The init files are copied into a small derived PostgreSQL image at build time;
there is no runtime bind mount from the macOS `Documents` directory.

## Backups and recovery

Two stores hold everything that cannot be recreated from the marketplaces: this
database and the `mcp-ozon-report-artifacts` volume. They are backed up
together, by one command, in one order.

### Taking backups

Install `age`, create a dedicated identity once, and put a protected copy of
the identity somewhere other than this Mac:

```bash
brew install age
./scripts/bootstrap-backup-age-key.sh
```

The script writes a secret identity and a public recipients file. The backup
writer receives only the public file; the restore verifier receives the secret
identity. An identity that exists only on the machine the backup protects
cannot be used after that machine is lost, which is the case the backup exists
for.

Then configure an executable offsite-copy hook and schedule all three
operations agents:

```bash
MCP_HEALTH_NOTIFY_COMMAND="$HOME/.local/libexec/mcp-ozon/notify" \
MCP_BACKUP_OFFSITE_COMMAND="$HOME/.local/libexec/mcp-ozon/offsite-copy" \
  ./scripts/install-operations-agents.sh
```

The installer takes one real backup, restores it into a disposable PostgreSQL
container and runs one health check before scheduling anything. It refuses to
install an agent whose first run did not work.

This creates `com.ofk.mcp-ozon-backup` (daily at 03:30),
`com.ofk.mcp-ozon-restore-verify` (Sunday at 04:30), and
`com.ofk.mcp-ozon-health` (every 15 minutes). They use
`StartCalendarInterval`: `StartInterval` drops any firing whose moment lands
while the Mac is asleep, while `StartCalendarInterval` runs the job on the next
wake.

| Variable | Default | Purpose |
| --- | --- | --- |
| `MCP_BACKUP_DIR` | `~/MCP_OZON-backups` | Where backups are written. Put this on separate storage. |
| `MCP_BACKUP_RETAIN` | `14` | Backups kept before the oldest are pruned. |
| `MCP_BACKUP_AGE_RECIPIENTS_FILE` | `$MCP_RUNTIME_DIR/backup-age-recipients.txt` | Public age recipients used by the backup writer, mode `600`. |
| `MCP_BACKUP_AGE_IDENTITY_FILE` | `$MCP_RUNTIME_DIR/backup-age-identity.txt` | Secret age identity used only by restore verification, mode `600`. |
| `MCP_BACKUP_PASSPHRASE_FILE` | `$MCP_RUNTIME_DIR/backup-passphrase` | Legacy v1 restore only; not used for new backups. |
| `MCP_BACKUP_OFFSITE_COMMAND` | required | One executable that must confirm copying the new backup off-host. |
| `MCP_BACKUP_ALLOW_LOCAL_ONLY` | `false` | Explicit accepted-risk exception when offsite storage is temporarily unavailable. |
| `MCP_HEALTH_NOTIFY_COMMAND` | unset | One executable; receives the findings report on stdin. |
| `MCP_HEALTH_CYCLE_STALE_SECONDS` | `1800` | Age at which a silent WB robot becomes a finding. |
| `MCP_HEALTH_BACKUP_STALE_SECONDS` | `129600` | Age at which the newest backup becomes a finding. |
| `MCP_HEALTH_RESTORE_STALE_SECONDS` | `691200` | Age at which the last successful disposable restore becomes a finding. |

Both hooks are executed directly rather than through a shell, so each must be a
single executable file. Wrap `mail`, `curl` or `osascript` in a small script
instead of passing a command line.

### Why the capture order matters

`persist_and_mark_ready` writes artifact bytes before the database row that
references them becomes ready. A database snapshot taken at T1 can therefore
only reference artifacts that already existed before T1, so capturing the
database first and the artifacts second yields an artifact set that is a
superset of what the dump references. The reverse order would produce dangling
`artifact_object_key` values for anything published between the two captures.

### Verifying a backup

```bash
./scripts/verify-position-backup.sh            # newest backup
./scripts/verify-position-backup.sh BACKUP_DIR # a specific one
```

This restores into a disposable container built from the exact pinned
PostgreSQL digest recorded in the manifest, with the restricted application
roles created first so ownership and grants apply as they do in production. It
then checks that every `artifact_object_key` the restored database references is
present in the artifact archive captured alongside it — the cross-store
consistency that two independently restored volumes silently lose.

The container has no network and its volume is removed on exit. Nothing touches
the live stack.

### Restoring for real

1. Stop every writer: the collectors, the report worker and both WB automation
   agents. Leave the database running.
2. Verify the backup first, with the command above. Never restore an archive
   that has not been restored successfully somewhere else.
3. Restore the database into a **new** volume rather than over the live one, so
   the damaged state remains available as evidence.
4. Restore the artifact volume from the same backup directory. Restoring the
   two stores from different backups is the one mistake this layout is designed
   to prevent.
5. Re-run the health check before re-enabling any writer.

New backups use authenticated age v1 files (`manifest_version: 2`). The
manifest also records each ciphertext SHA-256 for early corruption detection
and stable archive identity. Restore keeps read compatibility with legacy
`manifest_version: 1` AES-256-CBC backups; those require the old passphrase and
do not gain authentication retroactively. Keep the backup directory and the
recovery identity on separate protected storage.

### PostgreSQL major upgrades

The image is pinned by digest and Dependabot is configured to propose patch and
minor updates only. A major upgrade is deliberately excluded, because replacing
the image does not migrate `PGDATA`: the new major cannot read the old data
directory and the container fails to start, taking the whole data plane with it.

To move majors: take and verify a backup, start the new major against an empty
volume, restore the dump into it, run the health check, and only then repoint
the stack. Keep the previous volume until the new one has served a full daily
reporting cycle.

## Bootstrap

The stack can now be started as an inert infrastructure check. It cannot collect
positions until a separately reviewed live-source phase is shipped. When ready:

1. Generate the ignored local secret file with
   `./scripts/bootstrap-position-env.sh .position.env`, or copy
   `.position.env.example` to `.position.env` and generate seven different random passwords of at
   least 24 characters. Keep the file mode `0600`. Bootstrap rejects example placeholders, short
   values, reused passwords, and an admin username that collides with any restricted application
   role. The helper never prints generated passwords and refuses to overwrite an existing file.
3. Validate the Compose model:

   ```bash
   docker compose --env-file .position.env -f compose.position.yaml config --quiet
   ```

4. Start the database and disabled collector runtime:

   ```bash
   docker compose --env-file .position.env -f compose.position.yaml up -d --wait
   ```

The init scripts run only when the named volume is empty. A fresh database
applies the base schema, the additive Ozon collector contract, the additive WB
migration, restricted role grants, the Ozon adapter digest migration, then the
daily reporting outbox, normalized snapshot, optional-metric and strict
artifact-identity migrations, bounded generation backoff, observation-window
and collection-claim contracts, explicit delivery error classes, and the
append-only delivery-reconciliation contract.
The final migrations also add append-only generation-attempt history, bounded
retry backoff, an operator-only stalled-work view, and terminal operator
resolution for an ambiguous Gmail send without permitting an automatic resend,
the bounded recovery observation window, and the curated MCP read projections.
Password rotation and schema migration after initial deployment must use an
explicit migration, never volume deletion.

### Existing-volume daily reporting migration

Back up the initialized database first. Keep the Rust MCP reporting boundary
disabled and leave `MCP_REPORTING_DATABASE_URL` and
`MCP_REPORT_REFRESH_DATABASE_URL` unset until every database migration and the
authenticated database healthcheck below have succeeded. Do
not run `down -v`, remove `mcp-ozon-position-data`, or recreate the named volume.

The database image now owns a checksum-protected migration ledger in
`mcp_runtime.schema_migrations`. It refuses changed checksums, interrupted
`applying` rows, unknown newer migrations, and an existing schema without a
reviewed baseline. On the first upgrade from the pre-ledger image, rebuild
`position-db`, confirm the verified backup, and baseline exactly once. Later
upgrades run only the normal migrator:

```bash
docker compose --env-file .position.env -f compose.position.yaml exec -T position-db \
  migrate-position-db --baseline-current
docker compose --env-file .position.env -f compose.position.yaml exec -T position-db \
  migrate-position-db
```

The migrator preserves the required order between migration `018` and `019`
and never retries a migration left in `applying`: restore or reconcile that
state explicitly instead of guessing whether a one-shot transaction committed.

The migrations are transactional. The collection-claim migration gives each
exact account/marketplace/cutoff a fifteen-minute lease with a monotonically
increasing fencing generation. New source snapshots must carry the live claim,
and completion of all required sources is atomic (five for Ozon, four for
Wildberries). Existing published snapshots
remain readable without rewriting their provenance. The explicit canary
collectors resolve only the claimed account's marketplace credentials after
claim acquisition; a busy or completed claim reads no marketplace secret. The
migrations create no scheduler and send no email. The reconciliation migration
does not infer a provider outcome: it only gives the restricted report worker
an append-only, exact-attempt path after an operator has checked Gmail.

After all migrations commit, rebuild and recreate only `position-db` to install
the matching authenticated healthcheck, while retaining the named volume. The
explicit healthcheck invocation must succeed before the MCP reporting reader is
enabled:

```bash
docker compose --env-file .position.env -f compose.position.yaml build position-db
docker compose --env-file .position.env -f compose.position.yaml up -d \
  --no-deps --force-recreate --wait position-db
docker compose --env-file .position.env -f compose.position.yaml exec -T position-db \
  /usr/local/bin/position-db-healthcheck
```

Only after this database-first gate succeeds may the deployment secrets
`MCP_REPORTING_DATABASE_URL` for `position_reader` and
`MCP_REPORT_REFRESH_DATABASE_URL` for `report_refresh_requester` be configured
and the Rust MCP service be recreated. Do not print either URL or password in
logs or shell history.

### Enable the MCP reporting reader (explicit opt-in)

`compose.reporting-reader.yaml` is deliberately not part of the default MCP
deployment. It attaches only the MCP server to the already-created
`mcp-ozon-position-internal` network and passes two independent credentials:
the read-only `position_reader` URL and the function-only
`report_refresh_requester` URL. It does not start, migrate, or depend on the
database service, and it does not expose the admin, collector, or worker
passwords to the MCP container.

Before enabling it, verify an integrity-checked protected backup (encrypt it
when it leaves the protected host), migrations through `025`, the database
healthcheck above, and a protected `.position.env` whose reader password is
URL-safe (the documented hexadecimal bootstrap value is URL-safe). Existing
installations must add a separate URL-safe
`REPORT_REFRESH_REQUESTER_DB_PASSWORD`; never reuse the reader, collector,
worker, administrator, or application password. The bootstrap script emits it
only while creating a new `.position.env` and deliberately refuses to overwrite
an existing file.
Use `.position.env` only for Compose interpolation; never add it as the MCP
service's `env_file`.

Render-check the exact merged deployment without printing the resolved secret:

```bash
MCP_ACCESS_CONFIG_HOST="$HOME/.local/share/mcp-ozon-runtime/access.json" \
docker compose --env-file .position.env \
  -f compose.yaml -f compose.reporting-reader.yaml \
  config --quiet
```

Then recreate only the MCP server. The external database network must already
exist because the separately managed position stack is running:

```bash
MCP_ACCESS_CONFIG_HOST="$HOME/.local/share/mcp-ozon-runtime/access.json" \
docker compose --env-file .position.env \
  -f compose.yaml -f compose.reporting-reader.yaml \
  up -d --build --force-recreate --wait --wait-timeout 300 server
```

If the operator uses another protected access registry, replace the example
host path above with that exact file. Startup fails closed when the network,
database, migration, credentials, or read ACL contract is invalid. Verify the
MCP health endpoint and call `ofk_collection_status` with an authenticated MCP
client; do not print the container environment or the fully rendered Compose
JSON during secret-bearing operation.

Rollback is a base-only recreation. It removes the reader URL and detaches the
MCP server from the database network without stopping PostgreSQL or touching
the named volume:

```bash
MCP_ACCESS_CONFIG_HOST="$HOME/.local/share/mcp-ozon-runtime/access.json" \
docker compose -f compose.yaml \
  up -d --force-recreate --wait --wait-timeout 300 server
```

Do not use `down -v`. Re-running `scripts/install-local-runtime-agent.sh`
currently performs the same base-only MCP recreation and therefore disables
this opt-in reader; reapply the merged command only after all gates still pass.

### Existing-volume Ozon collector migration

Do not apply this migration to production merely because it exists in the
image. First create and verify an encrypted backup. The migration is
transactional and fails without deleting or coercing rows if an existing
monitor uses an interval other than 30 minutes, searches beyond top 100, has a
non-numeric Ozon product ID, or if multiple existing runs map to one half-hour
slot.

After reviewing those preconditions, use only the ledger-backed migrator from
the preceding section. Direct execution of this SQL file is unsupported because
it would bypass checksum and ordering evidence.

The migration adds exact slot idempotency, the one-way Ozon run state machine,
non-lossy placement, published reader views, a durable circuit and request
budget tables/functions. It revokes reader access to raw Ozon runs,
measurements and alerts. Applying the migration does not start a collector or
make a marketplace request.

The migrator applies the adapter digest migration after the collector contract
in the same reviewed maintenance window.

It adds an immutable SHA-256 payload digest. Existing historical rows receive a
non-replayable zero marker; no measurement or terminal run is mutated.

### Existing-volume WB migration

Back up the initialized database first. Then use only the ledger-backed
migrator from the preceding section; direct execution of the additive SQL is
unsupported.

The migration is transactional and, on an existing installation, grants only
the new WB tables/sequences to the already-created restricted roles. The ledger
applies it exactly once. After it commits, rebuild and recreate only the database
container so that the new authenticated healthcheck is installed; the named
data volume is retained:

```bash
docker compose --env-file .position.env -f compose.position.yaml build position-db
docker compose --env-file .position.env -f compose.position.yaml up -d \
  --no-deps --force-recreate --wait position-db
```

Do not run `down -v` and do not remove `mcp-ozon-position-data`.

### Recovery from a failed first bootstrap

The authenticated healthcheck verifies an admin login, Ozon and WB schema
objects, WB semantic flags and immutability triggers, application-role database
and table ACLs, append-only collector access, and the reader's read-only
default.
Application passwords are not exposed in the healthcheck command. A server that
merely accepts PostgreSQL connections is therefore not considered healthy while
role bootstrap is incomplete.

If the **very first** bootstrap fails, inspect it before retrying:

```bash
docker compose --env-file .position.env -f compose.position.yaml logs position-db
```

After correcting `.position.env`, and only after confirming that this is a new
volume containing no application data, discard the incomplete initialization
and start it again:

```bash
docker compose --env-file .position.env -f compose.position.yaml down
docker volume rm mcp-ozon-position-data
docker compose --env-file .position.env -f compose.position.yaml up -d --wait
```

Never remove this volume after collection has begun. For an initialized
deployment, recover from an encrypted backup and apply an explicit migration
instead.

No screenshots, raw HTML, cookies, authorization headers, or Excel files belong
in this database. Excel workbooks are generated on demand from bounded MCP query
results.
