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
08:00/17:00 EKB reports. It stores immutable report occurrences, consolidated
coverage, bounded delivery state, append-only provider attempts and normalized
sales, advertising, stock and price snapshots. A dedicated `report_collector`
can append and finalize snapshots; `report_worker` can read only terminal
published projections and operate the outbox. It does not store email bodies,
credentials or marketplace payloads.
There is still no `Поисковая видимость за сутки` Dashboard, task registry,
email job, or Excel generation process. These are later consumers of persisted
history. The planned compact view contains collection status and completeness,
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
PostgreSQL. See `docs/search-position-monitoring.md` for the complete reporting
contract.

The initial architecture has five security principals:

- `position_admin` owns the database and manages Ozon monitors and WB targets;
- `position_collector` may read target definitions and append runs and
  snapshots. It cannot change target identity or update/delete WB snapshots;
- `position_reader` is forced into read-only transactions and is the only role
  that the Rust MCP server may use;
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
job is installed yet. Define encrypted backups and a reviewed retention period
before enabling scheduled collection.

The database is not published on a host port. Future collector and MCP services
must join the internal Docker network `mcp-ozon-position-internal` explicitly.
The init files are copied into a small derived PostgreSQL image at build time;
there is no runtime bind mount from the macOS `Documents` directory.

## Bootstrap

The stack can now be started as an inert infrastructure check. It cannot collect
positions until a separately reviewed live-source phase is shipped. When ready:

1. Generate the ignored local secret file with
   `./scripts/bootstrap-position-env.sh .position.env`, or copy
   `.position.env.example` to `.position.env` and generate five different random passwords of at
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
daily reporting outbox migration and normalized snapshot migration.
Password rotation and schema migration after initial deployment must use an
explicit migration, never volume deletion.

### Existing-volume daily reporting migration

Back up the initialized database first. Create or rotate the restricted
`report_worker` and `report_collector` roles by running the current
`003_roles.sh` with all five
password environment variables, then apply the reporting migration exactly
once as the database owner:

```bash
docker compose --env-file .position.env -f compose.position.yaml exec -T position-db \
  sh -c 'PGPASSWORD="$POSTGRES_PASSWORD" REPORT_WORKER_DB_PASSWORD="$REPORT_WORKER_DB_PASSWORD" exec /docker-entrypoint-initdb.d/003_roles.sh'
docker compose --env-file .position.env -f compose.position.yaml exec -T position-db \
  sh -c 'PGPASSWORD="$POSTGRES_PASSWORD" exec psql --no-psqlrc --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"' \
  < position-monitor/initdb/005_daily_reporting_outbox.sql
docker compose --env-file .position.env -f compose.position.yaml exec -T position-db \
  sh -c 'PGPASSWORD="$POSTGRES_PASSWORD" exec psql --no-psqlrc --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"' \
  < position-monitor/initdb/006_daily_report_snapshots.sql
```

The migration is transactional. It creates no scheduler and sends no email.
Rebuild/recreate only `position-db` afterward to install the matching
healthcheck, while retaining the named volume.

### Existing-volume Ozon collector migration

Do not apply this migration to production merely because it exists in the
image. First create and verify an encrypted backup. The migration is
transactional and fails without deleting or coercing rows if an existing
monitor uses an interval other than 30 minutes, searches beyond top 100, has a
non-numeric Ozon product ID, or if multiple existing runs map to one half-hour
slot.

After reviewing those preconditions, apply the migration without deleting or
recreating the named volume:

```bash
docker compose --env-file .position.env -f compose.position.yaml exec -T position-db \
  sh -c 'PGPASSWORD="$POSTGRES_PASSWORD" exec psql --no-psqlrc --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"' \
  < position-monitor/initdb/002_ozon_collector_contract.sql
```

The migration adds exact slot idempotency, the one-way Ozon run state machine,
non-lossy placement, published reader views, a durable circuit and request
budget tables/functions. It revokes reader access to raw Ozon runs,
measurements and alerts. Applying the migration does not start a collector or
make a marketplace request.

After the collector contract is present, apply the adapter digest migration in
the same reviewed maintenance window:

```bash
docker compose --env-file .position.env -f compose.position.yaml exec -T position-db \
  sh -c 'PGPASSWORD="$POSTGRES_PASSWORD" exec psql --no-psqlrc --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"' \
  < position-monitor/initdb/004_ozon_postgres_adapter.sql
```

It adds an immutable SHA-256 payload digest. Existing historical rows receive a
non-replayable zero marker; no measurement or terminal run is mutated.

### Existing-volume WB migration

Back up the initialized database first. Then apply the additive migration to
the running database without deleting or recreating its named volume:

```bash
docker compose --env-file .position.env -f compose.position.yaml exec -T position-db \
  sh -c 'PGPASSWORD="$POSTGRES_PASSWORD" exec psql --no-psqlrc --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"' \
  < position-monitor/initdb/002_wb_official_history.sql
```

The migration is transactional and, on an existing installation, grants only
the new WB tables/sequences to the already-created restricted roles. Apply it
exactly once. After it commits, rebuild and recreate only the database
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
