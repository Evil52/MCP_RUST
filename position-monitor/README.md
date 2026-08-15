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

This directory deliberately contains no store configuration, API token,
cookie, authorization header, browser profile, collector implementation or
scheduler. The database schema and least-privilege roles are ready, but no WB
collection job is deployed by this change.

The initial architecture has three security principals:

- `position_admin` owns the database and manages Ozon monitors and WB targets;
- `position_collector` may read target definitions and append runs and
  snapshots. It cannot change target identity or update/delete WB snapshots;
- `position_reader` is forced into read-only transactions and is the only role
  that the Rust MCP server may use.

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

Do not start this stack until implementation phase 2. When ready:

1. Copy `.position.env.example` to `.position.env`.
2. Generate three different random passwords of at least 24 characters and keep the file mode
   `0600`. Bootstrap rejects the example placeholders, short values, reused passwords, and an
   admin username that collides with either restricted application role.
3. Validate the Compose model:

   ```bash
   docker compose --env-file .position.env -f compose.position.yaml config --quiet
   ```

4. Start only the database:

   ```bash
   docker compose --env-file .position.env -f compose.position.yaml up -d --wait
   ```

The init scripts run only when the named volume is empty. A fresh database
applies the base schema, the additive WB migration, then restricted role grants.
Password rotation and schema migration after initial deployment must use an
explicit migration, never volume deletion.

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
