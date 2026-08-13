# Ozon search-position monitoring

This directory contains the storage boundary for periodic Ozon search-position
measurements. It deliberately contains no product, store, search phrase, cookie,
Seller API key, or browser profile.

The initial architecture has three security principals:

- `position_admin` owns the database and manages monitor definitions;
- `position_collector` may read monitor definitions and append runs,
  measurements, and alerts;
- `position_reader` is forced into read-only transactions and is the only role
  that the Rust MCP server may use.

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

The init scripts run only when the named volume is empty. Password rotation and
schema migration after initial deployment must use an explicit migration, never
volume deletion.

### Recovery from a failed first bootstrap

The authenticated healthcheck verifies an admin login, core schema objects,
application-role database and table ACLs, and the reader's read-only default.
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
