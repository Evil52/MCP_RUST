# OzonOFK Suite 0.1.0

Two focused skills: DB-only Ozon/WB weekly analytics and the existing daily
Ozon manager report. Marketplace operations remain read-only; explicit refresh
requests enqueue internal PostgreSQL work. No write-runtime/control plugin is
bundled.

This is a **skills-only package for preparation and testing**. It requires an
already connected, authenticated OzonOFK MCP. It does not install a connection,
grant a role, migrate an account, configure a tunnel or enable production
collection. The existing account and deployment are left unchanged.

Canonical sources live under the repository's `skills/`. Run
`bash scripts/build-ozonofk-suite.sh` from the repository to synchronize this
self-contained package; `--check` detects drift, missing files, extra files and
unsafe symlinks. Only an explicit file allowlist is copied. Do not archive the
whole repository, private registry, `.env`, tokens, keys, logs or raw API data.

Before the later account migration, register and test the correct MCP
connection, then add its real mapping via `.app.json` (or the appropriate
`.mcp.json` for a distributed server). Do not embed a guessed connection ID,
tunnel ID or localhost endpoint. A marketplace entry and installation are
deliberately deferred together with that migration.

The shared quality contract is in
`skills/ozon-daily-manager-report/references/data-quality.md`. Its server-side
counterparts are `src/reporting/mcp_read.rs`, `src/server.rs` and the collector
lease/publication tests. The scenario set in `evals/scenarios.json` is for
behavioral acceptance; static validation alone does not prove model behavior.
