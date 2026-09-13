# Shared marketplace quotas

The read MCP, reporting collector, WB automation and Ozon/WB write clients
coordinate departures through PostgreSQL. This bounds the combined traffic of
participating processes. It does not increase vendor quotas, change collection
frequency, or control another integration that uses the same seller account.

## Contract

- Apply `032_marketplace_shared_quotas.sql` through the existing migration runner.
- `MCP_MARKETPLACE_QUOTA_DATABASE_URL` selects a restricted application identity
  on the shared position database. Never use an admin URL. The seven allowed
  roles can execute two bounded functions, but cannot read or modify quota rows
  directly. No business-data permissions are added.
- `MCP_MARKETPLACE_QUOTA_REQUIRED=true` makes a missing URL fail closed. A present
  malformed URL or an unavailable database never falls back to a local limiter.
  Both variables absent retain the legacy offline/development mode.
- The database-backed deployment Compose files require coordination. Custom
  launchers must pass the same settings. Partial deployment cannot guarantee
  coordination with an old binary or a process using legacy mode.
- Environment configuration is frozen on first use. One supervised quota
  connection is shared by all clients within each process. Credential/config
  rotation requires a process restart.

Keys are SHA-256 digests of vendor, stable account identity and method group.
Ozon Seller and Performance use their respective Client-Ids. WB uses the
canonical seller `sid` from the configured JWT, so reader/writer tokens and
rotated tokens share an allowance. A missing or malformed SID is rejected in
shared mode. Decoding this claim only groups traffic; it does not authenticate
the token or replace existing registry, scope, lease or approval validation.
Tokens, passwords and vendor payloads are never stored in the quota table.

An atomic database operation either reserves the next departure or returns the
remaining wait. Time is read after the row lock. Denied requests do not move the
deadline. A cancelled or uncertain reservation is not refunded. A vendor
cooldown extends the deadline monotonically and survives restarts.

Provider cooldowns of up to 366 days preserve their full duration. Larger
delays store an infinite deadline, blocking that key until administrator
reconciliation; callers receive a bounded one-day retry delay. Later shorter
cooldowns never shorten or clear that block. Departure intervals remain
limited to one day; this limit does not truncate a provider's Retry-After.

Existing endpoint-specific local guards still apply. The shared profiles add
conservative intervals: Seller general requests 100 ms plus Analytics 65 s;
Performance business requests 1 s and token requests 30 s; WB uses the existing
method-group policy and separate write-method groups. These are application
settings, not claims about current vendor allowances. A complete snapshot may
require many pages/requests. Measure full-account throughput and competing
traffic before increasing collection frequency.

The gate covers each explicit HTTP attempt, including read retries and OAuth.
Automatic protocol retries are disabled where they could bypass the gate.
Quota errors and vendor retry delays remain distinct from marketplace success.
Managers reading published PostgreSQL snapshots do not consume vendor quotas.

Writes run the existing final authorization before the departure reservation.
A quota refusal sends no marketplace bytes and never creates an automatic write
retry. Existing durable workflows may conservatively require reconciliation
after recording dispatch intent. Once a write has departed, failure to persist
a cooldown does not replace its actual vendor/ambiguous outcome.

## Verification and activation

Use `scripts/with-position-test-db.sh` for disposable PostgreSQL verification;
run the library and integration tests with `--include-ignored --test-threads=1`.
The tests exercise independent sessions/clients, same-seller token rotation,
cooldown persistence, invalid coordinator no-send behavior, lock-time clocks,
restricted ACLs and post-permit contention without real marketplace calls.
`scripts/test-position-schema.sh` checks new and existing database migrations.

Deploy migration 032 and the matching WB automation runtime in one coordinated
release. The migration raises `wb_automation_writer`'s connection limit from 2
to 4, allowing two overlapping runtimes to each hold a state session and a quota
session. Older WB binaries strictly require 2 and will reject the migrated
database; the new binary strictly requires 4. Pause WB automation scheduling
during the cutover, apply the migration and install the matching runtime before
resuming it. A rollback of only the WB binary after migration is incompatible.

Before activation, verify the immutable release and migration, every runtime's
required coordinator setting, common database identity, WB token SIDs and
database connection headroom. Run the existing canary and verify real per-source
publication, shared throttles and queue age. Keep API-frequency changes separate
from enabling coordination. Repository tests are not proof of live activation
or of sustainable freshness across fourteen accounts.
