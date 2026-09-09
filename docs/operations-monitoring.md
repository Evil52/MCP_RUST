# Operational monitoring contract

## PostgreSQL session contention

The application's existing HTTP `/metrics` surface exports fixed, unlabelled,
process-local measurements from `mcp-storage`; scraping them never takes the
database mutex or performs SQL. They contain no account, statement, URL or
credential labels. Worker processes without that HTTP surface do not become
remotely scrapeable automatically; each `SupervisedClient` also provides a
local `session_metrics()` snapshot for diagnostics.

- `mcp_postgres_session_waiters` / `mcp_postgres_sessions_held`: current gauges.
- `mcp_postgres_session_wait_seconds_count` / `_sum`: completed mutex waits,
  including cancellation; the sum is in seconds.
- `mcp_postgres_session_cancelled_waits_total`: waits dropped before acquisition.
- `mcp_postgres_session_hold_seconds_count` / `_sum`: completed exclusive
  ownership, including reconnect/verification while holding the mutex.
- `mcp_postgres_session_wait_max_seconds` / `hold_max_seconds`: maxima since
  process start, not percentiles or sliding-window values.

All totals reset on process restart. Concurrent snapshots are approximate, not
transactionally consistent. Durations use monotonic time and bounded counters;
finished intervals are recorded on drop, so a currently stalled holder is not
yet reflected in its duration sum. These are session/mutex metrics, not SQL
execution timings. Summary series have count/sum only, without quantiles.

Compare `rate(wait_seconds_sum[5m]) / rate(wait_seconds_count[5m])` (using the
full metric prefix) with request latency and cancellation rate; no observations
means unavailable, not zero. Process totals identify contention but not its
repository. Inspect per-client snapshots and SQL plans before deciding whether
to add a small read pool. A pool must not share Control advisory-lock or
transaction ownership, broaden database roles or bypass concurrency budgets.

## Runtime and reporting health

`scripts/check-runtime-health.sh` checks the always-on deployment, backup and
restore evidence, database-backed WB automation state, and stalled reporting
work. Its default Compose contract contains only the always-on base services:

- `position-db`
- `ozon-egress`

`position-collector`, `report-collector`, and `report-worker` are disabled by
default and have separate guarded cutovers. They must not be treated as missing
until an operator enables them. When an optional runtime is enabled, install
the operations agents with the complete expected service set, for example:

```bash
MCP_HEALTH_REQUIRED_SERVICES=position-db,ozon-egress,position-collector \
  ./scripts/install-operations-agents.sh
```

For the snapshot-first reporting rollout the production contract is
`position-db,ozon-egress,report-collector`; keep `report-collector` in the
required set after the guarded live overlay has been activated. The base
Compose mode remains disabled so a repository checkout or ordinary database
restart cannot start marketplace collection by itself.

The installer persists both `MCP_HEALTH_REQUIRED_SERVICES` and
`MCP_HEALTH_REQUIRED_LAUNCH_AGENTS` in the health LaunchAgent. The health probe
rejects empty or malformed comma-separated contracts. Do not remove an enabled
service from the contract merely to silence a finding.

The installer intentionally refuses to schedule backups without an executable
offsite-copy hook, unless the operator explicitly sets
`MCP_BACKUP_ALLOW_LOCAL_ONLY=true` to record that accepted risk. It proves one
encrypted backup and disposable restore before installing any LaunchAgent.

For enabled reporting, configure both `MCP_HEALTH_REPORTING_POLICY` and
`MCP_HEALTH_REPORTING_REGISTRY` with the collector's validated policy and access
registry. The installer preserves private copies for the installed health
probe. This contract is opt-in: unset paths or an explicitly disabled policy
add no snapshot expectations to the base deployment. An incomplete pair,
unreadable metadata, unknown account or invalid scope produces a finding.
When `report-collector` belongs to the required-service contract, an absent or
disabled reporting scope is itself a finding and the installer rejects it.

The probe resolves each policy account to its exact marketplace and checks the
latest 08:00 or 17:00 Yekaterinburg cutoff whose inclusive 30-minute collection
window has closed. All mandatory sources must have succeeded with complete
pagination at that exact cutoff: five for Ozon (including finance), four for
WB. Absent, old or partial snapshots therefore alert even when every container
is healthy. A current manual refresh cannot substitute another cutoff.

Recent failed refreshes, expired refresh leases, expired queue requests and
expired active collection claims also produce scoped findings. A newer refresh
supersedes an older failed request. Failures/claims older than the expected
scheduled cutoff do not permanently alarm after successful scheduled recovery;
unresolved queued/running refreshes remain visible until resolved. These checks
use existing database evidence in a read-only session. They detect missing
scheduled output, but do not provide a process heartbeat before a cutoff is due.

Validation without marketplace requests:

```bash
python3 -B -m unittest discover -s tests -p test_reporting_health_contract.py
./scripts/with-position-test-db.sh cargo test --locked --test reporting_health_sql
```


## Permanent operations resources

The operations installer persists `MCP_OPS_POSTGRES_IMAGE` as an immutable
PostgreSQL image reference and copies its reporting-health helper/SQL alongside
the installed scripts. Backup and health jobs receive private env paths and no
longer need the original checkout. Deleting a temporary release directory must
not disable their schedules. Manual developer invocations may still resolve
the image from `position-monitor/Dockerfile`.

`MCP_OPS_POSITION_ENV_SOURCE` can select an existing mode-600 database env file
when installing from a clean release checkout. When reporting monitoring is
configured, the installer validates the policy and registry and copies them to
the private runtime `ops` directory. Reinstall after changing that expected
scope. The portability regression deletes the source checkout before exercising
both installed jobs.

## Notifications, recovery and independent heartbeat

`operations_notify.py` and `operations_heartbeat.py` are installed alongside
the health script. They provide a delivery contract without choosing a
notification provider or an external monitoring service. Configuring these
helpers alone does not send anything: the operator must supply each executable
hook and its private destination/credentials.

| Variable | Default | Contract |
| --- | --- | --- |
| `MCP_HEALTH_NOTIFY_COMMAND` | empty | One absolute executable; receives the health report on stdin. |
| `MCP_HEALTH_HEARTBEAT_COMMAND` | empty | One absolute executable; receives a small core-availability JSON event on stdin. |
| `MCP_HEALTH_HOOK_TIMEOUT_SECONDS` | `10` | Each hook has a total deadline from 1 to 30 seconds. |
| `MCP_HEALTH_EVENT_STATE_DIR` | `$MCP_RUNTIME_DIR/ops/health-events` | Private notification delivery state; directory0700, files0600. |
| `MCP_HEALTH_CHECK_TUNNEL` | `false` | Enable explicitly for a deployment that requires the local tunnel. |
| `MCP_HEALTH_TUNNEL_URL_FILE` | `$HOME/Library/Application Support/tunnel-client/health/ozon-local.url` | File containing only the loopback HTTP base URL, including its dynamic port. |
| `MCP_HEALTH_TUNNEL_POLL_STALE_SECONDS` | `90` | Maximum age of the last successful control-plane poll, from 1 to 600 seconds. |

Hooks execute directly, without a shell or command-line credentials.
`MCP_HEALTH_EVENT` identifies `alert`, `recovery`, or `heartbeat`. Notification
stdin starts with the event type followed by the human-readable health report;
heartbeat stdin is JSON with `version`, `event`, `core_available`, and
`observed_at`. A hook must return zero only after its destination has accepted
the event. Hook output is discarded to avoid exposing credentials in health
logs. Timeout or nonzero exit becomes a visible health finding; timeout or
TERM/INT interruption kills the hook process group, including its child HTTP
client. Neither helper retries
external delivery within the same probe.

Notifications are sent when the set of actual findings changes. Time-varying
ages have stable finding keys, so an unchanged incident does not generate a new
notification every fifteen minutes. A transition from previously delivered
findings to a clean check sends a recovery event. Initial clean checks are
silent. Only acknowledged deliveries update the private fingerprint; a failed
delivery is attempted again by the next scheduled probe. A crash after remote
acceptance but before the local state replacement can duplicate an event.
Changing the hook path or executable contents automatically resends ongoing
findings to the replacement receiver. When changing a destination only in the
hook's external configuration, reset its `delivered.json` for the same effect.
The state contains only finding/hook fingerprints and a count, and can be
recreated after recovery. SIGKILL or a host crash cannot run process cleanup;
the external receiver must still detect a missing heartbeat independently.

The heartbeat represents core availability: Docker, healthy base services,
main MCP container/readiness, a completed read-only database probe, and the
tunnel when that check is enabled. The tunnel probe reads `/healthz`, `/readyz`,
and `/metrics` from the validated loopback address with proxy use and redirects
disabled, bounded responses and deadlines. It requires exactly one finite,
positive, nonfuture `commands_poll_last_successful_timestamp_seconds` sample.
It checks poll freshness after SQL and immediately before heartbeat delivery.

A WB or Ozon guard lock, reporting finding, backup finding, or failed alert
delivery does not falsely classify an available MCP as a core outage. Such
findings still affect the health report and notification channel. A failed DB
probe or unavailable/stale tunnel suppresses the success heartbeat; its external
receiver must detect the missing pulse independently. Heartbeat delivery errors
are also reported locally and through the configured notification hook.

For the backup and restore-verification LaunchAgents, a recorded nonzero last
exit code generates a finding immediately, even while the previous archive is
still younger than the stale-backup threshold. Other agents' exit codes are not
interpreted this way: in particular health exit1 describes findings, not a
broken health job. A missing or `never exited` result is unknown, not successful
execution; fresh backup/restore artifacts provide the separate proof until the
periodic job has run. The monitor never restarts a job to clear its failure.

Offline validation uses temporary local hooks, a fake Docker CLI and a loopback
tunnel fixture, with no marketplace requests or real external notifications:

```bash
python3 -B -m unittest discover -s tests -p test_operations_notifications.py
```
