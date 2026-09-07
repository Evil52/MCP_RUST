# Operational monitoring contract

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
