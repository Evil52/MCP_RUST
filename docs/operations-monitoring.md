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
