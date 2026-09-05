# ADR 0001: PostgreSQL time-series retention and capacity

- Status: accepted for implementation planning
- Date: 2026-09-05
- Scope: `search_position`, `daily_reporting`, structured MCP telemetry, and
  marketplace/control audit history

## Decision

PostgreSQL remains the authoritative normalized store, but unbounded history is
not an operational contract. Retention will be enforced by a separate,
least-privilege maintenance workflow after backup/restore rehearsal; ordinary
collectors and MCP roles never receive `DELETE` privileges.

The initial online windows are:

| Data | Online retention | Archive requirement |
| --- | ---: | --- |
| normalized collection staging | 24 hours after its claim becomes terminal | none |
| MCP tool-call telemetry | 90 days | aggregate counts only |
| raw position measurements and WB search/bid snapshots | 90 days | monthly encrypted archive when needed |
| published reporting snapshots and fact tables | 400 days | encrypted yearly archive before removal |
| delivery/generation attempts and terminal report batches | 400 days | preserve manifest and provider receipt audit |
| campaign/control and WB automation audit events | 400 days minimum | extend if the business audit policy requires it |

Rows referenced by unresolved, active, `sending`, or otherwise ambiguous work
are never eligible. A retention pass must select an explicit closed time range,
prove that all parent/child rows are terminal, create and verify a protected
backup, delete children before parents in one bounded transaction per partition
or chunk, and finish with `VACUUM (ANALYZE)`. Failure leaves the remaining range
untouched and visible to operations.

Monthly range partitioning is the preferred implementation for measurements,
WB snapshots, source snapshots/facts, tool telemetry, and append-only audit
events. Fact partitions need a stable time key copied from their immutable
snapshot, not a numeric `snapshot_id` range that can cross calendar boundaries.
Until that schema exists, a chunked maintenance function is acceptable only
with a dry-run row/byte estimate, statement timeout, lock timeout, and an
operator confirmation naming the exact cutoff.

## Capacity budget

The current local database is approximately 20 MiB and reporting fact tables
are empty, so it is not a useful steady-state sample. Capacity is based on row
rates instead:

- 500 position monitors every 30 minutes produce 24,000 measurements/day, or
  8.76 million/year. Heap plus current indexes is expected to consume roughly
  3-6 GB/year before temporary maintenance headroom.
- Fourteen marketplace accounts collected twice daily can add roughly
  5-20 GB/year of normalized sales, advertising, finance, warehouse-stock and
  price facts; SKU and warehouse cardinality dominate this range.
- Outbox, audit, refresh and MCP telemetry should normally remain below
  1-3 GB/year, but they are still bounded by the windows above.

Provision at least a 50 GB PostgreSQL volume for one online year. Prefer 100 GB
for two years of growth, staging, index builds, autovacuum/WAL bursts and one
safe migration copy. Encrypted backups and off-site replicas are separate from
this live-volume budget. Alert at 60% and 75% usage; stop nonessential
collection before 85% rather than allowing PostgreSQL to exhaust the volume.

## Consequences

Hot candidate and latest-position queries stop growing with terminal history,
while audit evidence remains available for a defined period. Implementing the
maintenance worker still requires a dedicated migration, role, dry-run tests,
restore proof, metrics for oldest/newest retained timestamps, and an operations
runbook. This ADR does not authorize deletion from production by itself.
