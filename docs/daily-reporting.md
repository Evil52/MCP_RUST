# Daily manager reports

The daily reporting subsystem is intentionally separate from search-position
monitoring and from the write-capable Control MCP.

## Current implementation

The first disabled-only phase provides:

- immutable morning and evening identities at 08:00 and 17:00
  `Asia/Yekaterinburg`;
- deterministic D-1 and preliminary same-day reporting intervals;
- catch-up after process downtime without duplicate report identities;
- one consolidated delivery after 17:00 when both daily reports were missed;
- automatic-delivery deadlines at 14:00 and 23:00 local time;
- a five-attempt bounded delivery state machine with explicit permanent and
  transient error classes;
- artifact SHA-256 and provider-message identities without storing message
  bodies or credentials in the state machine; and
- a strict routing policy validated against the authoritative access registry;
- a transactional PostgreSQL outbox with immutable occurrence coverage,
  bounded attempts, `FOR UPDATE SKIP LOCKED` claims and append-only delivery
  audit; and
- a dedicated least-privilege `report_worker` role that cannot read the raw
  marketplace-history schema; and
- a separate least-privilege `report_collector` role that can append normalized
  sales, advertising, stock and price facts but cannot use the delivery outbox;
- transactional source snapshots that start as `running`, become immutable at
  `succeeded`, `partial` or `failed`, verify their persisted row count and
  publish only terminal successful/partial projections; and
- a least-privilege PostgreSQL snapshot writer that validates bounded normalized
  facts before I/O, appends facts and publishes one source snapshot atomically,
  and rejects duplicate account/source/cutoff identities without overwriting; and
- a separate disabled `report-collector` runtime that validates the exact pilot
  account/source plan and the `report_collector` database role without loading
  marketplace credentials or making network requests; and
- a bounded PostgreSQL reader that loads only published source descriptors and
  revalidates the exact four-source manifest before report generation;
- a bounded published-fact reader that selects rows only by the frozen
  snapshot identities, rechecks every persisted row count and rejects foreign
  account data; and
- an account-aware report dataset that aggregates sales by `account + SKU`,
  advertising by `account + campaign + SKU`, and stock across warehouses
  without merging identical SKU numbers from different stores; and
- deterministic integer KPI formulas for ordered units, operational GMV, CTR,
  CPC, advertising conversion, CPO and advertising-revenue DRR; zero
  denominators remain `N/D` and aggregation overflow fails closed; and
- a bounded deterministic rule engine for stockouts, low stock cover,
  advertising without stock, spend without attributed orders and high DRR;
  incomplete manifests suppress actions and only five ranked problems survive;
- a bounded, dependency-free HTML email renderer with no images or external
  resources; it escapes display data and suppresses actions when source quality
  is not complete;
- a bounded six-sheet XLSX renderer for summary, SKU sales, advertising,
  inventory/prices, recommendations and source quality; calculations remain
  server-side, identifiers are written as literal text and no images are used;
- a single deterministic report-bundle constructor that derives the interval,
  account scope and quality from the immutable report key and frozen dataset,
  renders matching HTML/XLSX output and assigns a content SHA-256 plus a stable
  object key; its dry-run inspection performs no filesystem or network I/O;
- an isolated `report-worker` binary that accepts only a restricted
  `report_worker` PostgreSQL URL, access-registry path and strict report policy;
  it validates both least-privilege database contracts before serving and is
  disabled by default;
- hardened `report-collector` and `report-worker` images in
  `compose.position.yaml`; both run as the established non-root UID, have no
  published ports, mount only registry/policy metadata read-only and attach
  only to the internal database network while disabled;
- an idempotent scheduling kernel that reserves separate morning/evening
  outbox identities per audience, treats any existing batch state as covered,
  and permits a missed evening plan after a separate morning plan; and
- a frozen snapshot-manifest contract requiring exactly one sales,
  advertising, stock and price source per scoped account, with source-specific
  freshness limits and fail-closed recommendation suppression for partial or
  stale data.

The pilot example is disabled and scopes the temporary owner report to Diana's
Ozon account and the Vahrusheva/Torsunova Wildberries account. Sender and recipient
addresses are referenced only by environment-variable names. Passwords, OAuth
tokens and actual addresses must not be committed.

## Not implemented yet

The snapshot manifest, normalized PostgreSQL storage contract, atomic snapshot
writer, bounded fact reader and report dataset are present, but no marketplace
report collector adapter is wired yet. No enabled scheduler process, marketplace snapshot job, S3 writer or mail
provider is wired. The HTML/XLSX bundle is generated in memory but is not yet
persisted or connected to the delivery outbox. Consolidated two-period
catch-up rendering remains fail-closed until it has an explicit two-section
template rather than silently presenting one interval as two reports.
Consequently this phase cannot send email and cannot affect a
marketplace. Search-position collection remains disabled.

`report-worker` currently supports `healthcheck` and disabled idle operation
only. It deliberately rejects enabled delivery rather than creating a report
that cannot be persisted and reconciled.

The persistence layer transactionally stores report occurrences, delivery
coverage and attempts. Its unique occurrence key is
`(local_date, kind, recipient_id, report_version)`. Consolidated mail covers two
such keys in one delivery; either both keys are committed or neither is. A
crash before a provider call leaves work claimable. A crash after a delivery is
claimed deliberately leaves the batch in `sending` for operator reconciliation:
an ambiguous provider result is never retried automatically.

## Planned pilot

1. Wire read-only marketplace collectors into the normalized snapshot contract.
2. Add the scheduler/runtime around the persisted PostgreSQL outbox.
3. Run dry mode for Diana and Vahrusheva/Torsunova with delivery disabled.
4. Connect S3-compatible artifact storage and one service mailbox.
5. Send previews to the temporary owner recipient at 08:00 and 17:00 EKB.
6. Reconcile figures for five to seven days before enabling other managers.

ChatGPT remains the interactive analytics interface. Scheduled collection,
calculation, artifact generation and delivery must remain server-side so they
continue to work when ChatGPT is unavailable.
