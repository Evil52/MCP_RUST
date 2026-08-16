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
- a frozen snapshot-manifest contract requiring exactly one sales,
  advertising, stock and price source per scoped account, with source-specific
  freshness limits and fail-closed recommendation suppression for partial or
  stale data.

The pilot example is disabled and scopes the temporary owner report to Diana's
Ozon account and Anna Agzamova's Wildberries account. Sender and recipient
addresses are referenced only by environment-variable names. Passwords, OAuth
tokens and actual addresses must not be committed.

## Not implemented yet

The snapshot manifest and normalized PostgreSQL storage contract are present,
but no marketplace report collector is wired yet. No scheduler process,
marketplace snapshot job, HTML/XLSX generator, S3 writer or mail provider is
wired. Consequently this phase cannot send email and cannot affect a
marketplace. Search-position collection remains disabled.

The persistence layer transactionally stores report occurrences, delivery
coverage and attempts. Its unique occurrence key is
`(local_date, kind, recipient_id, report_version)`. Consolidated mail covers two
such keys in one delivery; either both keys are committed or neither is. A
crash before a provider call leaves work claimable. A crash after a delivery is
claimed deliberately leaves the batch in `sending` for operator reconciliation:
an ambiguous provider result is never retried automatically.

## Planned pilot

1. Wire read-only marketplace collectors into the normalized snapshot contract.
2. Generate deterministic HTML and XLSX from one frozen snapshot manifest.
3. Add the scheduler/runtime around the persisted PostgreSQL outbox.
4. Run dry mode for Diana and Anna with delivery disabled.
5. Connect S3-compatible artifact storage and one service mailbox.
6. Send previews to the temporary owner recipient at 08:00 and 17:00 EKB.
7. Reconcile figures for five to seven days before enabling other managers.

ChatGPT remains the interactive analytics interface. Scheduled collection,
calculation, artifact generation and delivery must remain server-side so they
continue to work when ChatGPT is unavailable.
