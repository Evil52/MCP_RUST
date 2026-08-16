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
- a strict routing policy validated against the authoritative access registry.

The pilot example is disabled and scopes the temporary owner report to Diana's
Ozon account and Anna Agzamova's Wildberries account. Sender and recipient
addresses are referenced only by environment-variable names. Passwords, OAuth
tokens and actual addresses must not be committed.

## Not implemented yet

No scheduler process, PostgreSQL outbox adapter, marketplace snapshot job,
HTML/XLSX generator, S3 writer or mail provider is wired. Consequently this
phase cannot send email and cannot affect a marketplace. Search-position
collection remains disabled.

The next persistence phase must transactionally store report occurrences,
delivery coverage and attempts. A unique occurrence key is
`(local_date, kind, recipient_id, report_version)`. Consolidated mail covers two
such keys in one delivery; either both keys are committed or neither is. A
crash/restart must recover pending work without creating a second provider
message.

## Planned pilot

1. Persist normalized snapshots for sales, advertising, stocks and prices.
2. Add a PostgreSQL outbox adapter and restart recovery tests.
3. Generate deterministic HTML and XLSX from one frozen snapshot manifest.
4. Run dry mode for Diana and Anna with delivery disabled.
5. Send previews to the temporary owner recipient at 08:00 and 17:00 EKB.
6. Reconcile figures for five to seven days before enabling other managers.

ChatGPT remains the interactive analytics interface. Scheduled collection,
calculation, artifact generation and delivery must remain server-side so they
continue to work when ChatGPT is unavailable.
