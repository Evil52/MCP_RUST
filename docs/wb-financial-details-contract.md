# WB financial details foundation — 2026-09-13

This change prepares a collector-only transport and strict normalizer. It does
not enqueue production jobs, migrate the database, publish an MCP tool, change
credentials, or claim financial completeness for any account.

## Verified vendor contract

The official [WB financial API documentation](https://dev.wildberries.ru/docs/openapi/financial-reports-and-accounting)
and [official documentation mirror](https://dev.wildberries.cn/en/docs/openapi/financial-reports-and-accounting)
describe `POST https://finance-api.wildberries.ru/api/finance/v1/sales-reports/detailed`.
The `.ru` page was available through indexed excerpts; the mirror exposed the
current full endpoint description. Neither source proves a particular account's
token permissions or subscription.

The JSON request uses `dateFrom`, `dateTo` in Moscow time, `limit`, `rrdId`,
`period`, and a `fields` projection. A 200 response is an array of camelCase
rows. Start with `rrdId: 0`; continue with the last returned row ID until HTTP
204. A short page is insufficient proof. Data availability starts 2024-01-29.
Documented limit: up to 100,000 rows per request.

The mirror distinguishes token tiers: Personal, Service and Base with secret
allow one departure per minute; Base allows two per day, twelve hours apart.
This implementation defaults to twelve hours because the token tier is not
validated. It does not retry vendor failures or switch credentials.

## Local collection rules

- Fixed daily period, endpoint, host and projection; 1,000 rows per page,
  25,000 rows per collection, 1,000-page ceiling, at most 31 requested days.
- Only observed HTTP 204 becomes terminal `None`. JSON `null`, empty arrays,
  malformed rows, repeated/backwards IDs and exceeded bounds fail closed.
- Checkpoints retain normalized rows only. Cursor/row validation runs again
  after replay. Incomplete collections never return a successful partial vector.
- Exact string amounts become integer coefficients plus decimal scales.
  Up to nine fractional digits are retained with checked integer bounds.
  Numeric JSON amounts, excess precision and overflow are rejected. Missing
  or null amounts remain unavailable; no zero is invented.
- The projection excludes buyer IDs, names, tax IDs, contact data, addresses,
  links and product prose. Unknown response fields are discarded. The source
  retains bounded document/operation labels as untrusted report data.

## Reconciliation and deployment gate

`retailAmount`, `forPay`, commissions and other columns are overlapping
measures, so these rows deliberately do not become additive `FinanceCategory`
facts. Refund and correction signs remain exactly as supplied. Unknown
operation names are preserved without guessed categorization. `rrDate` is
retained separately from the requested report period.

Before scheduling, verify a dedicated Finance credential's effective access
and tier with an explicitly scoped read, then implement versioned ledger
storage, account ownership checks, terminal-page publication and reconciliation
against WB report totals. Read-only access is preferred; an authorization
failure must not trigger an automatic read-write-key fallback. A verified
mapping must precede profit aggregation.

Synthetic tests exercise completion, pagination bounds, exact amounts,
refund preservation, missing data, projection privacy and checkpoint replay.
No live marketplace data was used in this change.
