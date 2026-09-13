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
The client starts conservatively at twelve hours. Only an observed successful
Finance HTTP 200 or 204 together with locally classified, unexpired Personal,
read-only, Finance token claims promotes the same token's client gate to sixty
seconds. Local JWT decoding alone never proves access. Unknown, Base, expired
and write-enabled tokens keep the conservative interval. Token aliases share
the gate, and promotion preserves every observed server `Retry-After`, including
delays shorter than the initial twelve-hour reservation. A process restart
forgets this in-memory proof; the collector's durable quota remains required.
Vendor failures do not cause an automatic retry or a credential switch.

The current official [Documents and Accounting contract](https://dev.wildberries.cn/docs/openapi/documents-and-accounting)
also specifies two collector-only reads for Personal and Service tokens:

- `POST /api/finance/v1/sales-reports/list`: date range, a typed daily/weekly
  period, `limit` and `offset`; data begins 2025-01-01. The local client limits
  each request to 1,000 reports and the offset window to 25,000 reports over at
  most 31 days. This endpoint has no `fields` parameter, so summary normalization
  must discard seller names and all other unnecessary fields before storage.
- `POST /api/finance/v1/sales-reports/detailed/{reportId}`: one canonical positive
  int64 report ID, `limit`, `rrdId` and the fixed privacy projection. Local pages
  contain at most 1,000 rows; HTTP 204 alone proves pagination completion.

Both document a one-minute account limit. The documentation does not guarantee
independent quota pools, so all Finance reads deliberately share one gate. The
Chinese mirror also carries a registration-country availability notice; access
must be checked against the actual Russian account. The allowlist admits only
the exact POST methods and canonical numeric ID path, rejecting alternate
verbs, encoded IDs, suffixes, queries and arbitrary hosts. Unexpected successful
statuses such as HTTP 201/202 do not establish access or terminal proof.

## Local collection rules

- Fixed daily period, endpoint, host and projection; 1,000 rows per page,
  25,000 rows per collection, 1,000-page ceiling, at most 31 requested days.
- Only observed HTTP 204 becomes terminal `None`. JSON `null`, empty arrays,
  malformed rows, repeated/backwards IDs and exceeded bounds fail closed.
- Checkpoints retain normalized rows only. Cursor/row validation runs again
  after replay. Incomplete collections never return a successful partial vector.
- Exact string amounts become i128 coefficients plus decimal scales.
  Up to eighteen fractional digits are retained with checked integer bounds.
  The scoped 2026-09-13 pilot observed sixteen places in `vw`; all digits are
  preserved, including this column. Coefficients serialize as canonical decimal
  JSON strings. Legacy int64 numeric checkpoint coefficients remain readable
  without conversion through floating point; new writes use strings.
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
The original foundation used synthetic data. The subsequent scoped read-only
pilot verified the extended decimal precision through redacted schema counters;
tests contain synthetic amounts and no copied financial rows.
