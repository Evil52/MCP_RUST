# Sales publication validation

The September 2026 operational audit found Sales jobs ending in
`invalid_source_publication` after their pages had been collected. A read-only
database check on September 11 distinguished these cutoffs (UTC):

| Cutoff | Published Sales sources | Failed Sales sources |
| --- | --- | --- |
| September 10, 12:00 | 5 of 14 | 9 of 14 |
| September 11, 03:00 | 14 of 14 | 0 of 14 |
| September 11, 12:00 | 4 of 14 | 10 of 14 |

All three ran with the same collector release. The failed afternoon jobs
cover the current business day; the successful morning jobs cover a closed
day. This is stronger evidence for a changing-data pagination problem than
for a universal deployment regression, but does not establish the root cause.

Both Sales collectors concatenate offset pages. A product that moves across
a page boundary while current-day statistics change can appear twice. The
publication validator correctly rejects duplicate `(business_date, sku)`
identities. WB documents offset pagination and hourly report updates in its
[Analytics API](https://dev.wildberries.ru/en/openapi/analytics).
That mechanism is a hypothesis for the historical incident. Failed-job
checkpoints are deleted immediately, and the old logs did not record which
validation failed, so the actual rejected facts could not be replayed.

The new warning `sales publication validation failed` contains fixed reasons
(`duplicate_identity`, `invalid_numeric_range`, `row_limit`) and bounded
counts: checked rows, distinct identities, duplicate rows, invalid SKU/count/
money rows, and a truncation flag. It contains no SKU, business date, account
name, revenue value, raw response or identity digest. The scan is capped at
the existing 25,000-row publication limit. The snapshot constructor also
continues to reject excessive row counts before fact validation.

Metadata errors use a separate `source publication metadata validation failed`
warning with a fixed reason for time range, descriptor/account scope, collector
version, row limit, incomplete pagination, source mismatch or missing observation
timestamps. Metadata is checked first; a rejected time range is never labelled
as a duplicate fact. Metadata warnings also omit all supplied values.

This patch improves diagnosis; it does not claim to repair live Sales
collection. It retains `invalid_source_publication`, terminal failure,
checkpoint cleanup, publication atomicity, and last-good snapshots. It never
merges duplicate facts. Silently deduplicating offset pages could hide missing
products as well as conflicting observations.

Validation includes a local checkpoint replay where two individually valid
pages overlap for each marketplace. It must reject the publication and publish
no snapshot. Unit tests separately cover the emitted duplicate diagnostic,
conflicting duplicate values, numeric ranges, same SKU on different dates,
bounded work, metadata priority and log redaction.

After release, observe both scheduled cutoffs for all configured accounts and
match a failed job with its new warning. Only then select a source-specific
pagination fix based on verified API semantics. Do not reset historical jobs,
deduplicate facts, or assume an unsupported stable sort. The existing
`ozon-dry-run` and `wb-dry-run` commands persist snapshots/canary evidence; they
are not read-only incident replay tools.
