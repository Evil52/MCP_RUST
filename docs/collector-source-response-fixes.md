# Collector: bounded source-response fixes

These fixes address two source failures observed after enabling independent
collection on release `68c0523`. They do not change marketplace permissions,
the HTTP response budget, the database schema, or source-job retry policy.

## Ozon fulfillment stocks

The `/v4/product/info/stocks` fallback returned `stocks[].type = "rfbs"` in
bounded live reads of both affected accounts on 2026-09-10. The normalizer
previously accepted only FBO and FBS, so otherwise valid pages failed with
`invalid_stocks_response`.

Accept the observed rFBS type as the distinct normalized fulfillment dimension
`RFBS`, following the existing uppercase canonicalization of `FBO` and `FBS`.
Do not merge it into `FBS`, invent a physical warehouse, or accept
arbitrary future type strings. Missing quantities, negative values, malformed
rows and unknown types still fail closed. Existing fulfillment-level stock
aggregation and pagination bounds remain unchanged.

The exact enum is grounded in responses from Ozon itself. The Seller API
documentation was unavailable through the documentation fetcher during this
investigation; no unverified enum extension is inferred from third-party SDKs.

## WB sales-funnel pages

The collector requested 1,000 product cards at once. Rich funnel responses from
the affected account exceeded the existing decoded HTTP response limit of
2 MiB before normalization. A live request for 250 cards succeeded under that
same limit; its serialized, redacted JSON contained approximately 514,000
characters. This is evidence for one page, not proof of complete collection.

Only sales-funnel pages change to 250 cards. WB documents `limit`/`offset`
pagination for this method, with an upper limit of 1,000 cards:
[WB Analytics API](https://dev.wildberries.ru/en/openapi/analytics#tag/Sales-Funnel/operation/postV3SalesFunnelProducts).

The sales-page count bound changes from 25 to 100 to preserve the same bounded
25,000-row window. A full final page still fails closed because completeness
has not been demonstrated. Stock and price page sizes are unchanged, as are
request pacing, deadlines, decoded-body limits and normalized checkpoint
storage limits. A response larger than 250 rows or containing a foreign
business date must not be published as the requested page.

Smaller pages can require up to four times as many requests at the unchanged
pacing. The production `run-sources` mode resumes these pages within the
existing job deadline. The legacy all-source manual collection deadline is
not extended, so a large catalogue can still exceed that shorter deadline.

Sales checkpoint identity includes a new namespace and page size. An older
1,000-row page must never be interpreted as a 250-row page, reused at the wrong
offset, or mistaken for the end of pagination. New-format pages remain
resumable without refetching completed requests.

## Deployment and validation boundaries

Local regression tests and successful bounded API reads do not establish a
production rollout or full reporting readiness. Release through the normal
immutable-image verification and isolated-canary process before replacing the
persistent collector. Preserve its existing policy, credentials, read-only
egress and single-worker fencing.

Already terminal `failed` jobs are not revived by changing the image or
restarting the process. Do not rewrite their status or erase their audit
history as part of this patch. A new scheduled cutoff creates new work;
reprocessing the same failed cutoff requires a separately reviewed recovery
procedure. Successful historical snapshots remain untouched.

After rollout, verify per-source publication, pagination completeness,
`source_as_of`, `observed_from`, and `ofk_data_completeness` for all configured
accounts. Healthy containers and a successful first page are insufficient.
