# Ozon finance collector audit — 2026-09-13

The background finance collector already calls the accrual endpoints. This
change fixes two classification errors and adds regression coverage; it is
not evidence of complete financial detail or verified account permissions.

## Upstream evidence

Ozon announced 2026-09-08 as the shutdown date for
`/v3/finance/transaction/list` and `/v3/finance/transaction/totals` in its
[verified Seller API channel](https://t.me/s/OzonSellerAPI),
[message 684](https://t.me/OzonSellerAPI/684).

The [linked Ozon notice](https://dev.ozon.ru/news/783-Izmeneniia-v-metodakh-dlia-raboty-s-finansovymi-otchetami-v-Seller-API/)
and [Seller API reference](https://docs.ozon.ru/api/seller/) returned redirect
loops during this audit. The bundled `docs/ozon-seller-api.postman.json`
contains the old transaction endpoints and no accrual endpoint schema. It
cannot verify the current accrual response contract. No seller credentials
or live marketplace responses were accessed.

## Verified collector path

`source_collection::collect_source` routes Ozon finance jobs to
`OzonReportSource::collect_finance_pages`, then
`ozon_finance_source::collect_finance_facts_checkpointed`. The only requests
in that collector are:

| Operation | HTTP | Path | Purpose |
| --- | --- | --- | --- |
| Dictionary | POST | `/v1/finance/accrual/types` | Read accrual type descriptions |
| Day page | POST | `/v1/finance/accrual/by-day` | Read one date with `last_id` pagination |

`collect_required_seller_facts` also calls the same accrual collector,
without durable page checkpoints. Neither collector path calls the old
transaction endpoints or falls back to them after an error.

The MCP catalog separately retains `ozon_finance_transactions` and
`ozon_finance_totals`, described as deprecated, along with the newer accrual
tools. Their registration does not mean the collector uses them. They are
not removed or silently redirected in this change. `/accrual/postings`
is an existing direct MCP read, not a source used by the daily collector.

## Changes and retained safeguards

- A generic `accrual_id` is no longer treated as a dictionary `type_id`.
  Explicit `type_id` determines classification. With an accrual identifier
  but no explicit type, the complete signed amount is retained as `other`
  and counted in `unknown_type_count`. A coincidental numeric ID match
  cannot turn an unknown accrual into a sale or fee category.
- The word fragment `платн` no longer means paid acceptance. Previously
  `Платная реклама` was classified as acceptance before advertising was
  considered. Acceptance matching now uses acceptance terminology and
  supports both `приемка` and `приёмка` spellings.
- Ozon dictionary/page checkpoint identities use a `v2` namespace so
  in-progress jobs cannot replay facts classified by the old rules.
- Existing bounds remain: at most 100 pages/day, 10,000 rows/page, bounded
  cursors, repeated-cursor rejection, exact RUB minor-unit parsing,
  checked arithmetic, and refusal to publish partial results after an
  invalid or failed page. Multi-SKU posting totals are left unattributed.

## Validation

Focused tests cover exact accrual request paths and day/cursor progression,
failed-page rejection without legacy fallback, replay across page quanta
without double counting, explicit/ambiguous/unknown type identifiers,
advertising classification, signed amounts, overflow and parser bounds.

Passed: `cargo test --locked --lib reporting::ozon_finance_source::tests -- --test-threads=1`
(10 tests), file formatting and `git diff --check`. Full shared checks belong
to the integrated change; no live marketplace validation was performed.
The shared source-collection fixture must use the `ozon_finance_types_v2`
and `ozon_finance_day_v2` checkpoint keys.

## Remaining work before full financial detail

The persisted contract is an aggregate by business date, optional SKU and
category, with amount and line/unknown counts. It does not preserve accrual
IDs, posting IDs, service-level components or the raw ledger. Type names
are still classified heuristically; a known category is not a vendor
guarantee of exact accounting attribution.

Obtain a current official schema or a controlled read-only, sanitized
response fixture before implementing nested posting commission, delivery,
item-fee and account-fee detail. Verify which field identifies each
component, whether totals include those components, and how to reconcile
them without double counting. Do not claim that `total_amount` plus the
current aggregate schema is a complete SKU profit ledger.

Actual cabinet key permissions, subscriptions, historical completeness,
and source freshness remain unverified. This local change does not alter
credentials, production configuration, database contents or deployment.
