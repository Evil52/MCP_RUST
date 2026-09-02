# Daily Ozon report contract

## Required scope

- Resolve the current actor and accessible Ozon accounts before data calls.
- Use exact local dates and `Asia/Yekaterinburg` cutoffs unless another
  authoritative business timezone is supplied.
- For a completed day `D`, use comparable morning identities: `D` maps to the
  08:00 cutoff on `D+1`; `D-1` maps to `D`; `D-7` maps to `D-6`; the seven-day
  baseline `D-7..D-1` maps to morning cutoffs `D-6..D`.
- For a preliminary report, use only comparable 17:00 evening identities.
- Obtain `ofk_collection_status` and `ofk_data_completeness` for every account.

## Role matrix

| Capability | manager/analyst | finance | admin |
| --- | --- | --- | --- |
| Accessible accounts and collection quality | yes | yes | yes |
| Canonical KPI history | no | yes | yes |
| Server manager actions | no | yes | yes |
| Ready/sent report catalog | no | no | yes |
| Request/status for a deduplicated current-day Ozon refresh | yes | yes | yes |

An RBAC denial is a result, not a reason to switch identity. A manager/analyst
report without canonical history must be explicitly limited; permitted raw
sales or operational drill-downs do not become server-published KPI history.

## Internal evidence ledger

Keep one row for each fact:

`fact_id | account_id | period/cutoff | metric | raw value | raw unit | normalized value | normalized unit | source tool | source_as_of/fetched_at | quality`

- Assign stable working IDs `F-01`, `F-02`, and so on.
- Attach a fact ID or a formula over fact IDs to every number in the draft.
- Never carry values from memory, examples, prior prose or market assumptions.
- A new point request may append evidence; it must not silently replace an
  existing fact without a new source result.
- Do not expose this ledger unless the user explicitly asks for an audit
  appendix. Never expose private reasoning.

## Quality gates

Apply the exact `ofk_data_completeness` result for each cutoff:

| Result | Permitted output |
| --- | --- |
| `COMPLETE` and `recommendations_allowed=true` | Verified facts, bounded interpretation and server actions |
| `PARTIAL` | Confirmed facts with named missing/stale sources; cautious hypotheses only |
| `N/D`, `stale`, `critical`, or recommendations disallowed | No trading recommendations; diagnostic/data-recovery P3 only |

Record source status, `cutoff_at`, `source_as_of`, pagination completeness and
available error class. Never replace missing, null or partial data with zero.

## KPI rules

- Convert `*_minor` to RUB by dividing by 100.
- Convert `*_bps` to percent by dividing by 100.
- Show absolute delta as `D - comparison`.
- Show relative delta as `(D - comparison) / comparison * 100` only for a
  present, nonzero comparison base. Otherwise show `N/D`.
- Average only complete comparable daily points. Name excluded dates; do not
  estimate them. The baseline must not silently change from seven days.
- Do not call operational GMV profit. Calculate margin, profit, ROMI or a
  break-even point only when all required cost inputs are confirmed.

## Cause and action rules

- A cause is established only when a measured drill-down supports it.
  Otherwise label it a hypothesis or diagnostic check.
- Never estimate SKU contribution by eye.
- Use server actions only when returned by `ofk_manager_actions`. Preserve SKU,
  kind, observed value, threshold and impact; red maps to P1 and yellow to P2.
- Maximum: five actions per account and ten in an administrator portfolio
  summary. Keep server ordering. If the tool returns no actions, write
  `серверных рекомендаций нет`; on an error, write `инструмент недоступен`.
- P3 is only for diagnostics, monitoring or data restoration and must not be
  described as a server trading recommendation.
- State approval needs only from a provided policy. If none exists, write
  `согласовать с руководителем` for price, budget, supply or content changes.
- Expected effect may be directional unless a verified calculation model is
  available. Never promise sales or profit growth.

Interpret server action units by kind; do not display the raw integer without
its unit:

| Action kind | `observed` / `threshold` | `impact_minor` |
| --- | --- | --- |
| `advertised_without_stock`, `stockout` | sellable units | divide by 100, RUB |
| `low_stock_cover` | divide by 10, days of cover | divide by 100, RUB |
| `spend_without_orders` | divide by 100, RUB | divide by 100, RUB |
| `high_drr` | divide by 100, percent | divide by 100, RUB |

## Minimal drill-down routing

Use a raw Ozon call only after a published KPI establishes a material anomaly
or the user explicitly asks for that detail. Keep the same account and period.

- Published sales and funnel: `ofk_ozon_sales_analytics`.
- Direct live sales and funnel: `ozon_analytics`, administrators only and only
  for a rare diagnostic. It never becomes a published reporting cutoff.
- Stock and turnover: `ozon_product_stocks`, `ozon_warehouse_stocks`,
  `ozon_stock_turnover`.
- Advertising: `ozon_performance_daily`,
  `ozon_performance_sku_statistics`, then campaign/product/expense tools only
  as needed. These calls are restricted to finance/admin.
- Prices: `ozon_product_prices`, `ozon_live_buyer_prices`.
- Cancellations and returns: matching FBO/FBS posting, cancel-reason and return
  tools for the verified fulfillment path.
- Rating, content and feedback: `ozon_seller_rating`,
  `ozon_product_content_diagnostics`, `ozon_reviews`, `ozon_questions`.

Do not use web search to replace an unavailable OzonOFK source. Do not treat a
raw live response as an immutable published reporting cutoff.

## Explicit current-data refresh

Do not request freshness automatically for an ordinary report. If the user
explicitly asks to refresh or obtain current Ozon data, call
`ofk_request_ozon_sales_refresh` exactly once for each selected account and
then call `ofk_ozon_sales_refresh_status`. Requests from multiple managers are
deduplicated by the server. Never loop on either tool, and never fall back to
direct `ozon_analytics` after `queued`, `running`, or `failed`.

On `succeeded`, read `ofk_ozon_sales_analytics` and cite its exact
`snapshot_cutoff_at` as the freshness boundary. On `queued` or `running`, give
the request ID and state that the fresh snapshot is not published yet. On
`failed`, retain and clearly date the last successful snapshot; do not present
it as current. The refresh is near-real-time asynchronous collection, not a
synchronous Ozon response.

## Final report structure

1. **Управленческий вывод.** One to three sentences: what needs attention
   today, with exact report date and cutoff.
2. **Качество данных.** Account, state, cutoff, source freshness, missing or
   partial sources, pagination and material errors.
3. **Сводка портфеля.** Only when multiple accounts are included. Show concise
   comparable facts and the responsible manager.
4. **KPI и сравнения.** `D`, `D-1`, `D-7`, seven-day baseline, absolute delta
   and relative delta where valid. Keep units in headers.
5. **Отклонения и причины.** Separate verified causes from hypotheses. Include
   only details that change today's decision.
6. **Действия на сегодня.** Priority, risk type, account/manager/SKU, measured
   basis and cutoff, one executable action, directional effect and approval
   need. For quiet accounts, one line with data status is enough.
7. **Ограничения и QA.** Name material restrictions, then the exact compact QA
   line below.

When `ofk_reports` is available to an administrator, include a concise
ready/sent delivery status without recipient addresses, provider identifiers,
artifact paths or hashes.

Avoid repeating the same KPI in multiple sections.

Default to no more than 500 words unless the user requests a detailed audit or
spreadsheet appendix.

## QA loop

One QA cycle is `check -> record defect -> fix -> recheck affected gates`.
Checking without a correction does not count as a corrective cycle.

- **G1 Coverage:** actor, role, accounts, timezone, four comparison windows,
  status and completeness calls are present.
- **G2 Evidence:** every number maps to evidence or a transparent formula.
- **G3 Units:** minor/bps conversion, totals, zero bases and N/D are correct.
- **G4 Causality:** no unsupported cause or estimated SKU contribution appears
  as fact.
- **G5 Actions/access:** RBAC, recommendation gate, server priorities, action
  limits and read-only boundaries are respected.
- **G6 Contract:** every required section exists without unnecessary
  duplication.
- **G7 Relevance:** scope, period, format and role answer the request; every
  action is executable today by the named owner.

Run all gates once. If defects exist, fix them and recheck only affected gates.
If any still fail, run one red-team pass against every remaining number, cause
and action, then fix and recheck. Stop after the initial pass plus at most two
corrective passes.

If a gate remains unresolved, lower the report to confirmed facts, change all
actions to diagnostic P3 tasks and name the gate and cause in limitations.

End exactly with:

`QA LOOP: проходов <число>; ворота <пройдено>/7; исправлено дефектов <число>; ограничения: <кратко или нет>.`

## Chart gate

Use a chart only with complete comparable daily points and consistent units.
Prefer one line chart for a 7–14 day trend or horizontal bars for an account/SKU
ranking. Do not chart N/D, mixed units or sparse points; explain the limitation
instead. Use no more than three calm, decision-relevant charts.
