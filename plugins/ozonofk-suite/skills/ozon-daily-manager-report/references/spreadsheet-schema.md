# Daily archive spreadsheet schema

Use this schema only when the user requests XLSX, CSV, Google Sheets or a
recurring archive. Load and follow the available Spreadsheets skill for file
creation, formulas, formatting and render/verification requirements.

## Workbook tables

### `report_runs`

One row per generated account report.

`report_date | account_id | account_name | manager_id | report_kind | cutoff_at | timezone | data_state | recommendations_allowed | qa_passes | qa_gates_passed | qa_defects_fixed | limitations`

Primary key: `report_date + account_id + report_kind + cutoff_at`.

### `account_kpis`

One row per account, comparison window and cutoff.

`report_date | account_id | window | business_date_from | business_date_to | cutoff_at | ordered_units | operational_gmv_rub | ad_spend_rub | attributed_orders | attributed_revenue_rub | ctr_pct | cpc_rub | ad_conversion_pct | cpo_rub | drr_pct | buyout_rate_pct | data_state`

`window` is one of `D`, `D-1`, `D-7`, `BASELINE_7D`. Leave unavailable cells
blank and set `data_state`; never write a fabricated zero.

### `metric_deltas`

`report_date | account_id | metric | comparison | current_value | comparison_value | absolute_delta | relative_delta_pct | formula_status`

Use spreadsheet formulas for deltas. Guard division by zero and missing bases;
return blank/`N/D` status instead of an error, infinity or `0%`.

### `source_quality`

`report_date | account_id | cutoff_at | source | status | quality | pagination_complete | row_count | source_as_of | error_class | http_status`

### `actions`

`report_date | account_id | manager_id | sku | priority | action_kind | observed | threshold | impact_rub | action_today | expected_effect | approval_requirement | action_origin`

Primary key: `report_date + account_id + sku + action_kind`. Set
`action_origin` to `server` for P1/P2 results or `diagnostic` for P3 work.

### `evidence`

`report_date | fact_id | account_id | period_or_cutoff | metric | raw_value | raw_unit | normalized_value | normalized_unit | source_tool | source_timestamp | quality`

Primary key: `report_date + account_id + fact_id`.

### `targets`

Keep business targets separate from actuals:

`effective_from | effective_to | account_id | sku | metric | target_value | unit | source | approved_by`

Never invent a target or copy a model suggestion into this table.

## Workbook rules

- Keep raw evidence and source quality separate from presentation tables.
- Store dates as dates and cutoffs as timezone-aware text or documented UTC;
  never mix local and UTC values without a timezone column.
- Store numeric values as numbers, not formatted strings. Apply RUB and percent
  number formats in the workbook.
- Use stable keys and append new report dates; do not overwrite prior runs.
- Create charts only from complete comparable rows with one unit per series.
- Before delivery, verify formulas, missing-value behavior, filters, frozen
  headers, readable widths and absence of spreadsheet errors.
