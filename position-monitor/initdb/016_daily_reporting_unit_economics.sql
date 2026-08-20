BEGIN;

-- Seller-managed inputs that do not exist in Ozon's API. Effective periods
-- make historical reports reproducible after a cost or tax rule changes.
CREATE TABLE daily_reporting.unit_economics_inputs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id text NOT NULL CHECK (
        account_id <> '' AND octet_length(account_id) <= 128
        AND account_id ~ '^[A-Za-z0-9_-]+$'
    ),
    sku bigint CHECK (sku IS NULL OR sku > 0),
    sku_key bigint GENERATED ALWAYS AS (coalesce(sku, 0)) STORED,
    category text NOT NULL CHECK (category IN (
        'cost_of_goods', 'tax', 'external_expense'
    )),
    amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
    allocation text NOT NULL CHECK (allocation IN ('per_unit', 'per_day', 'absolute')),
    effective_from date NOT NULL,
    effective_to date,
    description text NOT NULL DEFAULT '' CHECK (octet_length(description) <= 512),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (effective_to IS NULL OR effective_to >= effective_from),
    UNIQUE (account_id, sku_key, category, allocation, effective_from)
);

CREATE INDEX unit_economics_inputs_lookup
    ON daily_reporting.unit_economics_inputs
    (account_id, sku_key, effective_from, effective_to);

REVOKE ALL ON daily_reporting.unit_economics_inputs FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON daily_reporting.unit_economics_inputs TO report_collector;
GRANT SELECT ON daily_reporting.unit_economics_inputs TO report_worker;
GRANT USAGE, SELECT ON SEQUENCE daily_reporting.unit_economics_inputs_id_seq TO report_collector;

COMMIT;
