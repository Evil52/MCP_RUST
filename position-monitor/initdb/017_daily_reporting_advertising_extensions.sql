BEGIN;

ALTER TABLE daily_reporting.advertising_facts
    ADD COLUMN basket_additions integer NOT NULL DEFAULT 0
        CHECK (basket_additions >= 0),
    ADD COLUMN model_attributed_orders integer NOT NULL DEFAULT 0
        CHECK (model_attributed_orders >= 0),
    ADD COLUMN model_attributed_revenue_minor bigint NOT NULL DEFAULT 0
        CHECK (model_attributed_revenue_minor >= 0),
    ADD COLUMN product_price_minor bigint NOT NULL DEFAULT 0
        CHECK (product_price_minor >= 0),
    ADD COLUMN average_cpc_minor bigint CHECK (
        average_cpc_minor IS NULL OR average_cpc_minor >= 0
    ),
    ADD COLUMN cpm_minor bigint CHECK (cpm_minor IS NULL OR cpm_minor >= 0),
    ADD COLUMN cpl_minor bigint CHECK (cpl_minor IS NULL OR cpl_minor >= 0);

CREATE TABLE daily_reporting.advertising_expense_facts (
    snapshot_id bigint NOT NULL,
    source text NOT NULL DEFAULT 'advertising' CHECK (source = 'advertising'),
    business_date date NOT NULL,
    campaign_id bigint NOT NULL CHECK (campaign_id > 0),
    money_spent_minor bigint NOT NULL CHECK (money_spent_minor >= 0),
    bonus_spent_minor bigint NOT NULL CHECK (bonus_spent_minor >= 0),
    prepayment_spent_minor bigint NOT NULL CHECK (prepayment_spent_minor >= 0),
    currency char(3) NOT NULL DEFAULT 'RUB' CHECK (currency = 'RUB'),
    PRIMARY KEY (snapshot_id, business_date, campaign_id),
    FOREIGN KEY (snapshot_id, source)
        REFERENCES daily_reporting.source_snapshots (id, source) ON DELETE RESTRICT
);

CREATE TRIGGER advertising_expense_facts_require_running_snapshot
    BEFORE INSERT ON daily_reporting.advertising_expense_facts
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.require_running_fact_snapshot();
CREATE TRIGGER advertising_expense_facts_append_only
    BEFORE UPDATE OR DELETE ON daily_reporting.advertising_expense_facts
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.reject_fact_mutation();

CREATE OR REPLACE VIEW daily_reporting.published_advertising_facts AS
SELECT snapshot.account_id, snapshot.marketplace, snapshot.cutoff_at,
       snapshot.source_as_of, snapshot.status AS snapshot_status,
       snapshot.pagination_complete, fact.*
FROM daily_reporting.advertising_facts AS fact
JOIN daily_reporting.published_source_snapshots AS snapshot
    ON snapshot.id = fact.snapshot_id;

CREATE VIEW daily_reporting.published_advertising_expense_facts AS
SELECT snapshot.account_id, snapshot.marketplace, snapshot.cutoff_at,
       snapshot.source_as_of, snapshot.status AS snapshot_status,
       snapshot.pagination_complete, fact.*
FROM daily_reporting.advertising_expense_facts AS fact
JOIN daily_reporting.published_source_snapshots AS snapshot
    ON snapshot.id = fact.snapshot_id;

REVOKE ALL ON daily_reporting.advertising_expense_facts,
    daily_reporting.published_advertising_expense_facts FROM PUBLIC;
GRANT INSERT ON daily_reporting.advertising_expense_facts TO report_collector;
GRANT SELECT ON daily_reporting.published_advertising_expense_facts TO report_worker;

COMMIT;
