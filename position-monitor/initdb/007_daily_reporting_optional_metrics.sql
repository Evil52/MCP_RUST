-- Seller Analytics may expose only the universally available sales metrics
-- for a given account. NULL means "not supplied by this source", never zero.
ALTER TABLE daily_reporting.sales_facts
    ALTER COLUMN cancelled_units DROP NOT NULL,
    ALTER COLUMN cancelled_units DROP DEFAULT,
    ALTER COLUMN returned_units DROP NOT NULL,
    ALTER COLUMN returned_units DROP DEFAULT;
