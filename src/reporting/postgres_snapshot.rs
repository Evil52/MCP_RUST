use std::collections::BTreeSet;

use chrono::{DateTime, NaiveDate, Utc};
use tokio_postgres::{Client, Config};

use crate::postgres::SupervisedClient;

use super::postgres_collector::FinanceCategory;
use super::snapshot::{
    AccountScope, FrozenSnapshotManifest, Marketplace, SnapshotDescriptor, SnapshotSource,
    SnapshotStatus,
};

const MAX_MANIFEST_ACCOUNTS: usize = 64;
const MAX_REPORT_FACT_ROWS: usize = 25_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedSalesFact {
    pub account_id: String,
    pub business_date: NaiveDate,
    pub sku: u64,
    pub ordered_units: u64,
    pub operational_gmv_minor: u64,
    pub cancelled_units: Option<u64>,
    pub returned_units: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedAdvertisingFact {
    pub account_id: String,
    pub business_date: NaiveDate,
    pub campaign_id: u64,
    pub sku: u64,
    pub impressions: u64,
    pub clicks: u64,
    pub spend_minor: u64,
    pub attributed_orders: u64,
    pub attributed_revenue_minor: u64,
    pub basket_additions: u64,
    pub model_attributed_orders: u64,
    pub model_attributed_revenue_minor: u64,
    pub product_price_minor: u64,
    pub average_cpc_minor: Option<u64>,
    pub cpm_minor: Option<u64>,
    pub cpl_minor: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedAdvertisingExpenseFact {
    pub account_id: String,
    pub business_date: NaiveDate,
    pub campaign_id: u64,
    pub money_spent_minor: u64,
    pub bonus_spent_minor: u64,
    pub prepayment_spent_minor: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedFinanceFact {
    pub account_id: String,
    pub business_date: NaiveDate,
    pub sku: Option<u64>,
    pub category: FinanceCategory,
    pub amount_minor: i64,
    pub line_count: u64,
    pub unknown_type_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedStockFact {
    pub account_id: String,
    pub sku: u64,
    pub warehouse_id: String,
    pub sellable_units: u64,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedPriceFact {
    pub account_id: String,
    pub sku: u64,
    pub price_minor: u64,
    pub old_price_minor: Option<u64>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedReportFacts {
    pub sales: Vec<PublishedSalesFact>,
    pub advertising: Vec<PublishedAdvertisingFact>,
    pub advertising_expenses: Vec<PublishedAdvertisingExpenseFact>,
    pub finance: Vec<PublishedFinanceFact>,
    pub stocks: Vec<PublishedStockFact>,
    pub prices: Vec<PublishedPriceFact>,
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum PostgresSnapshotError {
    #[error("published report snapshots are unavailable")]
    Unavailable,
    #[error("published report snapshots violate the frozen manifest contract")]
    InvalidManifest,
}

pub struct PostgresSnapshotRepository {
    client: SupervisedClient,
}

impl PostgresSnapshotRepository {
    pub async fn connect(config: &Config) -> Result<Self, PostgresSnapshotError> {
        let client = SupervisedClient::connect(config, "mcp-ozon-report-worker")
            .await
            .map_err(|_| PostgresSnapshotError::Unavailable)?;
        Ok(Self { client })
    }

    pub fn from_client(client: Client) -> Self {
        Self {
            client: SupervisedClient::preconnected(client, "mcp-ozon-report-worker"),
        }
    }

    pub async fn verify_runtime_contract(&self) -> Result<(), PostgresSnapshotError> {
        // Checked before the guard is taken: the session mutex is not
        // reentrant, and this helper acquires it in its own right.
        self.client
            .verify_session_bounds()
            .await
            .map_err(|_| PostgresSnapshotError::Unavailable)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresSnapshotError::Unavailable)?;
        let row = client
            .query_one(
                "SELECT current_user = 'report_worker' \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.published_source_snapshots', 'SELECT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.published_sales_facts', 'SELECT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.published_advertising_facts', 'SELECT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.published_advertising_expense_facts', 'SELECT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.published_finance_facts', 'SELECT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.published_stock_facts', 'SELECT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.published_price_facts', 'SELECT') \
                    AND NOT has_table_privilege(current_user, \
                        'daily_reporting.source_snapshots', 'SELECT')",
                &[],
            )
            .await
            .map_err(|_| PostgresSnapshotError::Unavailable)?;
        row.get::<_, bool>(0)
            .then_some(())
            .ok_or(PostgresSnapshotError::Unavailable)
    }

    pub async fn load_manifest(
        &self,
        cutoff_at: DateTime<Utc>,
        accounts: Vec<AccountScope>,
    ) -> Result<FrozenSnapshotManifest, PostgresSnapshotError> {
        if accounts.is_empty() || accounts.len() > MAX_MANIFEST_ACCOUNTS {
            return Err(PostgresSnapshotError::InvalidManifest);
        }
        let account_ids = accounts
            .iter()
            .map(|account| account.account_id().to_owned())
            .collect::<Vec<_>>();
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresSnapshotError::Unavailable)?;
        let rows = client
            .query(
                "SELECT id, account_id, marketplace, source, cutoff_at, source_as_of, \
                        period_start, period_end, row_count, pagination_complete, status \
                 FROM daily_reporting.published_source_snapshots \
                 WHERE cutoff_at = $1 AND account_id::text = ANY($2::text[]) \
                 ORDER BY account_id, source",
                &[&cutoff_at, &account_ids],
            )
            .await
            .map_err(|_| PostgresSnapshotError::Unavailable)?;
        let snapshots = rows
            .into_iter()
            .map(|row| {
                let row_count = u32::try_from(row.get::<_, i32>(8))
                    .map_err(|_| PostgresSnapshotError::InvalidManifest)?;
                SnapshotDescriptor::new(
                    row.get(0),
                    row.get(1),
                    parse_marketplace(row.get(2))?,
                    parse_source(row.get(3))?,
                    row.get(4),
                    row.get(5),
                    row.get(6),
                    row.get(7),
                    row_count,
                    row.get(9),
                    parse_status(row.get(10))?,
                )
                .map_err(|_| PostgresSnapshotError::InvalidManifest)
            })
            .collect::<Result<Vec<_>, _>>()?;
        FrozenSnapshotManifest::new(cutoff_at, accounts, snapshots)
            .map_err(|_| PostgresSnapshotError::InvalidManifest)
    }

    /// Loads only the immutable published rows named by a frozen manifest.
    ///
    /// The persisted `row_count` of every source is rechecked before data is
    /// returned. This prevents a partial query, a foreign snapshot or a future
    /// schema mistake from silently producing a plausible-looking report.
    pub async fn load_report_facts(
        &self,
        manifest: &FrozenSnapshotManifest,
    ) -> Result<PublishedReportFacts, PostgresSnapshotError> {
        let expected_total = expected_fact_rows(manifest)?;
        let snapshot_ids = manifest
            .snapshots()
            .iter()
            .map(SnapshotDescriptor::snapshot_id)
            .collect::<Vec<_>>();
        let expected_accounts = manifest
            .snapshots()
            .iter()
            .map(|snapshot| snapshot.account_id())
            .collect::<BTreeSet<_>>();
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresSnapshotError::Unavailable)?;
        let sales_rows = client
            .query(
                "SELECT account_id, business_date, sku, ordered_units, \
                        operational_gmv_minor, cancelled_units, returned_units \
                 FROM daily_reporting.published_sales_facts \
                 WHERE snapshot_id = ANY($1::bigint[]) \
                 ORDER BY account_id, business_date, sku",
                &[&snapshot_ids],
            )
            .await
            .map_err(|_| PostgresSnapshotError::Unavailable)?;
        let advertising_rows = client
            .query(
                "SELECT account_id, business_date, campaign_id, sku, impressions, clicks, \
                        spend_minor, attributed_orders, attributed_revenue_minor, \
                        basket_additions, model_attributed_orders, \
                        model_attributed_revenue_minor, product_price_minor, \
                        average_cpc_minor, cpm_minor, cpl_minor \
                 FROM daily_reporting.published_advertising_facts \
                 WHERE snapshot_id = ANY($1::bigint[]) \
                 ORDER BY account_id, business_date, campaign_id, sku",
                &[&snapshot_ids],
            )
            .await
            .map_err(|_| PostgresSnapshotError::Unavailable)?;
        let advertising_expense_rows = client
            .query(
                "SELECT account_id, business_date, campaign_id, money_spent_minor, \
                        bonus_spent_minor, prepayment_spent_minor \
                 FROM daily_reporting.published_advertising_expense_facts \
                 WHERE snapshot_id = ANY($1::bigint[]) \
                 ORDER BY account_id, business_date, campaign_id",
                &[&snapshot_ids],
            )
            .await
            .map_err(|_| PostgresSnapshotError::Unavailable)?;
        validate_fact_row_limit(advertising_expense_rows.len())?;
        let finance_rows = client
            .query(
                "SELECT account_id, business_date, sku, category, amount_minor, \
                        line_count, unknown_type_count \
                 FROM daily_reporting.published_finance_facts \
                 WHERE snapshot_id = ANY($1::bigint[]) \
                 ORDER BY account_id, business_date, sku NULLS FIRST, category",
                &[&snapshot_ids],
            )
            .await
            .map_err(|_| PostgresSnapshotError::Unavailable)?;
        let stock_rows = client
            .query(
                "SELECT account_id, sku, warehouse_id, sellable_units, source_as_of \
                 FROM daily_reporting.published_stock_facts \
                 WHERE snapshot_id = ANY($1::bigint[]) \
                 ORDER BY account_id, sku, warehouse_id",
                &[&snapshot_ids],
            )
            .await
            .map_err(|_| PostgresSnapshotError::Unavailable)?;
        let price_rows = client
            .query(
                "SELECT account_id, sku, price_minor, old_price_minor, source_as_of \
                 FROM daily_reporting.published_price_facts \
                 WHERE snapshot_id = ANY($1::bigint[]) \
                 ORDER BY account_id, sku",
                &[&snapshot_ids],
            )
            .await
            .map_err(|_| PostgresSnapshotError::Unavailable)?;

        let actual_total = sales_rows
            .len()
            .checked_add(advertising_rows.len())
            .and_then(|value| value.checked_add(finance_rows.len()))
            .and_then(|value| value.checked_add(stock_rows.len()))
            .and_then(|value| value.checked_add(price_rows.len()))
            .ok_or(PostgresSnapshotError::InvalidManifest)?;
        validate_actual_total(expected_total, actual_total)?;
        let sales = sales_rows
            .into_iter()
            .map(|row| {
                Ok(PublishedSalesFact {
                    account_id: checked_account(row.get(0), &expected_accounts)?,
                    business_date: row.get(1),
                    sku: nonnegative_i64(row.get(2))?,
                    ordered_units: nonnegative_i32(row.get(3))?,
                    operational_gmv_minor: nonnegative_i64(row.get(4))?,
                    cancelled_units: nonnegative_optional_i32(row.get(5))?,
                    returned_units: nonnegative_optional_i32(row.get(6))?,
                })
            })
            .collect::<Result<Vec<_>, PostgresSnapshotError>>()?;
        let advertising = advertising_rows
            .into_iter()
            .map(|row| {
                Ok(PublishedAdvertisingFact {
                    account_id: checked_account(row.get(0), &expected_accounts)?,
                    business_date: row.get(1),
                    campaign_id: nonnegative_i64(row.get(2))?,
                    sku: nonnegative_i64(row.get(3))?,
                    impressions: nonnegative_i64(row.get(4))?,
                    clicks: nonnegative_i64(row.get(5))?,
                    spend_minor: nonnegative_i64(row.get(6))?,
                    attributed_orders: nonnegative_i32(row.get(7))?,
                    attributed_revenue_minor: nonnegative_i64(row.get(8))?,
                    basket_additions: nonnegative_i32(row.get(9))?,
                    model_attributed_orders: nonnegative_i32(row.get(10))?,
                    model_attributed_revenue_minor: nonnegative_i64(row.get(11))?,
                    product_price_minor: nonnegative_i64(row.get(12))?,
                    average_cpc_minor: row
                        .get::<_, Option<i64>>(13)
                        .map(nonnegative_i64)
                        .transpose()?,
                    cpm_minor: row
                        .get::<_, Option<i64>>(14)
                        .map(nonnegative_i64)
                        .transpose()?,
                    cpl_minor: row
                        .get::<_, Option<i64>>(15)
                        .map(nonnegative_i64)
                        .transpose()?,
                })
            })
            .collect::<Result<Vec<_>, PostgresSnapshotError>>()?;
        let advertising_expenses = advertising_expense_rows
            .into_iter()
            .map(|row| {
                Ok(PublishedAdvertisingExpenseFact {
                    account_id: checked_account(row.get(0), &expected_accounts)?,
                    business_date: row.get(1),
                    campaign_id: nonnegative_i64(row.get(2))?,
                    money_spent_minor: nonnegative_i64(row.get(3))?,
                    bonus_spent_minor: nonnegative_i64(row.get(4))?,
                    prepayment_spent_minor: nonnegative_i64(row.get(5))?,
                })
            })
            .collect::<Result<Vec<_>, PostgresSnapshotError>>()?;
        let finance = finance_rows
            .into_iter()
            .map(|row| {
                Ok(PublishedFinanceFact {
                    account_id: checked_account(row.get(0), &expected_accounts)?,
                    business_date: row.get(1),
                    sku: row
                        .get::<_, Option<i64>>(2)
                        .map(nonnegative_i64)
                        .transpose()?,
                    category: parse_finance_category(row.get(3))?,
                    amount_minor: row.get(4),
                    line_count: nonnegative_i32(row.get(5))?,
                    unknown_type_count: nonnegative_i32(row.get(6))?,
                })
            })
            .collect::<Result<Vec<_>, PostgresSnapshotError>>()?;
        let stocks = stock_rows
            .into_iter()
            .map(|row| {
                Ok(PublishedStockFact {
                    account_id: checked_account(row.get(0), &expected_accounts)?,
                    sku: nonnegative_i64(row.get(1))?,
                    warehouse_id: row.get(2),
                    sellable_units: nonnegative_i32(row.get(3))?,
                    observed_at: row.get(4),
                })
            })
            .collect::<Result<Vec<_>, PostgresSnapshotError>>()?;
        let prices = price_rows
            .into_iter()
            .map(|row| {
                Ok(PublishedPriceFact {
                    account_id: checked_account(row.get(0), &expected_accounts)?,
                    sku: nonnegative_i64(row.get(1))?,
                    price_minor: nonnegative_i64(row.get(2))?,
                    old_price_minor: row
                        .get::<_, Option<i64>>(3)
                        .map(nonnegative_i64)
                        .transpose()?,
                    observed_at: row.get(4),
                })
            })
            .collect::<Result<Vec<_>, PostgresSnapshotError>>()?;
        Ok(PublishedReportFacts {
            sales,
            advertising,
            advertising_expenses,
            finance,
            stocks,
            prices,
        })
    }
}

fn checked_account(
    value: String,
    expected: &BTreeSet<&str>,
) -> Result<String, PostgresSnapshotError> {
    expected
        .contains(value.as_str())
        .then_some(value)
        .ok_or(PostgresSnapshotError::InvalidManifest)
}

fn expected_fact_rows(manifest: &FrozenSnapshotManifest) -> Result<usize, PostgresSnapshotError> {
    let total = manifest
        .snapshots()
        .iter()
        .map(|snapshot| snapshot.row_count() as usize)
        .sum();
    validate_fact_row_limit(total)
}

fn validate_fact_row_limit(total: usize) -> Result<usize, PostgresSnapshotError> {
    (total <= MAX_REPORT_FACT_ROWS)
        .then_some(total)
        .ok_or(PostgresSnapshotError::InvalidManifest)
}

fn validate_actual_total(expected: usize, actual: usize) -> Result<(), PostgresSnapshotError> {
    (expected == actual)
        .then_some(())
        .ok_or(PostgresSnapshotError::InvalidManifest)
}

fn nonnegative_i32(value: i32) -> Result<u64, PostgresSnapshotError> {
    u64::try_from(value).map_err(|_| PostgresSnapshotError::InvalidManifest)
}

fn nonnegative_optional_i32(value: Option<i32>) -> Result<Option<u64>, PostgresSnapshotError> {
    value.map(nonnegative_i32).transpose()
}

fn nonnegative_i64(value: i64) -> Result<u64, PostgresSnapshotError> {
    u64::try_from(value).map_err(|_| PostgresSnapshotError::InvalidManifest)
}

fn parse_marketplace(value: &str) -> Result<Marketplace, PostgresSnapshotError> {
    match value {
        "ozon" => Ok(Marketplace::Ozon),
        "wildberries" => Ok(Marketplace::Wildberries),
        _ => Err(PostgresSnapshotError::InvalidManifest),
    }
}

fn parse_source(value: &str) -> Result<SnapshotSource, PostgresSnapshotError> {
    match value {
        "sales" => Ok(SnapshotSource::Sales),
        "advertising" => Ok(SnapshotSource::Advertising),
        "finance" => Ok(SnapshotSource::Finance),
        "stocks" => Ok(SnapshotSource::Stocks),
        "prices" => Ok(SnapshotSource::Prices),
        _ => Err(PostgresSnapshotError::InvalidManifest),
    }
}

fn parse_finance_category(value: &str) -> Result<FinanceCategory, PostgresSnapshotError> {
    match value {
        "sale" => Ok(FinanceCategory::Sale),
        "commission" => Ok(FinanceCategory::Commission),
        "acquiring" => Ok(FinanceCategory::Acquiring),
        "logistics" => Ok(FinanceCategory::Logistics),
        "storage" => Ok(FinanceCategory::Storage),
        "paid_acceptance" => Ok(FinanceCategory::PaidAcceptance),
        "compensation" => Ok(FinanceCategory::Compensation),
        "marketplace_discount" => Ok(FinanceCategory::MarketplaceDiscount),
        "advertising" => Ok(FinanceCategory::Advertising),
        "other" => Ok(FinanceCategory::Other),
        _ => Err(PostgresSnapshotError::InvalidManifest),
    }
}

fn parse_status(value: &str) -> Result<SnapshotStatus, PostgresSnapshotError> {
    match value {
        "succeeded" => Ok(SnapshotStatus::Succeeded),
        "partial" => Ok(SnapshotStatus::Partial),
        _ => Err(PostgresSnapshotError::InvalidManifest),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FinanceCategory, MAX_REPORT_FACT_ROWS, Marketplace, PostgresSnapshotError, SnapshotSource,
        SnapshotStatus, checked_account, nonnegative_i32, nonnegative_i64, parse_finance_category,
        parse_marketplace, parse_source, parse_status, validate_actual_total,
        validate_fact_row_limit,
    };

    #[test]
    fn database_text_mappings_are_exact_and_fail_closed() {
        assert_eq!(parse_marketplace("ozon"), Ok(Marketplace::Ozon));
        assert_eq!(
            parse_marketplace("wildberries"),
            Ok(Marketplace::Wildberries)
        );
        assert_eq!(parse_source("sales"), Ok(SnapshotSource::Sales));
        assert_eq!(parse_source("advertising"), Ok(SnapshotSource::Advertising));
        assert_eq!(parse_source("finance"), Ok(SnapshotSource::Finance));
        assert_eq!(parse_source("stocks"), Ok(SnapshotSource::Stocks));
        assert_eq!(parse_source("prices"), Ok(SnapshotSource::Prices));
        assert_eq!(parse_status("succeeded"), Ok(SnapshotStatus::Succeeded));
        assert_eq!(parse_status("partial"), Ok(SnapshotStatus::Partial));
        for invalid in ["Ozon", "orders", "running"] {
            let error = match invalid {
                "Ozon" => parse_marketplace(invalid).map(|_| ()),
                "orders" => parse_source(invalid).map(|_| ()),
                _ => parse_status(invalid).map(|_| ()),
            };
            assert_eq!(error, Err(PostgresSnapshotError::InvalidManifest));
        }
        for (raw, expected) in [
            ("sale", FinanceCategory::Sale),
            ("commission", FinanceCategory::Commission),
            ("acquiring", FinanceCategory::Acquiring),
            ("logistics", FinanceCategory::Logistics),
            ("storage", FinanceCategory::Storage),
            ("paid_acceptance", FinanceCategory::PaidAcceptance),
            ("compensation", FinanceCategory::Compensation),
            ("marketplace_discount", FinanceCategory::MarketplaceDiscount),
            ("advertising", FinanceCategory::Advertising),
            ("other", FinanceCategory::Other),
        ] {
            assert_eq!(parse_finance_category(raw), Ok(expected));
        }
        assert_eq!(
            parse_finance_category("unknown"),
            Err(PostgresSnapshotError::InvalidManifest)
        );
    }

    #[test]
    fn database_scalar_mappings_reject_foreign_or_negative_values() {
        let accounts = ["expected"].into_iter().collect();
        assert_eq!(
            checked_account("expected".to_owned(), &accounts),
            Ok("expected".to_owned())
        );
        assert_eq!(
            checked_account("foreign".to_owned(), &accounts),
            Err(PostgresSnapshotError::InvalidManifest)
        );
        assert_eq!(nonnegative_i32(7), Ok(7));
        assert_eq!(nonnegative_i64(9), Ok(9));
        assert_eq!(
            nonnegative_i32(-1),
            Err(PostgresSnapshotError::InvalidManifest)
        );
        assert_eq!(
            nonnegative_i64(-1),
            Err(PostgresSnapshotError::InvalidManifest)
        );
        assert_eq!(validate_actual_total(4, 4), Ok(()));
        assert_eq!(
            validate_actual_total(4, 3),
            Err(PostgresSnapshotError::InvalidManifest)
        );
        assert_eq!(
            validate_fact_row_limit(MAX_REPORT_FACT_ROWS),
            Ok(MAX_REPORT_FACT_ROWS)
        );
        assert_eq!(
            validate_fact_row_limit(MAX_REPORT_FACT_ROWS + 1),
            Err(PostgresSnapshotError::InvalidManifest)
        );
    }
}
