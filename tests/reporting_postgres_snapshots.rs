use std::str::FromStr;

use chrono::{DateTime, TimeZone, Utc};
use mcp_ozon::reporting::{
    postgres_snapshot::{PostgresSnapshotError, PostgresSnapshotRepository},
    snapshot::{AccountScope, Marketplace, SnapshotQuality},
};
use tokio_postgres::{Config, NoTls};

fn cutoff() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2098, 8, 16, 3, 0, 0).unwrap()
}

async fn client(config: &Config) -> tokio_postgres::Client {
    let (client, connection) = config.connect(NoTls).await.unwrap();
    std::mem::drop(tokio::spawn(connection));
    client
}

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .unwrap()
        .with_timezone(&Utc)
}

#[tokio::test]
async fn report_worker_loads_only_a_complete_published_manifest() {
    let (Ok(worker_url), Ok(collector_url)) = (
        std::env::var("REPORT_OUTBOX_TEST_WORKER_URL"),
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
    ) else {
        return;
    };
    let worker_config = Config::from_str(&worker_url).unwrap();
    let collector_config = Config::from_str(&collector_url).unwrap();
    let collector = client(&collector_config).await;

    for (source, source_as_of, period_start, period_end, partial) in [
        (
            "sales",
            timestamp("2098-08-16T02:30:00Z"),
            timestamp("2098-08-15T00:00:00Z"),
            timestamp("2098-08-16T00:00:00Z"),
            false,
        ),
        (
            "advertising",
            timestamp("2098-08-16T02:00:00Z"),
            timestamp("2098-08-15T00:00:00Z"),
            timestamp("2098-08-16T00:00:00Z"),
            true,
        ),
        (
            "stocks",
            timestamp("2098-08-16T02:45:00Z"),
            timestamp("2098-08-16T02:45:00Z"),
            timestamp("2098-08-16T02:45:00Z"),
            false,
        ),
        (
            "prices",
            timestamp("2098-08-16T02:40:00Z"),
            timestamp("2098-08-16T02:40:00Z"),
            timestamp("2098-08-16T02:40:00Z"),
            false,
        ),
    ] {
        let row = collector
            .query_one(
                "INSERT INTO daily_reporting.source_snapshots \
                    (account_id, marketplace, source, cutoff_at, source_as_of, \
                     period_start, period_end, collector_version) \
                 VALUES ('snapshot_integration', 'ozon', $1, $2, $3::timestamptz, \
                         $4, $5, 'integration-test') \
                 RETURNING id",
                &[
                    &source,
                    &cutoff(),
                    &source_as_of,
                    &period_start,
                    &period_end,
                ],
            )
            .await
            .unwrap();
        let snapshot_id: i64 = row.get(0);
        match source {
            "sales" => {
                collector
                    .execute(
                        "INSERT INTO daily_reporting.sales_facts \
                        (snapshot_id, business_date, sku, ordered_units, operational_gmv_minor) \
                     VALUES ($1, '2098-08-15', 3411079879, 3, 202500)",
                        &[&snapshot_id],
                    )
                    .await
                    .unwrap();
            }
            "advertising" => {
                collector
                    .execute(
                        "INSERT INTO daily_reporting.advertising_facts \
                        (snapshot_id, business_date, campaign_id, sku, impressions, clicks, \
                         spend_minor, attributed_orders, attributed_revenue_minor) \
                     VALUES ($1, '2098-08-15', 35751912, 3411079879, 1000, 20, 12000, 2, 135000)",
                        &[&snapshot_id],
                    )
                    .await
                    .unwrap();
            }
            "stocks" => {
                collector
                    .execute(
                        "INSERT INTO daily_reporting.stock_facts \
                        (snapshot_id, sku, warehouse_id, sellable_units) \
                     VALUES ($1, 3411079879, 'fbo-msk', 19)",
                        &[&snapshot_id],
                    )
                    .await
                    .unwrap();
            }
            "prices" => {
                collector
                    .execute(
                        "INSERT INTO daily_reporting.price_facts \
                        (snapshot_id, sku, price_minor, old_price_minor) \
                     VALUES ($1, 3411079879, 67500, 70200)",
                        &[&snapshot_id],
                    )
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let status = if partial { "partial" } else { "succeeded" };
        collector
            .execute(
                "UPDATE daily_reporting.source_snapshots \
                 SET status = $2, pagination_complete = $3, row_count = 1, \
                     payload_sha256 = repeat('c', 64), \
                     finished_at = '2098-08-16 02:50:00+00' \
                 WHERE id = $1",
                &[&snapshot_id, &status, &!partial],
            )
            .await
            .unwrap();
    }

    let repository = PostgresSnapshotRepository::connect(&worker_config)
        .await
        .unwrap();
    repository.verify_runtime_contract().await.unwrap();
    let manifest = repository
        .load_manifest(
            cutoff(),
            vec![AccountScope::new("snapshot_integration".to_owned(), Marketplace::Ozon).unwrap()],
        )
        .await
        .unwrap();
    assert_eq!(manifest.snapshots().len(), 4);
    assert_eq!(manifest.quality(), SnapshotQuality::Partial);
    assert!(!manifest.recommendations_allowed());
    let facts = repository.load_report_facts(&manifest).await.unwrap();
    assert_eq!(facts.sales.len(), 1);
    assert_eq!(facts.sales[0].account_id, "snapshot_integration");
    assert_eq!(facts.sales[0].business_date.to_string(), "2098-08-15");
    assert_eq!(facts.sales[0].sku, 3411079879);
    assert_eq!(facts.sales[0].ordered_units, 3);
    assert_eq!(facts.sales[0].operational_gmv_minor, 202500);
    assert_eq!(facts.sales[0].cancelled_units, 0);
    assert_eq!(facts.sales[0].returned_units, 0);
    assert_eq!(facts.advertising.len(), 1);
    assert_eq!(facts.advertising[0].campaign_id, 35751912);
    assert_eq!(facts.advertising[0].sku, 3411079879);
    assert_eq!(facts.advertising[0].impressions, 1000);
    assert_eq!(facts.advertising[0].clicks, 20);
    assert_eq!(facts.advertising[0].spend_minor, 12000);
    assert_eq!(facts.advertising[0].attributed_orders, 2);
    assert_eq!(facts.advertising[0].attributed_revenue_minor, 135000);
    assert_eq!(facts.stocks.len(), 1);
    assert_eq!(facts.stocks[0].warehouse_id, "fbo-msk");
    assert_eq!(facts.stocks[0].sellable_units, 19);
    assert_eq!(
        facts.stocks[0].observed_at,
        timestamp("2098-08-16T02:45:00Z")
    );
    assert_eq!(facts.prices.len(), 1);
    assert_eq!(facts.prices[0].price_minor, 67500);
    assert_eq!(facts.prices[0].old_price_minor, Some(70200));
    assert_eq!(
        facts.prices[0].observed_at,
        timestamp("2098-08-16T02:40:00Z")
    );

    assert_eq!(
        repository
            .load_manifest(
                cutoff(),
                vec![AccountScope::new("missing_account".to_owned(), Marketplace::Ozon,).unwrap()],
            )
            .await,
        Err(PostgresSnapshotError::InvalidManifest)
    );
    assert_eq!(
        repository.load_manifest(cutoff(), Vec::new()).await,
        Err(PostgresSnapshotError::InvalidManifest)
    );
    let too_many_accounts = (0..65)
        .map(|index| AccountScope::new(format!("account_{index}"), Marketplace::Ozon).unwrap())
        .collect();
    assert_eq!(
        repository.load_manifest(cutoff(), too_many_accounts).await,
        Err(PostgresSnapshotError::InvalidManifest)
    );

    let admin_config =
        Config::from_str(&std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL").unwrap()).unwrap();
    let wrong_role = PostgresSnapshotRepository::connect(&admin_config)
        .await
        .unwrap();
    assert_eq!(
        wrong_role.verify_runtime_contract().await,
        Err(PostgresSnapshotError::Unavailable)
    );
}
