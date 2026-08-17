use std::{collections::VecDeque, fs, future::Future, pin::Pin, str::FromStr, sync::Mutex};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use mcp_ozon::reporting::{
    collector_service::ReportCollectorConfig,
    ozon_adapter::OzonReportRequest,
    ozon_source::{
        OzonReportSource, OzonReportSourceError, OzonReportTransport, collect_and_persist,
    },
    postgres_collector::{
        CollectedAdvertisingFact, CollectedFacts, CollectedPriceFact, CollectedSalesFact,
        CollectedSnapshot, CollectedStockFact, PostgresCollectorError, PostgresSnapshotWriter,
    },
    postgres_snapshot::{PostgresSnapshotError, PostgresSnapshotRepository},
    snapshot::{AccountScope, Marketplace, SnapshotQuality, SnapshotStatus},
};
use serde_json::{Value, json};
use tokio_postgres::Config;

struct FixtureTransport(Mutex<VecDeque<Result<Value, OzonReportSourceError>>>);

impl OzonReportTransport for FixtureTransport {
    fn post<'a>(
        &'a self,
        _request: OzonReportRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OzonReportSourceError>> + Send + 'a>> {
        Box::pin(async move { self.0.lock().unwrap().pop_front().unwrap() })
    }
}

fn cutoff() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2098, 8, 16, 3, 0, 0).unwrap()
}

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .unwrap()
        .with_timezone(&Utc)
}

#[allow(clippy::too_many_arguments)]
fn collected(
    account_id: &str,
    source_as_of: DateTime<Utc>,
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
    partial: bool,
    facts: CollectedFacts,
) -> CollectedSnapshot {
    CollectedSnapshot::new(
        account_id.to_owned(),
        Marketplace::Ozon,
        cutoff(),
        source_as_of,
        period_start,
        period_end,
        if partial {
            SnapshotStatus::Partial
        } else {
            SnapshotStatus::Succeeded
        },
        !partial,
        "integration-test".to_owned(),
        facts,
    )
    .unwrap()
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
    let writer = PostgresSnapshotWriter::connect(&collector_config)
        .await
        .unwrap();
    writer.verify_runtime_contract().await.unwrap();
    let runtime_directory = std::env::temp_dir().join(format!(
        "mcp-ozon-report-collector-integration-{}",
        std::process::id()
    ));
    fs::create_dir_all(&runtime_directory).unwrap();
    let runtime_registry = runtime_directory.join("access.json");
    let runtime_policy = runtime_directory.join("policy.json");
    fs::write(
        &runtime_registry,
        r#"{"version":1,"actors":[{"id":"diana","name":"Diana","role":"manager","oidc":{"username":"diana"}}],"accounts":[{"id":"ozon","organization":"Ozon","marketplace":"ozon","seller_client_id":"1","manager_id":"diana","ozon":{"store_id":"1","client_id_env":"ID","api_key_env":"KEY","performance":{"client_id_env":"PERF_ID","client_secret_env":"PERF_SECRET"}}}]}"#,
    )
    .unwrap();
    fs::write(
        &runtime_policy,
        r#"{"version":1,"enabled":false,"timezone":"Asia/Yekaterinburg","sender_email_env":"SENDER","audiences":[{"id":"owner","email_env":"OWNER","managers":[{"actor_id":"diana","account_ids":["ozon"]}]}]}"#,
    )
    .unwrap();
    let runtime_config = ReportCollectorConfig::from_lookup(|key| match key {
        "REPORT_COLLECTOR_DATABASE_URL" => Some(collector_url.clone()),
        "MCP_ACCESS_CONFIG" => Some(runtime_registry.display().to_string()),
        "DAILY_REPORT_POLICY" => Some(runtime_policy.display().to_string()),
        _ => None,
    })
    .unwrap();
    runtime_config
        .connect_writer()
        .await
        .unwrap()
        .verify_runtime_contract()
        .await
        .unwrap();
    let account_id = format!("snapshot_integration_{}", std::process::id());
    let sales = collected(
        &account_id,
        timestamp("2098-08-16T02:30:00Z"),
        timestamp("2098-08-15T00:00:00Z"),
        timestamp("2098-08-16T00:00:00Z"),
        false,
        CollectedFacts::Sales(vec![CollectedSalesFact {
            business_date: NaiveDate::from_ymd_opt(2098, 8, 15).unwrap(),
            sku: 3411079879,
            ordered_units: 3,
            operational_gmv_minor: 202500,
            cancelled_units: Some(0),
            returned_units: Some(0),
        }]),
    );
    writer.persist(&sales).await.unwrap();
    assert_eq!(
        writer.persist(&sales).await,
        Err(PostgresCollectorError::Conflict)
    );

    let rollback_account = format!("snapshot_batch_{}", std::process::id());
    let batch_stock = collected(
        &rollback_account,
        timestamp("2098-08-16T02:45:00Z"),
        timestamp("2098-08-16T02:45:00Z"),
        timestamp("2098-08-16T02:45:00Z"),
        false,
        CollectedFacts::Stocks(Vec::new()),
    );
    assert_eq!(
        writer
            .persist_batch(&[batch_stock.clone(), batch_stock])
            .await,
        Err(PostgresCollectorError::Conflict)
    );
    let (rollback_client, rollback_connection) = collector_config
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(rollback_connection);
    assert_eq!(
        rollback_client
            .query_one(
                "SELECT count(*) FROM daily_reporting.source_snapshots WHERE account_id = $1",
                &[&rollback_account],
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    writer
        .persist(&collected(
            &account_id,
            timestamp("2098-08-16T02:00:00Z"),
            timestamp("2098-08-15T00:00:00Z"),
            timestamp("2098-08-16T00:00:00Z"),
            true,
            CollectedFacts::Advertising(vec![CollectedAdvertisingFact {
                business_date: NaiveDate::from_ymd_opt(2098, 8, 15).unwrap(),
                campaign_id: 35751912,
                sku: 3411079879,
                impressions: 1000,
                clicks: 20,
                spend_minor: 12000,
                attributed_orders: 2,
                attributed_revenue_minor: 135000,
            }]),
        ))
        .await
        .unwrap();
    let wb_as_of = timestamp("2098-08-16T02:35:00Z");
    writer
        .persist(
            &CollectedSnapshot::new(
                format!("writer_wb_{}", std::process::id()),
                Marketplace::Wildberries,
                cutoff(),
                wb_as_of,
                wb_as_of,
                wb_as_of,
                SnapshotStatus::Succeeded,
                true,
                "integration-test".to_owned(),
                CollectedFacts::Prices(Vec::new()),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    writer
        .persist(&collected(
            &account_id,
            timestamp("2098-08-16T02:45:00Z"),
            timestamp("2098-08-16T02:45:00Z"),
            timestamp("2098-08-16T02:45:00Z"),
            false,
            CollectedFacts::Stocks(vec![CollectedStockFact {
                sku: 3411079879,
                warehouse_id: "fbo-msk".to_owned(),
                sellable_units: 19,
            }]),
        ))
        .await
        .unwrap();
    writer
        .persist(&collected(
            &account_id,
            timestamp("2098-08-16T02:40:00Z"),
            timestamp("2098-08-16T02:40:00Z"),
            timestamp("2098-08-16T02:40:00Z"),
            false,
            CollectedFacts::Prices(vec![CollectedPriceFact {
                sku: 3411079879,
                price_minor: 67500,
                old_price_minor: Some(70200),
            }]),
        ))
        .await
        .unwrap();

    let repository = PostgresSnapshotRepository::connect(&worker_config)
        .await
        .unwrap();
    repository.verify_runtime_contract().await.unwrap();
    let manifest = repository
        .load_manifest(
            cutoff(),
            vec![AccountScope::new(account_id.clone(), Marketplace::Ozon).unwrap()],
        )
        .await
        .unwrap();
    assert_eq!(manifest.snapshots().len(), 4);
    assert_eq!(manifest.quality(), SnapshotQuality::Partial);
    assert!(!manifest.recommendations_allowed());
    let facts = repository.load_report_facts(&manifest).await.unwrap();
    assert_eq!(facts.sales.len(), 1);
    assert_eq!(facts.sales[0].account_id, account_id);
    assert_eq!(facts.sales[0].business_date.to_string(), "2098-08-15");
    assert_eq!(facts.sales[0].sku, 3411079879);
    assert_eq!(facts.sales[0].ordered_units, 3);
    assert_eq!(facts.sales[0].operational_gmv_minor, 202500);
    assert_eq!(facts.sales[0].cancelled_units, Some(0));
    assert_eq!(facts.sales[0].returned_units, Some(0));
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
    let wrong_writer = PostgresSnapshotWriter::connect(&admin_config)
        .await
        .unwrap();
    assert_eq!(
        wrong_writer.verify_runtime_contract().await,
        Err(PostgresCollectorError::Unavailable)
    );
}

#[tokio::test]
async fn complete_ozon_source_set_is_published_atomically() {
    let Ok(collector_url) = std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL") else {
        return;
    };
    let config = Config::from_str(&collector_url).unwrap();
    let writer = PostgresSnapshotWriter::connect(&config).await.unwrap();
    let source = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([
        Ok(json!({"result":{"data":[{
            "dimensions":[{"id":"3411079879"},{"id":"2098-08-15"}],
            "metrics":["675.00", 2, 0, 0]
        }]}})),
        Ok(json!({"items":[{"product_id":3411079879_u64,"stocks":[{
            "type":"FBO","present":19
        }]}],"cursor":""})),
        Ok(json!({"items":[{"product_id":3411079879_u64,"price":{
            "currency_code":"RUB","price":"675.00","old_price":"702.00"
        }}],"cursor":""})),
    ]))));
    let account_id = format!("ozon_source_atomic_{}", std::process::id());
    let ids = collect_and_persist(
        &source,
        &writer,
        account_id,
        timestamp("2098-08-16T19:00:00Z"),
        timestamp("2098-08-16T03:00:00Z"),
        timestamp("2098-08-15T19:00:00Z"),
        timestamp("2098-08-16T19:00:00Z"),
        "integration-test".to_owned(),
    )
    .await
    .unwrap();
    assert_eq!(ids.len(), 3);
    assert_eq!(
        writer.persist_batch(&[]).await,
        Err(PostgresCollectorError::InvalidInput)
    );
}
