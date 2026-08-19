use std::{collections::VecDeque, fs, future::Future, pin::Pin, str::FromStr, sync::Mutex};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use mcp_ozon::reporting::{
    ReportKey, ReportKind,
    collector_orchestrator::plan_due_collection,
    collector_plan::CollectionTarget,
    collector_service::ReportCollectorConfig,
    ozon_adapter::OzonReportRequest,
    ozon_source::{OzonReportSourceError, OzonReportTransport, collect_complete_snapshots},
    postgres_collector::{
        CollectedAdvertisingFact, CollectedFacts, CollectedPriceFact, CollectedSalesFact,
        CollectedSnapshot, CollectedStockFact, PostgresCollectorError, PostgresSnapshotWriter,
    },
    postgres_snapshot::{PostgresSnapshotError, PostgresSnapshotRepository},
    preview::render_published_preview,
    snapshot::{AccountScope, Marketplace, SnapshotQuality, SnapshotSource, SnapshotStatus},
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

fn collection_target(account_id: &str, marketplace: Marketplace) -> CollectionTarget {
    CollectionTarget {
        account_id: account_id.to_owned(),
        marketplace,
        sources: [
            SnapshotSource::Sales,
            SnapshotSource::Advertising,
            SnapshotSource::Stocks,
            SnapshotSource::Prices,
        ],
    }
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
    let runtime_config = ReportCollectorConfig::from_lookup(&mut |key| match key {
        "REPORT_COLLECTOR_DATABASE_URL" => Some(collector_url.clone()),
        "MCP_ACCESS_CONFIG" => Some(runtime_registry.display().to_string()),
        "DAILY_REPORT_POLICY" => Some(runtime_policy.display().to_string()),
        _ => None,
    })
    .unwrap();
    PostgresSnapshotWriter::connect(runtime_config.database_config())
        .await
        .unwrap()
        .verify_runtime_contract()
        .await
        .unwrap();
    let account_id = format!("snapshot_integration_{}", std::process::id());
    let target = collection_target(&account_id, Marketplace::Ozon);
    let claim = writer
        .claim_target(&target, cutoff(), "manifest-owner")
        .await
        .unwrap()
        .unwrap();
    assert!(claim.lease_until() > Utc::now());
    assert!(
        writer
            .claim_target(&target, cutoff(), "competing-owner")
            .await
            .unwrap()
            .is_none()
    );
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
    let advertising = collected(
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
    );
    let stocks = collected(
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
    );
    let prices = collected(
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
    );
    let snapshot_ids = writer
        .persist_claimed_batch(&claim, &[sales, advertising, stocks, prices])
        .await
        .unwrap();
    assert_eq!(snapshot_ids.len(), 4);
    assert!(
        writer
            .claim_target(&target, cutoff(), "after-completion")
            .await
            .unwrap()
            .is_none()
    );

    let rollback_account = format!("snapshot_batch_{}", std::process::id());
    let rollback_target = collection_target(&rollback_account, Marketplace::Ozon);
    let released_claim = writer
        .claim_target(&rollback_target, cutoff(), "release-owner")
        .await
        .unwrap()
        .unwrap();
    assert!(writer.release_claim(&released_claim).await.unwrap());
    assert!(!writer.release_claim(&released_claim).await.unwrap());
    let replacement_claim = writer
        .claim_target(&rollback_target, cutoff(), "replacement-owner")
        .await
        .unwrap()
        .unwrap();
    assert!(replacement_claim.lease_until() > Utc::now());
    let stale_batch = [
        collected(
            &rollback_account,
            timestamp("2098-08-16T02:30:00Z"),
            timestamp("2098-08-15T00:00:00Z"),
            timestamp("2098-08-16T00:00:00Z"),
            false,
            CollectedFacts::Sales(Vec::new()),
        ),
        collected(
            &rollback_account,
            timestamp("2098-08-16T02:30:00Z"),
            timestamp("2098-08-15T00:00:00Z"),
            timestamp("2098-08-16T00:00:00Z"),
            false,
            CollectedFacts::Advertising(Vec::new()),
        ),
        collected(
            &rollback_account,
            timestamp("2098-08-16T02:45:00Z"),
            timestamp("2098-08-16T02:45:00Z"),
            timestamp("2098-08-16T02:45:00Z"),
            false,
            CollectedFacts::Stocks(Vec::new()),
        ),
        collected(
            &rollback_account,
            timestamp("2098-08-16T02:40:00Z"),
            timestamp("2098-08-16T02:40:00Z"),
            timestamp("2098-08-16T02:40:00Z"),
            false,
            CollectedFacts::Prices(Vec::new()),
        ),
    ];
    assert_eq!(
        writer
            .persist_claimed_batch(&released_claim, &stale_batch)
            .await,
        Err(PostgresCollectorError::ClaimLost)
    );
    assert!(writer.release_claim(&replacement_claim).await.unwrap());

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
    assert_eq!(facts.advertising.len(), 1);
    assert_eq!(facts.advertising[0].sku, 3_411_079_879);
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
    let (Ok(collector_url), Ok(worker_url)) = (
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
        std::env::var("REPORT_OUTBOX_TEST_WORKER_URL"),
    ) else {
        return;
    };
    let config = Config::from_str(&collector_url).unwrap();
    let writer = PostgresSnapshotWriter::connect(&config).await.unwrap();
    let transport = FixtureTransport(Mutex::new(VecDeque::from([
        Ok(json!({"result":{"data":[{
            "dimensions":[{"id":"3411079879"},{"id":"2098-08-15"}],
            "metrics":["675.00", 2]
        }]}})),
        Ok(json!({
            "products":[{
                "sku":3411079879_u64,
                "warehouse_id":1001,
                "present":21,
                "reserved":2
            }],
            "cursor":"",
            "has_next":false
        })),
        Ok(json!({"products":[],"cursor":"","has_next":false})),
        Ok(json!({"items":[{"product_id":3411079879_u64,"price":{
            "currency_code":"RUB","price":"675.00","old_price":"702.00"
        }}],"cursor":""})),
    ])));
    let account_id = format!("ozon_source_atomic_{}", std::process::id());
    assert!(
        writer
            .published_targets(timestamp("2098-08-17T03:00:00Z"), &[])
            .await
            .unwrap()
            .is_empty()
    );
    let target = collection_target(&account_id, Marketplace::Ozon);
    assert!(
        writer
            .published_targets(
                timestamp("2098-08-17T03:00:00Z"),
                std::slice::from_ref(&target)
            )
            .await
            .unwrap()
            .is_empty()
    );
    let scheduled = plan_due_collection(
        &writer,
        timestamp("2098-08-17T03:05:00Z"),
        std::slice::from_ref(&target),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(scheduled.targets, vec![target.clone()]);
    assert!(scheduled.occurrence.delayed);
    let claim = writer
        .claim_target(
            &target,
            timestamp("2098-08-17T03:00:00Z"),
            "orchestrator-owner",
        )
        .await
        .unwrap()
        .unwrap();
    let snapshots = collect_complete_snapshots(
        &transport,
        vec![CollectedAdvertisingFact {
            business_date: NaiveDate::from_ymd_opt(2098, 8, 15).unwrap(),
            campaign_id: 35_751_912,
            sku: 3_411_079_879,
            impressions: 100,
            clicks: 10,
            spend_minor: 1_000,
            attributed_orders: 1,
            attributed_revenue_minor: 10_000,
        }],
        account_id.clone(),
        timestamp("2098-08-17T03:00:00Z"),
        || timestamp("2098-08-17T02:30:00Z"),
        timestamp("2098-08-15T19:00:00Z"),
        timestamp("2098-08-16T19:00:00Z"),
        "integration-test".to_owned(),
    )
    .await
    .unwrap();
    let ids = writer
        .persist_claimed_batch(&claim, &snapshots)
        .await
        .unwrap();
    assert_eq!(ids.len(), 4);
    assert_eq!(
        writer
            .published_targets(
                timestamp("2098-08-17T03:00:00Z"),
                std::slice::from_ref(&target),
            )
            .await
            .unwrap(),
        [(account_id.clone(), Marketplace::Ozon)]
            .into_iter()
            .collect()
    );
    let wrong_marketplace = CollectionTarget {
        account_id: account_id.clone(),
        marketplace: Marketplace::Wildberries,
        sources: target.sources,
    };
    assert!(
        writer
            .published_targets(timestamp("2098-08-17T03:00:00Z"), &[wrong_marketplace],)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        plan_due_collection(
            &writer,
            timestamp("2098-08-17T03:06:00Z"),
            std::slice::from_ref(&target),
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        plan_due_collection(
            &writer,
            timestamp("2098-08-17T04:00:00Z"),
            std::slice::from_ref(&target),
        )
        .await
        .unwrap()
        .is_none()
    );
    let worker_config = Config::from_str(&worker_url).unwrap();
    let repository = PostgresSnapshotRepository::connect(&worker_config)
        .await
        .unwrap();
    let manifest = repository
        .load_manifest(
            timestamp("2098-08-17T03:00:00Z"),
            vec![AccountScope::new(account_id, Marketplace::Ozon).unwrap()],
        )
        .await
        .unwrap();
    let facts = repository.load_report_facts(&manifest).await.unwrap();
    let key = ReportKey {
        local_date: NaiveDate::from_ymd_opt(2098, 8, 17).unwrap(),
        kind: ReportKind::Morning,
        recipient_id: "diana".to_owned(),
        report_version: 1,
    };
    let preview = render_published_preview(
        &key,
        "Диана",
        timestamp("2098-08-17T03:00:00Z"),
        &manifest,
        facts,
    )
    .unwrap();
    assert!(preview.bundle.html.contains("Диана"));
    assert!(preview.bundle.html.contains("Отменено / возвращено единиц"));
    assert!(preview.bundle.html.contains("N/D / N/D"));
    assert!(preview.bundle.xlsx.starts_with(b"PK"));
    assert_eq!(preview.receipt.size_bytes, preview.bundle.xlsx.len());
    assert!(!preview.receipt.persisted);
    assert_eq!(
        writer.persist_claimed_batch(&claim, &[]).await,
        Err(PostgresCollectorError::InvalidInput)
    );
}
