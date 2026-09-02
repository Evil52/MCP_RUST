use std::{collections::BTreeSet, str::FromStr};

use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use mcp_ozon::reporting::{
    ReportKind,
    collector_plan::CollectionTarget,
    due_deliveries,
    mcp_read::{
        DataState, ManagerActionKind, ReadyReportKind, ReadyReportState, ReportingReader,
        SalesAnalyticsDirection, SalesAnalyticsGroup, SalesAnalyticsQuery, SalesAnalyticsSort,
        SalesDateCoverageState,
    },
    outbox::{ArtifactIdentity, DeliveryErrorClass},
    postgres_collector::{
        CollectedAdvertisingExpenseFact, CollectedAdvertisingFact, CollectedFacts,
        CollectedFinanceFact, CollectedPriceFact, CollectedSalesFact, CollectedSnapshot,
        CollectedStockFact, FinanceCategory, PostgresSnapshotWriter,
    },
    postgres_outbox::{CreateOutcome, PostgresOutboxRepository},
    snapshot::{AccountScope, Marketplace, SnapshotSource, SnapshotStatus},
};
use sha2::{Digest, Sha256};
use tokio_postgres::{Config, NoTls};

static DB_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .unwrap()
        .with_timezone(&Utc)
}

fn target(account_id: &str) -> CollectionTarget {
    CollectionTarget {
        account_id: account_id.to_owned(),
        marketplace: Marketplace::Ozon,
        sources: vec![
            SnapshotSource::Sales,
            SnapshotSource::Advertising,
            SnapshotSource::Finance,
            SnapshotSource::Stocks,
            SnapshotSource::Prices,
        ],
    }
}

fn snapshot(
    account_id: &str,
    cutoff: DateTime<Utc>,
    source_as_of: DateTime<Utc>,
    facts: CollectedFacts,
) -> CollectedSnapshot {
    snapshot_with_status(
        account_id,
        cutoff,
        source_as_of,
        SnapshotStatus::Succeeded,
        facts,
    )
}

fn snapshot_with_status(
    account_id: &str,
    cutoff: DateTime<Utc>,
    source_as_of: DateTime<Utc>,
    status: SnapshotStatus,
    facts: CollectedFacts,
) -> CollectedSnapshot {
    let (period_start, period_end) = match &facts {
        CollectedFacts::Sales(_) | CollectedFacts::Advertising(_) | CollectedFacts::Finance(_) => (
            timestamp("2098-09-14T19:00:00Z"),
            timestamp("2098-09-15T19:00:00Z"),
        ),
        CollectedFacts::Stocks(_) | CollectedFacts::Prices(_) => (source_as_of, source_as_of),
    };
    CollectedSnapshot::new(
        account_id.to_owned(),
        Marketplace::Ozon,
        cutoff,
        source_as_of,
        period_start,
        period_end,
        status,
        status == SnapshotStatus::Succeeded,
        "mcp-read-integration".to_owned(),
        facts,
    )
    .unwrap()
}

fn artifact(recipient_id: &str) -> ArtifactIdentity {
    ArtifactIdentity {
        object_key: format!("daily-reports/2099/09/16/{recipient_id}/v1/morning.xlsx"),
        sha256: hex_sha256(b"mcp-read-integration-xlsx"),
        html_sha256: hex_sha256(b"<html>mcp read integration</html>"),
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            use std::fmt::Write as _;
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
}

#[tokio::test]
async fn restricted_reader_rebuilds_complete_history_actions_and_safe_report_metadata() {
    let (Ok(admin_url), Ok(reader_url), Ok(collector_url), Ok(worker_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("POSITION_REPOSITORY_TEST_READER_URL"),
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
        std::env::var("REPORT_OUTBOX_TEST_WORKER_URL"),
    ) else {
        return;
    };
    let _guard = DB_TEST_LOCK.lock().await;
    let account_id = format!("mcp_read_{}", std::process::id());
    let account = AccountScope::new(account_id.clone(), Marketplace::Ozon).unwrap();
    let cutoff = timestamp("2098-09-16T03:00:00Z");
    let source_as_of = cutoff - Duration::minutes(30);
    let business_date = NaiveDate::from_ymd_opt(2098, 9, 15).unwrap();
    let sku = 3_411_079_879;

    let writer = PostgresSnapshotWriter::connect(&Config::from_str(&collector_url).unwrap())
        .await
        .unwrap();
    writer.verify_runtime_contract().await.unwrap();
    let claim = writer
        .claim_target(&target(&account_id), cutoff, "mcp-read-owner")
        .await
        .unwrap()
        .unwrap();
    let sales = snapshot(
        &account_id,
        cutoff,
        source_as_of,
        CollectedFacts::Sales(vec![CollectedSalesFact {
            business_date,
            sku,
            ordered_units: 3,
            operational_gmv_minor: 202_500,
            cancelled_units: Some(0),
            returned_units: Some(0),
        }]),
    );
    let advertising = snapshot(
        &account_id,
        cutoff,
        source_as_of,
        CollectedFacts::Advertising(vec![CollectedAdvertisingFact {
            business_date,
            campaign_id: 35_751_912,
            sku,
            impressions: 1_000,
            clicks: 20,
            spend_minor: 12_000,
            attributed_orders: 0,
            attributed_revenue_minor: 0,
            basket_additions: 4,
            model_attributed_orders: 0,
            model_attributed_revenue_minor: 0,
            product_price_minor: 67_500,
            average_cpc_minor: Some(600),
            cpm_minor: Some(12_000),
            cpl_minor: None,
        }]),
    )
    .with_advertising_expenses(vec![CollectedAdvertisingExpenseFact {
        business_date,
        campaign_id: 35_751_912,
        money_spent_minor: 12_000,
        bonus_spent_minor: 0,
        prepayment_spent_minor: 12_000,
    }])
    .unwrap();
    let finance = snapshot(
        &account_id,
        cutoff,
        source_as_of,
        CollectedFacts::Finance(vec![CollectedFinanceFact {
            business_date,
            sku: Some(sku),
            category: FinanceCategory::Sale,
            amount_minor: 180_000,
            line_count: 1,
            unknown_type_count: 0,
        }]),
    );
    let stocks = snapshot(
        &account_id,
        cutoff,
        source_as_of,
        CollectedFacts::Stocks(vec![CollectedStockFact {
            sku,
            warehouse_id: "fbo-msk".to_owned(),
            sellable_units: 0,
        }]),
    );
    let prices = snapshot(
        &account_id,
        cutoff,
        source_as_of,
        CollectedFacts::Prices(vec![CollectedPriceFact {
            sku,
            price_minor: 67_500,
            old_price_minor: Some(70_200),
        }]),
    );
    let snapshot_ids = writer
        .persist_claimed_batch(&claim, &[sales, advertising, finance, stocks, prices])
        .await
        .unwrap();
    assert_eq!(snapshot_ids.len(), 5);

    let partial_account_id = format!("mcp_read_partial_{}", std::process::id());
    let partial_account = AccountScope::new(partial_account_id.clone(), Marketplace::Ozon).unwrap();
    let partial_cutoff = cutoff + Duration::hours(1);
    let partial_source_as_of = partial_cutoff - Duration::minutes(30);
    let partial_claim = writer
        .claim_target(
            &target(&partial_account_id),
            partial_cutoff,
            "mcp-read-partial-owner",
        )
        .await
        .unwrap()
        .unwrap();
    let partial_batch = [
        snapshot_with_status(
            &partial_account_id,
            partial_cutoff,
            partial_source_as_of,
            SnapshotStatus::Partial,
            CollectedFacts::Sales(Vec::new()),
        ),
        snapshot(
            &partial_account_id,
            partial_cutoff,
            partial_source_as_of,
            CollectedFacts::Advertising(Vec::new()),
        ),
        snapshot(
            &partial_account_id,
            partial_cutoff,
            partial_source_as_of,
            CollectedFacts::Finance(Vec::new()),
        ),
        snapshot(
            &partial_account_id,
            partial_cutoff,
            partial_source_as_of,
            CollectedFacts::Stocks(Vec::new()),
        ),
        snapshot(
            &partial_account_id,
            partial_cutoff,
            partial_source_as_of,
            CollectedFacts::Prices(Vec::new()),
        ),
    ];
    writer
        .persist_claimed_batch(&partial_claim, &partial_batch)
        .await
        .unwrap();

    let recipient_id = format!("mcp_reader_{}", std::process::id());
    let delivery = due_deliveries(
        Utc.with_ymd_and_hms(2099, 9, 16, 3, 0, 0).unwrap(),
        &recipient_id,
        1,
        &BTreeSet::new(),
    )
    .unwrap()
    .remove(0);
    assert_eq!(delivery.covered_keys[0].kind, ReportKind::Morning);
    let outbox = PostgresOutboxRepository::connect(&Config::from_str(&worker_url).unwrap())
        .await
        .unwrap();
    let batch_id = match outbox.create_planned(delivery).await.unwrap() {
        CreateOutcome::Inserted(id) => id,
        CreateOutcome::Existing(_) => unreachable!(),
    };
    outbox.start_generation(batch_id).await.unwrap();
    outbox
        .mark_ready(batch_id, &artifact(&recipient_id))
        .await
        .unwrap();

    let reader = ReportingReader::connect_optional(Some(&reader_url))
        .await
        .unwrap();
    assert!(reader.is_enabled());
    let debug = format!("{reader:?}");
    assert!(debug.contains("enabled: true"));
    assert!(!debug.contains(&reader_url));

    let status = reader.collection_status(&account, 50).await.unwrap();
    assert_eq!(status.account_id, account_id);
    assert_eq!(status.items.len(), 5);
    assert!(
        status
            .items
            .iter()
            .all(|item| item.last_published.is_some())
    );

    let completeness = reader.data_completeness(&account, None).await.unwrap();
    assert_eq!(completeness.state, DataState::Complete);
    assert!(completeness.recommendations_allowed);
    assert!(completeness.sources.iter().all(|source| source.available));
    assert_eq!(
        reader
            .data_completeness(&account, Some(cutoff))
            .await
            .unwrap(),
        completeness
    );

    let missing = AccountScope::new(
        format!("missing_mcp_read_{}", std::process::id()),
        Marketplace::Ozon,
    )
    .unwrap();
    assert_eq!(
        reader
            .data_completeness(&missing, None)
            .await
            .unwrap()
            .state,
        DataState::Unavailable
    );
    let missing_at_cutoff = reader
        .data_completeness(&missing, Some(cutoff))
        .await
        .unwrap();
    assert_eq!(missing_at_cutoff.state, DataState::Unavailable);
    assert!(missing_at_cutoff.cutoff_at.is_some());

    let history = reader
        .metrics_history(
            &account,
            Some(NaiveDate::from_ymd_opt(2098, 9, 16).unwrap()),
            Some(NaiveDate::from_ymd_opt(2098, 9, 16).unwrap()),
            100,
        )
        .await
        .unwrap();
    assert_eq!(history.points.len(), 1);
    assert_eq!(history.points[0].state, DataState::Complete);
    let kpis = history.points[0].kpis.as_ref().unwrap();
    assert_eq!(kpis.ordered_units, 3);
    assert_eq!(kpis.ad_spend_minor, 12_000);
    let sales_analytics = reader
        .sales_analytics(
            &account,
            SalesAnalyticsQuery {
                date_from: business_date,
                date_to: business_date,
                group_by: SalesAnalyticsGroup::DaySku,
                sort_by: SalesAnalyticsSort::OperationalGmv,
                direction: SalesAnalyticsDirection::Desc,
                limit: 100,
                offset: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(sales_analytics.state, DataState::Complete);
    assert_eq!(sales_analytics.total_rows, 1);
    assert_eq!(sales_analytics.rows.len(), 1);
    assert_eq!(sales_analytics.rows[0].sku.as_deref(), Some("3411079879"));
    assert_eq!(sales_analytics.rows[0].ordered_units, 3);
    assert_eq!(sales_analytics.rows[0].operational_gmv_minor, 202_500);
    assert_eq!(sales_analytics.coverage.len(), 1);
    assert_eq!(
        sales_analytics.coverage[0].state,
        SalesDateCoverageState::Complete
    );
    assert!(
        reader
            .metrics_history(
                &missing,
                Some(NaiveDate::from_ymd_opt(2098, 9, 16).unwrap()),
                Some(NaiveDate::from_ymd_opt(2098, 9, 16).unwrap()),
                100,
            )
            .await
            .unwrap()
            .points
            .is_empty()
    );

    let actions = reader.manager_actions(&account, None).await.unwrap();
    assert_eq!(actions.state, DataState::Complete);
    assert!(actions.recommendations_allowed);
    assert!(
        actions
            .actions
            .iter()
            .any(|action| action.kind == ManagerActionKind::AdvertisedWithoutStock)
    );
    assert_eq!(
        reader
            .manager_actions(&account, Some(cutoff))
            .await
            .unwrap(),
        actions
    );
    assert_eq!(
        reader.manager_actions(&missing, None).await.unwrap().state,
        DataState::Unavailable
    );
    let missing_actions = reader
        .manager_actions(&missing, Some(cutoff))
        .await
        .unwrap();
    assert_eq!(missing_actions.state, DataState::Unavailable);
    assert!(missing_actions.cutoff_at.is_some());
    let suppressed = reader
        .manager_actions(&partial_account, Some(partial_cutoff))
        .await
        .unwrap();
    assert_eq!(suppressed.state, DataState::Partial);
    assert!(!suppressed.recommendations_allowed);
    assert!(suppressed.actions.is_empty());

    let (debug_client, debug_connection) = Config::from_str(&reader_url)
        .unwrap()
        .connect(NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = debug_connection.await;
    });
    assert!(
        debug_client
            .query(
                "SELECT id FROM daily_reporting.delivery_batches LIMIT 1",
                &[],
            )
            .await
            .is_err()
    );
    assert!(
        debug_client
            .execute(
                "DELETE FROM daily_reporting.mcp_ready_reports WHERE false",
                &[],
            )
            .await
            .is_err()
    );
    let ready = reader.ready_reports(100).await.unwrap();
    let serialized = serde_json::to_string(&ready).unwrap();
    for forbidden in [
        "recipient",
        "email",
        "provider",
        "object_key",
        "sha256",
        "error",
    ] {
        assert!(!serialized.contains(forbidden));
    }
    let report = ready
        .reports
        .iter()
        .find(|report| report.batch_id == format!("rb_{batch_id:016x}"))
        .unwrap();
    assert_eq!(report.kind, ReadyReportKind::Morning);
    assert_eq!(report.state, ReadyReportState::Ready);
    assert!(report.artifact_ready);
    assert!(!report.sent);

    let (admin_client, admin_connection) = Config::from_str(&admin_url)
        .unwrap()
        .connect(NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = admin_connection.await;
    });
    admin_client
        .batch_execute("REVOKE SELECT ON daily_reporting.mcp_price_facts FROM position_reader")
        .await
        .unwrap();
    let missing_acl = ReportingReader::connect_optional(Some(&reader_url))
        .await
        .unwrap_err();
    admin_client
        .batch_execute("GRANT SELECT ON daily_reporting.mcp_price_facts TO position_reader")
        .await
        .unwrap();
    assert_eq!(
        missing_acl.to_string(),
        "reporting reader database contract is unavailable"
    );
    let claimed = outbox
        .claim_ready(timestamp("2099-09-16T03:01:00Z"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.batch_id, batch_id);
    outbox
        .record_permanent_failure(
            &claimed,
            timestamp("2099-09-16T03:01:00Z"),
            timestamp("2099-09-16T03:02:00Z"),
            DeliveryErrorClass::Authentication,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn configured_reader_startup_errors_are_sanitized() {
    let disabled = ReportingReader::connect_optional(None).await.unwrap();
    assert!(!disabled.is_enabled());

    let malformed = ReportingReader::connect_optional(Some("not a database url"))
        .await
        .unwrap_err();
    assert_eq!(
        malformed.to_string(),
        "reporting reader database configuration is invalid"
    );
    let wrong_role =
        ReportingReader::connect_optional(Some("postgresql://position_admin:secret@127.0.0.1/ofk"))
            .await
            .unwrap_err();
    assert_eq!(
        wrong_role.to_string(),
        "reporting reader database configuration is invalid"
    );
}
