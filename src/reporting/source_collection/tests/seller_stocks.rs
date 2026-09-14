use super::*;
use crate::reporting::checkpoint::StockPageScope;

#[tokio::test]
async fn seller_stock_quotas_survive_restart_and_cannot_bypass_analytics_or_other_sources() {
    let (Ok(admin_url), Ok(collector_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
    ) else {
        return;
    };
    let _database = DATABASE.lock().await;
    let fixture = Fixture::new(&admin_url, &collector_url).await;
    fixture.writer.verify_runtime_contract().await.unwrap();
    let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    fixture
        .writer
        .enqueue_source_jobs(
            fixture.config.collection_plan(),
            now,
            now - Duration::days(1),
            now,
        )
        .await
        .unwrap();
    let target = fixture
        .config
        .collection_plan()
        .iter()
        .find(|target| target.marketplace == Marketplace::Wildberries)
        .unwrap();
    fixture.select(&target.account_id, "stocks", now).await;
    let claim = fixture.claim().await;
    fixture.admin.execute("INSERT INTO daily_reporting.source_collection_departures VALUES($1,'wildberries','analytics',clock_timestamp()+interval '1 hour') ON CONFLICT(account_id,marketplace,source) DO UPDATE SET next_allowed_at=EXCLUDED.next_allowed_at",&[&target.account_id]).await.unwrap();
    let journal = fixture.writer.source_checkpoints(&claim).unwrap();
    journal
        .admit_stock_page(StockPageScope::SellerInventory)
        .await
        .unwrap();
    assert_eq!(
        journal.admit_stock_page(StockPageScope::Content).await,
        Err(CheckpointError::Deferred),
        "all scopes share one page quantum"
    );
    fixture
        .writer
        .defer_source_job(&claim, Some("rate_limited"), 120, false)
        .await
        .unwrap();
    let gates=fixture.admin.query_one("SELECT bool_and(CASE source WHEN 'analytics' THEN next_allowed_at>clock_timestamp()+interval '50 minutes' ELSE next_allowed_at>clock_timestamp()+interval '110 seconds' END),count(*) FROM daily_reporting.source_collection_departures WHERE account_id=$1 AND source IN ('analytics','wb_seller_inventory')",&[&target.account_id]).await.unwrap();
    assert!(gates.get::<_, bool>(0));
    assert_eq!(gates.get::<_, i64>(1), 2);
    fixture.select(&target.account_id, "stocks", now).await;
    let resumed = fixture.claim().await;
    assert_eq!(
        journal.admit_stock_page(StockPageScope::Content).await,
        Err(CheckpointError::Deferred)
    );
    assert_eq!(
        fixture
            .writer
            .source_checkpoints(&resumed)
            .unwrap()
            .admit_stock_page(StockPageScope::SellerInventory)
            .await,
        Err(CheckpointError::Deferred)
    );
    fixture
        .writer
        .source_checkpoints(&resumed)
        .unwrap()
        .admit_stock_page(StockPageScope::Content)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .writer
            .source_checkpoints(&resumed)
            .unwrap()
            .admit()
            .await,
        Err(CheckpointError::Deferred)
    );
    fixture
        .writer
        .defer_source_job(&resumed, None, 1, true)
        .await
        .unwrap();
    fixture.select(&target.account_id, "sales", now).await;
    let sales = fixture.claim().await;
    assert_eq!(
        fixture
            .writer
            .source_checkpoints(&sales)
            .unwrap()
            .admit_stock_page(StockPageScope::Content)
            .await,
        Err(CheckpointError::Unavailable)
    );
    fixture
        .writer
        .defer_source_job(&sales, None, 1, true)
        .await
        .unwrap();
    let roles:bool=fixture.admin.query_one("SELECT has_function_privilege('report_collector','daily_reporting.admit_source_page(bigint,bigint,text,text)','EXECUTE') AND NOT has_function_privilege('position_reader','daily_reporting.admit_source_page(bigint,bigint,text,text)','EXECUTE') AND NOT has_function_privilege('report_refresh_requester','daily_reporting.admit_source_page(bigint,bigint,text,text)','EXECUTE')",&[]).await.unwrap().get(0);
    assert!(roles);
}

#[tokio::test]
async fn complete_stock_publication_keeps_seller_warehouses_distinct_from_fbw() {
    let (Ok(admin_url), Ok(collector_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
    ) else {
        return;
    };
    let _database = DATABASE.lock().await;
    let fixture = Fixture::new(&admin_url, &collector_url).await;
    let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    fixture
        .writer
        .enqueue_source_jobs(
            fixture.config.collection_plan(),
            now,
            now - Duration::days(1),
            now,
        )
        .await
        .unwrap();
    let target = fixture
        .config
        .collection_plan()
        .iter()
        .find(|target| target.marketplace == Marketplace::Wildberries)
        .unwrap();
    fixture.select(&target.account_id, "stocks", now).await;
    let claim = fixture.claim().await;
    for (identity, page) in [
        (
            json!(["wb_stock", 0]),
            json!([[{"sku":7,"warehouse_id":"wb:10","sellable_units":3}],1]),
        ),
        (
            json!(["wb_seller_warehouses_v1"]),
            json!([{"id":10,"delivery_type":1}]),
        ),
        (
            json!(["wb_stock_cards_v1", null]),
            json!({"products":[[7,[100,101]]],"next":null}),
        ),
        (
            json!(["wb_seller_stock_v1", 10, [100, 101]]),
            json!([[100, 2], [101, 3]]),
        ),
    ] {
        fixture.seed(&claim, identity, page).await;
    }
    fixture
        .writer
        .defer_source_job(&claim, None, 1, false)
        .await
        .unwrap();
    fixture.select(&target.account_id, "stocks", now).await;
    fixture.forbid_departures(&target.account_id).await;
    assert!(
        run_quantum(&fixture.config, &fixture.writer, "stock-replay")
            .await
            .unwrap()
    );
    assert_eq!(fixture.state(&claim).await.0, "published");
    let rows=fixture.admin.query("SELECT warehouse_id,sellable_units::bigint FROM daily_reporting.mcp_stock_facts WHERE account_id=$1 ORDER BY warehouse_id",&[&target.account_id]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        (rows[0].get::<_, String>(0), rows[0].get::<_, i64>(1)),
        ("wb:10".into(), 3)
    );
    assert_eq!(
        (rows[1].get::<_, String>(0), rows[1].get::<_, i64>(1)),
        ("wb:seller:1:10".into(), 5)
    );
}
