use super::*;
use mcp_ozon::reporting::checkpoint::{StockPageScope, stock_checkpoints};

async fn due_again(admin: &Client, account: &str) {
    // Advance only the job fixture, preserving vendor departure reservations.
    admin.execute(
        "UPDATE daily_reporting.source_collection_jobs SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE account_id=$1 AND source='seller_stocks' AND status='ready'",
        &[&account],
    ).await.unwrap();
}

pub(super) async fn verify(admin: &Client, writer: &Arc<PostgresSnapshotWriter>) {
    let account = format!("seller_quota_{}", std::process::id());
    let target = CollectionTarget {
        account_id: account.clone(),
        marketplace: Marketplace::Wildberries,
        sources: vec![
            SnapshotSource::Sales,
            SnapshotSource::Advertising,
            SnapshotSource::Stocks,
            SnapshotSource::Prices,
        ],
    };
    let (cutoff, start, end) = collection_window(Utc::now());
    writer
        .enqueue_source_jobs(std::slice::from_ref(&target), cutoff, start, end)
        .await
        .unwrap();
    select_source(admin, &account, "seller_stocks").await;
    admin.execute(
        "INSERT INTO daily_reporting.source_collection_departures VALUES($1,'wildberries','analytics',clock_timestamp()+interval '1 hour')",
        &[&account],
    ).await.unwrap();
    let first = claim(writer, &target).await;
    assert_eq!(first.source, SnapshotSource::SellerStocks);
    let journal = writer.source_checkpoints(&first);
    checkpointed(
        &stock_checkpoints(&journal, StockPageScope::SellerInventory),
        json!("seller_quota_page"),
        || async { Ok::<_, CheckpointError>(vec![1]) },
    )
    .await
    .unwrap();
    assert_eq!(
        stock_checkpoints(&journal, StockPageScope::Content)
            .unwrap()
            .admit()
            .await,
        Err(CheckpointError::Deferred),
        "Content and Seller Inventory share the source's one-page quantum"
    );
    writer
        .defer_source_job(&first, Some("rate_limited"), 120, false)
        .await
        .unwrap();
    let gates = admin.query_one(
        "SELECT count(*),bool_and(CASE source WHEN 'analytics' THEN next_allowed_at>clock_timestamp()+interval '50 minutes' ELSE next_allowed_at>clock_timestamp()+interval '110 seconds' END) FROM daily_reporting.source_collection_departures WHERE account_id=$1 AND source IN ('analytics','wb_seller_inventory')",
        &[&account],
    ).await.unwrap();
    assert_eq!(gates.get::<_, i64>(0), 2);
    assert!(gates.get::<_, bool>(1));
    let active: String = admin.query_one(
        "SELECT active_quota_source FROM daily_reporting.source_collection_jobs WHERE account_id=$1 AND source='seller_stocks'",
        &[&account],
    ).await.unwrap().get(0);
    assert_eq!(active, "wb_seller_inventory");

    due_again(admin, &account).await;
    let resumed = claim(writer, &target).await;
    let resumed_journal = writer.source_checkpoints(&resumed);
    let saved: Vec<i32> = checkpointed(
        &stock_checkpoints(&resumed_journal, StockPageScope::SellerInventory),
        json!("seller_quota_page"),
        || async {
            panic!("saved seller page must replay during vendor cooldown");
            #[allow(unreachable_code)]
            Ok::<_, CheckpointError>(vec![])
        },
    )
    .await
    .unwrap();
    assert_eq!(saved, vec![1]);
    assert_eq!(
        stock_checkpoints(&resumed_journal, StockPageScope::SellerInventory)
            .unwrap()
            .admit()
            .await,
        Err(CheckpointError::Deferred),
        "the vendor cooldown survives the next lease"
    );
    writer
        .defer_source_job(&resumed, None, 1, false)
        .await
        .unwrap();
    due_again(admin, &account).await;
    let content_claim = claim(writer, &target).await;
    stock_checkpoints(
        &writer.source_checkpoints(&content_claim),
        StockPageScope::Content,
    )
    .unwrap()
    .admit()
    .await
    .unwrap();
    writer
        .defer_source_job(&content_claim, Some("rate_limited"), 90, false)
        .await
        .unwrap();
    let content: bool = admin.query_one(
        "SELECT next_allowed_at>clock_timestamp()+interval '80 seconds' FROM daily_reporting.source_collection_departures WHERE account_id=$1 AND source='wb_stock_content'",
        &[&account],
    ).await.unwrap().get(0);
    assert!(
        content,
        "Content cooldown belongs to its own persistent bucket"
    );
    due_again(admin, &account).await;
    let stopped = claim(writer, &target).await;
    writer
        .defer_source_job(&stopped, Some("invalid_json"), 1, true)
        .await
        .unwrap();
}
