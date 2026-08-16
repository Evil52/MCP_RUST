use std::{future::Future, pin::Pin, str::FromStr, time::Duration};

use chrono::{TimeZone, Utc};
use mcp_ozon::position_collector::{
    BatchPlan, Collector, CollectorRuntimeConfig, CollectorRuntimeMode, MonitorTarget,
    PersistOutcome, PersistenceBatch, PlacementKind, PositionRepository, PositionSource,
    PostgresRepository, QueryRequest, QueryScan, RepositoryError, SearchHit, SourceError,
};
use tokio_postgres::{Client, Config, NoTls};

#[derive(Clone)]
struct StaticSource {
    scan: QueryScan,
}

impl PositionSource for StaticSource {
    fn scan(
        &self,
        _request: QueryRequest,
    ) -> Pin<Box<dyn Future<Output = Result<QueryScan, SourceError>> + Send + '_>> {
        Box::pin(std::future::ready(Ok(self.scan.clone())))
    }
}

struct RateLimitedSource;

impl PositionSource for RateLimitedSource {
    fn scan(
        &self,
        _request: QueryRequest,
    ) -> Pin<Box<dyn Future<Output = Result<QueryScan, SourceError>> + Send + '_>> {
        Box::pin(std::future::ready(Err(SourceError::RateLimited)))
    }
}

async fn connect(url: &str) -> Client {
    let config = Config::from_str(url).expect("test database URL must be valid");
    let (client, connection) = config
        .connect(NoTls)
        .await
        .expect("test database must connect");
    tokio::spawn(async move {
        connection
            .await
            .expect("test database connection must remain healthy");
    });
    client
}

fn slot(hour: u32, minute: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 16, hour, minute, 0).unwrap()
}

fn target(monitor_id: i64, product_id: &str, phrase: &str) -> MonitorTarget {
    MonitorTarget::new(
        monitor_id,
        "store-integration",
        product_id,
        phrase,
        "moscow",
        "Москва",
        100,
    )
    .unwrap()
}

async fn successful_batch(
    plan: &BatchPlan,
    hits: Vec<SearchHit>,
    version: &str,
) -> PersistenceBatch {
    let started_at = plan.slot() + chrono::Duration::minutes(5);
    let collector = Collector::new(StaticSource {
        scan: QueryScan::new(started_at, "moscow", true, true, hits),
    });
    let result = collector.collect_at(plan, started_at).await.unwrap();
    PersistenceBatch::from_result(
        plan,
        &result,
        started_at,
        started_at + chrono::Duration::minutes(1),
        version,
    )
    .unwrap()
}

#[tokio::test]
async fn postgres_repository_is_atomic_idempotent_and_fail_closed() {
    let Ok(admin_url) = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL") else {
        return;
    };
    let collector_url = std::env::var("POSITION_REPOSITORY_TEST_COLLECTOR_URL")
        .expect("collector URL accompanies the admin URL");

    let runtime = CollectorRuntimeConfig::from_env().unwrap();
    assert_eq!(runtime.mode(), CollectorRuntimeMode::Disabled);
    let runtime_repository = runtime.connect_repository().await.unwrap();
    runtime_repository.verify_runtime_contract().await.unwrap();

    let admin = connect(&admin_url).await;
    let rows = admin
        .query(
            "INSERT INTO search_position.monitors (\
                store_id, product_id, search_phrase, region_code, region_name\
             ) VALUES \
                ('store-integration', '1001', 'ручка кнопка', 'moscow', 'Москва'), \
                ('store-integration', '1002', 'ручка кнопка', 'moscow', 'Москва'), \
                ('store-integration', '9999', 'rollback probe', 'moscow', 'Москва') \
             RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let first_id: i64 = rows[0].get(0);
    let second_id: i64 = rows[1].get(0);

    let collector_config = Config::from_str(&collector_url).unwrap();
    let repository = PostgresRepository::connect(&collector_config)
        .await
        .unwrap();
    repository.verify_runtime_contract().await.unwrap();
    let plan = BatchPlan::new(
        slot(7, 30),
        vec![
            target(first_id, "1001", "ручка кнопка"),
            target(second_id, "1002", "ручка кнопка"),
        ],
    )
    .unwrap();
    let batch = successful_batch(
        &plan,
        vec![
            SearchHit::new("1001", 17, PlacementKind::Unknown).unwrap(),
            SearchHit::new("1001", 11, PlacementKind::Organic).unwrap(),
        ],
        "integration-1",
    )
    .await;

    assert_eq!(
        repository.persist(&batch).await,
        Ok(PersistOutcome::Inserted)
    );
    assert_eq!(
        repository.persist(&batch).await,
        Ok(PersistOutcome::AlreadyExists)
    );

    let conflicting = successful_batch(
        &plan,
        vec![SearchHit::new("1001", 11, PlacementKind::Organic).unwrap()],
        "integration-2",
    )
    .await;
    assert_eq!(
        repository.persist(&conflicting).await,
        Err(RepositoryError::SlotConflict)
    );

    let published = admin
        .query_one(
            "SELECT count(*), min(overall_position), \
                    count(*) FILTER (WHERE outcome = 'not_found'), \
                    bool_and(run_status = 'succeeded') \
             FROM search_position.published_measurements \
             WHERE scheduled_for = $1",
            &[&plan.slot()],
        )
        .await
        .unwrap();
    assert_eq!(published.get::<_, i64>(0), 2);
    assert_eq!(published.get::<_, Option<i32>>(1), Some(11));
    assert_eq!(published.get::<_, i64>(2), 1);
    assert!(published.get::<_, bool>(3));
    let digest: String = admin
        .query_one(
            "SELECT payload_digest FROM search_position.collection_runs \
             WHERE scheduled_for = $1",
            &[&plan.slot()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(digest.len(), 64);
    assert!(digest.bytes().all(|byte| byte.is_ascii_hexdigit()));

    let invalid_plan = BatchPlan::new(
        slot(8, 0),
        vec![target(9_999_999, "9999", "rollback probe")],
    )
    .unwrap();
    let invalid_batch = successful_batch(&invalid_plan, Vec::new(), "integration-rollback").await;
    assert_eq!(
        repository.persist(&invalid_batch).await,
        Err(RepositoryError::Unavailable)
    );
    let rolled_back: i64 = admin
        .query_one(
            "SELECT count(*) FROM search_position.collection_runs \
             WHERE scheduled_for = $1",
            &[&invalid_plan.slot()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(rolled_back, 0);

    let protective_plan = BatchPlan::new(
        slot(8, 30),
        vec![target(first_id, "1001", "protective probe")],
    )
    .unwrap();
    let started_at = protective_plan.slot() + chrono::Duration::minutes(5);
    let protective_result = Collector::new(RateLimitedSource)
        .collect_at(&protective_plan, started_at)
        .await
        .unwrap();
    let protective_batch = PersistenceBatch::from_result(
        &protective_plan,
        &protective_result,
        started_at,
        started_at + chrono::Duration::minutes(1),
        "integration-protective",
    )
    .unwrap();
    assert_eq!(
        repository.persist(&protective_batch).await,
        Ok(PersistOutcome::Inserted)
    );
    let circuit = admin
        .query_one(
            "SELECT circuit_open, reason FROM search_position.ozon_collector_circuit \
             WHERE source = 'ozon_public_search'",
            &[],
        )
        .await
        .unwrap();
    assert!(circuit.get::<_, bool>(0));
    assert_eq!(circuit.get::<_, &str>(1), "rate_limited");

    let mut unavailable = Config::new();
    unavailable
        .host("127.0.0.1")
        .port(1)
        .connect_timeout(Duration::from_millis(100));
    assert_eq!(
        PostgresRepository::connect(&unavailable).await.err(),
        Some(RepositoryError::Unavailable)
    );

    let (client, connection) = collector_config.connect(NoTls).await.unwrap();
    tokio::spawn(async move {
        let _connection_result = connection.await;
    });
    let from_client = PostgresRepository::from_client(client);
    assert_eq!(
        from_client.persist(&batch).await,
        Ok(PersistOutcome::AlreadyExists)
    );

    let admin_repository = PostgresRepository::from_client(connect(&admin_url).await);
    assert_eq!(
        admin_repository.verify_runtime_contract().await,
        Err(RepositoryError::Unavailable)
    );
}
