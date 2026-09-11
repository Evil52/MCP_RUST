use super::*;
use crate::reporting::{
    checkpoint::{CheckpointError, checkpointed},
    mcp_read::{ReportingReader, SourceSnapshotQuery},
    ozon_adapter::{
        OzonReportRequest, product_page_request, sales_request, warehouse_stock_page_request,
    },
    postgres_collector::PostgresCollectorError,
    snapshot::AccountScope,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::{Value, json};
use std::{fs, path::PathBuf, str::FromStr};
use tokio_postgres::{Client, Config, NoTls};

mod sales_publication;

static DATABASE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn source_journal_enforces_bounds_and_publication_preserves_expenses() {
    let (Ok(admin_url), Ok(collector_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
    ) else {
        return;
    };
    let _database = DATABASE.lock().await;
    let fixture = Fixture::new(&admin_url, &collector_url).await;
    fixture.writer.dispatch_source_refreshes(&[]).await.unwrap();
    assert!(
        fixture
            .writer
            .claim_source_job(&[], "empty-scope")
            .await
            .unwrap()
            .is_none()
    );
    let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    let start = now - Duration::days(1);
    assert_eq!(
        fixture
            .writer
            .enqueue_source_jobs(fixture.config.collection_plan(), now, now, start)
            .await,
        Err(PostgresCollectorError::InvalidInput)
    );
    fixture
        .writer
        .enqueue_source_jobs(fixture.config.collection_plan(), now, start, now)
        .await
        .unwrap();
    fixture
        .admin
        .batch_execute("GRANT UPDATE ON daily_reporting.source_collection_jobs TO report_collector")
        .await
        .unwrap();
    let contract = fixture.writer.verify_source_job_contract().await;
    fixture
        .admin
        .batch_execute(
            "REVOKE UPDATE ON daily_reporting.source_collection_jobs FROM report_collector",
        )
        .await
        .unwrap();
    assert_eq!(contract, Err(PostgresCollectorError::Unavailable));
    let account = &fixture.config.collection_plan()[0].account_id;
    fixture.select(account, "advertising", now).await;
    let claim = fixture.claim().await;
    let journal = fixture.writer.source_checkpoints(&claim).unwrap();
    journal.admit().await.unwrap();
    assert_eq!(
        journal
            .save(&"a".repeat(64), Value::String("x".repeat(4_194_305)))
            .await,
        Err(CheckpointError::Invalid)
    );
    journal.save(&"b".repeat(64), json!([])).await.unwrap();
    assert_eq!(
        fixture
            .writer
            .publish_source_job(&claim, CollectedFacts::Advertising(vec![]), vec![], "")
            .await,
        Err(PostgresCollectorError::InvalidInput)
    );
    let expenses = vec![
        crate::reporting::postgres_collector::CollectedAdvertisingExpenseFact {
            business_date: business_date(start),
            campaign_id: 7,
            money_spent_minor: 123,
            bonus_spent_minor: 23,
            prepayment_spent_minor: 100,
        },
    ];
    let snapshot_id = fixture
        .writer
        .publish_source_job(
            &claim,
            CollectedFacts::Advertising(vec![]),
            expenses,
            "expense-test",
        )
        .await
        .unwrap();
    let amount:i64 = fixture.admin.query_one("SELECT money_spent_minor FROM daily_reporting.advertising_expense_facts WHERE snapshot_id=$1", &[&snapshot_id]).await.unwrap().get(0);
    assert_eq!(amount, 123);
    assert_eq!(
        journal.save(&"c".repeat(64), json!([])).await,
        Err(CheckpointError::Unavailable)
    );
}

struct Fixture {
    root: PathBuf,
    config: ReportCollectorConfig,
    admin: Client,
    writer: Arc<PostgresSnapshotWriter>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

impl Fixture {
    async fn new(admin_url: &str, collector_url: &str) -> Self {
        let identity = format!("{}_{}", std::process::id(), Utc::now().timestamp_micros());
        let root = std::env::temp_dir().join(format!("source-quantum-{identity}"));
        fs::create_dir(&root).unwrap();
        let ozon = format!("quantum_ozon_{identity}");
        let wb = format!("quantum_wb_{identity}");
        let registry = json!({"version":1,"actors":[{"id":"owner","name":"Owner","role":"admin"}],"accounts":[
            {"id":ozon,"organization":"Ozon fixture","marketplace":"ozon","seller_client_id":"1","manager_id":"owner","ozon":{"store_id":"1","client_id_env":"ID","api_key_env":"KEY","performance":{"client_id_env":"PERF_ID","client_secret_env":"PERF_SECRET"}}},
            {"id":wb,"organization":"WB fixture","marketplace":"wildberries","seller_client_id":"2","manager_id":"owner","wildberries":{"api_token_env":"WB_TOKEN"}}
        ]});
        fs::write(
            root.join("access.json"),
            serde_json::to_vec(&registry).unwrap(),
        )
        .unwrap();
        fs::write(root.join("policy.json"), serde_json::to_vec(&json!({"version":1,"enabled":true,"timezone":"Asia/Yekaterinburg","account_ids":[ozon,wb]})).unwrap()).unwrap();
        fs::write(root.join("disabled.json"), serde_json::to_vec(&json!({"version":1,"enabled":false,"timezone":"Asia/Yekaterinburg","account_ids":[ozon,wb]})).unwrap()).unwrap();
        for name in ["ID", "KEY", "PERF_ID", "PERF_SECRET"] {
            fs::write(root.join(name), format!("fixture-{name}")).unwrap();
        }
        let token = format!(
            "{}.{}.{}",
            URL_SAFE_NO_PAD.encode(br#"{"alg":"ES256","typ":"JWT"}"#),
            URL_SAFE_NO_PAD.encode(br#"{"acc":3}"#),
            URL_SAFE_NO_PAD.encode([0_u8; 64])
        );
        fs::write(root.join("WB_TOKEN"), token).unwrap();
        let config = Self::config(&root, collector_url, "scheduled");
        let (admin, connection) = tokio_postgres::connect(admin_url, NoTls).await.unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let writer = Arc::new(
            PostgresSnapshotWriter::connect(&Config::from_str(collector_url).unwrap())
                .await
                .unwrap(),
        );
        Self {
            root,
            config,
            admin,
            writer,
        }
    }

    fn config(root: &std::path::Path, url: &str, mode: &str) -> ReportCollectorConfig {
        ReportCollectorConfig::from_lookup(&mut |key| match key {
            "REPORT_COLLECTOR_DATABASE_URL" => Some(url.to_owned()),
            "REPORT_COLLECTOR_MODE" => Some(mode.to_owned()),
            "REPORT_COLLECTION_POLICY" => Some(
                root.join(if mode == "disabled" {
                    "disabled.json"
                } else {
                    "policy.json"
                })
                .display()
                .to_string(),
            ),
            "MCP_ACCESS_CONFIG" => Some(root.join("access.json").display().to_string()),
            "REPORT_COLLECTOR_CREDENTIAL_DIR" if mode != "disabled" => {
                Some(root.display().to_string())
            }
            _ => None,
        })
        .unwrap()
    }

    async fn select(&self, account: &str, source: &str, cutoff: DateTime<Utc>) {
        self.admin.execute("UPDATE daily_reporting.source_collection_jobs SET next_attempt_at=CASE WHEN account_id=$1 AND source=$2 AND cutoff_at=$3 THEN clock_timestamp()-interval '1 second' ELSE clock_timestamp()+interval '1 hour' END WHERE account_id=ANY($4) AND status='ready'", &[&account,&source,&cutoff,&self.accounts()]).await.unwrap();
    }

    fn accounts(&self) -> Vec<String> {
        self.config
            .collection_plan()
            .iter()
            .map(|t| t.account_id.clone())
            .collect()
    }

    async fn claim(&self) -> SourceJobClaim {
        self.writer
            .claim_source_job(self.config.collection_plan(), "quantum-test")
            .await
            .unwrap()
            .unwrap()
    }

    async fn state(&self, claim: &SourceJobClaim) -> (String, Option<String>, i32) {
        let row = self.admin.query_one("SELECT status,error_class,consecutive_failures FROM daily_reporting.source_collection_jobs WHERE account_id=$1 AND source=$2 AND cutoff_at=$3", &[&claim.account_id(),&source_name(claim.source),&claim.cutoff_at()]).await.unwrap();
        (row.get(0), row.get(1), row.get(2))
    }

    async fn seed(&self, claim: &SourceJobClaim, identity: Value, page: Value) {
        self.admin.execute("UPDATE daily_reporting.source_collection_departures SET next_allowed_at=clock_timestamp()-interval '1 second' WHERE account_id=$1", &[&claim.account_id()]).await.unwrap();
        checkpointed(&self.writer.source_checkpoints(claim), identity, || async {
            Ok::<_, CheckpointError>(page)
        })
        .await
        .unwrap();
    }

    async fn forbid_departures(&self, account: &str) {
        // A missing cache key must yield locally, never contact a marketplace.
        self.admin.execute("UPDATE daily_reporting.source_collection_departures SET next_allowed_at=clock_timestamp()+interval '1 hour' WHERE account_id=$1", &[&account]).await.unwrap();
    }
}

fn source_name(source: SnapshotSource) -> &'static str {
    match source {
        SnapshotSource::Sales => "sales",
        SnapshotSource::Stocks => "stocks",
        SnapshotSource::Prices => "prices",
        SnapshotSource::Advertising => "advertising",
        SnapshotSource::Finance => "finance",
    }
}

fn request_key(request: &OzonReportRequest) -> Value {
    json!([request.path, request.payload])
}

fn empty_pages(claim: &SourceJobClaim) -> Vec<(Value, Value)> {
    let date = business_date(claim.period_start);
    let end = business_date(claim.period_end - Duration::microseconds(1));
    match (claim.marketplace(), claim.source) {
        (Marketplace::Ozon, SnapshotSource::Sales) => {
            vec![(
                request_key(&sales_request(date, end, 0).unwrap()),
                json!([]),
            )]
        }
        (Marketplace::Ozon, SnapshotSource::Stocks) => [
            "/v1/product/info/stocks-by-warehouse/fbo",
            "/v2/product/info/stocks-by-warehouse/fbs",
        ]
        .into_iter()
        .map(|path| {
            (
                request_key(&warehouse_stock_page_request(path, None).unwrap()),
                json!([[], null]),
            )
        })
        .collect(),
        (Marketplace::Ozon, SnapshotSource::Prices) => vec![(
            request_key(&product_page_request("/v5/product/info/prices", None).unwrap()),
            json!([[], null]),
        )],
        (Marketplace::Ozon, SnapshotSource::Advertising) => vec![(
            json!(["ozon_performance_campaigns", date, 1]),
            json!([[], 0]),
        )],
        (Marketplace::Ozon, SnapshotSource::Finance) => vec![
            (json!(["ozon_finance_types"]), json!({})),
            (json!(["ozon_finance_day", date, ""]), json!([[], 0, ""])),
        ],
        (Marketplace::Wildberries, SnapshotSource::Sales) => {
            vec![(json!(["wb_sales_v2", date, 250, 0]), json!([[], 0]))]
        }
        (Marketplace::Wildberries, SnapshotSource::Stocks) => {
            vec![(json!(["wb_stock", 0]), json!([[], 0]))]
        }
        (Marketplace::Wildberries, SnapshotSource::Prices) => {
            vec![(json!(["wb_price", 0]), json!([[], 0]))]
        }
        (Marketplace::Wildberries, SnapshotSource::Advertising) => {
            vec![(json!(["wb_campaigns", date]), json!([]))]
        }
        (Marketplace::Wildberries, SnapshotSource::Finance) => unreachable!(),
    }
}

#[tokio::test]
async fn scheduler_replays_all_sources_and_isolates_corrupt_pages_and_credentials() {
    let (Ok(admin_url), Ok(collector_url), Ok(reader_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
        std::env::var("POSITION_REPOSITORY_TEST_READER_URL"),
    ) else {
        return;
    };
    let _database = DATABASE.lock().await;
    let fixture = Fixture::new(&admin_url, &collector_url).await;
    let disabled = Fixture::config(&fixture.root, &collector_url, "disabled");
    let wb = fixture
        .config
        .collection_plan()
        .iter()
        .find(|target| target.marketplace == Marketplace::Wildberries)
        .unwrap();
    let unsupported = SourceJobClaim::for_test(
        crate::reporting::postgres_collector::CollectionClaim::for_test(
            &wb.account_id,
            Marketplace::Wildberries,
            Utc::now() + Duration::minutes(1),
        ),
        SnapshotSource::Finance,
    );
    let failure = collect(&disabled, &fixture.writer, &unsupported)
        .await
        .unwrap_err();
    assert_eq!(
        failure.code, "source_invalid",
        "unsupported sources must be rejected before credential lookup"
    );
    assert!(require_enabled(&disabled, &fixture.writer).await.is_err());
    require_enabled(&fixture.config, &fixture.writer)
        .await
        .unwrap();
    let now = Utc::now();
    enqueue_recent(&fixture.config, &fixture.writer, now)
        .await
        .unwrap();
    enqueue_recent(&fixture.config, &fixture.writer, now)
        .await
        .unwrap();
    let row = fixture.admin.query_one("SELECT count(*),min(cutoff_at),max(cutoff_at) FROM daily_reporting.source_collection_jobs WHERE account_id=ANY($1)", &[&fixture.accounts()]).await.unwrap();
    assert_eq!(
        row.get::<_, i64>(0),
        18,
        "two daily occurrences, nine independent sources, deduplicated"
    );
    let old: DateTime<Utc> = row.get(1);
    let latest: DateTime<Utc> = row.get(2);
    assert!(old >= now - Duration::hours(24) && latest <= now);
    let reader = ReportingReader::connect_optional(Some(&reader_url))
        .await
        .unwrap();
    for target in fixture.config.collection_plan() {
        for &source in &target.sources {
            fixture
                .select(&target.account_id, source_name(source), latest)
                .await;
            let claim = fixture.claim().await;
            for (identity, page) in empty_pages(&claim) {
                fixture.seed(&claim, identity, page).await;
            }
            fixture
                .writer
                .defer_source_job(&claim, None, 1, false)
                .await
                .unwrap();
            fixture
                .select(&target.account_id, source_name(source), latest)
                .await;
            fixture.forbid_departures(&target.account_id).await;
            assert!(
                run_quantum(&fixture.config, &fixture.writer, "resume-test")
                    .await
                    .unwrap()
            );
            assert_eq!(
                fixture.state(&claim).await.0,
                "published",
                "{source:?} must replay without network"
            );
            let scope = AccountScope::new(target.account_id.clone(), target.marketplace).unwrap();
            let query = SourceSnapshotQuery {
                source,
                snapshot_id: None,
                limit: 1,
                offset: 0,
            };
            let published = reader.source_snapshot(&scope, query).await.unwrap();
            assert_eq!(published.total_rows, 0);
            assert_eq!(published.state, "available");

            fixture
                .select(&target.account_id, source_name(source), old)
                .await;
            let corrupt = fixture.claim().await;
            let identity = empty_pages(&corrupt).remove(0).0;
            fixture
                .seed(&corrupt, identity, json!("invalid normalized page"))
                .await;
            fixture
                .writer
                .defer_source_job(&corrupt, None, 1, false)
                .await
                .unwrap();
            fixture
                .select(&target.account_id, source_name(source), old)
                .await;
            fixture.forbid_departures(&target.account_id).await;
            assert!(
                run_quantum(&fixture.config, &fixture.writer, "corrupt-test")
                    .await
                    .unwrap()
            );
            let state = fixture.state(&corrupt).await;
            assert_eq!(state.0, "failed");
            assert_eq!(state.1.as_deref(), Some("checkpoint_invalid"));
            assert_eq!(
                reader
                    .source_snapshot(&scope, query)
                    .await
                    .unwrap()
                    .snapshot_id,
                published.snapshot_id
            );
        }
    }
    assert!(
        !run_quantum(&fixture.config, &fixture.writer, "idle-test")
            .await
            .unwrap()
    );
    let cutoff =
        DateTime::from_timestamp_micros((now - Duration::minutes(1)).timestamp_micros()).unwrap();
    let start = (business_date(now)
        .pred_opt()
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc())
        - Duration::hours(5);
    fixture
        .writer
        .enqueue_source_jobs(
            fixture.config.collection_plan(),
            cutoff,
            start,
            start + Duration::days(1),
        )
        .await
        .unwrap();
    // Lost credentials affect only each claimed source, with no vendor I/O.
    for name in ["KEY", "PERF_SECRET", "WB_TOKEN"] {
        fs::remove_file(fixture.root.join(name)).unwrap();
    }
    let missing = Fixture::config(&fixture.root, &collector_url, "scheduled");
    for target in missing.collection_plan() {
        for &source in &target.sources {
            fixture
                .select(&target.account_id, source_name(source), cutoff)
                .await;
            assert!(
                run_quantum(&missing, &fixture.writer, "credentials-test")
                    .await
                    .unwrap()
            );
        }
    }
    let failed:i64=fixture.admin.query_one("SELECT count(*) FROM daily_reporting.source_collection_jobs WHERE account_id=ANY($1) AND cutoff_at=$2 AND status='failed' AND error_class='credentials_unavailable'", &[&fixture.accounts(),&cutoff]).await.unwrap().get(0);
    assert_eq!(failed, 9);
}

#[tokio::test]
async fn retry_policy_preserves_vendor_delays_and_fences_publication() {
    let (Ok(admin_url), Ok(collector_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
    ) else {
        return;
    };
    let _database = DATABASE.lock().await;
    let fixture = Fixture::new(&admin_url, &collector_url).await;
    let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    let start = (business_date(now)
        .pred_opt()
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc())
        - Duration::hours(5);
    fixture
        .writer
        .enqueue_source_jobs(
            fixture.config.collection_plan(),
            now,
            start,
            start + Duration::days(1),
        )
        .await
        .unwrap();
    let account = &fixture.config.collection_plan()[0].account_id;
    fixture.select(account, "sales", now).await;
    let claim = fixture.claim().await;
    complete_quantum(&fixture.writer, &claim, Err("checkpoint_deferred".into()))
        .await
        .unwrap();
    assert_eq!(fixture.state(&claim).await, ("ready".to_owned(), None, 0));
    fixture.select(account, "sales", now).await;
    let claim = fixture.claim().await;
    complete_quantum(
        &fixture.writer,
        &claim,
        Err(SourceFailure {
            code: "rate_limited",
            retry_after: Some(3600),
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        fixture.state(&claim).await,
        ("ready".to_owned(), Some("rate_limited".to_owned()), 1)
    );
    let delayed:bool=fixture.admin.query_one("SELECT next_attempt_at>clock_timestamp()+interval '59 minutes' FROM daily_reporting.source_collection_jobs WHERE account_id=$1 AND source='sales' AND cutoff_at=$2", &[account,&now]).await.unwrap().get(0);
    assert!(delayed);
    fixture.select(account, "sales", now).await;
    let claim = fixture.claim().await;
    complete_quantum(
        &fixture.writer,
        &claim,
        Err(SourceFailure {
            code: "rate_limited",
            retry_after: Some(86401),
        }),
    )
    .await
    .unwrap();
    assert_eq!(fixture.state(&claim).await.0, "failed");
    fixture.select(account, "prices", now).await;
    let claim = fixture.claim().await;
    complete_quantum(
        &fixture.writer,
        &claim,
        Ok((CollectedFacts::Sales(vec![]), vec![])),
    )
    .await
    .unwrap();
    assert_eq!(
        fixture.state(&claim).await.1.as_deref(),
        Some("invalid_source_publication")
    );
    fixture.select(account, "stocks", now).await;
    let claim = fixture.claim().await;
    for (identity, page) in empty_pages(&claim) {
        fixture.seed(&claim, identity, page).await;
    }
    fixture
        .admin
        .batch_execute("REVOKE INSERT ON daily_reporting.source_snapshots FROM report_collector")
        .await
        .unwrap();
    let publication = complete_quantum(
        &fixture.writer,
        &claim,
        Ok((CollectedFacts::Stocks(vec![]), vec![])),
    )
    .await;
    fixture
        .admin
        .batch_execute("GRANT INSERT ON daily_reporting.source_snapshots TO report_collector")
        .await
        .unwrap();
    publication.unwrap();
    assert_eq!(
        fixture.state(&claim).await,
        (
            "ready".to_owned(),
            Some("database_unavailable".to_owned()),
            1
        ),
        "publication outages must retain a retryable source job"
    );
    fixture.select(account, "stocks", now).await;
    let claim = fixture.claim().await;
    fixture
        .writer
        .defer_source_job(&claim, None, 1, false)
        .await
        .unwrap();
    complete_quantum(
        &fixture.writer,
        &claim,
        Ok((CollectedFacts::Stocks(vec![]), vec![])),
    )
    .await
    .unwrap();
    assert_eq!(
        fixture.state(&claim).await.0,
        "ready",
        "expired owner cannot publish"
    );
    for code in [
        "timeout",
        "rate_limited",
        "network_error",
        "transport_error",
        "local_overloaded",
        "token_endpoint_cooldown",
        "upstream_http_error",
        "upstream_server_error",
        "checkpoint_unavailable",
    ] {
        assert!(retryable(code));
    }
    for code in [
        "credentials_unavailable",
        "checkpoint_invalid",
        "invalid_response",
    ] {
        assert!(!retryable(code));
    }
}
