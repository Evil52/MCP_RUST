//! Claim fencing, failure classes and durable deferral of the WB history worker.
use std::{
    fs,
    path::PathBuf,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Duration;
use serde_json::json;
use tokio_postgres::NoTls;

use super::*;
use crate::{reporting::advertising_history::HistoryGroup, wb::WbClient};

const OWNER: &str = "history-worker-test";
const UNUSED_DATABASE: &str = "postgresql://report_collector:password@position-db/ozon_positions";

static NEXT_FILE: AtomicU64 = AtomicU64::new(1);
/// Claims share one archive-wide lease, so database cases never overlap.
static DATABASE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn temp_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "mcp-ozon-history-worker-{label}-{}-{}",
        std::process::id(),
        NEXT_FILE.fetch_add(1, Ordering::Relaxed)
    ))
}

fn wb_account(id: &str) -> Value {
    json!({"id":id,"organization":"WB","marketplace":"wildberries","seller_client_id":"2",
        "manager_id":"wb","wildberries":{"api_token_env":"WB_TOKEN"}})
}

fn ozon_account(id: &str) -> Value {
    json!({"id":id,"organization":"Ozon","marketplace":"ozon","seller_client_id":"1",
        "manager_id":"wb","ozon":{"store_id":"1","client_id_env":"ID","api_key_env":"KEY",
        "performance":{"client_id_env":"PERF_ID","client_secret_env":"PERF_SECRET"}}})
}

/// A collector for `accounts`; only scheduled mode enables the policy and gets an
/// empty credential directory.
fn collector_config(
    database_url: &str,
    accounts: &[Value],
    scheduled: bool,
) -> (ReportCollectorConfig, Option<PathBuf>) {
    let registry = temp_path("registry");
    let actors = json!([{"id":"wb","name":"WB","role":"manager","oidc":{"username":"wb"}}]);
    fs::write(
        &registry,
        json!({"version":1,"actors":actors,"accounts":accounts}).to_string(),
    )
    .unwrap();
    let ids: Vec<_> = accounts
        .iter()
        .map(|account| account["id"].clone())
        .collect();
    let policy = temp_path("policy");
    let policy_body =
        json!({"version":1,"enabled":scheduled,"timezone":"Asia/Yekaterinburg","account_ids":ids});
    fs::write(&policy, policy_body.to_string()).unwrap();
    let mut values = vec![
        ("REPORT_COLLECTOR_DATABASE_URL", database_url.to_owned()),
        ("MCP_ACCESS_CONFIG", registry.display().to_string()),
        ("REPORT_COLLECTION_POLICY", policy.display().to_string()),
    ];
    let credentials = scheduled.then(|| {
        let directory = temp_path("credentials");
        fs::create_dir(&directory).unwrap();
        values.push(("REPORT_COLLECTOR_MODE", "scheduled".to_owned()));
        values.push((
            "REPORT_COLLECTOR_CREDENTIAL_DIR",
            directory.display().to_string(),
        ));
        directory
    });
    let config = ReportCollectorConfig::from_lookup(&mut |key| {
        values
            .iter()
            .find_map(|(name, value)| (*name == key).then(|| value.clone()))
    })
    .unwrap();
    (config, credentials)
}

fn personal_wb_token() -> String {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"ES256","typ":"JWT"}"#);
    let claims = URL_SAFE_NO_PAD.encode(br#"{"acc":3}"#);
    let signature = URL_SAFE_NO_PAD.encode([0_u8; 64]);
    format!("{header}.{claims}.{signature}")
}

/// A claim as `wb_history_claim` returns it, with optional task fields.
fn claim(account_id: &str, lease_until: DateTime<Utc>, task: &Value) -> HistoryClaim {
    let mut fields =
        json!({"job_id":1,"generation":1,"account_id":account_id,"lease_until":lease_until});
    fields
        .as_object_mut()
        .unwrap()
        .extend(task.as_object().unwrap().clone());
    HistoryClaim(serde_json::from_value(fields).unwrap())
}

/// The refusal text; a resolved client intentionally has no `Debug`.
fn refusal(result: Result<WbClient>) -> String {
    match result {
        Ok(_) => panic!("history credentials must be refused"),
        Err(error) => error.to_string(),
    }
}

#[test]
fn failures_back_off_by_upstream_delay_and_stop_on_permanent_errors() {
    use std::time::Duration as Wait;
    let cases = [
        (
            WbError::LocalRateLimited {
                retry_after: Wait::from_secs(5),
            },
            ("rate_limited", 20, false),
        ),
        (
            WbError::RateLimited {
                request_id: None,
                retry_after: Some(Wait::from_secs(299)),
            },
            ("rate_limited", 300, false),
        ),
        (
            WbError::RateLimited {
                request_id: None,
                retry_after: Some(Wait::from_secs(u64::MAX)),
            },
            ("rate_limited", i32::MAX, false),
        ),
        (
            WbError::RateLimited {
                request_id: None,
                retry_after: None,
            },
            ("rate_limited", 120, false),
        ),
        (WbError::Overloaded, ("local_overloaded", 120, false)),
        (
            WbError::Unauthorized { request_id: None },
            ("unauthorized", 120, true),
        ),
        (
            WbError::Forbidden { request_id: None },
            ("forbidden", 120, true),
        ),
        (
            WbError::MissingCredentials("wb".to_owned()),
            ("missing_credentials", 120, true),
        ),
        (
            WbError::InvalidArguments {
                field: "advert_ids",
            },
            ("invalid_arguments", 120, true),
        ),
        (
            WbError::EndpointNotAllowed {
                method: reqwest::Method::POST,
                path: "/adv/v0/start".to_owned(),
            },
            ("endpoint_not_allowed", 120, true),
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(failure(&anyhow::Error::from(error)), expected);
    }
    // Anything that is not a WB response is a local input defect.
    assert_eq!(
        failure(&anyhow::anyhow!("missing history range")),
        ("invalid_history_input", 120, true)
    );
}

#[test]
fn history_credentials_are_read_only_for_a_live_lease_inside_the_policy() {
    let id = "history_worker_credentials";
    let (scheduled, credentials) = collector_config(UNUSED_DATABASE, &[wb_account(id)], true);
    let credentials = credentials.unwrap();
    let live = Utc::now() + Duration::minutes(2);
    let no_task = json!({});
    assert!(refusal(scheduled.resolve_wb_history(&claim(id, live, &no_task))).contains("WB_TOKEN"));
    fs::write(credentials.join("WB_TOKEN"), "not-a-jwt").unwrap();
    assert!(refusal(scheduled.resolve_wb_history(&claim(id, live, &no_task))).contains("WB_TOKEN"));
    fs::write(credentials.join("WB_TOKEN"), personal_wb_token()).unwrap();
    assert!(
        scheduled
            .resolve_wb_history(&claim(id, live, &no_task))
            .is_ok()
    );

    let expired = claim(id, Utc::now() - Duration::seconds(1), &no_task);
    assert_eq!(
        refusal(scheduled.resolve_wb_history(&expired)),
        "history lease expired"
    );
    assert_eq!(
        refusal(scheduled.resolve_wb_history(&claim("foreign", live, &no_task))),
        "history account outside collection policy"
    );
    let (disabled, _) = collector_config(UNUSED_DATABASE, &[wb_account(id)], false);
    assert_eq!(
        refusal(disabled.resolve_wb_history(&claim(id, live, &no_task))),
        "history requires enabled collection"
    );
}

#[tokio::test]
async fn history_reads_require_a_complete_task_before_any_wb_request() {
    let id = "history_worker_task";
    let (config, credentials) = collector_config(UNUSED_DATABASE, &[wb_account(id)], true);
    fs::write(credentials.unwrap().join("WB_TOKEN"), personal_wb_token()).unwrap();
    let lease = Utc::now() + Duration::minutes(2);
    for (task, message) in [
        (
            json!({"task_id":5,"campaign_ids":[7],"date_to":"2026-10-01"}),
            "missing history range",
        ),
        (
            json!({"task_id":5,"campaign_ids":[7],"date_from":"2026-10-01"}),
            "missing history range",
        ),
        (
            json!({"task_id":5,"kind":"details","date_from":"2026-10-01","date_to":"2026-10-01"}),
            "missing history campaigns",
        ),
    ] {
        let error = fetch(&config, &claim(id, lease, &task)).await.unwrap_err();
        assert_eq!(error.to_string(), message);
    }
}

#[tokio::test]
async fn worker_and_quantum_refuse_disabled_collection_foreign_scope_and_a_disabled_archive() {
    let id = "history_worker_scope";
    let (disabled, _) = collector_config(UNUSED_DATABASE, &[wb_account(id)], false);
    assert_eq!(
        run(&disabled, CancellationToken::new())
            .await
            .unwrap_err()
            .to_string(),
        "history requires enabled scheduled collection"
    );
    let (config, _) = collector_config(UNUSED_DATABASE, &[wb_account(id)], true);
    let archive = HistoryRepository::connect_optional(None).await.unwrap();
    assert_eq!(
        format!("{archive:?}"),
        "HistoryRepository { enabled: false, .. }"
    );
    let outside = run_quantum(&config, &archive, &["foreign".to_owned()], OWNER)
        .await
        .unwrap_err();
    assert_eq!(
        outside.to_string(),
        "history worker scope outside collection policy"
    );
    let unavailable = run_quantum(&config, &archive, &[id.to_owned()], OWNER)
        .await
        .unwrap_err();
    assert_eq!(unavailable.to_string(), "advertising history is disabled");
    let refresh = refresh_initialized(&archive, &[id.to_owned()], last_closed_day())
        .await
        .unwrap_err();
    assert_eq!(refresh.to_string(), "advertising history is disabled");
}

fn env(name: &str) -> String {
    std::env::var(name)
        .expect("run through scripts/with-position-test-db.sh with --include-ignored")
}

fn unique(label: &str) -> String {
    format!("history_worker_{label}_{}", std::process::id())
}

async fn admin() -> tokio_postgres::Client {
    let (client, connection) =
        tokio_postgres::connect(&env("POSITION_REPOSITORY_TEST_ADMIN_URL"), NoTls)
            .await
            .unwrap();
    tokio::spawn(connection);
    client
}

async fn collector() -> HistoryRepository {
    let url = env("REPORT_SNAPSHOT_TEST_COLLECTOR_URL");
    HistoryRepository::connect_collector(&tokio_postgres::Config::from_str(&url).unwrap())
        .await
        .unwrap()
}

async fn requester() -> HistoryRepository {
    HistoryRepository::connect_optional(Some(&env("REPORT_REFRESH_TEST_REQUESTER_URL")))
        .await
        .unwrap()
}

/// Makes queued work claimable now, bypassing the shared WB departure gate.
async fn expedite(admin: &tokio_postgres::Client, account: &str) {
    admin.execute("UPDATE daily_reporting.wb_history_jobs SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE account_id=$1 AND status='queued'", &[&account]).await.unwrap();
    admin.execute("UPDATE daily_reporting.source_collection_departures SET next_allowed_at=clock_timestamp()-interval '1 second' WHERE account_id=$1", &[&account]).await.unwrap();
}

async fn claim_next(collector: &HistoryRepository, account: &str) -> HistoryClaim {
    let value = collector
        .json_query(
            "SELECT daily_reporting.wb_history_claim($1,$2)::text",
            &[&vec![account.to_owned()], &OWNER],
        )
        .await
        .unwrap();
    HistoryClaim(serde_json::from_value(value).unwrap())
}

#[tokio::test]
#[ignore = "requires the disposable PostgreSQL fixture"]
async fn quantum_idles_without_work_and_fails_closed_without_credentials() {
    let _database = DATABASE.lock().await;
    let account = unique("deferral");
    let url = env("REPORT_SNAPSHOT_TEST_COLLECTOR_URL");
    let (config, _) = collector_config(&url, &[wb_account(&account)], true);
    let (admin, collector, api) = (admin().await, collector().await, requester().await);
    let scope = [account.clone()];
    run_quantum(&config, &collector, &scope, OWNER)
        .await
        .unwrap();
    assert!(api.status(&account).await.unwrap()["job"].is_null());

    let date = NaiveDate::from_ymd_opt(2023, 10, 9).unwrap();
    api.request(&account, "finance", Some(date), date)
        .await
        .unwrap();
    expedite(&admin, &account).await;
    run_quantum(&config, &collector, &scope, OWNER)
        .await
        .unwrap();
    let job = &api.status(&account).await.unwrap()["job"];
    assert_eq!(job["status"], "failed", "{job}");
    assert_eq!(job["error_class"], "invalid_history_input");
}

#[tokio::test]
#[ignore = "requires the disposable PostgreSQL fixture"]
async fn publication_routes_inventory_details_and_statistics_through_the_fence() {
    let _database = DATABASE.lock().await;
    let account = unique("publication");
    let (admin, collector, api) = (admin().await, collector().await, requester().await);
    let date = NaiveDate::from_ymd_opt(2023, 10, 9).unwrap();
    // A full-history request first asks WB for unknown campaign creation dates.
    api.request(&account, "finance", None, date).await.unwrap();
    expedite(&admin, &account).await;
    let inventory = claim_next(&collector, &account).await;
    assert_eq!(inventory.0.task_id, None);
    let raw = json!({"all":1,"adverts":[{"status":7,"count":1,"advert_list":[{"advertId":7}]}]});
    let campaigns = normalize_inventory(&raw).unwrap();
    publish(&collector, &inventory, OWNER, &raw, &campaigns)
        .await
        .unwrap();

    expedite(&admin, &account).await;
    let details = claim_next(&collector, &account).await;
    assert_eq!(details.0.kind.as_deref(), Some("details"));
    assert_eq!(details.0.campaign_ids, Some(vec![7]));
    let created = json!([{"campaign_id":7,"created_on":date}]);
    publish(&collector, &details, OWNER, &json!({}), &created)
        .await
        .unwrap();

    expedite(&admin, &account).await;
    let statistics = claim_next(&collector, &account).await;
    let (from, to) = (
        statistics.0.date_from.unwrap(),
        statistics.0.date_to.unwrap(),
    );
    assert_eq!((from, to), (date, date));
    let raw = json!([{"advertId":7,"days":[{"date":date.to_string(),"sum":7,"sum_price":100,
        "orders":1,"views":100,"clicks":3,"apps":[{"nms":[{"nmId":106,"sum":7,"sum_price":100,
        "orders":1,"views":100,"clicks":3}]}]}]}]);
    let days = normalize_statistics(&raw, &[7], from, to).unwrap();
    publish(&collector, &statistics, OWNER, &raw, &days)
        .await
        .unwrap();
    let stats = api
        .stats(&account, None, None, HistoryGroup::Campaign, 100, 0)
        .await
        .unwrap();
    assert_eq!(stats["totals"]["spend_minor"], 700, "{stats}");
    assert_eq!(
        api.status(&account).await.unwrap()["job"]["status"],
        "succeeded"
    );

    // An initialized account gets one rolling refresh per closed day.
    let closed = last_closed_day();
    let scope = [account.clone()];
    refresh_initialized(&collector, &scope, closed)
        .await
        .unwrap();
    let refreshed = api.status(&account).await.unwrap()["job"].clone();
    assert_eq!(refreshed["status"], "queued");
    assert_eq!(refreshed["date_to"], closed.to_string());
    refresh_initialized(&collector, &scope, closed)
        .await
        .unwrap();
    assert_eq!(
        api.status(&account).await.unwrap()["job"]["job_id"],
        refreshed["job_id"]
    );
}

#[tokio::test]
#[ignore = "requires the disposable PostgreSQL fixture"]
async fn worker_loop_refreshes_initialized_accounts_until_cancelled() {
    let _database = DATABASE.lock().await;
    let url = env("REPORT_SNAPSHOT_TEST_COLLECTOR_URL");
    let (ozon_only, _) = collector_config(&url, &[ozon_account(&unique("ozon"))], true);
    assert_eq!(
        run(&ozon_only, CancellationToken::new())
            .await
            .unwrap_err()
            .to_string(),
        "history policy has no WB accounts"
    );

    let account = unique("loop");
    let (config, _) = collector_config(&url, &[wb_account(&account)], true);
    let (admin, collector, api) = (admin().await, collector().await, requester().await);
    // An initialized empty inventory makes the loop own the rolling refresh.
    let date = NaiveDate::from_ymd_opt(2023, 10, 9).unwrap();
    api.request(&account, "finance", Some(date), date)
        .await
        .unwrap();
    expedite(&admin, &account).await;
    let inventory = claim_next(&collector, &account).await;
    let empty = json!({"all":0,"adverts":null});
    publish(
        &collector,
        &inventory,
        OWNER,
        &empty,
        &normalize_inventory(&empty).unwrap(),
    )
    .await
    .unwrap();
    expedite(&admin, &account).await;

    let cancellation = CancellationToken::new();
    let worker = tokio::spawn({
        let cancellation = cancellation.clone();
        async move { run(&config, cancellation).await }
    });
    let closed = last_closed_day().to_string();
    let mut job = Value::Null;
    for _ in 0..100 {
        job = api.status(&account).await.unwrap()["job"].clone();
        if job["date_to"] == closed && job["status"] == "failed" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    cancellation.cancel();
    worker.await.unwrap().unwrap();
    // The refresh was claimed and, without a token, deferred as terminal.
    assert_eq!(job["date_to"], closed, "{job}");
    assert_eq!(job["error_class"], "invalid_history_input");
}
