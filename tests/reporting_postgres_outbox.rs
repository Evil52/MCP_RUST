use std::{collections::BTreeSet, fs, str::FromStr};

use chrono::{Duration, TimeZone, Utc};
use mcp_ozon::reporting::{
    artifact_store::{
        ArtifactPublicationError, LocalArtifactStore, PersistDisposition, persist_and_mark_ready,
    },
    bundle::ReportBundle,
    due_deliveries,
    outbox::{ArtifactIdentity, DeliveryErrorClass},
    policy::{AudiencePolicy, DailyReportPolicy, ManagerScope},
    postgres_outbox::{
        CreateOutcome, GenerationErrorClass, GenerationStatus, PostgresOutboxError,
        PostgresOutboxRepository,
    },
    service::ReportWorkerConfig,
};
use sha2::{Digest, Sha256};
use tokio_postgres::Config;

static DB_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn utc(hour: u32, minute: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2099, 8, 16, hour, minute, 0).unwrap()
}

fn artifact() -> ArtifactIdentity {
    artifact_for("2099/08/16", "integration_owner")
}

fn artifact_for(date: &str, recipient: &str) -> ArtifactIdentity {
    artifact_for_kind(date, recipient, "evening")
}

fn artifact_for_kind(date: &str, recipient: &str, kind: &str) -> ArtifactIdentity {
    ArtifactIdentity {
        object_key: format!("daily-reports/{date}/{recipient}/v1/{kind}.xlsx"),
        sha256: Sha256::digest(b"integration-xlsx")
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        html_sha256: Sha256::digest(b"<html><body>integration report</body></html>")
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
    }
}

fn artifact_bundle() -> ReportBundle {
    ReportBundle {
        html: "<html><body>integration report</body></html>".to_owned(),
        xlsx: b"integration-xlsx".to_vec(),
        attachment_name: "daily-report-2099-08-16-evening.xlsx".to_owned(),
        artifact: artifact(),
    }
}

fn disabled_policy(recipient_id: String) -> DailyReportPolicy {
    DailyReportPolicy {
        version: 1,
        enabled: false,
        timezone: "Asia/Yekaterinburg".to_owned(),
        sender_email_env: "DAILY_REPORT_SENDER_EMAIL".to_owned(),
        audiences: vec![AudiencePolicy {
            id: recipient_id,
            email_env: "DAILY_REPORT_PILOT_RECIPIENT_EMAIL".to_owned(),
            managers: vec![ManagerScope {
                actor_id: "diana_serafimovich".to_owned(),
                account_ids: ["furnitura_dlya_doma".to_owned()].into_iter().collect(),
            }],
        }],
    }
}

async fn verify_report_worker_runtime(url: &str) {
    let directory = std::env::temp_dir().join(format!(
        "mcp-ozon-report-worker-runtime-{}",
        std::process::id()
    ));
    fs::create_dir_all(&directory).unwrap();
    let registry = directory.join("access.json");
    let policy = directory.join("policy.json");
    let artifact_root = directory.join("artifacts");
    fs::create_dir_all(&artifact_root).unwrap();
    fs::write(
        &registry,
        r#"{"version":1,"actors":[{"id":"diana_serafimovich","name":"Diana","role":"manager","oidc":{"username":"diana"}}],"accounts":[{"id":"furnitura_dlya_doma","organization":"Ozon","marketplace":"ozon","seller_client_id":"1","manager_id":"diana_serafimovich","ozon":{"store_id":"ozon-1","client_id_env":"OZON_ID","api_key_env":"OZON_KEY"}}]}"#,
    )
    .unwrap();
    fs::write(
        &policy,
        r#"{"version":1,"enabled":false,"timezone":"Asia/Yekaterinburg","sender_email_env":"DAILY_REPORT_SENDER_EMAIL","audiences":[{"id":"pilot_owner","email_env":"DAILY_REPORT_PILOT_RECIPIENT_EMAIL","managers":[{"actor_id":"diana_serafimovich","account_ids":["furnitura_dlya_doma"]}]}]}"#,
    )
    .unwrap();
    let config = ReportWorkerConfig::from_lookup(|key| match key {
        "REPORT_WORKER_DATABASE_URL" => Some(url.to_owned()),
        "MCP_ACCESS_CONFIG" => Some(registry.display().to_string()),
        "DAILY_REPORT_POLICY" => Some(policy.display().to_string()),
        "REPORT_ARTIFACT_ROOT" => Some(artifact_root.display().to_string()),
        _ => None,
    })
    .unwrap();
    let (outbox, snapshots) = config.connect().await.unwrap();
    outbox.verify_runtime_contract().await.unwrap();
    snapshots.verify_runtime_contract().await.unwrap();
}

#[tokio::test]
async fn scheduler_persists_each_daily_identity_once_without_delivery() {
    let Ok(url) = std::env::var("REPORT_OUTBOX_TEST_WORKER_URL") else {
        return;
    };
    let _guard = DB_TEST_LOCK.lock().await;
    let repository = PostgresOutboxRepository::connect(&Config::from_str(&url).unwrap())
        .await
        .unwrap();
    let recipient = format!("scheduler_{}", std::process::id());
    let policy = disabled_policy(recipient);

    let morning = utc(3, 0);
    let first = repository.plan_due(morning, &policy).await.unwrap();
    assert!(matches!(
        first.as_slice(),
        [(_, CreateOutcome::Inserted(_))]
    ));
    assert!(
        repository
            .plan_due(morning, &policy)
            .await
            .unwrap()
            .is_empty()
    );

    let evening = repository.plan_due(utc(12, 0), &policy).await.unwrap();
    assert!(matches!(
        evening.as_slice(),
        [(_, CreateOutcome::Inserted(_))]
    ));
    let covered = repository
        .covered_keys(utc(12, 30), &policy.audiences[0].id, 1)
        .await
        .unwrap();
    assert_eq!(covered.len(), 2);

    assert_eq!(
        repository.generation_candidate(0, utc(3, 1)).await,
        Err(PostgresOutboxError::InvalidDelivery)
    );

    let recipient = format!("generation_{}", std::process::id());
    let delivery = due_deliveries(utc(3, 0), &recipient, 1, &BTreeSet::new())
        .unwrap()
        .remove(0);
    let batch_id = match repository.create_planned(delivery).await.unwrap() {
        CreateOutcome::Inserted(id) => id,
        CreateOutcome::Existing(_) => unreachable!(),
    };
    assert!(
        repository
            .pending_generation_ids(utc(3, 1), 16)
            .await
            .unwrap()
            .contains(&batch_id)
    );
    for limit in [0, 17] {
        assert_eq!(
            repository.pending_generation_ids(utc(3, 1), limit).await,
            Err(PostgresOutboxError::InvalidDelivery)
        );
    }
    assert_eq!(
        repository.generation_candidate(batch_id, utc(2, 59)).await,
        Err(PostgresOutboxError::Conflict)
    );
    let planned = repository
        .generation_candidate(batch_id, utc(3, 1))
        .await
        .unwrap();
    assert_eq!(planned.status, GenerationStatus::Planned);
    assert_eq!(planned.batch_id, batch_id);
    assert_eq!(planned.key.recipient_id, recipient);
    assert_eq!(planned.key.kind, mcp_ozon::reporting::ReportKind::Morning);
    assert!(planned.generated_at <= utc(3, 1));

    repository.start_generation(batch_id).await.unwrap();
    assert_eq!(
        repository
            .generation_candidate(batch_id, utc(3, 2))
            .await
            .unwrap()
            .status,
        GenerationStatus::Generating
    );
    repository
        .mark_ready(
            batch_id,
            &artifact_for_kind("2099/08/16", &recipient, "morning"),
        )
        .await
        .unwrap();
    assert_eq!(
        repository
            .generation_candidate(batch_id, utc(3, 3))
            .await
            .unwrap()
            .status,
        GenerationStatus::Ready
    );
    assert!(
        !repository
            .pending_generation_ids(utc(3, 3), 16)
            .await
            .unwrap()
            .contains(&batch_id)
    );

    let consolidated_recipient = format!("consolidated_{}", std::process::id());
    let consolidated = due_deliveries(utc(13, 30), &consolidated_recipient, 1, &BTreeSet::new())
        .unwrap()
        .remove(0);
    let consolidated_id = match repository.create_planned(consolidated).await.unwrap() {
        CreateOutcome::Inserted(id) => id,
        CreateOutcome::Existing(_) => unreachable!(),
    };
    assert_eq!(
        repository
            .generation_candidate(consolidated_id, utc(13, 31))
            .await,
        Err(PostgresOutboxError::Conflict)
    );
    assert!(
        !repository
            .pending_generation_ids(utc(13, 31), 16)
            .await
            .unwrap()
            .contains(&consolidated_id)
    );
    assert_eq!(
        repository.generation_candidate(batch_id, utc(9, 1)).await,
        Err(PostgresOutboxError::Conflict)
    );
    drop(repository);
    verify_report_worker_runtime(&url).await;
}

#[tokio::test]
async fn postgres_report_outbox_is_idempotent_bounded_and_audited() {
    let Ok(url) = std::env::var("REPORT_OUTBOX_TEST_WORKER_URL") else {
        return;
    };
    let _guard = DB_TEST_LOCK.lock().await;
    let config = Config::from_str(&url).expect("report worker URL must be valid");
    let repository = PostgresOutboxRepository::connect(&config).await.unwrap();
    let admin_url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL").unwrap();
    let admin_config = Config::from_str(&admin_url).unwrap();
    let wrong_role = PostgresOutboxRepository::connect(&admin_config)
        .await
        .unwrap();
    assert_eq!(
        wrong_role.verify_runtime_contract().await,
        Err(PostgresOutboxError::Unavailable)
    );
    let diagnostic = repository_test_client(&config).await;
    let privileges = diagnostic
        .query_one(
            "SELECT current_user = 'report_worker', \
                    has_table_privilege(current_user, 'daily_reporting.delivery_batches', 'SELECT'), \
                    has_table_privilege(current_user, 'daily_reporting.delivery_batches', 'INSERT'), \
                    has_column_privilege(current_user, 'daily_reporting.delivery_batches', 'status', 'UPDATE'), \
                    has_column_privilege(current_user, 'daily_reporting.delivery_batches', 'artifact_html_sha256', 'UPDATE'), \
                    has_table_privilege(current_user, 'daily_reporting.delivery_coverage', 'SELECT,INSERT'), \
                    has_table_privilege(current_user, 'daily_reporting.delivery_attempts', 'SELECT,INSERT'), \
                    NOT has_schema_privilege(current_user, 'search_position', 'USAGE')",
            &[],
        )
        .await
        .unwrap();
    let privilege_values = (0..8)
        .map(|index| privileges.get::<_, bool>(index))
        .collect::<Vec<_>>();
    assert!(
        privilege_values.iter().all(|value| *value),
        "{privilege_values:?}"
    );
    repository.verify_runtime_contract().await.unwrap();

    let delivery = due_deliveries(utc(13, 30), "integration_owner", 1, &BTreeSet::new())
        .unwrap()
        .remove(0);
    assert_eq!(delivery.covered_keys.len(), 2);
    let batch_id = match repository.create_planned(delivery.clone()).await.unwrap() {
        CreateOutcome::Inserted(id) => id,
        CreateOutcome::Existing(_) => panic!("fresh database unexpectedly contained the report"),
    };
    assert_eq!(
        repository.create_planned(delivery).await.unwrap(),
        CreateOutcome::Existing(batch_id)
    );

    let morning_only = due_deliveries(utc(3, 0), "integration_owner", 1, &BTreeSet::new())
        .unwrap()
        .remove(0);
    assert_eq!(
        repository.create_planned(morning_only).await,
        Err(PostgresOutboxError::Conflict)
    );

    let artifact_root =
        std::env::temp_dir().join(format!("mcp-ozon-report-artifacts-{}", std::process::id()));
    fs::create_dir_all(&artifact_root).unwrap();
    let store = LocalArtifactStore::open(&artifact_root).unwrap();
    assert!(matches!(
        persist_and_mark_ready(&store, &repository, batch_id, &artifact_bundle()).await,
        Err(ArtifactPublicationError::Outbox)
    ));
    repository.start_generation(batch_id).await.unwrap();
    let first_receipt = persist_and_mark_ready(&store, &repository, batch_id, &artifact_bundle())
        .await
        .unwrap();
    assert!(matches!(
        first_receipt.disposition,
        PersistDisposition::Created | PersistDisposition::Reused
    ));
    let retry_receipt = persist_and_mark_ready(&store, &repository, batch_id, &artifact_bundle())
        .await
        .unwrap();
    assert_eq!(retry_receipt.disposition, PersistDisposition::Reused);
    let mut wrong_bundle = artifact_bundle();
    wrong_bundle.artifact.object_key =
        "daily-reports/2099/08/16/foreign/v1/evening.xlsx".to_owned();
    assert!(matches!(
        persist_and_mark_ready(&store, &repository, batch_id, &wrong_bundle).await,
        Err(ArtifactPublicationError::Outbox)
    ));
    assert_eq!(
        repository
            .mark_ready(
                batch_id,
                &ArtifactIdentity {
                    object_key: artifact().object_key,
                    sha256: "c".repeat(64),
                    html_sha256: artifact().html_sha256,
                }
            )
            .await,
        Err(PostgresOutboxError::Conflict)
    );
    let first = repository.claim_ready(utc(13, 31)).await.unwrap().unwrap();
    assert_eq!(first.batch_id, batch_id);
    assert_eq!(first.attempt_no, 1);
    assert_eq!(first.covered_keys.len(), 2);
    assert_eq!(first.artifact, artifact());
    assert!(repository.claim_ready(utc(13, 31)).await.unwrap().is_none());

    repository
        .record_transient_failure(
            &first,
            utc(13, 31),
            utc(13, 32),
            DeliveryErrorClass::RateLimited,
            utc(13, 40),
        )
        .await
        .unwrap();
    assert!(repository.claim_ready(utc(13, 39)).await.unwrap().is_none());
    let second = repository.claim_ready(utc(13, 40)).await.unwrap().unwrap();
    assert_eq!(second.attempt_no, 2);
    repository
        .record_sent(&second, utc(13, 40), utc(13, 41), "gmail.message-2099")
        .await
        .unwrap();
    assert!(repository.claim_ready(utc(13, 42)).await.unwrap().is_none());

    let client = repository_test_client(&config).await;
    let audit = client
        .query_one(
            "SELECT batch.status, batch.delayed, batch.attempts, \
                    count(DISTINCT coverage.report_kind), \
                    count(DISTINCT attempt.id), batch.provider_message_id \
             FROM daily_reporting.delivery_batches AS batch \
             JOIN daily_reporting.delivery_coverage AS coverage \
               ON coverage.batch_id = batch.id \
             JOIN daily_reporting.delivery_attempts AS attempt \
               ON attempt.batch_id = batch.id \
             WHERE batch.id = $1 \
             GROUP BY batch.id",
            &[&batch_id],
        )
        .await
        .unwrap();
    assert_eq!(audit.get::<_, &str>(0), "sent");
    assert!(audit.get::<_, bool>(1));
    assert_eq!(audit.get::<_, i16>(2), 2);
    assert_eq!(audit.get::<_, i64>(3), 2);
    assert_eq!(audit.get::<_, i64>(4), 2);
    assert_eq!(audit.get::<_, &str>(5), "gmail.message-2099");

    assert_eq!(
        repository
            .record_sent(&second, utc(13, 40), utc(13, 41), "duplicate-message")
            .await,
        Err(PostgresOutboxError::Unavailable)
    );
}

async fn repository_test_client(config: &Config) -> tokio_postgres::Client {
    let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
    std::mem::drop(tokio::spawn(connection));
    client
}

#[tokio::test]
async fn repository_rejects_invalid_delivery_inputs_before_writing() {
    let Ok(url) = std::env::var("REPORT_OUTBOX_TEST_WORKER_URL") else {
        return;
    };
    let _guard = DB_TEST_LOCK.lock().await;
    let config = Config::from_str(&url).unwrap();
    let repository = PostgresOutboxRepository::connect(&config).await.unwrap();

    let delivery = due_deliveries(
        utc(13, 30) + Duration::days(1),
        "invalid_probe",
        1,
        &BTreeSet::new(),
    )
    .unwrap()
    .remove(0);
    let batch_id = match repository.create_planned(delivery).await.unwrap() {
        CreateOutcome::Inserted(id) => id,
        CreateOutcome::Existing(_) => unreachable!(),
    };
    repository.start_generation(batch_id).await.unwrap();
    assert_eq!(
        repository
            .mark_ready(
                batch_id,
                &ArtifactIdentity {
                    object_key: String::new(),
                    sha256: "x".repeat(64),
                    html_sha256: "x".repeat(64),
                }
            )
            .await,
        Err(PostgresOutboxError::InvalidDelivery)
    );
    repository
        .mark_ready(batch_id, &artifact_for("2099/08/17", "invalid_probe"))
        .await
        .unwrap();
    let claim = repository
        .claim_ready(utc(13, 31) + Duration::days(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        repository
            .record_transient_failure(
                &claim,
                utc(13, 32) + Duration::days(1),
                utc(13, 31) + Duration::days(1),
                DeliveryErrorClass::Transport,
                utc(13, 40) + Duration::days(1),
            )
            .await,
        Err(PostgresOutboxError::InvalidDelivery)
    );
    assert_eq!(
        repository
            .record_transient_failure(
                &claim,
                utc(13, 31) + Duration::days(1),
                utc(13, 32) + Duration::days(1),
                DeliveryErrorClass::Transport,
                utc(18, 1) + Duration::days(1),
            )
            .await,
        Err(PostgresOutboxError::InvalidDelivery)
    );
    repository
        .record_permanent_failure(
            &claim,
            utc(13, 31) + Duration::days(1),
            utc(13, 32) + Duration::days(1),
            DeliveryErrorClass::Authentication,
        )
        .await
        .unwrap();

    let budget_delivery = due_deliveries(
        utc(13, 30) + Duration::days(2),
        "retry_budget_probe",
        1,
        &BTreeSet::new(),
    )
    .unwrap()
    .remove(0);
    let budget_batch = match repository.create_planned(budget_delivery).await.unwrap() {
        CreateOutcome::Inserted(id) => id,
        CreateOutcome::Existing(_) => unreachable!(),
    };
    repository.start_generation(budget_batch).await.unwrap();
    repository
        .mark_ready(
            budget_batch,
            &artifact_for("2099/08/18", "retry_budget_probe"),
        )
        .await
        .unwrap();
    for attempt in 1..MAX_TEST_ATTEMPTS {
        let minute = 30 + i64::from(attempt) * 2;
        let claimed = repository
            .claim_ready(utc(13, 0) + Duration::days(2) + Duration::minutes(minute))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.attempt_no, attempt);
        let finished = utc(13, 0) + Duration::days(2) + Duration::minutes(minute + 1);
        repository
            .record_transient_failure(
                &claimed,
                finished - Duration::minutes(1),
                finished,
                DeliveryErrorClass::ProviderUnavailable,
                finished + Duration::minutes(1),
            )
            .await
            .unwrap();
    }
    let fifth = repository
        .claim_ready(utc(13, 40) + Duration::days(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fifth.attempt_no, MAX_TEST_ATTEMPTS);
    repository
        .record_transient_failure(
            &fifth,
            utc(13, 40) + Duration::days(2),
            utc(13, 41) + Duration::days(2),
            DeliveryErrorClass::Transport,
            utc(13, 42) + Duration::days(2),
        )
        .await
        .unwrap();
    let client = repository_test_client(&config).await;
    let status: String = client
        .query_one(
            "SELECT status FROM daily_reporting.delivery_batches WHERE id = $1",
            &[&budget_batch],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(status, "permanent_failure");
}

const MAX_TEST_ATTEMPTS: u8 = 5;

/// A batch whose generation keeps failing must stop occupying a candidate
/// slot. Before the backoff existed it stayed at the head of the ordering and
/// was retried every tick forever, starving every healthy batch behind it.
#[tokio::test]
async fn a_failing_generation_backs_off_and_then_exhausts_its_budget() {
    let Ok(url) = std::env::var("REPORT_OUTBOX_TEST_WORKER_URL") else {
        return;
    };
    let _guard = DB_TEST_LOCK.lock().await;
    let repository = PostgresOutboxRepository::connect(&Config::from_str(&url).unwrap())
        .await
        .unwrap();
    let recipient = format!("backoff_{}", std::process::id());
    let policy = disabled_policy(recipient);

    let now = utc(3, 0);
    let planned = repository.plan_due(now, &policy).await.unwrap();
    let CreateOutcome::Inserted(batch_id) = planned[0].1 else {
        panic!("the first planning pass inserts the batch");
    };
    assert!(
        repository
            .pending_generation_ids(now, 16)
            .await
            .unwrap()
            .contains(&batch_id),
        "a fresh batch is a generation candidate"
    );

    // One failure holds the batch back for the base delay, but not beyond it.
    repository
        .record_generation_failure(batch_id, now, GenerationErrorClass::Failed)
        .await
        .unwrap();
    assert!(
        !repository
            .pending_generation_ids(now, 16)
            .await
            .unwrap()
            .contains(&batch_id),
        "a just-failed batch is held back"
    );

    // Spend the rest of the budget. Each attempt is recorded against a batch
    // that is still generatable, so the append-only log stays consistent.
    for attempt in 2..=5 {
        let elapsed = now + Duration::seconds(60 * 2_i64.pow(attempt));
        assert!(
            repository
                .pending_generation_ids(elapsed, 16)
                .await
                .unwrap()
                .contains(&batch_id),
            "attempt {attempt} becomes eligible once its backoff elapses"
        );
        repository
            .record_generation_failure(batch_id, elapsed, GenerationErrorClass::Timeout)
            .await
            .unwrap();
    }

    // The budget is spent: no later time makes it a candidate again.
    let far_future = now + Duration::days(1);
    assert!(
        !repository
            .pending_generation_ids(far_future, 16)
            .await
            .unwrap()
            .contains(&batch_id),
        "an exhausted batch never returns to the queue"
    );
    // A sixth attempt is refused rather than silently extending the budget.
    assert!(
        repository
            .record_generation_failure(batch_id, far_future, GenerationErrorClass::Failed)
            .await
            .is_err(),
        "the attempt budget cannot be exceeded"
    );

    // Read as the operator role: `stalled_report_work` exists for humans
    // triaging stuck work, and granting it to the worker would widen the
    // worker's privileges for no runtime need.
    let admin_url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL").unwrap();
    let client = repository_test_client(&Config::from_str(&admin_url).unwrap()).await;
    let stalled = client
        .query(
            "SELECT stall_kind FROM daily_reporting.stalled_report_work \
             WHERE reference = $1::text",
            &[&batch_id.to_string()],
        )
        .await
        .unwrap();
    assert_eq!(
        stalled
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>(),
        vec!["generation_exhausted".to_owned()],
        "an exhausted batch is visible to operators instead of vanishing"
    );
}
