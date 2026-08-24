use std::{collections::BTreeSet, fs, str::FromStr};

use chrono::{Duration, TimeZone, Utc};
use mcp_ozon::reporting::{
    PendingDelivery, ReportKind,
    artifact_store::{
        ArtifactPublicationError, LocalArtifactStore, PersistDisposition, persist_and_mark_ready,
    },
    bundle::ReportBundle,
    due_deliveries,
    outbox::{ArtifactIdentity, DeliveryErrorClass},
    policy::{AudiencePolicy, DailyReportPolicy, ManagerScope},
    postgres_outbox::{
        ClaimedDelivery, CreateOutcome, GenerationErrorClass, GenerationStatus,
        PostgresOutboxError, PostgresOutboxRepository, ReconciliationDecision,
        ReconciliationOutcome,
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
        sha256: hex_sha256(b"integration-xlsx"),
        html_sha256: hex_sha256(b"<html><body>integration report</body></html>"),
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
                account_ids: std::iter::once("furnitura_dlya_doma".to_owned()).collect(),
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

    let recovered_recipient = format!("recovered_{}", std::process::id());
    let recovered = due_deliveries(utc(13, 30), &recovered_recipient, 1, &BTreeSet::new())
        .unwrap()
        .remove(0);
    let recovered_id = match repository.create_planned(recovered).await.unwrap() {
        CreateOutcome::Inserted(id) => id,
        CreateOutcome::Existing(_) => unreachable!(),
    };
    let recovered_candidate = repository
        .generation_candidate(recovered_id, utc(13, 31))
        .await
        .unwrap();
    assert_eq!(recovered_candidate.batch_id, recovered_id);
    assert_eq!(recovered_candidate.key.recipient_id, recovered_recipient);
    assert_eq!(
        recovered_candidate.key.kind,
        mcp_ozon::reporting::ReportKind::Morning
    );
    assert_eq!(recovered_candidate.status, GenerationStatus::Planned);
    assert!(
        repository
            .pending_generation_ids(utc(13, 31), 16)
            .await
            .unwrap()
            .contains(&recovered_id)
    );
    assert_eq!(
        repository.generation_candidate(batch_id, utc(9, 1)).await,
        Err(PostgresOutboxError::Conflict)
    );
    drop(repository);
    verify_report_worker_runtime(&url).await;
}

#[tokio::test]
async fn scheduled_mail_activation_requires_a_recent_completed_canary() {
    let Ok(url) = std::env::var("REPORT_OUTBOX_TEST_WORKER_URL") else {
        return;
    };
    let _guard = DB_TEST_LOCK.lock().await;
    let repository = PostgresOutboxRepository::connect(&Config::from_str(&url).unwrap())
        .await
        .unwrap();
    let now = utc(12, 0) + Duration::days(60);
    let recipient = format!("activation_{}", std::process::id());

    assert_eq!(
        repository.verify_mail_activation(&recipient, 1, now).await,
        Err(PostgresOutboxError::CanaryMissing)
    );
    assert_eq!(
        repository
            .verify_mail_activation("bad recipient", 1, now)
            .await,
        Err(PostgresOutboxError::InvalidDelivery)
    );

    let delivery = due_deliveries(now, &recipient, 1, &BTreeSet::new())
        .unwrap()
        .remove(1);
    let batch_id = match repository.create_planned(delivery).await.unwrap() {
        CreateOutcome::Inserted(id) => id,
        CreateOutcome::Existing(_) => unreachable!(),
    };
    repository.start_generation(batch_id).await.unwrap();
    repository
        .mark_ready(
            batch_id,
            &artifact_for(&now.format("%Y/%m/%d").to_string(), &recipient),
        )
        .await
        .unwrap();
    let claim = repository
        .claim_ready(now + Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        repository
            .verify_mail_activation(&recipient, 1, now + Duration::minutes(1))
            .await,
        Err(PostgresOutboxError::AmbiguousDelivery)
    );
    repository
        .record_sent(
            &claim,
            now + Duration::minutes(1),
            now + Duration::minutes(2),
            "gmail-canary-proof",
        )
        .await
        .unwrap();

    assert_eq!(
        repository
            .verify_mail_activation(&recipient, 1, now + Duration::minutes(3))
            .await
            .unwrap()
            .canary_sent_at,
        now + Duration::minutes(2)
    );
    assert_eq!(
        repository
            .verify_mail_activation(&recipient, 1, now + Duration::hours(25))
            .await,
        Err(PostgresOutboxError::CanaryMissing)
    );
}

#[tokio::test]
async fn ambiguous_delivery_reconciliation_is_scoped_idempotent_and_append_only() {
    let Ok(url) = std::env::var("REPORT_OUTBOX_TEST_WORKER_URL") else {
        return;
    };
    let _guard = DB_TEST_LOCK.lock().await;
    let config = Config::from_str(&url).unwrap();
    let repository = PostgresOutboxRepository::connect(&config).await.unwrap();

    let sent_recipient = format!("reconcile_sent_{}", std::process::id());
    let sent_time = utc(12, 0) + Duration::days(70);
    let sent_delivery = due_deliveries(sent_time, &sent_recipient, 1, &BTreeSet::new())
        .unwrap()
        .remove(1);
    let sent_batch = match repository.create_planned(sent_delivery).await.unwrap() {
        CreateOutcome::Inserted(id) => id,
        CreateOutcome::Existing(_) => unreachable!(),
    };
    repository.start_generation(sent_batch).await.unwrap();
    repository
        .mark_ready(
            sent_batch,
            &artifact_for(&sent_time.format("%Y/%m/%d").to_string(), &sent_recipient),
        )
        .await
        .unwrap();
    let sent_claim = repository
        .claim_ready(sent_time + Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    let confirmed = ReconciliationDecision::ConfirmedSent {
        provider_message_id: "gmail-reconciled-message".to_owned(),
    };
    for (batch_id, attempt_no, recipient_id, version, decision) in [
        (
            0,
            sent_claim.attempt_no,
            sent_recipient.as_str(),
            1,
            &confirmed,
        ),
        (sent_batch, 0, sent_recipient.as_str(), 1, &confirmed),
        (
            sent_batch,
            sent_claim.attempt_no,
            "wrong recipient",
            1,
            &confirmed,
        ),
        (
            sent_batch,
            sent_claim.attempt_no,
            sent_recipient.as_str(),
            0,
            &confirmed,
        ),
    ] {
        assert_eq!(
            repository
                .reconcile_sending(
                    batch_id,
                    attempt_no,
                    recipient_id,
                    version,
                    Utc::now(),
                    decision,
                )
                .await,
            Err(PostgresOutboxError::InvalidDelivery)
        );
    }
    assert_eq!(
        repository
            .reconcile_sending(
                sent_batch,
                sent_claim.attempt_no,
                &sent_recipient,
                1,
                Utc::now(),
                &ReconciliationDecision::ConfirmedSent {
                    provider_message_id: String::new(),
                },
            )
            .await,
        Err(PostgresOutboxError::InvalidDelivery)
    );
    assert_eq!(
        repository
            .reconcile_sending(
                sent_batch,
                sent_claim.attempt_no,
                "foreign_recipient",
                1,
                Utc::now(),
                &confirmed,
            )
            .await,
        Err(PostgresOutboxError::Conflict)
    );
    assert_eq!(
        repository
            .reconcile_sending(
                sent_batch,
                sent_claim.attempt_no + 1,
                &sent_recipient,
                1,
                Utc::now(),
                &confirmed,
            )
            .await,
        Err(PostgresOutboxError::Conflict)
    );
    assert_eq!(
        repository
            .reconcile_sending(
                sent_batch,
                sent_claim.attempt_no,
                &sent_recipient,
                1,
                Utc::now(),
                &confirmed,
            )
            .await
            .unwrap(),
        ReconciliationOutcome::Applied
    );
    assert_eq!(
        repository
            .reconcile_sending(
                sent_batch,
                sent_claim.attempt_no,
                &sent_recipient,
                1,
                Utc::now(),
                &confirmed,
            )
            .await
            .unwrap(),
        ReconciliationOutcome::Existing
    );
    assert_eq!(
        repository
            .reconcile_sending(
                sent_batch,
                sent_claim.attempt_no,
                &sent_recipient,
                1,
                Utc::now(),
                &ReconciliationDecision::SuppressedUnknown,
            )
            .await,
        Err(PostgresOutboxError::Conflict)
    );

    let suppressed_recipient = format!("reconcile_unknown_{}", std::process::id());
    let suppressed_time = sent_time + Duration::days(1);
    let suppressed_delivery =
        due_deliveries(suppressed_time, &suppressed_recipient, 1, &BTreeSet::new())
            .unwrap()
            .remove(1);
    let suppressed_batch = match repository
        .create_planned(suppressed_delivery)
        .await
        .unwrap()
    {
        CreateOutcome::Inserted(id) => id,
        CreateOutcome::Existing(_) => unreachable!(),
    };
    repository.start_generation(suppressed_batch).await.unwrap();
    repository
        .mark_ready(
            suppressed_batch,
            &artifact_for(
                &suppressed_time.format("%Y/%m/%d").to_string(),
                &suppressed_recipient,
            ),
        )
        .await
        .unwrap();
    let suppressed_claim = repository
        .claim_ready(suppressed_time + Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        repository
            .reconcile_sending(
                suppressed_batch,
                suppressed_claim.attempt_no,
                &suppressed_recipient,
                1,
                Utc::now(),
                &ReconciliationDecision::SuppressedUnknown,
            )
            .await
            .unwrap(),
        ReconciliationOutcome::Applied
    );
    assert_eq!(
        repository
            .reconcile_sending(
                suppressed_batch,
                suppressed_claim.attempt_no,
                &suppressed_recipient,
                1,
                Utc::now(),
                &ReconciliationDecision::SuppressedUnknown,
            )
            .await
            .unwrap(),
        ReconciliationOutcome::Existing
    );

    let client = repository_test_client(&config).await;
    let rows = client
        .query(
            "SELECT batch.status, batch.last_error_class, reconciliation.decision, \
                    reconciliation.provider_message_id \
             FROM daily_reporting.delivery_batches AS batch \
             JOIN daily_reporting.delivery_reconciliations AS reconciliation \
               ON reconciliation.batch_id = batch.id \
             WHERE batch.id = ANY($1) ORDER BY batch.id",
            &[&&[sent_batch, suppressed_batch][..]],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, &str>(0), "sent");
    assert_eq!(rows[0].get::<_, Option<&str>>(1), None);
    assert_eq!(rows[0].get::<_, &str>(2), "confirmed_sent");
    assert_eq!(
        rows[0].get::<_, Option<&str>>(3),
        Some("gmail-reconciled-message")
    );
    assert_eq!(rows[1].get::<_, &str>(0), "permanent_failure");
    assert_eq!(
        rows[1].get::<_, Option<&str>>(1),
        Some("operator_reconciled_unknown")
    );
    assert_eq!(rows[1].get::<_, &str>(2), "suppressed_unknown");
    assert_eq!(rows[1].get::<_, Option<&str>>(3), None);
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
                    has_table_privilege(current_user, 'daily_reporting.delivery_reconciliations', 'SELECT,INSERT'), \
                    NOT has_schema_privilege(current_user, 'search_position', 'USAGE')",
            &[],
        )
        .await
        .unwrap();
    let privilege_values = (0..9)
        .map(|index| privileges.get::<_, bool>(index))
        .collect::<Vec<_>>();
    assert!(
        privilege_values.iter().all(|value| *value),
        "{privilege_values:?}"
    );
    repository.verify_runtime_contract().await.unwrap();

    let delivery = due_deliveries(utc(13, 30), "integration_owner", 1, &BTreeSet::new())
        .unwrap()
        .remove(1);
    assert_eq!(delivery.covered_keys.len(), 1);
    let batch_id = match repository.create_planned(delivery.clone()).await.unwrap() {
        CreateOutcome::Inserted(id) => id,
        CreateOutcome::Existing(_) => panic!("fresh database unexpectedly contained the report"),
    };
    assert_eq!(
        repository.create_planned(delivery).await.unwrap(),
        CreateOutcome::Existing(batch_id)
    );
    let recovered = due_deliveries(utc(13, 30), "integration_owner", 1, &BTreeSet::new()).unwrap();
    let legacy_mixed = PendingDelivery {
        covered_keys: recovered
            .into_iter()
            .flat_map(|delivery| delivery.covered_keys)
            .collect(),
        scheduled_for: utc(12, 0),
        delayed: true,
    };
    assert_eq!(
        repository.create_planned(legacy_mixed).await,
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
    assert_eq!(first.covered_keys.len(), 1);
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
    assert_eq!(audit.get::<_, i64>(3), 1);
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
    .remove(1);
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
    .remove(1);
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

#[tokio::test]
async fn permanent_delivery_failures_keep_their_exact_audit_class() {
    let Ok(url) = std::env::var("REPORT_OUTBOX_TEST_WORKER_URL") else {
        return;
    };
    let _guard = DB_TEST_LOCK.lock().await;
    let config = Config::from_str(&url).unwrap();
    let repository = PostgresOutboxRepository::connect(&config).await.unwrap();

    for (offset, class, expected) in [
        (10, DeliveryErrorClass::InvalidArtifact, "invalid_artifact"),
        (11, DeliveryErrorClass::InvalidRouting, "invalid_routing"),
        (
            12,
            DeliveryErrorClass::ProviderRejected,
            "provider_rejected",
        ),
    ] {
        let now = utc(12, 0) + Duration::days(offset);
        let recipient = format!("permanent_{}_{}", std::process::id(), offset);
        let delivery = due_deliveries(now, &recipient, 1, &BTreeSet::new())
            .unwrap()
            .remove(1);
        let batch_id = match repository.create_planned(delivery).await.unwrap() {
            CreateOutcome::Inserted(id) => id,
            CreateOutcome::Existing(_) => unreachable!(),
        };
        repository.start_generation(batch_id).await.unwrap();
        repository
            .mark_ready(
                batch_id,
                &artifact_for(&now.format("%Y/%m/%d").to_string(), &recipient),
            )
            .await
            .unwrap();
        let claim = repository
            .claim_ready(now + Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claim.batch_id, batch_id);
        repository
            .record_permanent_failure(
                &claim,
                now + Duration::minutes(1),
                now + Duration::minutes(2),
                class,
            )
            .await
            .unwrap();
        let client = repository_test_client(&config).await;
        let row = client
            .query_one(
                "SELECT batch.status, batch.last_error_class, attempt.error_class \
                 FROM daily_reporting.delivery_batches AS batch \
                 JOIN daily_reporting.delivery_attempts AS attempt ON attempt.batch_id = batch.id \
                 WHERE batch.id = $1",
                &[&batch_id],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, &str>(0), "permanent_failure");
        assert_eq!(row.get::<_, &str>(1), expected);
        assert_eq!(row.get::<_, &str>(2), expected);
    }
}

#[tokio::test]
async fn exhausted_retryable_failure_is_terminal_with_its_exact_class() {
    let Ok(url) = std::env::var("REPORT_OUTBOX_TEST_WORKER_URL") else {
        return;
    };
    let _guard = DB_TEST_LOCK.lock().await;
    let config = Config::from_str(&url).unwrap();
    let repository = PostgresOutboxRepository::connect(&config).await.unwrap();
    let now = utc(12, 0) + Duration::days(30);
    let recipient = format!("exhausted_{}", std::process::id());
    let delivery = due_deliveries(now, &recipient, 1, &BTreeSet::new())
        .unwrap()
        .remove(1);
    let batch_id = match repository.create_planned(delivery).await.unwrap() {
        CreateOutcome::Inserted(id) => id,
        CreateOutcome::Existing(_) => unreachable!(),
    };
    repository.start_generation(batch_id).await.unwrap();
    repository
        .mark_ready(
            batch_id,
            &artifact_for(&now.format("%Y/%m/%d").to_string(), &recipient),
        )
        .await
        .unwrap();
    let claim = repository
        .claim_ready(now + Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    repository
        .record_exhausted_failure(
            &claim,
            now + Duration::minutes(1),
            now + Duration::minutes(2),
            DeliveryErrorClass::RateLimited,
        )
        .await
        .unwrap();

    let client = repository_test_client(&config).await;
    let row = client
        .query_one(
            "SELECT batch.status, batch.last_error_class, attempt.outcome, attempt.error_class \
             FROM daily_reporting.delivery_batches AS batch \
             JOIN daily_reporting.delivery_attempts AS attempt ON attempt.batch_id = batch.id \
             WHERE batch.id = $1",
            &[&batch_id],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), "permanent_failure");
    assert_eq!(row.get::<_, &str>(1), "rate_limited");
    assert_eq!(row.get::<_, &str>(2), "permanent");
    assert_eq!(row.get::<_, &str>(3), "rate_limited");
}

/// A morning occurrence recovered after the 17:00 EKB boundary is scheduled
/// into the evening window and persists its 23:00 EKB deadline. Recomputing
/// that deadline from the report kind gives back the 14:00 EKB window it was
/// deliberately moved out of, which turns the first transient failure into a
/// permanent one while hours of the delivery window remain.
#[tokio::test]
async fn recovered_morning_retries_inside_its_persisted_evening_window() {
    let Ok(url) = std::env::var("REPORT_OUTBOX_TEST_WORKER_URL") else {
        return;
    };
    let _guard = DB_TEST_LOCK.lock().await;
    let config = Config::from_str(&url).unwrap();
    let repository = PostgresOutboxRepository::connect(&config).await.unwrap();
    // 13:30 UTC is 18:30 EKB: past the morning deadline, inside the evening
    // window, so the morning occurrence is recovered rather than dropped.
    let now = utc(13, 30) + Duration::days(90);
    let recipient = format!("recovered_retry_{}", std::process::id());
    let recovered = due_deliveries(now, &recipient, 1, &BTreeSet::new())
        .unwrap()
        .remove(0);
    assert_eq!(recovered.covered_keys.len(), 1);
    assert_eq!(recovered.covered_keys[0].kind, ReportKind::Morning);
    let batch_id = match repository.create_planned(recovered).await.unwrap() {
        CreateOutcome::Inserted(id) => id,
        CreateOutcome::Existing(_) => unreachable!(),
    };
    repository.start_generation(batch_id).await.unwrap();
    let date = now.format("%Y/%m/%d").to_string();
    repository
        .mark_ready(batch_id, &artifact_for_kind(&date, &recipient, "morning"))
        .await
        .unwrap();

    let claim = repository
        .claim_ready(now + Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.batch_id, batch_id);
    // 23:00 EKB on the report's local date, not the morning 14:00 EKB.
    assert_eq!(claim.deadline_at, utc(18, 0) + Duration::days(90));

    repository
        .record_transient_failure(
            &claim,
            now + Duration::minutes(1),
            now + Duration::minutes(2),
            DeliveryErrorClass::RateLimited,
            now + Duration::minutes(3),
        )
        .await
        .unwrap();
    // Still refused past the persisted deadline.
    let second = repository
        .claim_ready(now + Duration::minutes(3))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.attempt_no, 2);
    assert_eq!(
        repository
            .record_transient_failure(
                &second,
                now + Duration::minutes(3),
                now + Duration::minutes(4),
                DeliveryErrorClass::RateLimited,
                utc(18, 1) + Duration::days(90),
            )
            .await,
        Err(PostgresOutboxError::InvalidDelivery)
    );
    repository
        .record_sent(
            &second,
            now + Duration::minutes(3),
            now + Duration::minutes(4),
            "gmail.recovered-morning",
        )
        .await
        .unwrap();

    let client = repository_test_client(&config).await;
    let row = client
        .query_one(
            "SELECT batch.status, batch.attempts, coverage.deadline_at \
             FROM daily_reporting.delivery_batches AS batch \
             JOIN daily_reporting.delivery_coverage AS coverage ON coverage.batch_id = batch.id \
             WHERE batch.id = $1",
            &[&batch_id],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), "sent");
    assert_eq!(row.get::<_, i16>(1), 2);
    assert_eq!(
        row.get::<_, chrono::DateTime<Utc>>(2),
        utc(18, 0) + Duration::days(90)
    );
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

/// Once the outbox has claimed a delivery the worker is already committed to
/// sending mail, so a database that disappears mid-flight must be reported as
/// `Unavailable` on every entry point. Silently succeeding would let the worker
/// believe a send was recorded; silently returning "nothing to do" would hide a
/// pending delivery. Aborting the connection task reproduces a severed socket
/// or a backend restart while the client handle is still alive.
#[tokio::test]
async fn every_outbox_entry_point_reports_unavailable_when_the_database_is_gone() {
    verify_every_outbox_entry_point_reports_unavailable(None).await;
    verify_every_outbox_entry_point_reports_unavailable(
        std::env::var("REPORT_OUTBOX_TEST_WORKER_URL").ok(),
    )
    .await;
}

async fn verify_every_outbox_entry_point_reports_unavailable(url: Option<String>) {
    let Some(url) = url else {
        return;
    };
    let _guard = DB_TEST_LOCK.lock().await;
    let (client, connection) = Config::from_str(&url)
        .unwrap()
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection_task = tokio::spawn(connection);
    let repository = PostgresOutboxRepository::from_client(client);
    // The repository is genuinely healthy before the connection disappears, so
    // none of the assertions below can pass for a trivial reason.
    repository.verify_runtime_contract().await.unwrap();

    connection_task.abort();
    let _ = connection_task.await;

    let now = utc(13, 30);
    let recipient = format!("unavailable_{}", std::process::id());
    let mut due = due_deliveries(now, &recipient, 1, &BTreeSet::new()).unwrap();
    let planned = due.remove(0);
    let covered_keys = planned.covered_keys.clone();
    let claim = ClaimedDelivery {
        batch_id: 1,
        recipient_id: recipient.clone(),
        report_version: 1,
        attempt_no: 1,
        artifact: artifact(),
        covered_keys: covered_keys.clone(),
        deadline_at: utc(18, 0),
    };

    assert_eq!(
        repository.verify_runtime_contract().await,
        Err(PostgresOutboxError::Unavailable)
    );
    assert_eq!(
        repository.verify_mail_activation(&recipient, 1, now).await,
        Err(PostgresOutboxError::Unavailable)
    );
    assert_eq!(
        repository.create_planned(planned).await,
        Err(PostgresOutboxError::Unavailable)
    );
    assert_eq!(
        repository.covered_keys(now, &recipient, 1).await,
        Err(PostgresOutboxError::Unavailable)
    );
    assert_eq!(
        repository
            .plan_due(now, &disabled_policy(recipient.clone()))
            .await,
        Err(PostgresOutboxError::Unavailable)
    );
    assert_eq!(
        repository.start_generation(1).await,
        Err(PostgresOutboxError::Unavailable)
    );
    assert_eq!(
        repository.generation_candidate(1, now).await,
        Err(PostgresOutboxError::Unavailable)
    );
    assert_eq!(
        repository.pending_generation_ids(now, 10).await,
        Err(PostgresOutboxError::Unavailable)
    );
    assert_eq!(
        repository
            .record_generation_failure(1, now, GenerationErrorClass::Failed)
            .await,
        Err(PostgresOutboxError::Unavailable)
    );
    assert_eq!(
        repository.mark_ready(1, &artifact()).await,
        Err(PostgresOutboxError::Unavailable)
    );
    assert_eq!(
        repository.claim_ready(now).await,
        Err(PostgresOutboxError::Unavailable)
    );
    assert_eq!(
        repository
            .reconcile_sending(
                1,
                1,
                &recipient,
                1,
                now,
                &ReconciliationDecision::SuppressedUnknown,
            )
            .await,
        Err(PostgresOutboxError::Unavailable)
    );
    assert_eq!(
        repository
            .record_sent(&claim, now, now, "provider-message-id")
            .await,
        Err(PostgresOutboxError::Unavailable)
    );
    assert_eq!(
        repository
            .record_transient_failure(
                &claim,
                now,
                now,
                DeliveryErrorClass::Transport,
                now + Duration::minutes(5),
            )
            .await,
        Err(PostgresOutboxError::Unavailable)
    );
    assert_eq!(
        repository
            .record_permanent_failure(&claim, now, now, DeliveryErrorClass::ProviderRejected)
            .await,
        Err(PostgresOutboxError::Unavailable)
    );
    assert_eq!(
        repository
            .record_exhausted_failure(&claim, now, now, DeliveryErrorClass::Transport)
            .await,
        Err(PostgresOutboxError::Unavailable)
    );
}
