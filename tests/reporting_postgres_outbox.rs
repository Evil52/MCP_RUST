use std::{collections::BTreeSet, fs, str::FromStr};

use chrono::{Duration, TimeZone, Utc};
use mcp_ozon::reporting::{
    due_deliveries,
    outbox::{ArtifactIdentity, DeliveryErrorClass},
    policy::{AudiencePolicy, DailyReportPolicy, ManagerScope},
    postgres_outbox::{CreateOutcome, PostgresOutboxError, PostgresOutboxRepository},
    service::ReportWorkerConfig,
};
use tokio_postgres::Config;

fn utc(hour: u32, minute: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2099, 8, 16, hour, minute, 0).unwrap()
}

fn artifact() -> ArtifactIdentity {
    ArtifactIdentity {
        object_key: "daily/2099-08-16/integration_owner.xlsx".to_owned(),
        sha256: "b".repeat(64),
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

#[tokio::test]
async fn report_worker_runtime_uses_only_the_restricted_database_role() {
    let Ok(url) = std::env::var("REPORT_OUTBOX_TEST_WORKER_URL") else {
        return;
    };
    let directory = std::env::temp_dir().join(format!(
        "mcp-ozon-report-worker-runtime-{}",
        std::process::id()
    ));
    fs::create_dir_all(&directory).unwrap();
    let registry = directory.join("access.json");
    let policy = directory.join("policy.json");
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
        "REPORT_WORKER_DATABASE_URL" => Some(url.clone()),
        "MCP_ACCESS_CONFIG" => Some(registry.display().to_string()),
        "DAILY_REPORT_POLICY" => Some(policy.display().to_string()),
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
}

#[tokio::test]
async fn postgres_report_outbox_is_idempotent_bounded_and_audited() {
    let Ok(url) = std::env::var("REPORT_OUTBOX_TEST_WORKER_URL") else {
        return;
    };
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
                    has_table_privilege(current_user, 'daily_reporting.delivery_coverage', 'SELECT,INSERT'), \
                    has_table_privilege(current_user, 'daily_reporting.delivery_attempts', 'SELECT,INSERT'), \
                    NOT has_schema_privilege(current_user, 'search_position', 'USAGE')",
            &[],
        )
        .await
        .unwrap();
    let privilege_values = (0..7)
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

    repository.start_generation(batch_id).await.unwrap();
    repository.mark_ready(batch_id, &artifact()).await.unwrap();
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
                }
            )
            .await,
        Err(PostgresOutboxError::InvalidDelivery)
    );
    repository.mark_ready(batch_id, &artifact()).await.unwrap();
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
        .mark_ready(budget_batch, &artifact())
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
