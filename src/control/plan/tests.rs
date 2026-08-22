use super::*;
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use tokio_postgres::Client;

use crate::control::policy::WbBidPlacement;
use crate::control::wb::{WbBidChange, WbCampaignBidSnapshot, WbPreparedBidChange};

const POLICY_DIGEST: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const NEXT_POLICY_DIGEST: &str = "3333333333333333333333333333333333333333333333333333333333333333";
const FIXTURE_PREPARE_RESERVATION_ID: &str =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn quota() -> WbActionQuota {
    WbActionQuota {
        max_actions_per_hour: 10,
        max_actions_per_day: 20,
        cooldown_seconds: 30,
        max_cumulative_abs_delta_kopecks_per_day: 1_000,
    }
}

fn apply_context<'a>(
    plan: &'a WbControlPlan,
    actor_id: &'a str,
    now: DateTime<Utc>,
) -> WbApplyContext<'a> {
    WbApplyContext {
        plan_id: &plan.plan_id,
        actor_id,
        expected_plan_digest: &plan.plan_digest,
        expected_schema_version: 1,
        expected_policy_revision: 7,
        expected_policy_digest: POLICY_DIGEST,
        now,
    }
}

fn fixture(
    advert_id: u64,
) -> (
    Vec<WbBidChange>,
    Vec<WbPreparedBidChange>,
    WbCampaignBidSnapshot,
) {
    let requested = vec![WbBidChange {
        nm_id: 1001,
        placement: WbBidPlacement::Search,
        bid_kopecks: 1050,
    }];
    let changes = vec![WbPreparedBidChange {
        nm_id: 1001,
        placement: WbBidPlacement::Search,
        before_bid_kopecks: 1000,
        bid_kopecks: 1050,
    }];
    let before = WbCampaignBidSnapshot {
        seller_sid: "123e4567-e89b-42d3-a456-426614174000".to_owned(),
        advert_id,
        status: 9,
        bid_type: "manual".to_owned(),
        payment_type: "cpm".to_owned(),
        bids: vec![super::super::wb::WbSnapshotBid {
            nm_id: 1001,
            placement: WbBidPlacement::Search,
            bid_kopecks: 1000,
        }],
    };
    (requested, changes, before)
}

#[test]
fn plan_ids_and_digests_are_bounded_and_domain_separated() {
    let now = Utc::now();
    let (requested, changes, before) = fixture(1);
    let requested_json = serde_json::to_string(&requested).unwrap();
    let changes_json = serde_json::to_string(&changes).unwrap();
    let before_json = serde_json::to_string(&before).unwrap();
    let first_digest = make_plan_digest(
        FIXTURE_PREPARE_RESERVATION_ID,
        "actor",
        "account",
        1,
        1,
        7,
        POLICY_DIGEST,
        quota(),
        &requested_json,
        &changes_json,
        &before_json,
        now,
        now + PLAN_TTL,
    );
    let changed_policy_digest = make_plan_digest(
        FIXTURE_PREPARE_RESERVATION_ID,
        "actor",
        "account",
        1,
        1,
        8,
        POLICY_DIGEST,
        quota(),
        &requested_json,
        &changes_json,
        &before_json,
        now,
        now + PLAN_TTL,
    );
    assert_eq!(first_digest.len(), 64);
    assert_ne!(first_digest, changed_policy_digest);
    let first_id = make_plan_id(&first_digest, now);
    let second_id = make_plan_id(&first_digest, now);
    assert_ne!(first_id, second_id);
    assert!(validate_plan_id(&first_id).is_ok());
    assert!(validate_plan_id("1 OR 1=1").is_err());
}

#[test]
fn quotas_are_bounded_and_delta_is_checked() {
    assert!(quota().validate().is_ok());
    assert!(
        WbActionQuota {
            max_actions_per_hour: 2,
            max_actions_per_day: 1,
            ..quota()
        }
        .validate()
        .is_err()
    );
    let (_, changes, _) = fixture(1);
    assert_eq!(cumulative_abs_delta(&changes).unwrap(), 50);
}

#[test]
fn statuses_and_local_validation_cover_every_fail_closed_mapping() {
    for (status, database_value) in [
        (WbPlanStatus::Prepared, "prepared"),
        (WbPlanStatus::Approved, "approved"),
        (WbPlanStatus::Applying, "applying"),
        (WbPlanStatus::Applied, "applied"),
        (
            WbPlanStatus::ReconciliationRequired,
            "reconciliation_required",
        ),
        (WbPlanStatus::Ambiguous, "ambiguous"),
        (WbPlanStatus::Rejected, "rejected"),
        (WbPlanStatus::Failed, "failed"),
        (WbPlanStatus::Expired, "expired"),
    ] {
        assert_eq!(status.as_db(), database_value);
        assert_eq!(WbPlanStatus::from_db(database_value).unwrap(), status);
    }
    assert_eq!(
        WbPlanStatus::from_db("foreign_state"),
        Err(PlanStoreError::Unavailable)
    );

    assert_eq!(
        validate_digest("not-a-digest"),
        Err(PlanStoreError::InvalidPlan)
    );
    assert_eq!(
        validate_digest(&"A".repeat(64)),
        Err(PlanStoreError::InvalidPlan)
    );
    assert_eq!(
        validate_actor_or_account("contains/slash"),
        Err(PlanStoreError::InvalidPlan)
    );
    assert_eq!(
        validate_approval_reason("free form"),
        Err(PlanStoreError::InvalidPlan)
    );
    assert_eq!(cumulative_abs_delta(&[]), Err(PlanStoreError::InvalidPlan));
    let zero_delta = [WbPreparedBidChange {
        nm_id: 1,
        placement: WbBidPlacement::Search,
        before_bid_kopecks: 10,
        bid_kopecks: 10,
    }];
    assert_eq!(
        cumulative_abs_delta(&zero_delta),
        Err(PlanStoreError::InvalidPlan)
    );
    let overflowing_delta = [
        WbPreparedBidChange {
            nm_id: 1,
            placement: WbBidPlacement::Search,
            before_bid_kopecks: 0,
            bid_kopecks: u64::MAX,
        },
        WbPreparedBidChange {
            nm_id: 2,
            placement: WbBidPlacement::Search,
            before_bid_kopecks: 0,
            bid_kopecks: 1,
        },
    ];
    assert_eq!(
        cumulative_abs_delta(&overflowing_delta),
        Err(PlanStoreError::InvalidPlan)
    );
}

#[tokio::test]
async fn prepare_error_mapper_handles_transport_errors_without_fabricating_db_state() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let closer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        drop(socket);
    });
    let connect_result = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=control_writer password=test dbname=test",
            address.port()
        ),
        tokio_postgres::NoTls,
    )
    .await;
    closer.await.unwrap();
    let error = connect_result
        .err()
        .expect("closed local socket must fail PostgreSQL startup");
    assert_eq!(map_prepare_insert_error(error), PlanStoreError::Unavailable);
}

async fn classify_database_failures_with_optional_test_database(
    admin_url: Result<String, std::env::VarError>,
) {
    let Ok(admin_url) = admin_url else {
        return;
    };
    let _database_guard = CONTROL_DB_TEST_LOCK.lock().await;
    let (mut admin, connection) = tokio_postgres::connect(&admin_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection_task = tokio::spawn(connection);

    for (message, expected) in [
        ("unresolved incident", PlanStoreError::CampaignLocked),
        ("attempt limit", PlanStoreError::PrepareLimitExceeded),
        (
            "outstanding prepare limit",
            PlanStoreError::PrepareLimitExceeded,
        ),
        ("active policy", PlanStoreError::PolicyChanged),
        ("unclassified database failure", PlanStoreError::Unavailable),
    ] {
        let statement = format!("DO $coverage$ BEGIN RAISE EXCEPTION '{message}'; END $coverage$");
        let error = admin.batch_execute(&statement).await.unwrap_err();
        assert_eq!(map_prepare_insert_error(error), expected);
    }

    admin
        .batch_execute(
            "CREATE TEMP TABLE coverage_prepare_unique (id integer PRIMARY KEY); \
                 INSERT INTO coverage_prepare_unique VALUES (1);",
        )
        .await
        .unwrap();
    let unique_error = admin
        .execute("INSERT INTO coverage_prepare_unique VALUES (1)", &[])
        .await
        .unwrap_err();
    assert_eq!(
        map_prepare_insert_error(unique_error),
        PlanStoreError::InvalidState
    );

    let (_, _, before) = fixture(1);
    let orphan_approval_row = admin
        .query_one(
            "SELECT repeat('a',64)::text, repeat('b',64)::text, \
                        'coverage_actor'::text, 'coverage_account'::text, \
                        1::bigint, 1::integer, 7::bigint, $1::text, \
                        10::integer, 20::integer, 30::integer, 1000::bigint, \
                        'prepared'::text, '[]'::text, '[]'::text, $2::text, \
                        clock_timestamp(), clock_timestamp()+interval '5 minutes', \
                        NULL::timestamptz, NULL::text, NULL::text, NULL::text, \
                        repeat('c',64)::text, NULL::text, 'orphan_approver'::text, \
                        NULL::text, NULL::timestamptz, NULL::timestamptz",
            &[&POLICY_DIGEST, &serde_json::to_string(&before).unwrap()],
        )
        .await
        .unwrap();
    assert!(matches!(
        plan_from_row(&orphan_approval_row),
        Err(PlanStoreError::Unavailable)
    ));

    let transaction = admin.transaction().await.unwrap();
    assert_eq!(
        expire_plan(
            &transaction,
            "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            "coverage_actor",
            Utc::now(),
        )
        .await,
        Err(PlanStoreError::InvalidState)
    );
    transaction.rollback().await.unwrap();

    drop(admin);
    connection_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn prepare_error_mapper_classifies_database_failures() {
    classify_database_failures_with_optional_test_database(std::env::var(
        "POSITION_REPOSITORY_TEST_ADMIN_URL",
    ))
    .await;
    classify_database_failures_with_optional_test_database(Err(std::env::VarError::NotPresent))
        .await;
}

#[test]
fn database_url_requires_restricted_identity_and_network_target() {
    assert!(
        validate_control_database_url(
            "postgresql://control_writer:secret@position-db:5432/ozon_positions"
        )
        .is_ok()
    );
    assert!(
        validate_control_database_url("postgresql://postgres:secret@position-db/ozon_positions")
            .is_err()
    );
    assert!(validate_control_database_url("postgresql://control_writer@/ozon_positions").is_err());
    assert!(
        validate_control_database_url(
            "postgresql://control_writer:secret@first:5432,second:5432/ozon_positions"
        )
        .is_err()
    );
    assert!(
        validate_control_database_url(
            "user=control_writer password=secret dbname=ozon_positions host=/tmp port=5432"
        )
        .is_err()
    );
}

async fn set_gate(
    admin: &Client,
    gate_key: &str,
    scope_kind: &str,
    account_id: Option<&str>,
    advert_id: Option<i64>,
    enabled: bool,
    now: DateTime<Utc>,
) {
    admin
        .execute(
            "INSERT INTO control.wb_runtime_gates \
                    (gate_key, scope_kind, account_id, advert_id, enabled, lease_expires_at, \
                     disabled_until, revision, reason, updated_by, updated_at) \
                 VALUES ($1,$2,$3,$4,$5,$6,NULL,1,'integration_test','integration_test',$7) \
                 ON CONFLICT (gate_key) DO UPDATE SET \
                    enabled=EXCLUDED.enabled, lease_expires_at=EXCLUDED.lease_expires_at, \
                    disabled_until=NULL, revision=control.wb_runtime_gates.revision+1, \
                    reason=EXCLUDED.reason, updated_by=EXCLUDED.updated_by, \
                    updated_at=EXCLUDED.updated_at",
            &[
                &gate_key,
                &scope_kind,
                &account_id,
                &advert_id,
                &enabled,
                &(now + Duration::minutes(10)),
                &now,
            ],
        )
        .await
        .unwrap();
}

async fn enable_gates(admin: &Client, account_id: &str, advert_id: u64, now: DateTime<Utc>) {
    set_gate(admin, "global", "global", None, None, true, now).await;
    let account_gate = format!("account/{account_id}");
    set_gate(
        admin,
        &account_gate,
        "account",
        Some(account_id),
        None,
        true,
        now,
    )
    .await;
    let campaign_gate = format!("campaign/{account_id}/{advert_id}");
    set_gate(
        admin,
        &campaign_gate,
        "campaign",
        Some(account_id),
        Some(i64::try_from(advert_id).unwrap()),
        true,
        now,
    )
    .await;
}

async fn create_fixture_plan(
    repository: &WbPlanRepository,
    actor_id: &str,
    account_id: &str,
    advert_id: u64,
    action_quota: WbActionQuota,
    now: DateTime<Utc>,
) -> WbControlPlan {
    let (requested, changes, before) = fixture(advert_id);
    let prepare_reservation = repository
        .reserve_prepare_attempt(
            actor_id,
            account_id,
            advert_id,
            1,
            7,
            POLICY_DIGEST,
            action_quota,
            now,
        )
        .await
        .unwrap();
    repository
        .create(
            actor_id,
            account_id,
            advert_id,
            1,
            7,
            POLICY_DIGEST,
            action_quota,
            &prepare_reservation.reservation_id,
            &requested,
            &changes,
            &before,
            now,
        )
        .await
        .unwrap()
}

async fn create_approved_fixture_plan(
    repository: &WbPlanRepository,
    actor_id: &str,
    account_id: &str,
    advert_id: u64,
    action_quota: WbActionQuota,
    now: DateTime<Utc>,
) -> WbControlPlan {
    let plan = create_fixture_plan(
        repository,
        actor_id,
        account_id,
        advert_id,
        action_quota,
        now,
    )
    .await;
    repository
        .approve(
            &plan.plan_id,
            "integration_approver",
            &plan.plan_digest,
            "coverage/approval",
            now,
        )
        .await
        .unwrap();
    plan
}

async fn create_applying_fixture_plan(
    repository: &WbPlanRepository,
    admin: &Client,
    actor_id: &str,
    account_id: &str,
    advert_id: u64,
    action_quota: WbActionQuota,
    now: DateTime<Utc>,
) -> WbControlPlan {
    let plan = create_approved_fixture_plan(
        repository,
        actor_id,
        account_id,
        advert_id,
        action_quota,
        now,
    )
    .await;
    enable_gates(admin, account_id, advert_id, now).await;
    repository
        .claim_for_apply(apply_context(&plan, actor_id, now))
        .await
        .unwrap();
    plan
}

async fn run_repository_scenarios_with_optional_test_database(
    database_url: Result<String, std::env::VarError>,
    admin_url: Result<String, std::env::VarError>,
) {
    let (Ok(database_url), Ok(admin_url)) = (database_url, admin_url) else {
        return;
    };
    let _database_guard = CONTROL_DB_TEST_LOCK.lock().await;
    let config = validate_control_database_url(&database_url).unwrap();
    let repository = WbPlanRepository::connect(&config).await.unwrap();
    repository.verify_runtime_contract().await.unwrap();
    let (mut admin, admin_connection) = tokio_postgres::connect(&admin_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let admin_connection_task = tokio::spawn(admin_connection);
    let (preconnected_client, preconnected_connection) =
        tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
            .await
            .unwrap();
    let preconnected_connection_task = tokio::spawn(preconnected_connection);
    let preconnected_repository = WbPlanRepository::from_client(preconnected_client);
    preconnected_repository
        .verify_runtime_contract()
        .await
        .unwrap();
    drop(preconnected_repository);
    preconnected_connection_task.await.unwrap().unwrap();
    let (direct_writer, direct_writer_connection) =
        tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
            .await
            .unwrap();
    let direct_writer_connection_task = tokio::spawn(direct_writer_connection);

    admin
        .execute(
            "ALTER TABLE control.wb_prepare_reservations \
                 DISABLE TRIGGER wb_prepare_reservations_append_only",
            &[],
        )
        .await
        .unwrap();
    let disabled_trigger_contract = repository.verify_runtime_contract().await;
    admin
        .execute(
            "ALTER TABLE control.wb_prepare_reservations \
                 ENABLE TRIGGER wb_prepare_reservations_append_only",
            &[],
        )
        .await
        .unwrap();
    assert!(matches!(
        disabled_trigger_contract,
        Err(PlanStoreError::Unavailable)
    ));
    repository.verify_runtime_contract().await.unwrap();
    admin
        .execute("ALTER ROLE control_writer CONNECTION LIMIT 5", &[])
        .await
        .unwrap();
    let widened_role_contract = repository.verify_runtime_contract().await;
    admin
        .execute("ALTER ROLE control_writer CONNECTION LIMIT 4", &[])
        .await
        .unwrap();
    assert!(matches!(
        widened_role_contract,
        Err(PlanStoreError::Unavailable)
    ));
    admin
        .execute(
            "GRANT TEMPORARY ON DATABASE ozon_positions TO control_writer",
            &[],
        )
        .await
        .unwrap();
    let widened_database_contract = repository.verify_runtime_contract().await;
    admin
        .execute(
            "REVOKE TEMPORARY ON DATABASE ozon_positions FROM control_writer",
            &[],
        )
        .await
        .unwrap();
    assert!(matches!(
        widened_database_contract,
        Err(PlanStoreError::Unavailable)
    ));
    admin
        .execute(
            "GRANT CREATE ON DATABASE ozon_positions TO control_writer",
            &[],
        )
        .await
        .unwrap();
    let database_create_contract = repository.verify_runtime_contract().await;
    admin
        .execute(
            "REVOKE CREATE ON DATABASE ozon_positions FROM control_writer",
            &[],
        )
        .await
        .unwrap();
    assert!(matches!(
        database_create_contract,
        Err(PlanStoreError::Unavailable)
    ));
    admin
        .execute("GRANT CREATE ON SCHEMA public TO control_writer", &[])
        .await
        .unwrap();
    let schema_create_contract = repository.verify_runtime_contract().await;
    admin
        .execute("REVOKE CREATE ON SCHEMA public FROM control_writer", &[])
        .await
        .unwrap();
    assert!(matches!(
        schema_create_contract,
        Err(PlanStoreError::Unavailable)
    ));
    repository.verify_runtime_contract().await.unwrap();

    let now = Utc::now();
    repository
        .register_policy(1, 7, POLICY_DIGEST, now)
        .await
        .unwrap();
    assert_eq!(
        repository
            .reserve_prepare_attempt(
                "wrong_policy_actor",
                "wrong_policy_account",
                41,
                1,
                6,
                POLICY_DIGEST,
                quota(),
                now,
            )
            .await,
        Err(PlanStoreError::PolicyChanged)
    );
    assert_eq!(
        repository.register_policy(0, 7, POLICY_DIGEST, now).await,
        Err(PlanStoreError::InvalidPlan)
    );
    repository
        .register_policy(1, 7, POLICY_DIGEST, now)
        .await
        .unwrap();
    assert!(matches!(
        repository
            .register_policy(1, 7, NEXT_POLICY_DIGEST, now)
            .await,
        Err(PlanStoreError::PolicyChanged)
    ));
    assert_eq!(
        repository.register_policy(1, 8, POLICY_DIGEST, now).await,
        Err(PlanStoreError::PolicyChanged)
    );
    admin
        .execute(
            "REVOKE INSERT ON control.wb_policy_revisions FROM control_writer",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        repository
            .register_policy(1, 8, NEXT_POLICY_DIGEST, now)
            .await,
        Err(PlanStoreError::Unavailable)
    );
    admin
        .execute(
            "GRANT INSERT ON control.wb_policy_revisions TO control_writer",
            &[],
        )
        .await
        .unwrap();
    repository.verify_runtime_contract().await.unwrap();
    let plan = create_fixture_plan(
        &repository,
        "integration_actor",
        "integration_account",
        42,
        quota(),
        now + Duration::days(365),
    )
    .await;
    assert!(plan.created_at < now + Duration::minutes(1));
    assert_eq!(
        repository
            .load_by_id_for_approval(&plan.plan_id)
            .await
            .unwrap()
            .plan_digest,
        plan.plan_digest
    );
    assert_eq!(
        repository
            .reserve_prepare_attempt(
                "integration_actor",
                "integration_account",
                0,
                1,
                7,
                POLICY_DIGEST,
                quota(),
                now,
            )
            .await,
        Err(PlanStoreError::InvalidPlan)
    );
    let (fixture_requested, fixture_changes, fixture_before) = fixture(42);
    assert!(matches!(
        repository
            .create(
                "integration_actor",
                "integration_account",
                42,
                1,
                7,
                POLICY_DIGEST,
                quota(),
                &plan.prepare_reservation_id,
                &fixture_requested,
                &[],
                &fixture_before,
                now,
            )
            .await,
        Err(PlanStoreError::InvalidPlan)
    ));
    let mut excessive_change = fixture_changes.clone();
    excessive_change[0].bid_kopecks = 5_000;
    assert!(matches!(
        repository
            .create(
                "integration_actor",
                "integration_account",
                42,
                1,
                7,
                POLICY_DIGEST,
                quota(),
                &plan.prepare_reservation_id,
                &fixture_requested,
                &excessive_change,
                &fixture_before,
                now,
            )
            .await,
        Err(PlanStoreError::QuotaExceeded)
    ));
    assert!(matches!(
        repository
            .create(
                "integration_actor",
                "integration_account",
                42,
                1,
                6,
                POLICY_DIGEST,
                quota(),
                &plan.prepare_reservation_id,
                &fixture_requested,
                &fixture_changes,
                &fixture_before,
                now,
            )
            .await,
        Err(PlanStoreError::PolicyChanged)
    ));
    assert!(matches!(
        repository
            .create(
                "different_actor",
                "integration_account",
                42,
                1,
                7,
                POLICY_DIGEST,
                quota(),
                &plan.prepare_reservation_id,
                &fixture_requested,
                &fixture_changes,
                &fixture_before,
                now,
            )
            .await,
        Err(PlanStoreError::InvalidPlan)
    ));
    assert!(matches!(
        repository
            .create(
                "integration_actor",
                "integration_account",
                42,
                1,
                7,
                POLICY_DIGEST,
                quota(),
                &plan.prepare_reservation_id,
                &fixture_requested,
                &fixture_changes,
                &fixture_before,
                now,
            )
            .await,
        Err(PlanStoreError::InvalidState)
    ));

    let expired_prepare = repository
        .reserve_prepare_attempt(
            "expired_prepare_actor",
            "expired_prepare_account",
            43,
            1,
            7,
            POLICY_DIGEST,
            quota(),
            now,
        )
        .await
        .unwrap();
    admin
        .execute(
            "ALTER TABLE control.wb_prepare_reservations \
                 DISABLE TRIGGER wb_prepare_reservations_append_only",
            &[],
        )
        .await
        .unwrap();
    admin
            .execute(
                "UPDATE control.wb_prepare_reservations reservation SET \
                     reserved_at=skew.reserved_at, expires_at=skew.reserved_at+interval '2 minutes' \
                 FROM (SELECT clock_timestamp()-interval '3 minutes' AS reserved_at) skew \
                 WHERE reservation.reservation_id=$1",
                &[&expired_prepare.reservation_id],
            )
            .await
            .unwrap();
    admin
        .execute(
            "ALTER TABLE control.wb_prepare_reservations \
                 ENABLE TRIGGER wb_prepare_reservations_append_only",
            &[],
        )
        .await
        .unwrap();
    let (expired_requested, expired_changes, expired_before) = fixture(43);
    assert!(matches!(
        repository
            .create(
                "expired_prepare_actor",
                "expired_prepare_account",
                43,
                1,
                7,
                POLICY_DIGEST,
                quota(),
                &expired_prepare.reservation_id,
                &expired_requested,
                &expired_changes,
                &expired_before,
                now,
            )
            .await,
        Err(PlanStoreError::PrepareLimitExceeded)
    ));

    let mut outstanding_prepares = Vec::new();
    for _ in 0..3 {
        outstanding_prepares.push(
            repository
                .reserve_prepare_attempt(
                    "outstanding_prepare_actor",
                    "outstanding_prepare_account",
                    49,
                    1,
                    7,
                    POLICY_DIGEST,
                    quota(),
                    now,
                )
                .await
                .unwrap(),
        );
    }
    admin
        .execute(
            "ALTER TABLE control.wb_prepare_reservations \
                 DISABLE TRIGGER wb_prepare_reservations_validate",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO control.wb_prepare_reservations \
                    (reservation_id, actor_id, account_id, advert_id, schema_version, \
                     policy_revision, policy_digest, quota_max_actions_per_hour, \
                     quota_max_actions_per_day, quota_cooldown_seconds, \
                     quota_max_cumulative_abs_delta_kopecks_per_day, reserved_at, expires_at) \
                 SELECT repeat('e',64), actor_id, account_id, advert_id, schema_version, \
                        policy_revision, policy_digest, quota_max_actions_per_hour, \
                        quota_max_actions_per_day, quota_cooldown_seconds, \
                        quota_max_cumulative_abs_delta_kopecks_per_day, reserved_at, expires_at \
                 FROM control.wb_prepare_reservations WHERE reservation_id=$1",
            &[&outstanding_prepares[0].reservation_id],
        )
        .await
        .unwrap();
    admin
        .execute(
            "ALTER TABLE control.wb_prepare_reservations \
                 ENABLE TRIGGER wb_prepare_reservations_validate",
            &[],
        )
        .await
        .unwrap();
    let (outstanding_requested, outstanding_changes, outstanding_before) = fixture(49);
    assert!(matches!(
        repository
            .create(
                "outstanding_prepare_actor",
                "outstanding_prepare_account",
                49,
                1,
                7,
                POLICY_DIGEST,
                quota(),
                &outstanding_prepares[0].reservation_id,
                &outstanding_requested,
                &outstanding_changes,
                &outstanding_before,
                now,
            )
            .await,
        Err(PlanStoreError::PrepareLimitExceeded)
    ));
    assert!(matches!(
        repository
            .claim_for_apply(apply_context(&plan, "integration_actor", now))
            .await,
        Err(PlanStoreError::ApprovalRequired)
    ));
    let mut wrong_claim_digest = apply_context(&plan, "integration_actor", now);
    wrong_claim_digest.expected_plan_digest = NEXT_POLICY_DIGEST;
    assert!(matches!(
        repository.claim_for_apply(wrong_claim_digest).await,
        Err(PlanStoreError::PlanChanged)
    ));
    assert!(matches!(
        repository
            .approve(
                &plan.plan_id,
                "integration_approver",
                "2222222222222222222222222222222222222222222222222222222222222222",
                "integration/approval",
                now,
            )
            .await,
        Err(PlanStoreError::PlanChanged)
    ));
    assert!(matches!(
        repository
            .approve(
                &plan.plan_id,
                "integration_actor",
                &plan.plan_digest,
                "self-approval",
                now,
            )
            .await,
        Err(PlanStoreError::InvalidState)
    ));
    let approved = repository
        .approve(
            &plan.plan_id,
            "integration_approver",
            &plan.plan_digest,
            "integration/approval",
            now + Duration::days(365),
        )
        .await
        .unwrap();
    assert_eq!(approved.status, WbPlanStatus::Approved);
    assert!(approved.approval.is_some());
    repository
        .approve(
            &plan.plan_id,
            "integration_approver",
            &plan.plan_digest,
            "integration/approval",
            now,
        )
        .await
        .unwrap();
    assert!(matches!(
        repository
            .approve(
                &plan.plan_id,
                "integration_approver",
                &plan.plan_digest,
                "integration/different",
                now,
            )
            .await,
        Err(PlanStoreError::InvalidState)
    ));
    let mut wrong_claim_policy = apply_context(&plan, "integration_actor", now);
    wrong_claim_policy.expected_policy_revision = 6;
    assert!(matches!(
        repository.claim_for_apply(wrong_claim_policy).await,
        Err(PlanStoreError::PolicyChanged)
    ));
    assert!(matches!(
        repository
            .claim_for_apply(apply_context(&plan, "integration_actor", now))
            .await,
        Err(PlanStoreError::RuntimeDisabled)
    ));
    enable_gates(&admin, "integration_account", 42, now).await;
    assert!(
        admin
            .execute(
                "UPDATE control.wb_runtime_gates \
                     SET revision=revision+1, updated_at=$1, lease_expires_at=$2 \
                     WHERE gate_key='global'",
                &[&(now + Duration::days(365)), &(now + Duration::days(365))],
            )
            .await
            .is_err()
    );
    let claimed = repository
        .claim_for_apply(apply_context(&plan, "integration_actor", now))
        .await
        .unwrap();
    assert_eq!(claimed.status, WbPlanStatus::Applying);
    assert!(matches!(
        repository
            .claim_for_apply(apply_context(&plan, "integration_actor", now))
            .await,
        Err(PlanStoreError::InvalidState)
    ));
    let mut wrong_revalidate_digest = apply_context(&plan, "integration_actor", now);
    wrong_revalidate_digest.expected_plan_digest = NEXT_POLICY_DIGEST;
    assert!(matches!(
        repository
            .revalidate_before_write(wrong_revalidate_digest)
            .await,
        Err(PlanStoreError::PlanChanged)
    ));
    let mut wrong_revalidate_policy = apply_context(&plan, "integration_actor", now);
    wrong_revalidate_policy.expected_policy_revision = 6;
    assert!(matches!(
        repository
            .revalidate_before_write(wrong_revalidate_policy)
            .await,
        Err(PlanStoreError::PolicyChanged)
    ));
    repository
        .revalidate_before_write(apply_context(&plan, "integration_actor", now))
        .await
        .unwrap();
    let (_, _, before) = fixture(42);
    let wrong_readback_json = serde_json::to_string(&before).unwrap();
    assert!(
        direct_writer
            .execute(
                "UPDATE control.wb_plans SET status='applied', \
                         finished_at=clock_timestamp(), write_response_json='{}', \
                         readback_json=$2 WHERE plan_id=$1 AND status='applying'",
                &[&plan.plan_id, &wrong_readback_json],
            )
            .await
            .is_err()
    );
    let mut after = before.clone();
    after.bids[0].bid_kopecks = 1050;
    let mut wrong_seller_readback = serde_json::to_value(&after).unwrap();
    wrong_seller_readback["seller_sid"] =
        Value::String("22222222-2222-4222-8222-222222222222".to_owned());
    let wrong_seller_readback = serde_json::to_string(&wrong_seller_readback).unwrap();
    assert!(
        direct_writer
            .execute(
                "UPDATE control.wb_plans SET status='applied', \
                         finished_at=clock_timestamp(), write_response_json='{}', \
                         readback_json=$2 WHERE plan_id=$1 AND status='applying'",
                &[&plan.plan_id, &wrong_seller_readback],
            )
            .await
            .is_err()
    );
    let exact_readback_json = serde_json::to_string(&after).unwrap();
    assert!(
        direct_writer
            .execute(
                "UPDATE control.wb_plans SET status='applied', \
                         finished_at=clock_timestamp(), write_response_json=NULL, \
                         readback_json=$2 WHERE plan_id=$1 AND status='applying'",
                &[&plan.plan_id, &exact_readback_json],
            )
            .await
            .is_err()
    );
    assert!(matches!(
        repository
            .finish(
                &plan.plan_id,
                "integration_actor",
                WbPlanFinish {
                    status: WbPlanStatus::Prepared,
                    error_class: None,
                    write_response: None,
                    readback: None,
                    now,
                },
            )
            .await,
        Err(PlanStoreError::InvalidState)
    ));
    assert!(matches!(
        repository
            .finish(
                &plan.plan_id,
                "integration_actor",
                WbPlanFinish {
                    status: WbPlanStatus::Applied,
                    error_class: None,
                    write_response: None,
                    readback: Some(&after),
                    now,
                },
            )
            .await,
        Err(PlanStoreError::InvalidPlan)
    ));
    repository
        .finish(
            &plan.plan_id,
            "integration_actor",
            WbPlanFinish {
                status: WbPlanStatus::Applied,
                error_class: None,
                write_response: Some(&serde_json::json!({"ok": true})),
                readback: Some(&after),
                now,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        repository
            .load_for_actor(&plan.plan_id, "integration_actor")
            .await
            .unwrap()
            .status,
        WbPlanStatus::Applied
    );
    repository
        .confirm_reconciled(&plan.plan_id, "integration_actor", &after, now)
        .await
        .unwrap();
    let rejected_reservation = admin.transaction().await.unwrap();
    assert_eq!(
        reserve_action_quota(&rejected_reservation, &plan, Utc::now() + Duration::days(2),).await,
        Err(PlanStoreError::Unavailable)
    );
    rejected_reservation.rollback().await.unwrap();
    assert!(matches!(
        repository
            .approve(
                &plan.plan_id,
                "integration_approver",
                &plan.plan_digest,
                "integration/approval",
                now,
            )
            .await,
        Err(PlanStoreError::InvalidState)
    ));
    assert!(matches!(
        repository
            .revalidate_before_write(apply_context(&plan, "integration_actor", now))
            .await,
        Err(PlanStoreError::InvalidState)
    ));
    assert!(matches!(
        repository
            .finish(
                &plan.plan_id,
                "integration_actor",
                WbPlanFinish {
                    status: WbPlanStatus::Applied,
                    error_class: None,
                    write_response: Some(&serde_json::json!({"ok": true})),
                    readback: Some(&after),
                    now,
                },
            )
            .await,
        Err(PlanStoreError::InvalidState)
    ));

    let expired_approval_plan = create_fixture_plan(
        &repository,
        "approval_expiry_actor",
        "integration_account",
        45,
        quota(),
        now,
    )
    .await;
    repository
        .approve(
            &expired_approval_plan.plan_id,
            "integration_approver",
            &expired_approval_plan.plan_digest,
            "approval/expiry",
            now,
        )
        .await
        .unwrap();
    admin
        .execute(
            "ALTER TABLE control.wb_plan_approvals \
                 DISABLE TRIGGER wb_plan_approvals_append_only",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.wb_plan_approvals approval \
                 SET approved_at=skew.approved_at, \
                     expires_at=skew.approved_at + interval '1 minute' \
                 FROM (SELECT clock_timestamp() - interval '3 minutes' AS approved_at) skew \
                 WHERE approval.plan_id=$1",
            &[&expired_approval_plan.plan_id],
        )
        .await
        .unwrap();
    admin
        .execute(
            "ALTER TABLE control.wb_plan_approvals \
                 ENABLE TRIGGER wb_plan_approvals_append_only",
            &[],
        )
        .await
        .unwrap();
    assert!(matches!(
        repository
            .approve(
                &expired_approval_plan.plan_id,
                "integration_approver",
                &expired_approval_plan.plan_digest,
                "approval/expiry",
                now + Duration::days(365),
            )
            .await,
        Err(PlanStoreError::ApprovalExpired)
    ));
    assert_eq!(
        repository
            .load_for_actor(&expired_approval_plan.plan_id, "approval_expiry_actor")
            .await
            .unwrap()
            .status,
        WbPlanStatus::Expired
    );
    repository.verify_runtime_contract().await.unwrap();

    let expired_plan = create_fixture_plan(
        &repository,
        "expired_plan_actor",
        "coverage_account",
        50,
        quota(),
        now,
    )
    .await;
    admin
        .execute(
            "ALTER TABLE control.wb_plans DISABLE TRIGGER wb_plans_transition_guard",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.wb_plans plan SET \
                     created_at=skew.created_at, \
                     expires_at=skew.created_at+interval '5 minutes' \
                 FROM (SELECT clock_timestamp()-interval '6 minutes' AS created_at) skew \
                 WHERE plan.plan_id=$1",
            &[&expired_plan.plan_id],
        )
        .await
        .unwrap();
    admin
        .execute(
            "ALTER TABLE control.wb_plans ENABLE TRIGGER wb_plans_transition_guard",
            &[],
        )
        .await
        .unwrap();
    assert!(matches!(
        repository
            .approve(
                &expired_plan.plan_id,
                "integration_approver",
                &expired_plan.plan_digest,
                "coverage/expired-plan",
                now,
            )
            .await,
        Err(PlanStoreError::Expired)
    ));
    assert!(matches!(
        repository
            .mark_stale_applying_ambiguous(&expired_plan.plan_id, "expired_plan_actor", now,)
            .await,
        Err(PlanStoreError::InvalidState)
    ));

    let claim_approval_expired = create_approved_fixture_plan(
        &repository,
        "claim_approval_expired_actor",
        "coverage_account",
        51,
        quota(),
        now,
    )
    .await;
    admin
        .execute(
            "ALTER TABLE control.wb_plan_approvals \
                 DISABLE TRIGGER wb_plan_approvals_append_only",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.wb_plan_approvals approval SET \
                     approved_at=skew.approved_at, \
                     expires_at=skew.approved_at+interval '1 minute' \
                 FROM (SELECT clock_timestamp()-interval '3 minutes' AS approved_at) skew \
                 WHERE approval.plan_id=$1",
            &[&claim_approval_expired.plan_id],
        )
        .await
        .unwrap();
    admin
        .execute(
            "ALTER TABLE control.wb_plan_approvals \
                 ENABLE TRIGGER wb_plan_approvals_append_only",
            &[],
        )
        .await
        .unwrap();
    assert!(matches!(
        repository
            .claim_for_apply(apply_context(
                &claim_approval_expired,
                "claim_approval_expired_actor",
                now,
            ))
            .await,
        Err(PlanStoreError::ApprovalExpired)
    ));

    let claim_plan_expired = create_approved_fixture_plan(
        &repository,
        "claim_plan_expired_actor",
        "coverage_account",
        52,
        quota(),
        now,
    )
    .await;
    admin
        .execute(
            "ALTER TABLE control.wb_plans DISABLE TRIGGER wb_plans_transition_guard",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.wb_plans plan SET \
                     created_at=skew.created_at, \
                     expires_at=skew.created_at+interval '5 minutes' \
                 FROM (SELECT clock_timestamp()-interval '6 minutes' AS created_at) skew \
                 WHERE plan.plan_id=$1",
            &[&claim_plan_expired.plan_id],
        )
        .await
        .unwrap();
    admin
        .execute(
            "ALTER TABLE control.wb_plans ENABLE TRIGGER wb_plans_transition_guard",
            &[],
        )
        .await
        .unwrap();
    assert!(matches!(
        repository
            .claim_for_apply(apply_context(
                &claim_plan_expired,
                "claim_plan_expired_actor",
                now,
            ))
            .await,
        Err(PlanStoreError::Expired)
    ));

    let revalidate_plan_expired = create_applying_fixture_plan(
        &repository,
        &admin,
        "revalidate_plan_expired_actor",
        "coverage_account",
        53,
        quota(),
        now,
    )
    .await;
    admin
        .execute(
            "ALTER TABLE control.wb_plans DISABLE TRIGGER wb_plans_transition_guard",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.wb_plans plan SET \
                     created_at=skew.created_at, \
                     expires_at=skew.created_at+interval '5 minutes' \
                 FROM (SELECT clock_timestamp()-interval '6 minutes' AS created_at) skew \
                 WHERE plan.plan_id=$1",
            &[&revalidate_plan_expired.plan_id],
        )
        .await
        .unwrap();
    admin
        .execute(
            "ALTER TABLE control.wb_plans ENABLE TRIGGER wb_plans_transition_guard",
            &[],
        )
        .await
        .unwrap();
    assert!(matches!(
        repository
            .revalidate_before_write(apply_context(
                &revalidate_plan_expired,
                "revalidate_plan_expired_actor",
                now,
            ))
            .await,
        Err(PlanStoreError::Expired)
    ));
    repository
        .finish(
            &revalidate_plan_expired.plan_id,
            "revalidate_plan_expired_actor",
            WbPlanFinish {
                status: WbPlanStatus::Failed,
                error_class: Some("coverage_expired"),
                write_response: None,
                readback: None,
                now,
            },
        )
        .await
        .unwrap();

    let revalidate_approval_expired = create_applying_fixture_plan(
        &repository,
        &admin,
        "revalidate_approval_expired_actor",
        "coverage_account",
        54,
        quota(),
        now,
    )
    .await;
    admin
        .execute(
            "ALTER TABLE control.wb_plan_approvals \
                 DISABLE TRIGGER wb_plan_approvals_append_only",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.wb_plan_approvals approval SET \
                     approved_at=skew.approved_at, \
                     expires_at=skew.approved_at+interval '1 minute' \
                 FROM (SELECT clock_timestamp()-interval '3 minutes' AS approved_at) skew \
                 WHERE approval.plan_id=$1",
            &[&revalidate_approval_expired.plan_id],
        )
        .await
        .unwrap();
    admin
        .execute(
            "ALTER TABLE control.wb_plan_approvals \
                 ENABLE TRIGGER wb_plan_approvals_append_only",
            &[],
        )
        .await
        .unwrap();
    assert!(matches!(
        repository
            .revalidate_before_write(apply_context(
                &revalidate_approval_expired,
                "revalidate_approval_expired_actor",
                now,
            ))
            .await,
        Err(PlanStoreError::ApprovalExpired)
    ));
    repository
        .finish(
            &revalidate_approval_expired.plan_id,
            "revalidate_approval_expired_actor",
            WbPlanFinish {
                status: WbPlanStatus::Failed,
                error_class: Some("coverage_approval_expired"),
                write_response: None,
                readback: None,
                now,
            },
        )
        .await
        .unwrap();

    let missing_reservation_plan = create_applying_fixture_plan(
        &repository,
        &admin,
        "missing_reservation_actor",
        "coverage_account",
        55,
        quota(),
        now,
    )
    .await;
    admin
        .execute(
            "ALTER TABLE control.wb_action_reservations \
                 DISABLE TRIGGER wb_action_reservations_append_only",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute(
            "DELETE FROM control.wb_action_reservations WHERE plan_id=$1",
            &[&missing_reservation_plan.plan_id],
        )
        .await
        .unwrap();
    admin
        .execute(
            "ALTER TABLE control.wb_action_reservations \
                 ENABLE TRIGGER wb_action_reservations_append_only",
            &[],
        )
        .await
        .unwrap();
    assert!(matches!(
        repository
            .revalidate_before_write(apply_context(
                &missing_reservation_plan,
                "missing_reservation_actor",
                now,
            ))
            .await,
        Err(PlanStoreError::InvalidState)
    ));
    repository
        .finish(
            &missing_reservation_plan.plan_id,
            "missing_reservation_actor",
            WbPlanFinish {
                status: WbPlanStatus::Failed,
                error_class: Some("coverage_missing_reservation"),
                write_response: None,
                readback: None,
                now,
            },
        )
        .await
        .unwrap();

    let stale_plan = create_applying_fixture_plan(
        &repository,
        &admin,
        "stale_apply_actor",
        "coverage_account",
        56,
        quota(),
        now,
    )
    .await;
    admin
        .execute(
            "ALTER TABLE control.wb_plans DISABLE TRIGGER wb_plans_transition_guard",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.wb_plans SET \
                     apply_started_at=clock_timestamp()-interval '4 minutes' \
                 WHERE plan_id=$1",
            &[&stale_plan.plan_id],
        )
        .await
        .unwrap();
    admin
        .execute(
            "ALTER TABLE control.wb_plans ENABLE TRIGGER wb_plans_transition_guard",
            &[],
        )
        .await
        .unwrap();
    repository
        .mark_stale_applying_ambiguous(&stale_plan.plan_id, "stale_apply_actor", now)
        .await
        .unwrap();
    repository
        .mark_stale_applying_ambiguous(&stale_plan.plan_id, "stale_apply_actor", now)
        .await
        .unwrap();

    let invalid_reconcile_plan = create_fixture_plan(
        &repository,
        "invalid_reconcile_actor",
        "coverage_account",
        57,
        quota(),
        now,
    )
    .await;
    let (_, _, mut invalid_reconcile_after) = fixture(57);
    invalid_reconcile_after.bids[0].bid_kopecks = 1050;
    assert!(matches!(
        repository
            .confirm_reconciled(
                &invalid_reconcile_plan.plan_id,
                "invalid_reconcile_actor",
                &invalid_reconcile_after,
                now,
            )
            .await,
        Err(PlanStoreError::InvalidState)
    ));

    let incident_now = now + Duration::seconds(1);
    enable_gates(&admin, "integration_account", 43, incident_now).await;
    let incident_plan = create_fixture_plan(
        &repository,
        "integration_actor",
        "integration_account",
        43,
        quota(),
        incident_now,
    )
    .await;
    repository
        .approve(
            &incident_plan.plan_id,
            "integration_approver",
            &incident_plan.plan_digest,
            "incident/test",
            incident_now,
        )
        .await
        .unwrap();
    repository
        .claim_for_apply(apply_context(
            &incident_plan,
            "integration_actor",
            incident_now,
        ))
        .await
        .unwrap();
    assert!(matches!(
        repository
            .mark_stale_applying_ambiguous(
                &incident_plan.plan_id,
                "integration_actor",
                incident_now + STALE_APPLY_AFTER,
            )
            .await,
        Err(PlanStoreError::ApplyInProgress)
    ));
    repository
        .finish(
            &incident_plan.plan_id,
            "integration_actor",
            WbPlanFinish {
                status: WbPlanStatus::Ambiguous,
                error_class: Some("integration_ambiguous"),
                write_response: None,
                readback: None,
                now: incident_now,
            },
        )
        .await
        .unwrap();
    let incomplete_readback = serde_json::json!({
        "bids": [{
            "nm_id": 1001,
            "placement": "search",
            "bid_kopecks": 1050
        }]
    })
    .to_string();
    assert!(
        direct_writer
            .execute(
                "UPDATE control.wb_plans \
                     SET status='applied', finished_at=clock_timestamp(), \
                         last_error_class=NULL, readback_json=$2 \
                     WHERE plan_id=$1 AND status='ambiguous'",
                &[&incident_plan.plan_id, &incomplete_readback],
            )
            .await
            .is_err()
    );
    let (requested, changes, mut incident_before) = fixture(43);
    incident_before.status = 9;
    assert!(matches!(
        repository
            .reserve_prepare_attempt(
                "integration_actor",
                "integration_account",
                43,
                1,
                7,
                POLICY_DIGEST,
                quota(),
                incident_now + STALE_APPLY_AFTER,
            )
            .await,
        Err(PlanStoreError::CampaignLocked)
    ));
    assert!(matches!(
        repository
            .confirm_reconciled(
                &incident_plan.plan_id,
                "integration_actor",
                &incident_before,
                incident_now + STALE_APPLY_AFTER,
            )
            .await,
        Err(PlanStoreError::InvalidPlan)
    ));
    assert_eq!(
        repository
            .load_for_actor(&incident_plan.plan_id, "integration_actor")
            .await
            .unwrap()
            .status,
        WbPlanStatus::Ambiguous
    );
    let mut incident_after = incident_before.clone();
    incident_after.bids[0].bid_kopecks = 1050;
    let mut wrong_incident_seller = serde_json::to_value(&incident_after).unwrap();
    wrong_incident_seller["seller_sid"] =
        Value::String("22222222-2222-4222-8222-222222222222".to_owned());
    let wrong_incident_seller = serde_json::to_string(&wrong_incident_seller).unwrap();
    assert!(
        direct_writer
            .execute(
                "UPDATE control.wb_plans \
                     SET status='applied', finished_at=clock_timestamp(), \
                         last_error_class=NULL, readback_json=$2 \
                     WHERE plan_id=$1 AND status='ambiguous'",
                &[&incident_plan.plan_id, &wrong_incident_seller],
            )
            .await
            .is_err()
    );
    repository
        .confirm_reconciled(
            &incident_plan.plan_id,
            "integration_actor",
            &incident_after,
            incident_now + STALE_APPLY_AFTER,
        )
        .await
        .unwrap();
    let reconciled_prepare = repository
        .reserve_prepare_attempt(
            "integration_actor",
            "integration_account",
            43,
            1,
            7,
            POLICY_DIGEST,
            quota(),
            incident_now + STALE_APPLY_AFTER,
        )
        .await
        .unwrap();
    assert!(
        repository
            .create(
                "integration_actor",
                "integration_account",
                43,
                1,
                7,
                POLICY_DIGEST,
                quota(),
                &reconciled_prepare.reservation_id,
                &requested,
                &changes,
                &incident_before,
                incident_now + STALE_APPLY_AFTER,
            )
            .await
            .is_ok()
    );

    let approval_lock_pending = repository
        .reserve_prepare_attempt(
            "approval_lock_actor",
            "approval_lock_account",
            58,
            1,
            7,
            POLICY_DIGEST,
            quota(),
            now,
        )
        .await
        .unwrap();
    let approval_lock_plan = create_fixture_plan(
        &repository,
        "approval_lock_actor",
        "approval_lock_account",
        58,
        quota(),
        now,
    )
    .await;
    let approval_incident = create_applying_fixture_plan(
        &repository,
        &admin,
        "approval_incident_actor",
        "approval_lock_account",
        58,
        quota(),
        now,
    )
    .await;
    repository
        .finish(
            &approval_incident.plan_id,
            "approval_incident_actor",
            WbPlanFinish {
                status: WbPlanStatus::Ambiguous,
                error_class: Some("coverage_approval_incident"),
                write_response: None,
                readback: None,
                now,
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        repository
            .approve(
                &approval_lock_plan.plan_id,
                "integration_approver",
                &approval_lock_plan.plan_digest,
                "coverage/incident-lock",
                now,
            )
            .await,
        Err(PlanStoreError::CampaignLocked)
    ));
    let (lock_requested, lock_changes, lock_before) = fixture(58);
    assert!(matches!(
        repository
            .create(
                "approval_lock_actor",
                "approval_lock_account",
                58,
                1,
                7,
                POLICY_DIGEST,
                quota(),
                &approval_lock_pending.reservation_id,
                &lock_requested,
                &lock_changes,
                &lock_before,
                now,
            )
            .await,
        Err(PlanStoreError::CampaignLocked)
    ));

    let claim_incident = create_approved_fixture_plan(
        &repository,
        "claim_incident_actor",
        "claim_lock_account",
        59,
        quota(),
        now,
    )
    .await;
    let claim_lock_plan = create_approved_fixture_plan(
        &repository,
        "claim_lock_actor",
        "claim_lock_account",
        59,
        quota(),
        now,
    )
    .await;
    enable_gates(&admin, "claim_lock_account", 59, now).await;
    repository
        .claim_for_apply(apply_context(&claim_incident, "claim_incident_actor", now))
        .await
        .unwrap();
    repository
        .finish(
            &claim_incident.plan_id,
            "claim_incident_actor",
            WbPlanFinish {
                status: WbPlanStatus::Ambiguous,
                error_class: Some("coverage_claim_incident"),
                write_response: None,
                readback: None,
                now,
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        repository
            .claim_for_apply(apply_context(&claim_lock_plan, "claim_lock_actor", now))
            .await,
        Err(PlanStoreError::CampaignLocked)
    ));

    let revalidate_incident_plan = create_applying_fixture_plan(
        &repository,
        &admin,
        "revalidate_incident_actor",
        "revalidate_lock_account",
        60,
        quota(),
        now,
    )
    .await;
    let injected_incident_plan = create_fixture_plan(
        &repository,
        "injected_incident_actor",
        "revalidate_lock_account",
        60,
        quota(),
        now,
    )
    .await;
    admin
        .execute(
            "ALTER TABLE control.wb_plans DISABLE TRIGGER wb_plans_transition_guard",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.wb_plans SET \
                     status='ambiguous', apply_started_at=clock_timestamp(), \
                     finished_at=clock_timestamp(), last_error_class='coverage_injected' \
                 WHERE plan_id=$1",
            &[&injected_incident_plan.plan_id],
        )
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.wb_plans SET \
                     apply_started_at=clock_timestamp()-interval '4 minutes' \
                 WHERE plan_id=$1",
            &[&revalidate_incident_plan.plan_id],
        )
        .await
        .unwrap();
    admin
        .execute(
            "ALTER TABLE control.wb_plans ENABLE TRIGGER wb_plans_transition_guard",
            &[],
        )
        .await
        .unwrap();
    assert!(matches!(
        repository
            .revalidate_before_write(apply_context(
                &revalidate_incident_plan,
                "revalidate_incident_actor",
                now,
            ))
            .await,
        Err(PlanStoreError::CampaignLocked)
    ));
    assert!(matches!(
        repository
            .mark_stale_applying_ambiguous(
                &revalidate_incident_plan.plan_id,
                "revalidate_incident_actor",
                now,
            )
            .await,
        Err(PlanStoreError::CampaignLocked)
    ));
    assert!(matches!(
        repository
            .finish(
                &revalidate_incident_plan.plan_id,
                "revalidate_incident_actor",
                WbPlanFinish {
                    status: WbPlanStatus::Ambiguous,
                    error_class: Some("coverage_duplicate_incident"),
                    write_response: None,
                    readback: None,
                    now,
                },
            )
            .await,
        Err(PlanStoreError::CampaignLocked)
    ));
    repository
        .finish(
            &revalidate_incident_plan.plan_id,
            "revalidate_incident_actor",
            WbPlanFinish {
                status: WbPlanStatus::Failed,
                error_class: Some("coverage_incident_cleanup"),
                write_response: None,
                readback: None,
                now,
            },
        )
        .await
        .unwrap();

    let busy_first = create_approved_fixture_plan(
        &repository,
        "busy_first_actor",
        "busy_account",
        61,
        quota(),
        now,
    )
    .await;
    let busy_second = create_approved_fixture_plan(
        &repository,
        "busy_second_actor",
        "busy_account",
        61,
        quota(),
        now,
    )
    .await;
    enable_gates(&admin, "busy_account", 61, now).await;
    repository
        .claim_for_apply(apply_context(&busy_first, "busy_first_actor", now))
        .await
        .unwrap();
    admin
        .execute(
            "ALTER TABLE control.wb_action_reservations \
                 DISABLE TRIGGER wb_action_reservations_append_only",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute(
            "DELETE FROM control.wb_action_reservations WHERE plan_id=$1",
            &[&busy_first.plan_id],
        )
        .await
        .unwrap();
    admin
        .execute(
            "ALTER TABLE control.wb_action_reservations \
                 ENABLE TRIGGER wb_action_reservations_append_only",
            &[],
        )
        .await
        .unwrap();
    assert!(matches!(
        repository
            .claim_for_apply(apply_context(&busy_second, "busy_second_actor", now))
            .await,
        Err(PlanStoreError::Busy)
    ));
    admin
        .execute(
            "ALTER TABLE control.wb_action_reservations \
                 DISABLE TRIGGER wb_action_reservations_validate",
            &[],
        )
        .await
        .unwrap();
    let duplicate_reservation = admin.transaction().await.unwrap();
    reserve_action_quota(&duplicate_reservation, &busy_second, Utc::now())
        .await
        .unwrap();
    assert_eq!(
        reserve_action_quota(
            &duplicate_reservation,
            &busy_second,
            Utc::now() + Duration::seconds(31),
        )
        .await,
        Err(PlanStoreError::InvalidState)
    );
    duplicate_reservation.rollback().await.unwrap();
    admin
        .execute(
            "ALTER TABLE control.wb_action_reservations \
                 ENABLE TRIGGER wb_action_reservations_validate",
            &[],
        )
        .await
        .unwrap();
    repository
        .finish(
            &busy_first.plan_id,
            "busy_first_actor",
            WbPlanFinish {
                status: WbPlanStatus::Failed,
                error_class: Some("coverage_busy_cleanup"),
                write_response: None,
                readback: None,
                now,
            },
        )
        .await
        .unwrap();

    let suppressed_approval = create_fixture_plan(
        &repository,
        "suppress_approval_actor",
        "fault_account",
        62,
        quota(),
        now,
    )
    .await;
    let raised_approval = create_fixture_plan(
        &repository,
        "raise_approval_actor",
        "fault_account",
        69,
        quota(),
        now,
    )
    .await;
    let raised_claim = create_approved_fixture_plan(
        &repository,
        "raise_claim_actor",
        "fault_account",
        63,
        quota(),
        now,
    )
    .await;
    enable_gates(&admin, "fault_account", 63, now).await;
    let raised_stale = create_applying_fixture_plan(
        &repository,
        &admin,
        "raise_stale_actor",
        "fault_account",
        64,
        quota(),
        now,
    )
    .await;
    let suppressed_stale = create_applying_fixture_plan(
        &repository,
        &admin,
        "suppress_stale_actor",
        "fault_account",
        65,
        quota(),
        now,
    )
    .await;
    let raised_finish = create_applying_fixture_plan(
        &repository,
        &admin,
        "raise_finish_actor",
        "fault_account",
        66,
        quota(),
        now,
    )
    .await;
    let suppressed_finish = create_applying_fixture_plan(
        &repository,
        &admin,
        "suppress_finish_actor",
        "fault_account",
        67,
        quota(),
        now,
    )
    .await;
    let suppressed_reconcile = create_applying_fixture_plan(
        &repository,
        &admin,
        "suppress_reconcile_actor",
        "fault_account",
        68,
        quota(),
        now,
    )
    .await;
    let raised_reconcile = create_applying_fixture_plan(
        &repository,
        &admin,
        "raise_reconcile_actor",
        "fault_account",
        70,
        quota(),
        now,
    )
    .await;
    repository
        .finish(
            &suppressed_reconcile.plan_id,
            "suppress_reconcile_actor",
            WbPlanFinish {
                status: WbPlanStatus::Ambiguous,
                error_class: Some("coverage_reconcile_fault"),
                write_response: None,
                readback: None,
                now,
            },
        )
        .await
        .unwrap();
    repository
        .finish(
            &raised_reconcile.plan_id,
            "raise_reconcile_actor",
            WbPlanFinish {
                status: WbPlanStatus::Ambiguous,
                error_class: Some("coverage_reconcile_fault"),
                write_response: None,
                readback: None,
                now,
            },
        )
        .await
        .unwrap();
    let stale_plan_ids = vec![
        raised_stale.plan_id.clone(),
        suppressed_stale.plan_id.clone(),
    ];
    admin
        .execute(
            "ALTER TABLE control.wb_plans DISABLE TRIGGER wb_plans_transition_guard",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.wb_plans SET \
                     apply_started_at=clock_timestamp()-interval '4 minutes' \
                 WHERE plan_id=ANY($1)",
            &[&stale_plan_ids],
        )
        .await
        .unwrap();
    admin
        .execute(
            "ALTER TABLE control.wb_plans ENABLE TRIGGER wb_plans_transition_guard",
            &[],
        )
        .await
        .unwrap();
    admin
        .batch_execute(
            "CREATE FUNCTION control.coverage_plan_update_fault() \
                 RETURNS trigger LANGUAGE plpgsql AS $$ \
                 BEGIN \
                     IF OLD.actor_id LIKE 'raise_%' THEN \
                         RAISE EXCEPTION 'coverage injected plan update failure'; \
                     ELSIF OLD.actor_id LIKE 'suppress_%' THEN \
                         RETURN NULL; \
                     END IF; \
                     RETURN NEW; \
                 END $$; \
                 CREATE TRIGGER zz_coverage_plan_update_fault \
                 BEFORE UPDATE ON control.wb_plans FOR EACH ROW \
                 EXECUTE FUNCTION control.coverage_plan_update_fault();",
        )
        .await
        .unwrap();
    assert!(matches!(
        repository
            .approve(
                &suppressed_approval.plan_id,
                "integration_approver",
                &suppressed_approval.plan_digest,
                "coverage/suppressed-approval",
                now,
            )
            .await,
        Err(PlanStoreError::InvalidState)
    ));
    assert!(matches!(
        repository
            .approve(
                &raised_approval.plan_id,
                "integration_approver",
                &raised_approval.plan_digest,
                "coverage/raised-approval",
                now,
            )
            .await,
        Err(PlanStoreError::Unavailable)
    ));
    assert!(matches!(
        repository
            .claim_for_apply(apply_context(&raised_claim, "raise_claim_actor", now))
            .await,
        Err(PlanStoreError::Unavailable)
    ));
    assert!(matches!(
        repository
            .mark_stale_applying_ambiguous(&raised_stale.plan_id, "raise_stale_actor", now,)
            .await,
        Err(PlanStoreError::Unavailable)
    ));
    assert!(matches!(
        repository
            .mark_stale_applying_ambiguous(&suppressed_stale.plan_id, "suppress_stale_actor", now,)
            .await,
        Err(PlanStoreError::InvalidState)
    ));
    assert!(matches!(
        repository
            .finish(
                &raised_finish.plan_id,
                "raise_finish_actor",
                WbPlanFinish {
                    status: WbPlanStatus::Failed,
                    error_class: Some("coverage_raised_finish"),
                    write_response: None,
                    readback: None,
                    now,
                },
            )
            .await,
        Err(PlanStoreError::Unavailable)
    ));
    assert!(matches!(
        repository
            .finish(
                &suppressed_finish.plan_id,
                "suppress_finish_actor",
                WbPlanFinish {
                    status: WbPlanStatus::Failed,
                    error_class: Some("coverage_suppressed_finish"),
                    write_response: None,
                    readback: None,
                    now,
                },
            )
            .await,
        Err(PlanStoreError::InvalidState)
    ));
    let (_, _, mut suppress_reconcile_after) = fixture(68);
    suppress_reconcile_after.bids[0].bid_kopecks = 1050;
    assert!(matches!(
        repository
            .confirm_reconciled(
                &suppressed_reconcile.plan_id,
                "suppress_reconcile_actor",
                &suppress_reconcile_after,
                now,
            )
            .await,
        Err(PlanStoreError::InvalidState)
    ));
    let (_, _, mut raise_reconcile_after) = fixture(70);
    raise_reconcile_after.bids[0].bid_kopecks = 1050;
    assert!(matches!(
        repository
            .confirm_reconciled(
                &raised_reconcile.plan_id,
                "raise_reconcile_actor",
                &raise_reconcile_after,
                now,
            )
            .await,
        Err(PlanStoreError::Unavailable)
    ));
    admin
        .batch_execute(
            "DROP TRIGGER zz_coverage_plan_update_fault ON control.wb_plans; \
                 DROP FUNCTION control.coverage_plan_update_fault();",
        )
        .await
        .unwrap();
    for (fault_plan, actor_id) in [
        (&raised_stale, "raise_stale_actor"),
        (&suppressed_stale, "suppress_stale_actor"),
        (&raised_finish, "raise_finish_actor"),
        (&suppressed_finish, "suppress_finish_actor"),
    ] {
        repository
            .finish(
                &fault_plan.plan_id,
                actor_id,
                WbPlanFinish {
                    status: WbPlanStatus::Failed,
                    error_class: Some("coverage_fault_cleanup"),
                    write_response: None,
                    readback: None,
                    now,
                },
            )
            .await
            .unwrap();
    }

    let quota_now = now + Duration::seconds(2);
    enable_gates(&admin, "integration_account", 44, quota_now).await;
    let cooldown_quota = WbActionQuota {
        cooldown_seconds: 60,
        ..quota()
    };
    let quota_plan = create_fixture_plan(
        &repository,
        "integration_actor",
        "integration_account",
        44,
        cooldown_quota,
        quota_now,
    )
    .await;
    repository
        .approve(
            &quota_plan.plan_id,
            "integration_approver",
            &quota_plan.plan_digest,
            "quota/first",
            quota_now,
        )
        .await
        .unwrap();
    repository
        .claim_for_apply(apply_context(&quota_plan, "integration_actor", quota_now))
        .await
        .unwrap();
    let (_, _, mut quota_after) = fixture(44);
    quota_after.bids[0].bid_kopecks = 1050;
    repository
        .finish(
            &quota_plan.plan_id,
            "integration_actor",
            WbPlanFinish {
                status: WbPlanStatus::Applied,
                error_class: None,
                write_response: Some(&serde_json::json!({"ok": true})),
                readback: Some(&quota_after),
                now: quota_now,
            },
        )
        .await
        .unwrap();
    let second_quota_plan = create_fixture_plan(
        &repository,
        "integration_actor",
        "integration_account",
        44,
        cooldown_quota,
        quota_now + Duration::seconds(1),
    )
    .await;
    repository
        .approve(
            &second_quota_plan.plan_id,
            "integration_approver",
            &second_quota_plan.plan_digest,
            "quota/second",
            quota_now + Duration::seconds(1),
        )
        .await
        .unwrap();
    assert!(matches!(
        repository
            .claim_for_apply(apply_context(
                &second_quota_plan,
                "integration_actor",
                quota_now + Duration::seconds(1),
            ))
            .await,
        Err(PlanStoreError::QuotaExceeded)
    ));

    let claim_wait_plan = create_fixture_plan(
        &repository,
        "clock_claim_actor",
        "clock_account",
        46,
        quota(),
        now,
    )
    .await;
    repository
        .approve(
            &claim_wait_plan.plan_id,
            "integration_approver",
            &claim_wait_plan.plan_digest,
            "clock/claim",
            now,
        )
        .await
        .unwrap();
    enable_gates(&admin, "clock_account", 46, now).await;
    admin
        .execute(
            "UPDATE control.wb_runtime_gates \
                 SET revision=revision+1, \
                     lease_expires_at=clock_timestamp()+interval '750 milliseconds', \
                     updated_at=clock_timestamp() \
                 WHERE gate_key='campaign/clock_account/46'",
            &[],
        )
        .await
        .unwrap();
    let claim_lock = admin.transaction().await.unwrap();
    claim_lock
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&"wb/clock_account/46"],
        )
        .await
        .unwrap();
    let claim_repository = repository.clone();
    let claim_plan_id = claim_wait_plan.plan_id.clone();
    let claim_plan_digest = claim_wait_plan.plan_digest.clone();
    let delayed_claim = tokio::spawn(async move {
        claim_repository
            .claim_for_apply(WbApplyContext {
                plan_id: &claim_plan_id,
                actor_id: "clock_claim_actor",
                expected_plan_digest: &claim_plan_digest,
                expected_schema_version: 1,
                expected_policy_revision: 7,
                expected_policy_digest: POLICY_DIGEST,
                now: Utc::now(),
            })
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    claim_lock.commit().await.unwrap();
    assert!(matches!(
        delayed_claim.await.unwrap(),
        Err(PlanStoreError::RuntimeDisabled)
    ));

    let revalidate_wait_plan = create_fixture_plan(
        &repository,
        "clock_revalidate_actor",
        "clock_account",
        47,
        quota(),
        now,
    )
    .await;
    repository
        .approve(
            &revalidate_wait_plan.plan_id,
            "integration_approver",
            &revalidate_wait_plan.plan_digest,
            "clock/revalidate",
            now,
        )
        .await
        .unwrap();
    enable_gates(&admin, "clock_account", 47, now).await;
    repository
        .claim_for_apply(apply_context(
            &revalidate_wait_plan,
            "clock_revalidate_actor",
            now,
        ))
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.wb_runtime_gates \
                 SET revision=revision+1, \
                     lease_expires_at=clock_timestamp()+interval '750 milliseconds', \
                     updated_at=clock_timestamp() \
                 WHERE gate_key='campaign/clock_account/47'",
            &[],
        )
        .await
        .unwrap();
    let revalidate_lock = admin.transaction().await.unwrap();
    revalidate_lock
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&"wb/clock_account/47"],
        )
        .await
        .unwrap();
    let revalidate_repository = repository.clone();
    let revalidate_plan_id = revalidate_wait_plan.plan_id.clone();
    let revalidate_plan_digest = revalidate_wait_plan.plan_digest.clone();
    let delayed_revalidate = tokio::spawn(async move {
        revalidate_repository
            .revalidate_before_write(WbApplyContext {
                plan_id: &revalidate_plan_id,
                actor_id: "clock_revalidate_actor",
                expected_plan_digest: &revalidate_plan_digest,
                expected_schema_version: 1,
                expected_policy_revision: 7,
                expected_policy_digest: POLICY_DIGEST,
                now: Utc::now(),
            })
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    revalidate_lock.commit().await.unwrap();
    assert!(matches!(
        delayed_revalidate.await.unwrap(),
        Err(PlanStoreError::RuntimeDisabled)
    ));
    repository
        .finish(
            &revalidate_wait_plan.plan_id,
            "clock_revalidate_actor",
            WbPlanFinish {
                status: WbPlanStatus::Failed,
                error_class: Some("runtime_gate_expired"),
                write_response: None,
                readback: None,
                now,
            },
        )
        .await
        .unwrap();

    drop(direct_writer);
    direct_writer_connection_task.await.unwrap().unwrap();
    let concurrent_repository_a = WbPlanRepository::connect(&config).await.unwrap();
    let concurrent_repository_b = WbPlanRepository::connect(&config).await.unwrap();
    let concurrent_repository_c = WbPlanRepository::connect(&config).await.unwrap();
    let (attempt_a, attempt_b, attempt_c, attempt_d) = tokio::join!(
        repository.reserve_prepare_attempt(
            "concurrent_prepare_actor",
            "concurrent_prepare_account",
            900,
            1,
            7,
            POLICY_DIGEST,
            quota(),
            now,
        ),
        concurrent_repository_a.reserve_prepare_attempt(
            "concurrent_prepare_actor",
            "concurrent_prepare_account",
            900,
            1,
            7,
            POLICY_DIGEST,
            quota(),
            now,
        ),
        concurrent_repository_b.reserve_prepare_attempt(
            "concurrent_prepare_actor",
            "concurrent_prepare_account",
            900,
            1,
            7,
            POLICY_DIGEST,
            quota(),
            now,
        ),
        concurrent_repository_c.reserve_prepare_attempt(
            "concurrent_prepare_actor",
            "concurrent_prepare_account",
            900,
            1,
            7,
            POLICY_DIGEST,
            quota(),
            now,
        ),
    );
    let concurrent_attempts: [_; 4] = (attempt_a, attempt_b, attempt_c, attempt_d).into();
    assert_eq!(
        concurrent_attempts
            .iter()
            .filter(|attempt| attempt.is_ok())
            .count(),
        3
    );
    assert_eq!(
        concurrent_attempts
            .iter()
            .filter(|attempt| matches!(attempt, Err(PlanStoreError::PrepareLimitExceeded)))
            .count(),
        1
    );

    for advert_id in 1_000..1_060 {
        repository
            .reserve_prepare_attempt(
                "actor_hour_limit",
                "actor_hour_account",
                advert_id,
                1,
                7,
                POLICY_DIGEST,
                quota(),
                now + Duration::days(365),
            )
            .await
            .unwrap();
    }
    assert!(matches!(
        repository
            .reserve_prepare_attempt(
                "actor_hour_limit",
                "actor_hour_account",
                1_060,
                1,
                7,
                POLICY_DIGEST,
                quota(),
                now - Duration::days(365),
            )
            .await,
        Err(PlanStoreError::PrepareLimitExceeded)
    ));

    repository
        .register_policy(1, 8, NEXT_POLICY_DIGEST, now)
        .await
        .unwrap();
    assert!(matches!(
        repository.register_policy(1, 7, POLICY_DIGEST, now).await,
        Err(PlanStoreError::PolicyChanged)
    ));
    drop(admin);
    admin_connection_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn repository_enforces_approval_gates_incidents_and_quotas_when_test_database_is_available() {
    Box::pin(run_repository_scenarios_with_optional_test_database(
        std::env::var("WB_CONTROL_TEST_DATABASE_URL"),
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
    ))
    .await;
    Box::pin(run_repository_scenarios_with_optional_test_database(
        Err(std::env::VarError::NotPresent),
        Err(std::env::VarError::NotPresent),
    ))
    .await;
}
