use super::{static_safety::GuardLogs, *};
use crate::control::{
    ozon::{
        launch_workflow::tests::adapter_fixture::{
            AuthorizationFixture, Database, EMPTY_CAMPAIGNS, EMPTY_PRODUCTS, TOKEN, campaign,
            mock_reader, mock_writer, products,
        },
        model::OzonLaunchStatus,
    },
    plan::CONTROL_DB_TEST_LOCK,
};
use tracing::instrument::WithSubscriber as _;

#[derive(Clone, Copy)]
enum Workflow {
    Launch,
    Guard,
}

async fn run_task(
    database: &Database,
    authorization: &AuthorizationFixture,
    reader: &Arc<PerformanceClient>,
    writer: &Arc<OzonAdsWriteClient>,
    workflow: Workflow,
) -> bool {
    let store = StoreId::from("store");
    let launch_io = PerformanceOzonLaunchIo::new(
        Arc::clone(&database.executor),
        Arc::clone(reader),
        Arc::clone(writer),
        authorization.registry.clone(),
        Arc::clone(&authorization.policy),
        "account".to_owned(),
        store.clone(),
    );
    let durable_reader = PerformanceGuardReader {
        client: reader,
        store: &store,
    };
    let durable_writer = PerformanceGuardWriter {
        client: writer,
        repository: &database.executor,
    };
    let tasks = DurableOzonWorkflowTasks {
        repository: &database.executor,
        launch_io: &launch_io,
        launch_failpoints: &NoOzonLaunchFailpoints,
        guard_reader: &durable_reader,
        guard_writer: &durable_writer,
        guard_clock: &TokioOzonGuardClock,
        guard_failpoints: &NoOzonGuardFailpoints,
        account_id: "account",
        worker_id: "worker",
    };
    match workflow {
        Workflow::Launch => tasks.drain_launch_once().await,
        Workflow::Guard => tasks.run_guard_once().await,
    }
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL roles"]
async fn postgres_durable_tasks_report_query_failure_without_marketplace_io() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("isolated PostgreSQL roles are configured");
    let authorization = AuthorizationFixture::new();
    database.prepare_plan(&authorization).await;
    let (reader, reads) = mock_reader(vec![]);
    let (writer, requests) = mock_writer(vec![]);
    for workflow in [Workflow::Launch, Workflow::Guard] {
        assert!(run_task(&database, &authorization, &reader, &writer, workflow).await);
    }
    database.admin.batch_execute("REVOKE SELECT ON control.ozon_campaign_plans, control.ozon_campaign_guards FROM ozon_control_executor").await.unwrap();
    let logs = GuardLogs::default();
    let failed_launch = run_task(
        &database,
        &authorization,
        &reader,
        &writer,
        Workflow::Launch,
    )
    .with_subscriber(logs.subscriber())
    .await;
    let failed_guard = run_task(&database, &authorization, &reader, &writer, Workflow::Guard)
        .with_subscriber(logs.subscriber())
        .await;
    database.admin.batch_execute("GRANT SELECT ON control.ozon_campaign_plans, control.ozon_campaign_guards TO ozon_control_executor").await.unwrap();
    assert!(!failed_launch);
    assert!(!failed_guard);
    assert!(logs.contains("durable Ozon launch drain failed"));
    assert!(logs.contains("Ozon campaign guard cycle failed"));
    assert_eq!(reads.try_iter().count(), 0);
    assert_eq!(requests.try_iter().count(), 0);
}

async fn queued_plan(
    database: &Database,
    authorization: &AuthorizationFixture,
) -> crate::control::ozon::OzonCampaignPlan {
    let plan = database.prepare_plan(authorization).await;
    database
        .planner
        .approve(
            &plan.plan_id,
            "approver",
            &plan.plan_digest,
            "runtime/tasks",
        )
        .await
        .unwrap();
    database
        .planner
        .enqueue_launch(&plan.plan_id, "actor", &plan.plan_digest)
        .await
        .unwrap();
    plan
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL roles"]
async fn postgres_durable_tasks_preserve_prewrite_failure_for_later_recovery() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("isolated PostgreSQL roles are configured");
    let authorization = AuthorizationFixture::new();
    let plan = queued_plan(&database, &authorization).await;
    let (reader, reads) = mock_reader(vec![
        (200, EMPTY_CAMPAIGNS.to_owned()),
        (200, EMPTY_CAMPAIGNS.to_owned()),
    ]);
    let (writer, requests) = mock_writer(vec![(401, "{}".to_owned())]);
    let logs = GuardLogs::default();
    assert!(
        run_task(
            &database,
            &authorization,
            &reader,
            &writer,
            Workflow::Launch
        )
        .with_subscriber(logs.subscriber())
        .await
    );
    assert!(logs.contains("durable Ozon launch batch requires continued recovery"));
    assert!(logs.contains("persisted_failures=1"));
    let plan = database.planner.load(&plan.plan_id).await.unwrap();
    assert_eq!(plan.status, OzonLaunchStatus::Approved);
    assert!(plan.workflow_write_started_at.is_none());
    assert!(plan.last_error_class.is_none());
    let retry = database.admin.query_one(
        "SELECT last_error_class, available_at > clock_timestamp(), write_started_at IS NULL FROM control.ozon_campaign_launch_workflows WHERE plan_id=$1",
        &[&plan.plan_id],
    ).await.unwrap();
    assert_eq!(retry.get::<_, String>(0), "ozon_create_not_started");
    assert!(retry.get::<_, bool>(1));
    assert!(retry.get::<_, bool>(2));
    assert_eq!(reads.try_iter().count(), 3);
    assert_eq!(requests.try_iter().count(), 1);
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL roles"]
async fn postgres_durable_tasks_finish_three_launch_stages_before_reporting_completion() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("isolated PostgreSQL roles are configured");
    let authorization = AuthorizationFixture::new();
    let plan = queued_plan(&database, &authorization).await;
    let inactive = campaign(&plan, "CAMPAIGN_STATE_INACTIVE");
    let (reader, reads) = mock_reader(vec![
        (200, EMPTY_CAMPAIGNS.to_owned()),
        (200, EMPTY_CAMPAIGNS.to_owned()),
        (200, inactive.clone()),
        (200, inactive.clone()),
        (200, EMPTY_PRODUCTS.to_owned()),
        (200, inactive.clone()),
        (200, products()),
        (200, inactive),
        (200, products()),
        (200, campaign(&plan, "CAMPAIGN_STATE_RUNNING")),
        (200, products()),
    ]);
    let (writer, requests) = mock_writer(vec![
        (200, TOKEN.to_owned()),
        (200, r#"{"campaignId":"42"}"#.to_owned()),
        (200, "{}".to_owned()),
        (200, "{}".to_owned()),
    ]);
    let logs = GuardLogs::default();
    assert!(
        run_task(
            &database,
            &authorization,
            &reader,
            &writer,
            Workflow::Launch
        )
        .with_subscriber(logs.subscriber())
        .await
    );
    assert!(logs.contains("durable Ozon launch batch completed"));
    assert!(logs.contains("processed=3"));
    assert_eq!(
        database.planner.load(&plan.plan_id).await.unwrap().status,
        OzonLaunchStatus::Applied
    );
    assert_eq!(
        database
            .executor
            .active_guards_for_account("account")
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(reads.try_iter().count(), 12);
    let requests = requests.try_iter().collect::<Vec<_>>();
    assert_eq!(requests.len(), 4);
    assert!(requests[1].starts_with("POST /api/client/campaign/cpc/v2/product "));
    assert!(requests[2].starts_with("POST /api/client/campaign/42/products "));
    assert!(requests[3].starts_with("POST /api/client/campaign/42/activate "));
}

#[derive(Default)]
struct FailedGuardTasks {
    launch_calls: AtomicUsize,
    guard_calls: AtomicUsize,
}

impl OzonWorkflowTasks for FailedGuardTasks {
    async fn drain_launch_once(&self) -> bool {
        self.launch_calls.fetch_add(1, Ordering::Relaxed);
        true
    }

    async fn run_guard_once(&self) -> bool {
        self.guard_calls.fetch_add(1, Ordering::Relaxed);
        false
    }
}

#[tokio::test]
async fn independent_guard_failure_limit_stops_both_pollers() {
    let tasks = FailedGuardTasks::default();
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(1),
            run_independent_workflow_loops(
                &tasks,
                Duration::from_millis(1),
                Duration::from_millis(1),
                std::future::pending(),
            )
        )
        .await
        .unwrap(),
        Err(OzonWorkflowLoopFailure::Guard)
    );
    assert_eq!(
        tasks.guard_calls.load(Ordering::Relaxed),
        MAX_CONSECUTIVE_WORKFLOW_FAILURES
    );
    assert!(tasks.launch_calls.load(Ordering::Relaxed) > 0);
}
