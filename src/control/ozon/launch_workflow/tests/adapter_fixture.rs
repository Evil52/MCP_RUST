use super::*;
use crate::{config::PerformanceCredentials, test_support::mock_http};
use std::{collections::BTreeMap, fs, path::PathBuf, sync::atomic::AtomicU64};

pub(in crate::control::ozon) const TOKEN: &str =
    r#"{"access_token":"local-fixture-token","token_type":"Bearer","expires_in":1800}"#;
pub(in crate::control::ozon) const EMPTY_CAMPAIGNS: &str = r#"{"list":[]}"#;
pub(in crate::control::ozon) const EMPTY_PRODUCTS: &str = r#"{"products":[]}"#;
static FIXTURE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(in crate::control::ozon) struct AuthorizationFixture {
    pub(in crate::control::ozon) path: PathBuf,
    pub(in crate::control::ozon) registry: RegistrySource,
    pub(in crate::control::ozon) policy: Arc<ControlPolicy>,
}

impl AuthorizationFixture {
    pub(in crate::control::ozon) fn new() -> Self {
        let sequence = FIXTURE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "ozon-launch-adapter-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        let registry_path = path.join("registry.json");
        let registry_json = serde_json::json!({
            "version": 1,
            "actors": [
                {"id":"actor","name":"Actor","role":"manager","oidc":{"subject":"actor-subject"}},
                {"id":"approver","name":"Approver","role":"finance","account_ids":["account"],"oidc":{"subject":"approver-subject"}}
            ],
            "accounts": [{
                "id":"account","organization":"Test","marketplace":"ozon","seller_client_id":"seller","manager_id":"actor",
                "ozon":{"store_id":"store","client_id_env":"UNUSED_CLIENT_ID","api_key_env":"UNUSED_API_KEY",
                    "performance":{"client_id_env":"UNUSED_PERFORMANCE_ID","client_secret_env":"UNUSED_PERFORMANCE_SECRET"}}
            }]
        });
        fs::write(&registry_path, serde_json::to_vec(&registry_json).unwrap()).unwrap();
        let registry = RegistrySource::new(&registry_path).unwrap();
        let policy_path = path.join("policy.json");
        fs::write(&policy_path, serde_json::to_vec(&serde_json::json!({
            "version":1,"revision":7,"mode":"enabled","actors":[{
                "actor_id":"actor","ozon_campaign_launch_targets":[{
                    "account_id":"account","skus":[1001],"weekly_budget_microrubles":2_000_000_000_u64,
                    "per_sku_spend_cap_microrubles":2_000_000_000_u64,"initial_cpc_bid_microrubles":7_000_000,
                    "max_cpc_bid_microrubles":12_000_000,"target_drr_percent":15,"target_position":10,"approver_actor_ids":["approver"]
                }]
            }]
        })).unwrap()).unwrap();
        let policy = Arc::new(ControlPolicy::load(policy_path, &registry.load().unwrap()).unwrap());
        Self {
            path,
            registry,
            policy,
        }
    }

    pub(in crate::control::ozon) fn plan(&self) -> OzonCampaignPlan {
        let mut plan = lease(
            OzonLaunchAction::CreateCampaign,
            OzonLaunchClaimMode::Execute,
            OzonLaunchStatus::Approved,
        )
        .plan;
        plan.policy_digest = self.policy.digest().to_owned();
        plan.manifest.policy_digest = plan.policy_digest.clone();
        plan
    }

    pub(in crate::control::ozon) fn manifest(
        &self,
    ) -> super::super::super::OzonCampaignLaunchManifest {
        prepare_campaign_launch_manifest(
            "actor",
            1,
            7,
            self.policy.digest(),
            "account",
            &[1001],
            2_000_000_000,
            2_000_000_000,
            7_000_000,
            12_000_000,
            15,
            10,
            manifest().spec,
        )
        .unwrap()
    }
}

impl Drop for AuthorizationFixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).unwrap();
    }
}

pub(in crate::control::ozon) fn credentials() -> PerformanceCredentials {
    PerformanceCredentials {
        client_id: "adapter-client".to_owned(),
        client_secret: "adapter-secret".to_owned(),
    }
}

pub(in crate::control::ozon) fn mock_reader(
    responses: Vec<(u16, String)>,
) -> (Arc<PerformanceClient>, std::sync::mpsc::Receiver<String>) {
    let (url, requests) = mock_http(
        std::iter::once((200, TOKEN.to_owned()))
            .chain(responses)
            .collect(),
    );
    (
        Arc::new(PerformanceClient::new_for_test(
            url,
            Duration::from_secs(2),
            BTreeMap::from([(StoreId::from("store"), credentials())]),
        )),
        requests,
    )
}

pub(in crate::control::ozon) fn mock_writer(
    responses: Vec<(u16, String)>,
) -> (Arc<OzonAdsWriteClient>, std::sync::mpsc::Receiver<String>) {
    let (url, requests) = mock_http(responses);
    (
        Arc::new(OzonAdsWriteClient::new_for_test(
            &url,
            credentials(),
            Duration::from_secs(2),
        )),
        requests,
    )
}

pub(in crate::control::ozon) fn campaign(plan: &OzonCampaignPlan, state: &str) -> String {
    serde_json::json!({"list":[{"id":42,"title":plan.manifest.create_request.title,"state":state}]})
        .to_string()
}

pub(in crate::control::ozon) fn products() -> String {
    serde_json::json!({"products":[{"sku":1001,"bid":7_000_000}]}).to_string()
}

#[derive(Default)]
pub(in crate::control::ozon) struct AdapterClock {
    pub(in crate::control::ozon) timeout_at: Option<usize>,
    calls: AtomicUsize,
}

impl OzonLaunchClock for AdapterClock {
    async fn timeout<T, F>(
        &self,
        _duration: Duration,
        future: F,
    ) -> Result<T, OzonLaunchDeadlineExceeded>
    where
        T: Send,
        F: Future<Output = T> + Send,
    {
        if self.timeout_at == Some(self.calls.fetch_add(1, Ordering::Relaxed)) {
            Err(OzonLaunchDeadlineExceeded)
        } else {
            Ok(future.await)
        }
    }
    fn sleep(&self, _duration: Duration) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

pub(in crate::control::ozon) struct Database {
    pub(in crate::control::ozon) planner: Arc<OzonPlanRepository>,
    pub(in crate::control::ozon) executor: Arc<OzonPlanRepository>,
    pub(in crate::control::ozon) admin: tokio_postgres::Client,
    connection: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
}

impl Database {
    pub(in crate::control::ozon) async fn connect() -> Option<Self> {
        let planner = std::env::var("OZON_CONTROL_TEST_DATABASE_URL").ok()?;
        let executor = std::env::var("OZON_EXECUTOR_TEST_DATABASE_URL").ok()?;
        let admin = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL").ok()?;
        let (admin, connection) = tokio_postgres::connect(&admin, tokio_postgres::NoTls)
            .await
            .unwrap();
        Some(Self {
            planner: Arc::new(
                OzonPlanRepository::connect(&planner.parse().unwrap())
                    .await
                    .unwrap(),
            ),
            executor: Arc::new(
                OzonPlanRepository::connect(&executor.parse().unwrap())
                    .await
                    .unwrap(),
            ),
            admin,
            connection: tokio::spawn(connection),
        })
    }

    pub(in crate::control::ozon) async fn prepare(
        &self,
        authorization: &AuthorizationFixture,
    ) -> OzonLaunchLease {
        self.admin.batch_execute("TRUNCATE control.ozon_static_guard_audit_events, control.ozon_campaign_audit_events, control.ozon_campaign_guards, control.ozon_campaign_action_reservations, control.ozon_campaign_plan_approvals, control.ozon_campaign_launch_workflows, control.ozon_campaign_plans, control.ozon_runtime_gates, control.ozon_policy_revisions RESTART IDENTITY CASCADE").await.unwrap();
        self.planner
            .register_policy(1, 7, authorization.policy.digest())
            .await
            .unwrap();
        for (key, scope, account, sku) in [
            ("global", "global", None, None),
            ("account/account", "account", Some("account"), None),
            ("sku/account/1001", "sku", Some("account"), Some(1001_i64)),
        ] {
            self.admin.execute("INSERT INTO control.ozon_runtime_gates(gate_key,scope_kind,account_id,sku,enabled,lease_expires_at,revision,reason,updated_by,updated_at) VALUES($1,$2,$3,$4,true,clock_timestamp()+interval '10 minutes',1,'test','test',clock_timestamp())", &[&key, &scope, &account, &sku]).await.unwrap();
        }
        let plan = self
            .planner
            .create(&authorization.manifest())
            .await
            .unwrap();
        self.planner
            .approve(&plan.plan_id, "approver", &plan.plan_digest, "adapter/test")
            .await
            .unwrap();
        self.planner
            .enqueue_launch(&plan.plan_id, "actor", &plan.plan_digest)
            .await
            .unwrap();
        OzonLaunchRepositoryPort::claim_execution(self.executor.as_ref(), "account", "worker")
            .await
            .unwrap()
            .unwrap()
    }

    pub(in crate::control::ozon) fn io(
        &self,
        authorization: &AuthorizationFixture,
        reader: Arc<PerformanceClient>,
        writer: Arc<OzonAdsWriteClient>,
    ) -> PerformanceOzonLaunchIo<AdapterClock> {
        let io = PerformanceOzonLaunchIo::new(
            Arc::clone(&self.executor),
            reader,
            writer,
            authorization.registry.clone(),
            Arc::clone(&authorization.policy),
            "account".to_owned(),
            StoreId::from("store"),
        );
        PerformanceOzonLaunchIo {
            repository: io.repository,
            reader: io.reader,
            writer: io.writer,
            registry: io.registry,
            policy: io.policy,
            account_id: io.account_id,
            store_id: io.store_id,
            clock: AdapterClock::default(),
            final_permit_deadline: io.final_permit_deadline,
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.connection.abort();
    }
}
