use super::*;
use crate::control::ozon::launch_workflow::tests::adapter_fixture::{
    AuthorizationFixture, Database,
};

pub(super) struct StaticFixture {
    pub(super) authorization: AuthorizationFixture,
    pub(super) guard: OzonStaticCampaignGuard,
    pub(super) config_path: PathBuf,
    pub(super) state_path: PathBuf,
    pub(super) digest: String,
    pub(super) store: StoreId,
    pub(super) fingerprint: String,
}

impl StaticFixture {
    pub(super) fn new() -> Self {
        let mut authorization = AuthorizationFixture::new();
        fs::set_permissions(&authorization.path, fs::Permissions::from_mode(0o700)).unwrap();
        let fingerprint = credential_sha256("adapter-client");
        let registry_path = authorization.path.join("registry.json");
        let mut registry: serde_json::Value =
            serde_json::from_slice(&fs::read(&registry_path).unwrap()).unwrap();
        registry["accounts"][0]["ozon"]["performance"]["control_planner_client_id_sha256"] =
            serde_json::json!(credential_sha256("planner-client"));
        registry["accounts"][0]["ozon"]["performance"]["control_executor_client_id_sha256"] =
            serde_json::json!(fingerprint);
        fs::write(&registry_path, serde_json::to_vec(&registry).unwrap()).unwrap();
        authorization.registry = RegistrySource::new(&registry_path).unwrap();
        let config_path = authorization.path.join("static.json");
        fs::write(&config_path,serde_json::to_vec(&serde_json::json!({
            "account_id":"account","guards":[{"campaign_id":21,"sku":1001,"date_from":"2026-09-01",
                "spend_cap_microrubles":2_000_000_000_u64,"target_drr_percent":15,"min_cpc_bid_microrubles":7_000_000,"max_cpc_bid_microrubles":12_000_000}]
        })).unwrap()).unwrap();
        let (config, digest) = load_static_guards(&config_path, "account").unwrap();
        let state_path = authorization.path.join("state.json");
        Self {
            authorization,
            guard: config.guards.into_iter().next().unwrap(),
            config_path,
            state_path,
            digest,
            store: StoreId::from("store"),
            fingerprint,
        }
    }

    pub(super) fn write_authorization<'a>(
        &'a self,
        database: &'a Database,
    ) -> StaticGuardWriteAuthorization<'a> {
        StaticGuardWriteAuthorization {
            repository: &database.executor,
            policy: &self.authorization.policy,
            registry: &self.authorization.registry,
            config_path: &self.config_path,
            runtime_account_id: "account",
            store: &self.store,
            executor_fingerprint: &self.fingerprint,
            worker_id: "worker",
            config_digest: &self.digest,
        }
    }

    pub(super) async fn initialize(&self, database: &Database) -> StaticGuardState {
        database.prepare(&self.authorization).await;
        let mut state = StaticGuardState::default();
        database
            .executor
            .initialize_static_guard_state(
                1,
                7,
                self.authorization.policy.digest(),
                "account",
                &self.digest,
                "worker",
                None,
                |event_id| {
                    let state = &mut state;
                    async move {
                        persist_static_initialization_cursor(state, &self.state_path, event_id)
                            .map_err(|_| OzonPlanStoreError::Unavailable)
                    }
                },
            )
            .await
            .unwrap();
        state
    }
}

pub(super) fn campaign(state: &str) -> String {
    serde_json::json!({"list":[{"id":21,"state":state}]}).to_string()
}
pub(super) fn product(bid: u64) -> String {
    serde_json::json!({"products":[{"sku":1001,"bid":bid}]}).to_string()
}
pub(super) fn observed_at() -> DateTime<Utc> {
    "2026-09-01T12:00:00Z".parse().unwrap()
}
pub(super) fn metrics(spend: &str, revenue: &str) -> String {
    serde_json::json!({"rows":[{"id":"21","title":"Static fixture","date":"2026-09-01","views":"10","clicks":"1","moneySpent":spend,"orders":"1","ordersMoney":revenue}]}).to_string()
}
