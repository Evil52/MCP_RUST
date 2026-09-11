//! Startup never creates/imports/resets robot execution state or incident locks.
//! The Nexus runner must already have produced two independently persisted
//! periodic cycles; its next cycle performs ordinary guarded bid management.
use super::{ACCOUNT, Journal, Operator, read_policy_json, write_error};
use crate::control::{
    WbAutomationObservation, WbAutomationObserver, WbAutomationPolicy, WbAutomationPostgresStore,
    WbAutomationStateView, wb_automation_business_date,
};
use anyhow::{Context, Result, ensure};
use chrono::Utc;
use serde_json::{Value, json};
use std::str::FromStr;

impl Operator {
    pub(super) async fn start(&self, journal: &Journal) -> Result<Value> {
        self.manifest.authorize_stage("start")?;
        let id = journal.campaign_id()?;
        journal.assert_not_attempted("start")?;
        journal.require_receipt("fund")?;
        validate_start_status(self.inactive(id, true).await?.status)?;
        let policy: WbAutomationPolicy = read_policy_json(&self.manifest.robot_policy)?;
        ensure!(
            policy == self.target_policy(id),
            "installed Nexus robot policy differs from reviewed copy"
        );
        let observer = WbAutomationObserver::from_files(
            &self.manifest.robot_policy,
            &self.manifest.registry,
            &self.manifest.reader_token,
            self.manifest.allow_broad_reader,
            std::time::Duration::from_secs(30),
            Some(&self.manifest.reader_proxy),
        )?;
        let database_url = std::env::var("WB_AUTOMATION_DATABASE_URL")
            .context("WB automation PostgreSQL is required for guarded startup")?;
        let config = tokio_postgres::Config::from_str(&database_url)
            .map_err(|_| anyhow::anyhow!("invalid automation database configuration"))?;
        let store = WbAutomationPostgresStore::connect(&config).await?;
        store.verify_runtime_contract().await?;
        self.execute_start(journal, id, &policy, &observer, &store)
            .await
    }

    /// Dependencies are explicit so the entire guarded write/readback can be
    /// exercised against an isolated test database and mock WB transport.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "campaign lease intentionally spans WB write and independent readback"
    )]
    pub(super) async fn execute_start(
        &self,
        journal: &Journal,
        id: u64,
        policy: &WbAutomationPolicy,
        observer: &WbAutomationObserver,
        store: &WbAutomationPostgresStore,
    ) -> Result<Value> {
        self.manifest.authorize_stage("start")?;
        journal.assert_not_attempted("start")?;
        journal.require_receipt("fund")?;
        ensure!(
            journal.campaign_id()? == id && *policy == self.target_policy(id),
            "startup scope drifted"
        );
        let lease = store
            .try_acquire_campaign(ACCOUNT, id)
            .await?
            .context("Nexus campaign lock is contended; no write attempted")?;
        self.writer
            .start_campaign_with_permit(id, || async {
                self.fresh_authorization()?;
                ensure!(
                    read_policy_json::<WbAutomationPolicy>(&self.manifest.robot_policy)? == *policy,
                    "robot policy changed while waiting for write slot"
                );
                ensure!(
                    lease
                        .verify_launch_cycles(observer.policy_sha256(), Utc::now())
                        .await?,
                    "two fresh periodic Nexus robot cycles are required before start"
                );
                let state = lease
                    .load_state()
                    .await?
                    .context("Nexus protective state is not installed")?;
                ensure!(
                    state.policy_digest == observer.policy_sha256()
                        && state.incident_class.is_none()
                        && state.pending_idempotency_key.is_none()
                        && state.paused_for_daily_cap_on.is_none()
                        && state.actions_today == 0
                        && state.business_date == wb_automation_business_date(Utc::now()),
                    "protective robot state is not clean/current; no locks may be bypassed"
                );
                let snapshot = observer
                    .observe(Utc::now(), WbAutomationStateView::default())
                    .await?;
                validate_initial_observation(&snapshot.observation, policy)?;
                self.inactive(id, true).await?;
                self.fresh_authorization()?;
                validate_initial_observation(&snapshot.observation, policy)?;
                journal.attempt("start", &json!({"campaign_id":id,"snapshot":snapshot}))
            })
            .await
            .map_err(write_error)?;
        let state = self.details(id).await?;
        ensure!(
            state.status == 9 && state.bids == self.manifest.bids_kopecks,
            "start readback differs; reconcile only"
        );
        journal.receipt("start", &json!({"campaign_id":id,"wb_http":200,"status":9,
            "checked_at":Utc::now(),"bids_kopecks":state.bids,"budget_rubles":self.budget(id).await?}))?;
        lease.release().await?;
        self.reconcile(journal).await
    }
}

fn validate_start_status(status: i32) -> Result<()> {
    ensure!(
        status == 11,
        "guarded startup needs verified spend evidence: WB fullstats supports statuses 7/9/11, not a new status-4 campaign; a separately reviewed first-launch protocol is required, no write attempted"
    );
    Ok(())
}

fn validate_initial_observation(
    observation: &WbAutomationObservation,
    policy: &WbAutomationPolicy,
) -> Result<()> {
    ensure!(
        matches!(observation.campaign_status, 4 | 11)
            && observation.budget_remaining_minor == 100_000
            && observation.daily_spend_complete
            && observation.daily_spend_minor == 0
            && observation.attribution_complete
            && !observation.paused_by_automation
            && observation.actions_today == 0,
        "initial guard snapshot is incomplete, already spent, paused by protection or not funded"
    );
    ensure!(
        (0..=90).contains(&(Utc::now() - observation.observed_at).num_seconds()),
        "initial guard snapshot is stale"
    );
    ensure!(
        observation.skus.len() == policy.nm_ids.len()
            && observation
                .skus
                .iter()
                .map(|sku| sku.nm_id)
                .collect::<std::collections::BTreeSet<_>>()
                == policy.nm_ids.iter().copied().collect()
            && observation
                .skus
                .iter()
                .all(|sku| policy.nm_ids.contains(&sku.nm_id)
                    && sku.sellable_stock >= policy.min_sellable_stock
                    && sku.minimum_bid_kopecks > 0
                    && sku.current_bid_kopecks >= sku.minimum_bid_kopecks
                    && (policy.min_bid_kopecks..=policy.max_bid_kopecks)
                        .contains(&sku.current_bid_kopecks)
                    && sku.spend_minor == 0),
        "initial guard SKU evidence is incompatible"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::WbAutomationSkuObservation;

    #[test]
    fn new_campaign_never_attempts_unsupported_stats_or_start() {
        assert!(validate_start_status(11).is_ok());
        for status in [4, 7, 9, -1] {
            assert!(validate_start_status(status).is_err());
        }
    }

    fn fixture() -> (WbAutomationObservation, WbAutomationPolicy) {
        let (_, mut policy) = super::super::tests::fixture();
        policy.nm_ids = super::super::NMS.to_vec();
        let observation = WbAutomationObservation {
            observed_at: Utc::now(),
            campaign_status: 11,
            paused_by_automation: false,
            budget_remaining_minor: 100_000,
            daily_spend_minor: 0,
            daily_spend_complete: true,
            actions_today: 0,
            last_action_at: None,
            attribution_complete: true,
            campaign_level_metrics: None,
            current_campaign_metrics: None,
            skus: policy
                .nm_ids
                .iter()
                .map(|&nm_id| WbAutomationSkuObservation {
                    nm_id,
                    minimum_bid_kopecks: 500,
                    current_bid_kopecks: 922,
                    sellable_stock: 20,
                    impressions: 0,
                    clicks: 0,
                    spend_minor: 0,
                    attributed_orders: 0,
                    attributed_revenue_minor: 0,
                })
                .collect(),
        };
        (observation, policy)
    }

    #[test]
    fn startup_requires_full_zero_spend_funded_snapshot() {
        let (observation, policy) = fixture();
        validate_initial_observation(&observation, &policy).unwrap();
        let mut changed = observation.clone();
        changed.daily_spend_complete = false;
        assert!(validate_initial_observation(&changed, &policy).is_err());
        changed = observation.clone();
        changed.attribution_complete = false;
        assert!(validate_initial_observation(&changed, &policy).is_err());
        changed = observation.clone();
        changed.daily_spend_minor = 1;
        assert!(validate_initial_observation(&changed, &policy).is_err());
        changed = observation.clone();
        changed.budget_remaining_minor = 0;
        assert!(validate_initial_observation(&changed, &policy).is_err());
        changed = observation.clone();
        changed.campaign_status = 9;
        assert!(validate_initial_observation(&changed, &policy).is_err());
        changed = observation;
        changed.paused_by_automation = true;
        assert!(validate_initial_observation(&changed, &policy).is_err());
    }

    #[test]
    fn startup_rejects_stale_or_incompatible_sku_evidence() {
        let (observation, policy) = fixture();
        let mut changed = observation.clone();
        changed.observed_at -= chrono::Duration::minutes(2);
        assert!(validate_initial_observation(&changed, &policy).is_err());
        changed = observation.clone();
        changed.skus[0].sellable_stock = 0;
        assert!(validate_initial_observation(&changed, &policy).is_err());
        changed = observation.clone();
        changed.skus[0].minimum_bid_kopecks = 1000;
        assert!(validate_initial_observation(&changed, &policy).is_err());
        changed = observation;
        changed.skus[0].nm_id = changed.skus[1].nm_id;
        assert!(validate_initial_observation(&changed, &policy).is_err());
    }
}
