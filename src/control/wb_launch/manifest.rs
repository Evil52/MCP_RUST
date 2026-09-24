use super::{
    ACCOUNT, Context, DateTime, LaunchScope, Manifest, NAME, NMS, Result, SOURCE, Utc,
    WbAutomationPolicy, ensure, journal,
};

pub(super) const fn legacy_version() -> u32 {
    1
}
#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde skip_serializing_if requires a reference"
)]
pub(super) const fn is_legacy(version: &u32) -> bool {
    *version == 1
}

impl Manifest {
    pub(super) fn nm_ids(&self) -> Vec<u64> {
        self.bids_kopecks.keys().copied().collect()
    }

    pub(super) fn minimum_launch_stock(&self, policy: &WbAutomationPolicy) -> u64 {
        if self.version == 1 {
            20
        } else {
            policy.min_sellable_stock.max(1)
        }
    }

    pub(super) fn validate_identity(&self) -> Result<()> {
        ensure!(
            matches!(self.version, 1 | 2),
            "unsupported WB launch manifest version"
        );
        if self.version == 1 {
            ensure!(
                self.account_id == ACCOUNT
                    && self.campaign_name == NAME
                    && self.bids_kopecks.keys().copied().eq(NMS)
                    && self.manual_control.is_none(),
                "Nexus account/name/SKU scope mismatch"
            );
        } else {
            ensure!(
                self.recreate.is_none() && self.continue_created.is_none(),
                "legacy recovery authorization cannot be used by a reusable launch"
            );
            ensure!(
                !self.account_id.is_empty()
                    && self.account_id.len() <= 128
                    && self
                        .account_id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
                "invalid account identity"
            );
            ensure!(
                !self.campaign_name.trim().is_empty()
                    && self.campaign_name.trim() == self.campaign_name
                    && self.campaign_name.len() <= 128
                    && !self.campaign_name.chars().any(char::is_control),
                "campaign name must contain 1..=128 UTF-8 bytes without surrounding whitespace or controls"
            );
            ensure!(
                (1..=50).contains(&self.bids_kopecks.len())
                    && self.bids_kopecks.iter().all(|(nm, bid)| *nm > 0
                        && i64::try_from(*nm).is_ok()
                        && *bid > 0
                        && i64::try_from(*bid).is_ok()),
                "launch requires 1..=50 distinct positive product IDs and bids in kopecks"
            );
        }
        ensure!(
            self.financial_scope_matches(),
            "funding scope does not match the authorized amount"
        );
        Ok(())
    }

    pub(super) const fn financial_scope_matches(&self) -> bool {
        self.funding_type == 1
            && (self.recreate.is_none() || matches!(self.scope, LaunchScope::CreateOnly))
            && (self.continue_created.is_none()
                || (self.recreate.is_none() && matches!(self.scope, LaunchScope::FundAndStart)))
            && match self.scope {
                LaunchScope::CreateOnly => self.budget_rubles == 0,
                LaunchScope::FundAndStart => {
                    if self.version == 1 {
                        self.budget_rubles == 1000
                    } else {
                        self.budget_rubles > 0 && self.budget_rubles <= 1_000_000
                    }
                }
            }
    }

    pub(super) fn authorize_stage(&self, mode: &str) -> Result<()> {
        if self.continue_created.is_some() {
            ensure!(
                self.financial_scope_matches()
                    && matches!(
                        mode,
                        "preflight" | "prepare" | "bids" | "fund" | "start" | "reconcile"
                    ),
                "confirmed Nexus continuation cannot create another campaign"
            );
            return Ok(());
        }
        ensure!(
            matches!(mode, "preflight" | "create" | "bids" | "reconcile")
                || (self.scope == LaunchScope::FundAndStart && matches!(mode, "fund" | "start")),
            "create-only authorization forbids funding and campaign start"
        );
        Ok(())
    }

    pub(super) fn target_policy(&self, source: &WbAutomationPolicy, id: u64) -> WbAutomationPolicy {
        let mut policy = source.clone();
        policy.campaign_id = id;
        policy.account_id.clone_from(&self.account_id);
        policy.campaign_name.clone_from(&self.campaign_name);
        policy.nm_ids = self.nm_ids();
        policy.authorized_by_actor_id.clone_from(&self.actor_id);
        policy
            .authorization_reference
            .clone_from(&self.authorization_reference);
        policy
    }

    pub(super) fn validate(
        &self,
        policy: &WbAutomationPolicy,
        now: DateTime<Utc>,
        allow_expired: bool,
    ) -> Result<()> {
        self.validate_identity()?;
        if let Some(approval) = &self.continue_created {
            approval.validate_scope(self)?;
            ensure!(
                policy.hard_drr_basis_points == 1500
                    && policy.min_bid_kopecks == 500
                    && policy.max_bid_kopecks == 1050
                    && policy.cooldown_seconds == 1800,
                "continuation must preserve reviewed hard DRR, corridor and cooldown"
            );
        }
        ensure!(
            !self.authorization_reference.trim().is_empty()
                && (allow_expired || (self.authorized_at <= now && now < self.expires_at))
                && (chrono::Duration::seconds(1)..=chrono::Duration::hours(24))
                    .contains(&(self.expires_at - self.authorized_at)),
            "launch authorization is absent or expired (maximum 24 hours)"
        );
        crate::control::validate_wb_automation_policy(policy)?;
        ensure!(
            policy.account_id == self.account_id
                && (self.version == 2
                    || (policy.campaign_id == SOURCE
                        && policy.campaign_name == "Одуванчик"
                        && policy.daily_spend_cap_minor == 50_000
                        && policy.daily_pause_threshold_minor == 45_000
                        && policy.target_drr_basis_points == 1500))
                && policy.payment_type == "cpc"
                && policy.placement == "search"
                && !policy.allow_budget_top_up
                && policy.write_enabled
                && policy.bid_writes_enabled
                && (allow_expired
                    || (policy.authorized_at <= now && now < policy.authorization_expires_at)),
            "source robot protection policy is incompatible or inactive"
        );
        ensure!(
            self.bids_kopecks
                .values()
                .all(|bid| (policy.min_bid_kopecks..=policy.max_bid_kopecks).contains(bid)),
            "initial bids exceed the source policy corridor"
        );
        ensure!(
            journal::digest(&serde_json::to_vec(policy)?) == self.source_policy_sha256,
            "source policy changed since review"
        );
        // The campaign ID is not known before create. Validate every derived
        // field with an already valid ID before allowing the first WB write.
        crate::control::validate_wb_automation_policy(
            &self.target_policy(policy, policy.campaign_id),
        )
        .context("derived target robot policy is invalid")?;
        ensure!(
            !self.reader_proxy.is_empty() && !self.writer_proxy.is_empty(),
            "dedicated egress proxies are mandatory"
        );
        Ok(())
    }
}
