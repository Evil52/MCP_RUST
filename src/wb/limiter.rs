//! Per-token pacing; shared seller reservations remain in quota.rs.
use super::{
    Duration, FinanceAccess, MAX_IN_FLIGHT_REQUESTS_PER_TOKEN, PacingGate, RequestClass, Semaphore,
    TokioInstant, WbError,
};

#[derive(Debug)]
pub(super) struct TokenLimiter {
    pub(super) in_flight: Semaphore,
    pub(super) analytics_ping: PacingGate,
    analytics_reports: PacingGate,
    pub(super) statistics_reports: PacingGate,
    content_reports: PacingGate,
    prices_reports: PacingGate,
    commission_tariffs: PacingGate,
    logistics_tariffs: PacingGate,
    acceptance_tariffs: PacingGate,
    pub(super) promotion_campaigns: PacingGate,
    promotion_balance: PacingGate,
    pub(super) promotion_stats: PacingGate,
    pub(super) search_reports: PacingGate,
    promotion_minimum_bids: PacingGate,
    promotion_recommendations: PacingGate,
    promotion_cluster_bids: PacingGate,
    seller_inventory: PacingGate,
    feedback_reports: PacingGate,
    return_claims: PacingGate,
    supply_reports: PacingGate,
    card_errors: PacingGate,
    fbs_orders: PacingGate,
    promotion_costs: PacingGate,
    promotion_payments: PacingGate,
    pub(super) finance_reports: PacingGate,
    pub(super) finance_access: FinanceAccess,
}

impl TokenLimiter {
    pub(super) fn new() -> Self {
        Self {
            in_flight: Semaphore::new(MAX_IN_FLIGHT_REQUESTS_PER_TOKEN),
            analytics_ping: PacingGate::new(),
            analytics_reports: PacingGate::new(),
            statistics_reports: PacingGate::new(),
            content_reports: PacingGate::new(),
            prices_reports: PacingGate::new(),
            commission_tariffs: PacingGate::new(),
            logistics_tariffs: PacingGate::new(),
            acceptance_tariffs: PacingGate::new(),
            promotion_campaigns: PacingGate::new(),
            promotion_balance: PacingGate::new(),
            promotion_stats: PacingGate::new(),
            search_reports: PacingGate::new(),
            promotion_minimum_bids: PacingGate::new(),
            promotion_recommendations: PacingGate::new(),
            promotion_cluster_bids: PacingGate::new(),
            seller_inventory: PacingGate::new(),
            feedback_reports: PacingGate::new(),
            return_claims: PacingGate::new(),
            supply_reports: PacingGate::new(),
            card_errors: PacingGate::new(),
            fbs_orders: PacingGate::new(),
            promotion_costs: PacingGate::new(),
            promotion_payments: PacingGate::new(),
            finance_reports: PacingGate::new(),
            finance_access: FinanceAccess::new(),
        }
    }

    pub(super) const fn gate(&self, request_class: RequestClass) -> &PacingGate {
        match request_class {
            RequestClass::AnalyticsPing => &self.analytics_ping,
            RequestClass::AnalyticsReport => &self.analytics_reports,
            RequestClass::StatisticsReport => &self.statistics_reports,
            RequestClass::ContentReport => &self.content_reports,
            RequestClass::PricesReport => &self.prices_reports,
            RequestClass::CommissionTariff => &self.commission_tariffs,
            RequestClass::LogisticsTariff => &self.logistics_tariffs,
            RequestClass::AcceptanceTariff => &self.acceptance_tariffs,
            RequestClass::PromotionCampaign => &self.promotion_campaigns,
            RequestClass::PromotionBalance => &self.promotion_balance,
            RequestClass::PromotionStats => &self.promotion_stats,
            RequestClass::SearchReport => &self.search_reports,
            RequestClass::PromotionMinimumBids => &self.promotion_minimum_bids,
            RequestClass::PromotionRecommendedBids => &self.promotion_recommendations,
            RequestClass::PromotionClusterBids => &self.promotion_cluster_bids,
            RequestClass::SellerInventory => &self.seller_inventory,
            RequestClass::FeedbackReport => &self.feedback_reports,
            RequestClass::ReturnClaims => &self.return_claims,
            RequestClass::SupplyReport => &self.supply_reports,
            RequestClass::CardErrors => &self.card_errors,
            RequestClass::FbsOrders => &self.fbs_orders,
            RequestClass::PromotionCosts => &self.promotion_costs,
            RequestClass::PromotionPayments => &self.promotion_payments,
            RequestClass::FinanceReport => &self.finance_reports,
        }
    }

    pub(super) async fn wait_until_ready(
        &self,
        request_class: RequestClass,
        retry: bool,
        deadline: TokioInstant,
    ) -> Result<(), WbError> {
        let gate = self.gate(request_class);
        // Classes whose quota slot is minute-scale are never queued for: a
        // caller that missed the slot is told when to come back instead of
        // parking on it. `StatisticsReport` paces at a full 60s — the same as
        // `CommissionTariff` — but was absent here, so its callers queued for
        // an entire interval only to expire against the 60s logical timeout.
        if !retry
            && matches!(
                request_class,
                RequestClass::PromotionCosts
                    | RequestClass::PromotionPayments
                    | RequestClass::CommissionTariff
                    | RequestClass::StatisticsReport
                    | RequestClass::PromotionStats
                    | RequestClass::SearchReport
            )
        {
            return gate
                .ensure_ready_now()
                .await
                .map_err(|retry_after| WbError::LocalRateLimited { retry_after });
        }
        // A wait that cannot end before the caller's deadline is not a wait,
        // it is a timeout dressed as one — and an expensive one, because the
        // MCP request slot and the HTTP connection stay held for its whole
        // duration before failing. `StatisticsReport` paces at exactly the
        // logical timeout, so a second concurrent caller was guaranteed to
        // spend a full minute reaching `Timeout`. Refuse now instead, naming
        // the instant a retry could actually succeed.
        let ready_in = gate.ready_in().await;
        if TokioInstant::now() + ready_in >= deadline {
            return Err(WbError::LocalRateLimited {
                retry_after: ready_in,
            });
        }
        gate.wait_until_ready().await;
        Ok(())
    }

    pub(super) async fn try_claim(
        &self,
        request_class: RequestClass,
        interval: Duration,
    ) -> Result<(), Duration> {
        self.gate(request_class).try_claim(interval).await
    }

    pub(super) async fn extend_cooldown(&self, request_class: RequestClass, delay: Duration) {
        if request_class == RequestClass::FinanceReport {
            self.finance_access
                .extend_cooldown(&self.finance_reports, delay)
                .await;
        } else {
            self.gate(request_class).extend_cooldown(delay).await;
        }
    }

    pub(super) async fn ready_in(&self, request_class: RequestClass) -> Duration {
        self.gate(request_class).ready_in().await
    }
}
