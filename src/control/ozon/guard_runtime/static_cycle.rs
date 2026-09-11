use super::super::pacing::{OzonBidPacingAdjustment, evaluate_ozon_bid_increase_after_guard};
use super::{
    OzonAdsWriteClient, OzonBidPacingObservation, OzonBidPacingPolicy, OzonBidPositionReader,
    OzonGuardMetrics, OzonStaticCampaignGuard, OzonStaticDynamicBidControl,
    OzonStaticGuardFirstStep, PerformanceClient, StaticGuardState, StaticGuardWriteAuthorization,
    StoreId, campaign_product_snapshot, change_static_campaign_bid, guard_campaign_static,
    plan_static_guard_first_step, reconcile_pending_static_bid, recover_pending_static_bids,
    recover_pending_static_campaign_mutations, running_static_campaigns, static_guard_metrics,
    validate_ozon_campaign_product_guard,
};
use anyhow::{Context as _, Result};
use chrono::{DateTime, Utc};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
    time::Duration,
};

#[derive(Clone, Copy)]
struct StaticGuardCycle<'a> {
    state_path: &'a Path,
    reader: &'a Arc<PerformanceClient>,
    writer: &'a Arc<OzonAdsWriteClient>,
    store: &'a StoreId,
    write_authorization: StaticGuardWriteAuthorization<'a>,
    dynamic_bid_control: Option<&'a OzonStaticDynamicBidControl>,
    position_reader: Option<&'a OzonBidPositionReader>,
    observed_at: DateTime<Utc>,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn guard_once_static(
    guards: &[OzonStaticCampaignGuard],
    state: &mut StaticGuardState,
    state_path: &Path,
    reader: &Arc<PerformanceClient>,
    writer: &Arc<OzonAdsWriteClient>,
    store: &StoreId,
    write_authorization: StaticGuardWriteAuthorization<'_>,
    dynamic_bid_control: Option<&OzonStaticDynamicBidControl>,
    position_reader: Option<&OzonBidPositionReader>,
    observed_at: DateTime<Utc>,
) -> Result<()> {
    StaticGuardCycle {
        state_path,
        reader,
        writer,
        store,
        write_authorization,
        dynamic_bid_control,
        position_reader,
        observed_at,
    }
    .run(guards, state)
    .await
}

impl StaticGuardCycle<'_> {
    async fn run(
        &self,
        guards: &[OzonStaticCampaignGuard],
        state: &mut StaticGuardState,
    ) -> Result<()> {
        let Self {
            state_path,
            reader,
            writer,
            store,
            write_authorization,
            observed_at,
            ..
        } = *self;

        recover_pending_static_campaign_mutations(
            state,
            state_path,
            reader.as_ref(),
            writer.as_ref(),
            store,
            guards,
            write_authorization,
        )
        .await?;
        recover_pending_static_bids(state, state_path, reader, store, guards).await?;
        let running = running_static_campaigns(reader, store, guards).await?;
        tracing::info!(
            running = running.len(),
            guards = guards.len(),
            "static Ozon guard cycle ready"
        );
        if running.is_empty() {
            return Ok(());
        }
        let metrics = match static_guard_metrics(reader, store, guards, &running, observed_at).await
        {
            Ok(metrics) => metrics,
            Err(error) => {
                tracing::error!(
                    running = running.len(),
                    %error,
                    "statistics unavailable or incomplete; fail-closed stops requested"
                );
                self.stop_unobserved_campaigns(guards, state, &running)
                    .await;
                return Err(error.context("static guard telemetry failed closed"));
            }
        };
        for static_guard in guards {
            let guard = &static_guard.guard;
            if state.incident_campaign_ids.contains(&guard.campaign_id)
                || !running.contains(&guard.campaign_id)
            {
                continue;
            }
            self.guard_observation(static_guard, state, &metrics)
                .await?;
        }
        Ok(())
    }

    async fn stop_unobserved_campaigns(
        &self,
        guards: &[OzonStaticCampaignGuard],
        state: &mut StaticGuardState,
        running: &BTreeSet<u64>,
    ) {
        let Self {
            state_path,
            reader,
            writer,
            store,
            write_authorization,
            ..
        } = *self;
        for static_guard in guards {
            if running.contains(&static_guard.guard.campaign_id)
                && !state
                    .incident_campaign_ids
                    .contains(&static_guard.guard.campaign_id)
                && let Err(stop_error) = guard_campaign_static(
                    state,
                    state_path,
                    reader,
                    writer,
                    store,
                    write_authorization,
                    static_guard,
                    None,
                    None,
                    Some("telemetry_unavailable"),
                )
                .await
            {
                tracing::error!(
                    campaign_id = static_guard.guard.campaign_id,
                    %stop_error,
                    "telemetry fail-closed stop failed"
                );
            }
        }
    }

    async fn guard_observation(
        &self,
        static_guard: &OzonStaticCampaignGuard,
        state: &mut StaticGuardState,
        metrics: &BTreeMap<u64, (u64, u64)>,
    ) -> Result<()> {
        let Self {
            state_path,
            reader,
            writer,
            store,
            write_authorization,
            ..
        } = *self;
        let guard = &static_guard.guard;
        let (spend_minor, revenue_minor) = metrics
            .get(&guard.campaign_id)
            .copied()
            .context("complete static telemetry lost a requested campaign")?;
        if let OzonStaticGuardFirstStep::Stop(reason) = plan_static_guard_first_step(
            guard,
            OzonGuardMetrics {
                spend_minor,
                attributed_revenue_minor: revenue_minor,
            },
        )? {
            if let Err(error) = guard_campaign_static(
                state,
                state_path,
                reader,
                writer,
                store,
                write_authorization,
                static_guard,
                Some(spend_minor),
                Some(revenue_minor),
                Some(reason.as_str()),
            )
            .await
            {
                tracing::error!(campaign_id=guard.campaign_id,sku=guard.sku,%error,"static Ozon hard-stop failed");
            }
            tokio::time::sleep(Duration::from_secs(7)).await;
            return Ok(());
        }
        let Some(current_bid_microrubles) = self
            .read_validated_bid(static_guard, state, spend_minor, revenue_minor)
            .await?
        else {
            return Ok(());
        };

        reconcile_pending_static_bid(state, state_path, static_guard, current_bid_microrubles)?;
        if state.incident_campaign_ids.contains(&guard.campaign_id) {
            return Ok(());
        }

        if let Some(dynamic) = self.dynamic_bid_control {
            return self
                .apply_dynamic_bid_control(
                    static_guard,
                    state,
                    dynamic,
                    current_bid_microrubles,
                    spend_minor,
                    revenue_minor,
                )
                .await;
        }

        if let Err(error) = guard_campaign_static(
            state,
            state_path,
            reader,
            writer,
            store,
            write_authorization,
            static_guard,
            Some(spend_minor),
            Some(revenue_minor),
            None,
        )
        .await
        {
            tracing::error!(campaign_id=guard.campaign_id,sku=guard.sku,%error,"static Ozon guard item failed");
        }
        Ok(())
    }

    async fn read_validated_bid(
        &self,
        static_guard: &OzonStaticCampaignGuard,
        state: &mut StaticGuardState,
        spend_minor: u64,
        revenue_minor: u64,
    ) -> Result<Option<u64>> {
        let Self {
            state_path,
            reader,
            writer,
            store,
            write_authorization,
            ..
        } = *self;
        let guard = &static_guard.guard;
        let product_snapshot =
            match campaign_product_snapshot(reader, store, guard.campaign_id).await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    tracing::warn!(
                        campaign_id = guard.campaign_id,
                        sku = guard.sku,
                        %error,
                        "campaign product read unavailable; no-write hold"
                    );
                    return Ok(None);
                }
            };
        let product_guard = validate_ozon_campaign_product_guard(
            &product_snapshot,
            guard.sku,
            static_guard.min_cpc_bid_microrubles,
            static_guard.max_cpc_bid_microrubles,
        );
        let current_bid_microrubles = match product_guard {
            Err(error) => {
                tracing::warn!(campaign_id=guard.campaign_id,sku=guard.sku,%error,"product or bid corridor invalid; fail-closed stop requested");
                if let Err(error) = guard_campaign_static(
                    state,
                    state_path,
                    reader,
                    writer,
                    store,
                    write_authorization,
                    static_guard,
                    Some(spend_minor),
                    Some(revenue_minor),
                    Some("product_guard_failed"),
                )
                .await
                {
                    tracing::error!(campaign_id=guard.campaign_id,sku=guard.sku,%error,"static Ozon guard item failed");
                }
                tokio::time::sleep(Duration::from_secs(7)).await;
                return Ok(None);
            }
            Ok(bid_microrubles) => {
                tracing::debug!(
                    campaign_id = guard.campaign_id,
                    sku = guard.sku,
                    bid_microrubles,
                    "static product guard passed"
                );
                bid_microrubles
            }
        };
        Ok(Some(current_bid_microrubles))
    }

    async fn apply_dynamic_bid_control(
        &self,
        static_guard: &OzonStaticCampaignGuard,
        state: &mut StaticGuardState,
        dynamic: &OzonStaticDynamicBidControl,
        current_bid_microrubles: u64,
        spend_minor: u64,
        revenue_minor: u64,
    ) -> Result<()> {
        let Self {
            state_path,
            reader,
            writer,
            store,
            write_authorization,
            position_reader,
            observed_at,
            ..
        } = *self;
        let guard = &static_guard.guard;
        let position = match position_reader {
            Some(position_reader) => position_reader
                .latest_position(
                    &dynamic.position_store_id,
                    guard.sku,
                    &dynamic.position_region_name,
                )
                .await
                .inspect_err(|error| {
                    tracing::warn!(
                        campaign_id = guard.campaign_id,
                        sku = guard.sku,
                        %error,
                        "position unavailable; upward bid changes are held"
                    );
                })
                .ok()
                .flatten(),
            None => None,
        };
        let action = evaluate_ozon_bid_increase_after_guard(
            OzonBidPacingPolicy {
                min_bid_microrubles: static_guard.min_cpc_bid_microrubles,
                max_bid_microrubles: static_guard.max_cpc_bid_microrubles,
                bid_step_microrubles: dynamic.bid_step_microrubles,
                spend_cap_microrubles: guard.spend_cap_microrubles,
                target_drr_percent: guard.target_drr_percent,
                target_position: dynamic.target_position,
                cooldown_seconds: dynamic.cooldown_seconds,
                max_position_age_seconds: dynamic.max_position_age_seconds,
            },
            OzonBidPacingObservation {
                observed_at,
                current_bid_microrubles,
                spend_minor,
                attributed_revenue_minor: revenue_minor,
                position,
                last_bid_change_at: state.last_bid_change_at.get(&guard.campaign_id).copied(),
            },
        )?;
        match action {
            OzonBidPacingAdjustment::Hold(reason) => {
                tracing::info!(
                    campaign_id = guard.campaign_id,
                    sku = guard.sku,
                    spend_minor,
                    revenue_minor,
                    reason = reason.as_str(),
                    "dynamic Ozon bid hold"
                );
            }
            OzonBidPacingAdjustment::ChangeBid {
                from_microrubles,
                to_microrubles,
            } => {
                if let Err(error) = change_static_campaign_bid(
                    state,
                    state_path,
                    reader,
                    writer,
                    store,
                    write_authorization,
                    static_guard,
                    from_microrubles,
                    to_microrubles,
                    observed_at,
                )
                .await
                {
                    tracing::error!(campaign_id=guard.campaign_id,sku=guard.sku,%error,"dynamic Ozon bid change failed");
                }
            }
        }
        Ok(())
    }
}
