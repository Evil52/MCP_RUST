use super::{
    AuditEvent, PolicyTransition, WbAutomationCampaignLease, WbAutomationPostgresError,
    WbAutomationStateTransitionReceipt, audit_event_key, ensure_one_row,
    ensure_protective_live_guard, insert_audit_event, validate_digest,
};
use crate::control::WbAutomationPolicy;

impl PolicyTransition {
    pub(super) const fn event_type(self) -> &'static str {
        match self {
            Self::ProtectiveLive => "protective_live_activated",
            Self::AuthorizedCorridorAdjusted { .. } => "authorized_corridor_adjusted",
            Self::BidWrites => "bid_writes_activated",
            Self::BoundedPacingActivated { .. } => "bounded_pacing_activated",
            Self::TrafficFrontierV2Activated { .. } => "traffic_frontier_v2_activated",
            Self::TrafficFrontierV3Activated { .. } => "traffic_frontier_v3_activated",
            Self::TrafficFrontierV4Activated { .. } => "traffic_frontier_v4_activated",
            Self::TrafficFrontierLimitsRaised { .. } => "traffic_frontier_limits_raised",
            Self::TrafficFrontierCorridorTightened { .. } => "traffic_frontier_corridor_tightened",
            Self::TrafficFrontierV4CorridorAdjusted { .. } => {
                "traffic_frontier_v4_corridor_adjusted"
            }
        }
    }

    pub(super) const fn mode(self) -> &'static str {
        match self {
            Self::ProtectiveLive => "protective_live",
            Self::AuthorizedCorridorAdjusted { .. }
            | Self::BidWrites
            | Self::BoundedPacingActivated { .. }
            | Self::TrafficFrontierV2Activated { .. }
            | Self::TrafficFrontierV3Activated { .. }
            | Self::TrafficFrontierV4Activated { .. }
            | Self::TrafficFrontierLimitsRaised { .. }
            | Self::TrafficFrontierCorridorTightened { .. }
            | Self::TrafficFrontierV4CorridorAdjusted { .. } => "bid_live",
        }
    }

    pub(super) const fn bid_writes_enabled(self) -> bool {
        !matches!(self, Self::ProtectiveLive)
    }

    pub(super) const fn max_bid_change(self) -> Option<(u64, u64)> {
        match self {
            Self::AuthorizedCorridorAdjusted {
                from_max_bid_kopecks,
                to_max_bid_kopecks,
                ..
            }
            | Self::BoundedPacingActivated {
                from_max_bid_kopecks,
                to_max_bid_kopecks,
                ..
            }
            | Self::TrafficFrontierV2Activated {
                from_max_bid_kopecks,
                to_max_bid_kopecks,
                ..
            }
            | Self::TrafficFrontierCorridorTightened {
                from_max_bid_kopecks,
                to_max_bid_kopecks,
                ..
            }
            | Self::TrafficFrontierV4CorridorAdjusted {
                from_max_bid_kopecks,
                to_max_bid_kopecks,
                ..
            } => Some((from_max_bid_kopecks, to_max_bid_kopecks)),
            _ => None,
        }
    }

    pub(super) const fn target_impressions_per_day(self) -> Option<u64> {
        match self {
            Self::BoundedPacingActivated {
                target_impressions_per_day,
                ..
            }
            | Self::TrafficFrontierV3Activated {
                target_impressions_per_day,
                ..
            }
            | Self::TrafficFrontierV4Activated {
                target_impressions_per_day,
                ..
            } => Some(target_impressions_per_day),
            _ => None,
        }
    }
}

impl WbAutomationCampaignLease<'_> {
    pub(super) async fn activate_policy_transition(
        &mut self,
        source_policy_digest: &str,
        target_policy_digest: &str,
        transition: PolicyTransition,
    ) -> Result<WbAutomationStateTransitionReceipt, WbAutomationPostgresError> {
        self.activate_policy_transition_with_authorization(
            source_policy_digest,
            target_policy_digest,
            transition,
            None,
        )
        .await
    }

    pub(super) async fn activate_policy_transition_with_authorization(
        &mut self,
        source_policy_digest: &str,
        target_policy_digest: &str,
        transition: PolicyTransition,
        authorization: Option<&WbAutomationPolicy>,
    ) -> Result<WbAutomationStateTransitionReceipt, WbAutomationPostgresError> {
        validate_digest(source_policy_digest)?;
        validate_digest(target_policy_digest)?;
        if source_policy_digest == target_policy_digest {
            return Err(WbAutomationPostgresError::InvalidInput);
        }
        let client = self
            .client
            .as_mut()
            .ok_or(WbAutomationPostgresError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        let state = transaction
            .query_opt(
                "SELECT policy_digest, pending_idempotency_key, incident_class, revision \
                 FROM wb_automation.execution_state \
                 WHERE account_id=$1 AND advert_id=$2 FOR UPDATE",
                &[&self.account_id, &self.campaign_id],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?
            .ok_or(WbAutomationPostgresError::StateChanged)?;
        let current_digest = state.get::<_, &str>(0);
        let pending = state.get::<_, Option<&str>>(1);
        let incident = state.get::<_, Option<&str>>(2);
        let revision = state.get::<_, i64>(3);
        ensure_protective_live_guard(pending.is_none() && incident.is_none() && revision > 0)?;
        if current_digest == target_policy_digest {
            let state_revision =
                u64::try_from(revision).map_err(|_| WbAutomationPostgresError::StateChanged)?;
            transaction
                .commit()
                .await
                .map_err(|_| WbAutomationPostgresError::Unavailable)?;
            return Ok(WbAutomationStateTransitionReceipt {
                changed: false,
                state_revision,
            });
        }
        if current_digest != source_policy_digest {
            return Err(WbAutomationPostgresError::StateChanged);
        }
        let unresolved = transaction
            .query_one(
                "SELECT EXISTS (\
                    SELECT 1 FROM wb_automation.action_attempts \
                    WHERE account_id=$1 AND advert_id=$2 \
                      AND status IN ('reserved','write_started','awaiting_readback','reconciliation_required')\
                 )",
                &[&self.account_id, &self.campaign_id],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?
            .get::<_, bool>(0);
        ensure_protective_live_guard(!unresolved)?;
        let cycle_id = transaction
            .query_opt(
                "SELECT cycle_id FROM wb_automation.cycles \
                 WHERE account_id=$1 AND advert_id=$2 AND policy_digest=$3 \
                 ORDER BY observed_at DESC LIMIT 1",
                &[&self.account_id, &self.campaign_id, &source_policy_digest],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?
            .map(|row| row.get::<_, String>(0))
            .ok_or(WbAutomationPostgresError::StateChanged)?;
        let next_revision = revision
            .checked_add(1)
            .ok_or(WbAutomationPostgresError::StateChanged)?;
        let updated = transaction
            .execute(
                "UPDATE wb_automation.execution_state \
                 SET policy_digest=$3, revision=$4 \
                 WHERE account_id=$1 AND advert_id=$2 AND policy_digest=$5 \
                   AND revision=$6 AND pending_idempotency_key IS NULL \
                   AND incident_class IS NULL",
                &[
                    &self.account_id,
                    &self.campaign_id,
                    &target_policy_digest,
                    &next_revision,
                    &source_policy_digest,
                    &revision,
                ],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        ensure_one_row(updated)?;
        let event_key = audit_event_key(
            source_policy_digest,
            transition.event_type(),
            target_policy_digest,
        );
        let mut payload = serde_json::json!({
            "from_policy_sha256": source_policy_digest,
            "to_policy_sha256": target_policy_digest,
            "mode": transition.mode(),
            "bid_writes_enabled": transition.bid_writes_enabled(),
            "state_revision": next_revision,
        });
        if let Some((from_max_bid_kopecks, to_max_bid_kopecks)) = transition.max_bid_change() {
            payload["from_max_bid_kopecks"] = from_max_bid_kopecks.into();
            payload["to_max_bid_kopecks"] = to_max_bid_kopecks.into();
        }
        if let Some(target_impressions_per_day) = transition.target_impressions_per_day() {
            payload["target_impressions_per_day"] = target_impressions_per_day.into();
            payload["autonomous_pacing_enabled"] = true.into();
        }
        if let PolicyTransition::TrafficFrontierV2Activated {
            frontier_bid_kopecks,
            max_actions_per_day,
            cooldown_seconds,
            feedback_timeout_seconds,
            ..
        } = transition
        {
            payload["autonomous_pacing"] = "traffic_frontier_v2".into();
            payload["traffic_frontier_bid_kopecks"] = frontier_bid_kopecks.into();
            payload["max_actions_per_day"] = max_actions_per_day.into();
            payload["cooldown_seconds"] = cooldown_seconds.into();
            payload["feedback_timeout_seconds"] = feedback_timeout_seconds.into();
        }
        if let PolicyTransition::TrafficFrontierV3Activated {
            target_impressions_per_day,
            target_orders_per_day,
            max_actions_per_day,
            cooldown_seconds,
            feedback_timeout_seconds,
            min_feedback_impressions,
            min_feedback_clicks,
        } = transition
        {
            payload["autonomous_pacing"] = "traffic_frontier_v3".into();
            payload["target_impressions_per_day"] = target_impressions_per_day.into();
            payload["target_orders_per_day"] = target_orders_per_day.into();
            payload["max_actions_per_day"] = max_actions_per_day.into();
            payload["cooldown_seconds"] = cooldown_seconds.into();
            payload["feedback_timeout_seconds"] = feedback_timeout_seconds.into();
            payload["min_feedback_impressions"] = min_feedback_impressions.into();
            payload["min_feedback_clicks"] = min_feedback_clicks.into();
        }
        if let PolicyTransition::TrafficFrontierV4Activated {
            target_drr_basis_points,
            hard_drr_basis_points,
            frontier_bid_kopecks,
            bid_step_percent,
            target_impressions_per_day,
            target_orders_per_day,
            max_actions_per_day,
            cooldown_seconds,
            feedback_timeout_seconds,
            min_feedback_impressions,
            min_feedback_clicks,
        } = transition
        {
            payload["autonomous_pacing"] = "traffic_frontier_v4".into();
            payload["target_drr_basis_points"] = target_drr_basis_points.into();
            payload["hard_drr_basis_points"] = hard_drr_basis_points.into();
            payload["traffic_frontier_bid_kopecks"] = frontier_bid_kopecks.into();
            payload["bid_step_percent"] = bid_step_percent.into();
            payload["target_impressions_per_day"] = target_impressions_per_day.into();
            payload["target_orders_per_day"] = target_orders_per_day.into();
            payload["max_actions_per_day"] = max_actions_per_day.into();
            payload["cooldown_seconds"] = cooldown_seconds.into();
            payload["feedback_timeout_seconds"] = feedback_timeout_seconds.into();
            payload["min_feedback_impressions"] = min_feedback_impressions.into();
            payload["min_feedback_clicks"] = min_feedback_clicks.into();
            payload["zero_cost_probe_enabled"] = true.into();
        }
        if let PolicyTransition::TrafficFrontierLimitsRaised {
            from_frontier_bid_kopecks,
            to_frontier_bid_kopecks,
            from_daily_pause_threshold_minor,
            to_daily_pause_threshold_minor,
            from_daily_spend_cap_minor,
            to_daily_spend_cap_minor,
        } = transition
        {
            payload["autonomous_pacing"] = "traffic_frontier_v2".into();
            payload["from_traffic_frontier_bid_kopecks"] = from_frontier_bid_kopecks.into();
            payload["to_traffic_frontier_bid_kopecks"] = to_frontier_bid_kopecks.into();
            payload["from_daily_pause_threshold_minor"] = from_daily_pause_threshold_minor.into();
            payload["to_daily_pause_threshold_minor"] = to_daily_pause_threshold_minor.into();
            payload["from_daily_spend_cap_minor"] = from_daily_spend_cap_minor.into();
            payload["to_daily_spend_cap_minor"] = to_daily_spend_cap_minor.into();
        }
        if let PolicyTransition::TrafficFrontierCorridorTightened {
            from_frontier_bid_kopecks,
            to_frontier_bid_kopecks,
            ..
        } = transition
        {
            payload["autonomous_pacing"] = "traffic_frontier_v2".into();
            payload["from_traffic_frontier_bid_kopecks"] = from_frontier_bid_kopecks.into();
            payload["to_traffic_frontier_bid_kopecks"] = to_frontier_bid_kopecks.into();
        }
        if let PolicyTransition::TrafficFrontierV4CorridorAdjusted {
            from_min_bid_kopecks,
            to_min_bid_kopecks,
            ..
        } = transition
        {
            payload["autonomous_pacing"] = "traffic_frontier_v4".into();
            payload["from_min_bid_kopecks"] = from_min_bid_kopecks.into();
            payload["to_min_bid_kopecks"] = to_min_bid_kopecks.into();
        }
        if let PolicyTransition::AuthorizedCorridorAdjusted {
            from_min_bid_kopecks,
            to_min_bid_kopecks,
            ..
        } = transition
        {
            payload["from_min_bid_kopecks"] = from_min_bid_kopecks.into();
            payload["to_min_bid_kopecks"] = to_min_bid_kopecks.into();
        }
        if let Some(policy) = authorization {
            payload["authorization_reference"] = policy.authorization_reference.clone().into();
            payload["authorized_by_actor_id"] = policy.authorized_by_actor_id.clone().into();
            payload["authorized_at"] = policy.authorized_at.to_rfc3339().into();
            payload["authorization_expires_at"] =
                policy.authorization_expires_at.to_rfc3339().into();
        }
        let payload_json = payload.to_string();
        insert_audit_event(
            &transaction,
            &AuditEvent {
                event_key: &event_key,
                cycle_id: &cycle_id,
                account_id: &self.account_id,
                campaign_id: self.campaign_id,
                event_type: transition.event_type(),
                idempotency_key: None,
                payload_json: &payload_json,
            },
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        Ok(WbAutomationStateTransitionReceipt {
            changed: true,
            state_revision: u64::try_from(next_revision)
                .map_err(|_| WbAutomationPostgresError::StateChanged)?,
        })
    }
}
