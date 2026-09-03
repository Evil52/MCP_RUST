use chrono::{DateTime, Duration, Utc};
use thiserror::Error;

const MICRORUBLES_PER_RUBLE: u64 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OzonBidPacingPolicy {
    pub min_bid_microrubles: u64,
    pub max_bid_microrubles: u64,
    pub bid_step_microrubles: u64,
    pub spend_cap_microrubles: u64,
    pub target_drr_percent: u8,
    pub target_position: u16,
    pub cooldown_seconds: u64,
    pub max_position_age_seconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OzonBidPacingObservation {
    pub observed_at: DateTime<Utc>,
    pub current_bid_microrubles: u64,
    pub spend_minor: u64,
    pub attributed_revenue_minor: u64,
    pub position: Option<OzonPositionSignal>,
    pub last_bid_change_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OzonPositionSignal {
    pub observed_at: DateTime<Utc>,
    pub position: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OzonBidPacingAction {
    Hold(OzonBidPacingHoldReason),
    ChangeBid {
        from_microrubles: u64,
        to_microrubles: u64,
    },
    Pause(OzonBidPacingPauseReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OzonBidPacingHoldReason {
    Cooldown,
    PositionUnavailable,
    PositionStale,
    TargetPositionReached,
    BidCeilingReached,
}

impl OzonBidPacingHoldReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cooldown => "cooldown",
            Self::PositionUnavailable => "position_unavailable",
            Self::PositionStale => "position_stale",
            Self::TargetPositionReached => "target_position_reached",
            Self::BidCeilingReached => "bid_ceiling_reached",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OzonBidPacingPauseReason {
    SpendCapReached,
    DrrExceededAtBidFloor,
}

impl OzonBidPacingPauseReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SpendCapReached => "spend_cap_reached",
            Self::DrrExceededAtBidFloor => "drr_exceeded_at_bid_floor",
        }
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum OzonBidPacingError {
    #[error("Ozon bid pacing policy is invalid")]
    InvalidPolicy,
    #[error("Ozon bid pacing observation is invalid")]
    InvalidObservation,
}

pub fn evaluate_ozon_bid_pacing(
    policy: OzonBidPacingPolicy,
    observation: OzonBidPacingObservation,
) -> Result<OzonBidPacingAction, OzonBidPacingError> {
    validate_policy(policy)?;
    if !(policy.min_bid_microrubles..=policy.max_bid_microrubles)
        .contains(&observation.current_bid_microrubles)
    {
        return Err(OzonBidPacingError::InvalidObservation);
    }

    let spend_cap_minor = policy.spend_cap_microrubles / 10_000;
    if observation.spend_minor >= spend_cap_minor {
        return Ok(OzonBidPacingAction::Pause(
            OzonBidPacingPauseReason::SpendCapReached,
        ));
    }

    let drr_exceeded = observation.attributed_revenue_minor > 0
        && u128::from(observation.spend_minor) * 100
            > u128::from(observation.attributed_revenue_minor)
                * u128::from(policy.target_drr_percent);
    if drr_exceeded {
        if observation.current_bid_microrubles == policy.min_bid_microrubles {
            return Ok(OzonBidPacingAction::Pause(
                OzonBidPacingPauseReason::DrrExceededAtBidFloor,
            ));
        }
        if cooldown_active(policy, observation)? {
            return Ok(OzonBidPacingAction::Hold(OzonBidPacingHoldReason::Cooldown));
        }
        return Ok(OzonBidPacingAction::ChangeBid {
            from_microrubles: observation.current_bid_microrubles,
            to_microrubles: observation
                .current_bid_microrubles
                .saturating_sub(policy.bid_step_microrubles)
                .max(policy.min_bid_microrubles),
        });
    }

    let Some(position) = observation.position else {
        return Ok(OzonBidPacingAction::Hold(
            OzonBidPacingHoldReason::PositionUnavailable,
        ));
    };
    let age = observation
        .observed_at
        .signed_duration_since(position.observed_at);
    let max_age = Duration::seconds(
        i64::try_from(policy.max_position_age_seconds)
            .map_err(|_| OzonBidPacingError::InvalidPolicy)?,
    );
    if age < Duration::zero() || age > max_age {
        return Ok(OzonBidPacingAction::Hold(
            OzonBidPacingHoldReason::PositionStale,
        ));
    }
    if position.position <= policy.target_position {
        return Ok(OzonBidPacingAction::Hold(
            OzonBidPacingHoldReason::TargetPositionReached,
        ));
    }
    if observation.current_bid_microrubles == policy.max_bid_microrubles {
        return Ok(OzonBidPacingAction::Hold(
            OzonBidPacingHoldReason::BidCeilingReached,
        ));
    }
    if cooldown_active(policy, observation)? {
        return Ok(OzonBidPacingAction::Hold(OzonBidPacingHoldReason::Cooldown));
    }
    Ok(OzonBidPacingAction::ChangeBid {
        from_microrubles: observation.current_bid_microrubles,
        to_microrubles: observation
            .current_bid_microrubles
            .checked_add(policy.bid_step_microrubles)
            .ok_or(OzonBidPacingError::InvalidObservation)?
            .min(policy.max_bid_microrubles),
    })
}

fn validate_policy(policy: OzonBidPacingPolicy) -> Result<(), OzonBidPacingError> {
    if policy.min_bid_microrubles == 0
        || policy.min_bid_microrubles > policy.max_bid_microrubles
        || policy.bid_step_microrubles == 0
        || !policy
            .min_bid_microrubles
            .is_multiple_of(MICRORUBLES_PER_RUBLE)
        || !policy
            .max_bid_microrubles
            .is_multiple_of(MICRORUBLES_PER_RUBLE)
        || !policy
            .bid_step_microrubles
            .is_multiple_of(MICRORUBLES_PER_RUBLE)
        || policy.spend_cap_microrubles == 0
        || !policy.spend_cap_microrubles.is_multiple_of(10_000)
        || !(10..=100).contains(&policy.target_drr_percent)
        || policy.target_position == 0
        || policy.cooldown_seconds == 0
        || policy.max_position_age_seconds < policy.cooldown_seconds
    {
        return Err(OzonBidPacingError::InvalidPolicy);
    }
    Ok(())
}

fn cooldown_active(
    policy: OzonBidPacingPolicy,
    observation: OzonBidPacingObservation,
) -> Result<bool, OzonBidPacingError> {
    let Some(last_change) = observation.last_bid_change_at else {
        return Ok(false);
    };
    let age = observation.observed_at.signed_duration_since(last_change);
    if age < Duration::zero() {
        return Err(OzonBidPacingError::InvalidObservation);
    }
    let cooldown = Duration::seconds(
        i64::try_from(policy.cooldown_seconds).map_err(|_| OzonBidPacingError::InvalidPolicy)?,
    );
    Ok(age < cooldown)
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};

    use super::*;

    fn policy() -> OzonBidPacingPolicy {
        OzonBidPacingPolicy {
            min_bid_microrubles: 7_000_000,
            max_bid_microrubles: 12_000_000,
            bid_step_microrubles: 1_000_000,
            spend_cap_microrubles: 2_000_000_000,
            target_drr_percent: 15,
            target_position: 30,
            cooldown_seconds: 1_800,
            max_position_age_seconds: 2_700,
        }
    }

    fn observation() -> OzonBidPacingObservation {
        let observed_at = Utc.with_ymd_and_hms(2026, 9, 3, 7, 0, 0).unwrap();
        OzonBidPacingObservation {
            observed_at,
            current_bid_microrubles: 7_000_000,
            spend_minor: 10_000,
            attributed_revenue_minor: 100_000,
            position: Some(OzonPositionSignal {
                observed_at: observed_at - Duration::minutes(5),
                position: 31,
            }),
            last_bid_change_at: None,
        }
    }

    #[test]
    fn action_reasons_have_stable_audit_labels() {
        for (reason, expected) in [
            (OzonBidPacingHoldReason::Cooldown, "cooldown"),
            (
                OzonBidPacingHoldReason::PositionUnavailable,
                "position_unavailable",
            ),
            (OzonBidPacingHoldReason::PositionStale, "position_stale"),
            (
                OzonBidPacingHoldReason::TargetPositionReached,
                "target_position_reached",
            ),
            (
                OzonBidPacingHoldReason::BidCeilingReached,
                "bid_ceiling_reached",
            ),
        ] {
            assert_eq!(reason.as_str(), expected);
        }
        assert_eq!(
            OzonBidPacingPauseReason::SpendCapReached.as_str(),
            "spend_cap_reached"
        );
        assert_eq!(
            OzonBidPacingPauseReason::DrrExceededAtBidFloor.as_str(),
            "drr_exceeded_at_bid_floor"
        );
    }

    #[test]
    fn bad_position_raises_one_ruble_and_never_crosses_ceiling() {
        assert_eq!(
            evaluate_ozon_bid_pacing(policy(), observation()),
            Ok(OzonBidPacingAction::ChangeBid {
                from_microrubles: 7_000_000,
                to_microrubles: 8_000_000,
            })
        );
        let at_ceiling = OzonBidPacingObservation {
            current_bid_microrubles: 12_000_000,
            ..observation()
        };
        assert_eq!(
            evaluate_ozon_bid_pacing(policy(), at_ceiling),
            Ok(OzonBidPacingAction::Hold(
                OzonBidPacingHoldReason::BidCeilingReached
            ))
        );
    }

    #[test]
    fn target_position_holds_and_unavailable_or_stale_positions_never_raise() {
        let reached = OzonBidPacingObservation {
            position: Some(OzonPositionSignal {
                position: 30,
                ..observation().position.unwrap()
            }),
            ..observation()
        };
        assert_eq!(
            evaluate_ozon_bid_pacing(policy(), reached),
            Ok(OzonBidPacingAction::Hold(
                OzonBidPacingHoldReason::TargetPositionReached
            ))
        );
        assert_eq!(
            evaluate_ozon_bid_pacing(
                policy(),
                OzonBidPacingObservation {
                    position: None,
                    ..observation()
                }
            ),
            Ok(OzonBidPacingAction::Hold(
                OzonBidPacingHoldReason::PositionUnavailable
            ))
        );
        let stale = OzonBidPacingObservation {
            position: Some(OzonPositionSignal {
                observed_at: observation().observed_at - Duration::minutes(46),
                position: 31,
            }),
            ..observation()
        };
        assert_eq!(
            evaluate_ozon_bid_pacing(policy(), stale),
            Ok(OzonBidPacingAction::Hold(
                OzonBidPacingHoldReason::PositionStale
            ))
        );
    }

    #[test]
    fn high_drr_reduces_to_floor_then_pauses() {
        let high_drr = OzonBidPacingObservation {
            current_bid_microrubles: 9_000_000,
            spend_minor: 15_001,
            attributed_revenue_minor: 100_000,
            position: None,
            ..observation()
        };
        assert_eq!(
            evaluate_ozon_bid_pacing(policy(), high_drr),
            Ok(OzonBidPacingAction::ChangeBid {
                from_microrubles: 9_000_000,
                to_microrubles: 8_000_000,
            })
        );
        assert_eq!(
            evaluate_ozon_bid_pacing(
                policy(),
                OzonBidPacingObservation {
                    current_bid_microrubles: 7_000_000,
                    ..high_drr
                }
            ),
            Ok(OzonBidPacingAction::Pause(
                OzonBidPacingPauseReason::DrrExceededAtBidFloor
            ))
        );
    }

    #[test]
    fn cooldown_applies_to_bid_changes_but_not_protective_pauses() {
        let recent_change = observation().observed_at - Duration::minutes(29);
        let during_cooldown = OzonBidPacingObservation {
            last_bid_change_at: Some(recent_change),
            ..observation()
        };
        assert_eq!(
            evaluate_ozon_bid_pacing(policy(), during_cooldown),
            Ok(OzonBidPacingAction::Hold(OzonBidPacingHoldReason::Cooldown))
        );
        let high_drr_during_cooldown = OzonBidPacingObservation {
            current_bid_microrubles: 8_000_000,
            spend_minor: 15_001,
            attributed_revenue_minor: 100_000,
            position: None,
            last_bid_change_at: Some(recent_change),
            ..observation()
        };
        assert_eq!(
            evaluate_ozon_bid_pacing(policy(), high_drr_during_cooldown),
            Ok(OzonBidPacingAction::Hold(OzonBidPacingHoldReason::Cooldown))
        );
        let cap = OzonBidPacingObservation {
            spend_minor: 200_000,
            last_bid_change_at: Some(recent_change),
            ..observation()
        };
        assert_eq!(
            evaluate_ozon_bid_pacing(policy(), cap),
            Ok(OzonBidPacingAction::Pause(
                OzonBidPacingPauseReason::SpendCapReached
            ))
        );
        let high_drr_at_floor = OzonBidPacingObservation {
            spend_minor: 15_001,
            attributed_revenue_minor: 100_000,
            last_bid_change_at: Some(recent_change),
            position: None,
            ..observation()
        };
        assert_eq!(
            evaluate_ozon_bid_pacing(policy(), high_drr_at_floor),
            Ok(OzonBidPacingAction::Pause(
                OzonBidPacingPauseReason::DrrExceededAtBidFloor
            ))
        );
    }

    #[test]
    fn zero_revenue_waits_for_attribution_and_uses_position_signal() {
        let zero_revenue = OzonBidPacingObservation {
            spend_minor: 1,
            attributed_revenue_minor: 0,
            ..observation()
        };
        assert!(matches!(
            evaluate_ozon_bid_pacing(policy(), zero_revenue),
            Ok(OzonBidPacingAction::ChangeBid { .. })
        ));
    }

    #[test]
    fn malformed_policy_observation_and_future_times_fail_closed() {
        let invalid_policy = OzonBidPacingPolicy {
            bid_step_microrubles: 500_000,
            ..policy()
        };
        assert_eq!(
            evaluate_ozon_bid_pacing(invalid_policy, observation()),
            Err(OzonBidPacingError::InvalidPolicy)
        );
        let out_of_range = OzonBidPacingObservation {
            current_bid_microrubles: 6_000_000,
            ..observation()
        };
        assert_eq!(
            evaluate_ozon_bid_pacing(policy(), out_of_range),
            Err(OzonBidPacingError::InvalidObservation)
        );
        let future_change = OzonBidPacingObservation {
            last_bid_change_at: Some(observation().observed_at + Duration::seconds(1)),
            ..observation()
        };
        assert_eq!(
            evaluate_ozon_bid_pacing(policy(), future_change),
            Err(OzonBidPacingError::InvalidObservation)
        );
    }
}
