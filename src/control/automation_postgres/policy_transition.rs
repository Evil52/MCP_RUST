use super::PolicyTransition;

impl PolicyTransition {
    pub(super) const fn event_type(self) -> &'static str {
        match self {
            Self::ProtectiveLive => "protective_live_activated",
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
            Self::BidWrites
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
            Self::BoundedPacingActivated {
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
