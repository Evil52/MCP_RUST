use reqwest::StatusCode;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::policy::WbBidPlacement;
#[cfg(test)]
use super::policy::{BidLimits, WbPromotionBidTargetPolicy};

pub use client::WbBidWriteClient;
#[cfg(test)]
use client::{
    MAX_ERROR_RESPONSE_BYTES, MAX_REQUEST_ID_BYTES, MIN_CREATE_INTERVAL, MIN_WRITE_INTERVAL,
    WritePacer, validate_create_campaign_request, validate_write_request,
};
pub(super) use snapshot::{campaign_snapshot, prepare_changes, snapshot_matches_plan_state};
#[cfg(test)]
use snapshot::{snapshot_matches_expected, validate_bid_delta};

mod client;
mod snapshot;

const MAX_CHANGES: usize = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WbCampaignBidType {
    Manual,
    Unified,
}

impl WbCampaignBidType {
    #[must_use]
    pub const fn as_api_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Unified => "unified",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WbCampaignPaymentType {
    Cpm,
    Cpc,
}

impl WbCampaignPaymentType {
    #[must_use]
    pub const fn as_api_str(self) -> &'static str {
        match self {
            Self::Cpm => "cpm",
            Self::Cpc => "cpc",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbCreateCampaignRequest {
    pub name: String,
    pub nm_ids: Vec<u64>,
    pub bid_type: WbCampaignBidType,
    pub payment_type: WbCampaignPaymentType,
    /// Manual campaigns require one or both concrete placements. Unified
    /// campaigns require an empty list because WB fixes both placements.
    pub placement_types: Vec<WbBidPlacement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbBidChange {
    pub nm_id: u64,
    pub placement: WbBidPlacement,
    pub bid_kopecks: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbPreparedBidChange {
    pub nm_id: u64,
    pub placement: WbBidPlacement,
    pub before_bid_kopecks: u64,
    pub bid_kopecks: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct WbCampaignBidSnapshot {
    pub(super) seller_sid: String,
    pub(super) advert_id: u64,
    pub(super) status: i32,
    pub(super) bid_type: String,
    pub(super) payment_type: String,
    pub(super) bids: Vec<WbSnapshotBid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct WbSnapshotBid {
    pub(super) nm_id: u64,
    pub(super) placement: WbBidPlacement,
    pub(super) bid_kopecks: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WbWriteOutcomeKind {
    DefiniteFailure,
    Ambiguous,
}

#[derive(Debug)]
pub(super) enum WbGuardedWriteError<E> {
    Permit(E),
    Write(WbWriteError),
}

#[derive(Error, Debug)]
pub(super) enum WbWriteError {
    #[error("некорректный WB bid write request: {0}")]
    InvalidRequest(&'static str),
    #[error("WB вернул HTTP {status} после отправки bid write (request-id: {request_id:?})")]
    HttpStatus {
        status: StatusCode,
        request_id: Option<String>,
    },
    #[error("результат WB bid write неоднозначен ({reason}, request-id: {request_id:?})")]
    Ambiguous {
        reason: &'static str,
        request_id: Option<String>,
    },
}

impl WbWriteError {
    pub(super) const fn outcome_kind(&self) -> WbWriteOutcomeKind {
        match self {
            Self::InvalidRequest(_) => WbWriteOutcomeKind::DefiniteFailure,
            // Once request bytes may have reached WB, an HTTP status alone is
            // not evidence that a batch had no partial/late effect.
            Self::HttpStatus { .. } | Self::Ambiguous { .. } => WbWriteOutcomeKind::Ambiguous,
        }
    }

    #[cfg(test)]
    // The nested option is intentional test introspection: the outer layer
    // identifies the HttpStatus variant, while the inner layer preserves a
    // present or absent upstream request id.
    #[allow(clippy::option_option)]
    fn http_status_request_id(&self) -> Option<Option<&str>> {
        match self {
            Self::HttpStatus { request_id, .. } => Some(request_id.as_deref()),
            Self::InvalidRequest(_) | Self::Ambiguous { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests;
