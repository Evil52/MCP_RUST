use crate::{
    reporting::{checkpoint::CheckpointError, wb_adapter::WbReportParseError},
    wb::WbErrorKind,
};
use thiserror::Error;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum WbReportSourceError {
    #[error("collection quota requires a pause")]
    RetryAfter { seconds: u64 },
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error("Wildberries daily-report source request failed")]
    Upstream(WbErrorKind),
    #[error("Wildberries daily-report source response is invalid")]
    InvalidResponse,
    #[error("Wildberries sales response is invalid")]
    InvalidSalesResponse,
    #[error("Wildberries sales pages overlap")]
    SalesPageOverlap,
    #[error("Wildberries stock response is invalid")]
    InvalidStockResponse,
    #[error("Wildberries seller inventory does not cover every requested size")]
    SellerStockCoverageIncomplete,
    #[error("Wildberries price response is invalid")]
    InvalidPriceResponse,
    #[error("Wildberries campaign response is invalid")]
    InvalidCampaignResponse,
    #[error("Wildberries campaign inventory exceeds the bounded collection capacity")]
    CampaignInventoryLimit,
    #[error("Wildberries promotion statistics response is invalid")]
    InvalidPromotionResponse,
    #[error("Wildberries promotion counters are inconsistent")]
    InconsistentPromotionCounts,
    #[error("Wildberries daily-report source pagination exceeded its fixed bound")]
    PaginationLimit,
    #[error("Wildberries daily-report snapshot input is invalid")]
    InvalidSnapshotInput,
}

impl WbReportSourceError {
    #[must_use]
    pub const fn failure(&self) -> crate::reporting::source_collection::SourceFailure {
        crate::reporting::source_collection::SourceFailure {
            code: self.code(),
            retry_after: match self {
                Self::RetryAfter { seconds } => Some(*seconds),
                _ => None,
            },
        }
    }
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::RetryAfter { .. } => "rate_limited",
            Self::Checkpoint(error) => error.code(),
            Self::Upstream(kind) => kind.code(),
            Self::InvalidResponse => "invalid_response",
            Self::InvalidSalesResponse => "invalid_sales_response",
            Self::SalesPageOverlap => "sales_page_overlap",
            Self::InvalidStockResponse => "invalid_stock_response",
            Self::SellerStockCoverageIncomplete => "seller_stock_coverage_incomplete",
            Self::InvalidPriceResponse => "invalid_price_response",
            Self::InvalidCampaignResponse => "invalid_campaign_response",
            Self::CampaignInventoryLimit => "campaign_inventory_limit",
            Self::InvalidPromotionResponse => "invalid_promotion_response",
            Self::InconsistentPromotionCounts => "promotion_counts_inconsistent",
            Self::PaginationLimit => "pagination_limit",
            Self::InvalidSnapshotInput => "invalid_snapshot_input",
        }
    }
}

impl From<WbReportParseError> for WbReportSourceError {
    fn from(_: WbReportParseError) -> Self {
        Self::InvalidResponse
    }
}
