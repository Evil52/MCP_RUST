use super::{OzonError, OzonErrorKind};
use crate::reporting::checkpoint::CheckpointError;
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum OzonReportSourceError {
    #[error("collection quota requires a pause")]
    RetryAfter { seconds: u64 },
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error("Ozon daily-report source request failed")]
    Upstream(OzonErrorKind),
    #[error("Ozon daily-report source request failed")]
    Transport,
    #[error("Ozon daily-report source response is invalid")]
    InvalidResponse,
    #[error("Ozon daily-report sales response is invalid")]
    InvalidSalesResponse { shape: String },
    #[error("Ozon sales pages overlap")]
    SalesPageOverlap,
    #[error("Ozon daily-report stocks response is invalid")]
    InvalidStocksResponse,
    #[error("Ozon daily-report prices response is invalid")]
    InvalidPricesResponse,
    #[error("Ozon daily-report finance response is invalid")]
    InvalidFinanceResponse,
    #[error("Ozon daily-report snapshot input is invalid")]
    InvalidSnapshotInput,
    #[error("Ozon daily-report source pagination exceeded its fixed bound")]
    PaginationLimit,
}

impl OzonReportSourceError {
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
    /// A stable, non-sensitive diagnostic code suitable for operator logs.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::RetryAfter { .. } => "rate_limited",
            Self::Checkpoint(error) => error.code(),
            Self::Upstream(kind) => kind.code(),
            Self::Transport => "transport_error",
            Self::InvalidResponse => "invalid_response",
            Self::InvalidSalesResponse { .. } => "invalid_sales_response",
            Self::SalesPageOverlap => "sales_page_overlap",
            Self::InvalidStocksResponse => "invalid_stocks_response",
            Self::InvalidPricesResponse => "invalid_prices_response",
            Self::InvalidFinanceResponse => "invalid_finance_response",
            Self::InvalidSnapshotInput => "invalid_snapshot_input",
            Self::PaginationLimit => "pagination_limit",
        }
    }

    /// A value-free, bounded structural fingerprint for the one sales parse
    /// failure that needs operator investigation. It never includes an Ozon
    /// response value, identifier, amount, name, or credential.
    #[must_use]
    pub fn diagnostic(&self) -> Option<&str> {
        match self {
            Self::InvalidSalesResponse { shape } => Some(shape),
            _ => None,
        }
    }
}

pub(super) fn source_failure(error: &OzonError) -> OzonReportSourceError {
    match error {
        OzonError::RateLimited {
            retry_after,
            local_cooldown,
            ..
        } => (*retry_after).max(*local_cooldown).map_or(
            OzonReportSourceError::Upstream(OzonErrorKind::RateLimited),
            |delay| OzonReportSourceError::RetryAfter {
                seconds: crate::reporting::checkpoint::delay_seconds(delay),
            },
        ),
        OzonError::LocalRateLimited { retry_after }
        | OzonError::SharedQuota(crate::marketplace_quota::QuotaError::Limited { retry_after }) => {
            OzonReportSourceError::RetryAfter {
                seconds: crate::reporting::checkpoint::delay_seconds(*retry_after),
            }
        }
        _ => OzonReportSourceError::Upstream(error.kind()),
    }
}
