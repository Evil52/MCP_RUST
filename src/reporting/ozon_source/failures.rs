use super::{OzonError, OzonErrorKind, OzonReportSourceError};

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
