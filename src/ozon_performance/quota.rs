use std::time::Duration;

use reqwest::{Response, StatusCode};

use super::{AccountState, PerformanceClient, PerformanceError, TOKEN_FAILURE_COOLDOWN};
use crate::marketplace_quota::{QuotaKey, SharedQuota};

fn response_cooldown(status: StatusCode, vendor: Option<Duration>) -> Option<Duration> {
    match status {
        StatusCode::TOO_MANY_REQUESTS => Some(vendor.unwrap_or(TOKEN_FAILURE_COOLDOWN)),
        StatusCode::SERVICE_UNAVAILABLE => vendor,
        _ => None,
    }
}

impl PerformanceClient {
    #[must_use]
    pub fn with_shared_quota(mut self, quota: SharedQuota) -> Self {
        self.shared_quota = quota;
        self
    }

    pub(super) async fn admit_shared_request(
        &self,
        state: &AccountState,
        bucket: &str,
        interval: Duration,
    ) -> Result<(), PerformanceError> {
        if self.shared_quota.is_enabled() {
            self.shared_quota
                .admit(
                    &QuotaKey::ozon_performance(&state.credentials.client_id, bucket)?,
                    interval,
                )
                .await?;
        }
        Ok(())
    }

    pub(super) async fn defer_shared_response(
        &self,
        state: &AccountState,
        bucket: &str,
        response: &Response,
    ) -> Result<(), PerformanceError> {
        if self.shared_quota.is_enabled()
            && let Some(delay) = response_cooldown(
                response.status(),
                crate::ozon::retry_after_duration(response.headers()),
            )
        {
            self.shared_quota
                .defer(
                    &QuotaKey::ozon_performance(&state.credentials.client_id, bucket)?,
                    delay,
                )
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Duration, StatusCode, response_cooldown};

    #[test]
    fn zero_and_long_vendor_cooldowns_are_forwarded_unchanged() {
        for delay in [
            Duration::ZERO,
            Duration::from_hours(2),
            Duration::from_hours(48),
        ] {
            assert_eq!(
                response_cooldown(StatusCode::TOO_MANY_REQUESTS, Some(delay)),
                Some(delay)
            );
            assert_eq!(
                response_cooldown(StatusCode::SERVICE_UNAVAILABLE, Some(delay)),
                Some(delay)
            );
        }
        assert_eq!(
            response_cooldown(StatusCode::SERVICE_UNAVAILABLE, None),
            None
        );
    }
}
