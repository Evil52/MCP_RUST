use std::time::Duration;

use reqwest::StatusCode;

use super::{
    ANALYTICS_DATA_PATH, ANALYTICS_REQUEST_INTERVAL, OzonClient, OzonError, SHARED_REQUEST_INTERVAL,
};
use crate::marketplace_quota::{QuotaKey, SharedQuota};

pub(super) fn response_cooldown(
    status: StatusCode,
    vendor: Option<Duration>,
    local: Option<Duration>,
) -> Option<Duration> {
    match status {
        StatusCode::TOO_MANY_REQUESTS => Some(vendor.max(local).unwrap_or(Duration::from_secs(1))),
        StatusCode::SERVICE_UNAVAILABLE => vendor,
        _ => None,
    }
}

impl OzonClient {
    #[must_use]
    pub fn with_shared_quota(mut self, quota: SharedQuota) -> Self {
        self.shared_quota = quota;
        self
    }

    pub(super) async fn admit_shared_request(
        &self,
        client_id: &str,
        path: &str,
    ) -> Result<(), OzonError> {
        if !self.shared_quota.is_enabled() {
            return Ok(());
        }
        if path == ANALYTICS_DATA_PATH {
            self.shared_quota
                .admit(
                    &QuotaKey::ozon_seller(client_id, "analytics")?,
                    ANALYTICS_REQUEST_INTERVAL,
                )
                .await?;
        }
        // Generic admission must be the last await before wire dispatch. The
        // Analytics lookup can block on another process's database row lock.
        self.shared_quota
            .admit(
                &QuotaKey::ozon_seller(client_id, "api")?,
                SHARED_REQUEST_INTERVAL,
            )
            .await?;
        Ok(())
    }

    pub(super) async fn defer_shared_request(
        &self,
        client_id: &str,
        path: &str,
        delay: Duration,
    ) -> Result<(), OzonError> {
        if self.shared_quota.is_enabled() {
            let bucket = if path == ANALYTICS_DATA_PATH {
                "analytics"
            } else {
                "api"
            };
            self.shared_quota
                .defer(&QuotaKey::ozon_seller(client_id, bucket)?, delay)
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Duration, StatusCode, response_cooldown};

    #[test]
    fn cooldown_preserves_zero_and_vendor_delays_beyond_local_ceiling() {
        assert_eq!(
            response_cooldown(StatusCode::TOO_MANY_REQUESTS, Some(Duration::ZERO), None),
            Some(Duration::ZERO)
        );
        for delay in [Duration::from_hours(2), Duration::from_hours(48)] {
            assert_eq!(
                response_cooldown(
                    StatusCode::TOO_MANY_REQUESTS,
                    Some(delay),
                    Some(Duration::from_hours(1))
                ),
                Some(delay)
            );
            assert_eq!(
                response_cooldown(StatusCode::SERVICE_UNAVAILABLE, Some(delay), None),
                Some(delay)
            );
        }
        assert_eq!(
            response_cooldown(StatusCode::SERVICE_UNAVAILABLE, None, None),
            None
        );
        assert_eq!(
            response_cooldown(StatusCode::OK, Some(Duration::from_hours(2)), None),
            None
        );
    }
}
