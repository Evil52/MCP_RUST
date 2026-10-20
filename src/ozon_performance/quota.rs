use std::time::{Duration, Instant};

use reqwest::{Response, StatusCode};
use tokio::sync::Mutex;

use super::{AccountState, PerformanceClient, PerformanceError, TOKEN_FAILURE_COOLDOWN};
use crate::marketplace_quota::{QuotaKey, SharedQuota};

// Match the shared-quota ceiling so malformed headers cannot disable a
// credential indefinitely. The original response error retains the full delay.
const MAX_LOCAL_COOLDOWN: Duration = Duration::from_hours(24);

/// A per-account gate shared by client clones. Shorter responses never reduce
/// an active delay; untrusted vendor seconds are bounded before being stored.
#[derive(Debug, Default)]
pub(super) struct ResponseCooldown(Mutex<Option<(Instant, Duration)>>);

impl ResponseCooldown {
    pub(super) async fn check(&self) -> Result<(), PerformanceError> {
        let cooldown = *self.0.lock().await;
        if let Some((received, delay)) = cooldown {
            let remaining = delay.saturating_sub(received.elapsed());
            if !remaining.is_zero() {
                return Err(PerformanceError::RateLimited {
                    request_id: None,
                    retry_after: Some(remaining),
                });
            }
        }
        Ok(())
    }

    async fn extend(&self, delay: Duration) {
        let delay = delay.min(MAX_LOCAL_COOLDOWN);
        let mut cooldown = self.0.lock().await;
        let remaining = cooldown
            .as_ref()
            .map_or(Duration::ZERO, |(received, delay)| {
                delay.saturating_sub(received.elapsed())
            });
        if delay > remaining {
            *cooldown = Some((Instant::now(), delay));
        }
    }
}

impl AccountState {
    fn response_cooldown(&self, bucket: &str) -> &ResponseCooldown {
        if bucket == "oauth" {
            &self.oauth_cooldown
        } else {
            &self.api_cooldown
        }
    }
}

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
        state.response_cooldown(bucket).check().await?;
        if self.shared_quota.is_enabled() {
            self.shared_quota
                .admit(
                    &QuotaKey::ozon_performance(&state.credentials.client_id, bucket)?,
                    interval,
                )
                .await?;
        }
        // A sibling response may have extended the local delay while the
        // deployment quota admission was in progress.
        state.response_cooldown(bucket).check().await
    }

    pub(super) async fn defer_shared_response(
        &self,
        state: &AccountState,
        bucket: &str,
        response: &Response,
    ) -> Result<(), PerformanceError> {
        if let Some(delay) = response_cooldown(
            response.status(),
            crate::ozon::retry_after_duration(response.headers()),
        ) {
            // Install locally before any fallible/awaiting shared-quota work.
            state.response_cooldown(bucket).extend(delay).await;
            if self.shared_quota.is_enabled() {
                self.shared_quota
                    .defer(
                        &QuotaKey::ozon_performance(&state.credentials.client_id, bucket)?,
                        delay,
                    )
                    .await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Duration, MAX_LOCAL_COOLDOWN, PerformanceError, ResponseCooldown, StatusCode,
        response_cooldown,
    };

    #[tokio::test]
    async fn local_cooldown_is_monotonic_bounded_and_eventually_expires() {
        let cooldown = ResponseCooldown::default();
        cooldown.check().await.unwrap();
        cooldown.extend(Duration::from_secs(81)).await;
        cooldown.extend(Duration::ZERO).await;
        cooldown.extend(Duration::from_secs(1)).await;
        assert!(matches!(
            cooldown.check().await,
            Err(PerformanceError::RateLimited { retry_after: Some(delay), .. })
                if delay > Duration::from_secs(75)
        ));
        cooldown.extend(Duration::from_secs(u64::MAX)).await;
        assert!(matches!(
            cooldown.check().await,
            Err(PerformanceError::RateLimited { retry_after: Some(delay), .. })
                if delay <= MAX_LOCAL_COOLDOWN && delay > Duration::from_hours(23)
        ));
        let received = std::time::Instant::now()
            .checked_sub(Duration::from_secs(2))
            .unwrap();
        *cooldown.0.lock().await = Some((received, Duration::from_secs(1)));
        cooldown.check().await.unwrap();
    }

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
