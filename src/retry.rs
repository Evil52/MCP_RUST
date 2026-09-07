//! Bounded exponential backoff shared by the outbound marketplace clients.
//!
//! Ozon and Wildberries previously carried byte-identical backoff arithmetic,
//! each reading its own module constants. The duplicated arithmetic is the part
//! worth sharing; the numbers themselves are deliberately different and stay
//! per-vendor. Ozon caps total retry overhead at five seconds, while
//! Wildberries retries inside a sixty-second logical request deadline, so a
//! single set of constants would break one of them.

use std::time::Duration;

/// How often a transient failure may be retried, and how long to wait between
/// attempts.
///
/// Holding the budget in a value rather than in module constants makes the
/// per-vendor difference a stated policy instead of a property of which file
/// the code happens to live in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    max_attempts: usize,
    base_delay: Duration,
    max_delay: Duration,
}

impl RetryPolicy {
    pub const fn new(max_attempts: usize, base_delay: Duration, max_delay: Duration) -> Self {
        Self {
            max_attempts,
            base_delay,
            max_delay,
        }
    }

    /// The longest vendor-supplied `Retry-After` this policy will honor.
    /// A longer cooldown is refused rather than slept through, so a caller
    /// fails inside its own deadline instead of stalling on it.
    pub const fn max_delay(self) -> Duration {
        self.max_delay
    }

    /// Whether another attempt is allowed once `attempt` attempts have failed.
    pub const fn allows_attempt(self, attempt: usize) -> bool {
        attempt < self.max_attempts
    }

    /// A vendor-supplied delay is honored as given; otherwise the wait doubles
    /// from the base delay and is capped at the policy maximum. The shift is
    /// clamped so a long-lived caller cannot overflow it.
    pub fn delay(self, attempt: usize, server_delay: Option<Duration>) -> Duration {
        server_delay.unwrap_or_else(|| {
            self.base_delay
                .saturating_mul(1_u32 << attempt.saturating_sub(1).min(8))
                .min(self.max_delay)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::RetryPolicy;
    use std::time::Duration;

    const POLICY: RetryPolicy =
        RetryPolicy::new(3, Duration::from_millis(100), Duration::from_secs(5));

    #[test]
    fn a_vendor_supplied_delay_is_honored_verbatim() {
        assert_eq!(
            POLICY.delay(1, Some(Duration::from_secs(2))),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn a_missing_vendor_delay_doubles_from_the_base_and_is_capped() {
        assert_eq!(POLICY.delay(1, None), Duration::from_millis(100));
        assert_eq!(POLICY.delay(2, None), Duration::from_millis(200));
        assert_eq!(POLICY.delay(3, None), Duration::from_millis(400));
        // The cap holds well past the point where doubling would exceed it.
        assert_eq!(POLICY.delay(64, None), Duration::from_secs(5));
    }

    #[test]
    fn attempt_zero_does_not_underflow_the_shift() {
        assert_eq!(POLICY.delay(0, None), Duration::from_millis(100));
    }

    #[test]
    fn attempts_are_allowed_only_below_the_configured_maximum() {
        assert!(POLICY.allows_attempt(0));
        assert!(POLICY.allows_attempt(2));
        assert!(!POLICY.allows_attempt(3));
        assert!(!POLICY.allows_attempt(4));
    }
}
