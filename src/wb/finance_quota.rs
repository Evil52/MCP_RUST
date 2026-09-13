//! Finance quota promotion requires both local token classification and an
//! observed successful official Finance response. It is never an MCP argument.

use std::time::{Duration, Instant};

use jsonwebtoken::dangerous::insecure_decode;
use serde::Deserialize;
use tokio::sync::Mutex;

use super::{PacingGate, WbClient, WbError};

const PERSONAL_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct FinanceAccessState {
    personal_read_verified: bool,
    server_cooldown_until: Instant,
}

#[derive(Debug)]
pub(super) struct FinanceAccess {
    state: Mutex<FinanceAccessState>,
}

#[derive(Deserialize)]
struct CapabilityHints {
    acc: u8,
    s: u64,
    exp: i64,
}

impl FinanceAccess {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(FinanceAccessState {
                personal_read_verified: false,
                server_cooldown_until: Instant::now(),
            }),
        }
    }

    pub(super) async fn interval(&self, conservative: Duration) -> Duration {
        if self.state.lock().await.personal_read_verified {
            conservative.min(PERSONAL_INTERVAL)
        } else {
            conservative
        }
    }

    /// Called only after an allowlisted Finance HTTP 200/204 response. JWT
    /// decoding alone is not proof of server authorization or subscription.
    pub(super) async fn confirm_read(
        &self,
        token: &str,
        gate: &PacingGate,
        conservative: Duration,
    ) {
        let Ok(decoded) = insecure_decode::<CapabilityHints>(token) else {
            return;
        };
        let hints = decoded.claims;
        if hints.acc != 3
            || hints.s & (1 << 30) == 0
            || hints.s & (1 << 13) == 0
            || hints.exp <= chrono::Utc::now().timestamp()
        {
            return;
        }
        let mut state = self.state.lock().await;
        if state.personal_read_verified {
            return;
        }
        state.personal_read_verified = true;
        // This only replaces the initial conservative departure reservation.
        // The server cooldown is separate and cannot be lost during promotion.
        let mut next_allowed = gate.next_allowed.lock().await;
        *next_allowed =
            (Instant::now() + conservative.min(PERSONAL_INTERVAL)).max(state.server_cooldown_until);
    }

    pub(super) async fn extend_cooldown(&self, gate: &PacingGate, delay: Duration) {
        let mut state = self.state.lock().await;
        state.server_cooldown_until = state.server_cooldown_until.max(Instant::now() + delay);
        // Keep the state lock through the gate change; a concurrent successful
        // response must never overwrite a newly received Retry-After.
        gate.extend_cooldown(delay).await;
        drop(state);
    }
}

impl WbClient {
    /// Current in-process Finance pacing interval. A restart intentionally
    /// forgets the access proof; durable collector quotas remain authoritative.
    pub async fn financial_report_min_interval(&self, account: &str) -> Result<Duration, WbError> {
        let limiter = self
            .limiters
            .get(account)
            .ok_or_else(|| WbError::MissingCredentials(account.to_owned()))?;
        Ok(limiter
            .finance_access
            .interval(self.policy.finance_interval)
            .await)
    }
}

#[cfg(test)]
mod tests;
