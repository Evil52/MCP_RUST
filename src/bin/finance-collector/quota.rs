//! Persisted pacing and vendor backoff are distinct. Legacy unknown deadlines
//! stay intact unless a trusted operator supplies an exact successful-probe receipt.

use anyhow::{Result, ensure};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use super::valid_digest;

const CONSERVATIVE: Duration = Duration::hours(12);
const PERSONAL: Duration = Duration::seconds(60);

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Quota {
    pub(super) next_allowed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    version: u8,
    #[serde(default)]
    last_attempt_at: Option<DateTime<Utc>>,
    #[serde(default)]
    retry_after_until: Option<DateTime<Utc>>,
    #[serde(default)]
    personal_key_fingerprint: Option<String>,
    #[serde(default)]
    legacy_receipt_sha256: Option<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LegacySuccessReceipt {
    version: u8,
    actor_id: String,
    key_fingerprint: String,
    seller_scope: String,
    endpoint: String,
    http_status: u16,
    request_started_at: DateTime<Utc>,
    expected_next_allowed_at: DateTime<Utc>,
    operator_verified_at: DateTime<Utc>,
    evidence_ref: String,
}

impl Quota {
    pub(super) fn validate(&self) -> Result<()> {
        ensure!(
            self.next_allowed_at.is_some(),
            "financial quota journal is incomplete"
        );
        match self.version {
            0 => ensure!(
                self.last_attempt_at.is_none()
                    && self.retry_after_until.is_none()
                    && self.personal_key_fingerprint.is_none()
                    && self.legacy_receipt_sha256.is_none(),
                "legacy quota journal contains incompatible fields"
            ),
            2 => {
                let last = self
                    .last_attempt_at
                    .ok_or_else(|| anyhow::anyhow!("quota attempt is missing"))?;
                let minimum = last
                    .checked_add_signed(if self.personal_key_fingerprint.is_some() {
                        PERSONAL
                    } else {
                        CONSERVATIVE
                    })
                    .ok_or_else(|| anyhow::anyhow!("quota time is outside supported range"))?;
                ensure!(
                    self.next_allowed_at.is_some_and(|next| next >= minimum)
                        && self.retry_after_until.is_none_or(|until| self
                            .next_allowed_at
                            .is_some_and(|next| next >= until))
                        && self
                            .personal_key_fingerprint
                            .as_deref()
                            .is_none_or(valid_digest)
                        && self
                            .legacy_receipt_sha256
                            .as_deref()
                            .is_none_or(valid_digest),
                    "financial quota journal is invalid"
                );
            }
            _ => anyhow::bail!("unsupported financial quota journal version"),
        }
        Ok(())
    }

    pub(super) fn reserve(&mut self, now: DateTime<Utc>, key: Option<&str>) -> Result<bool> {
        if self.next_allowed_at.is_some_and(|next| now < next) {
            return Ok(false);
        }
        let interval = if key.is_some() && key == self.personal_key_fingerprint.as_deref() {
            PERSONAL
        } else {
            CONSERVATIVE
        };
        let next = now
            .checked_add_signed(interval)
            .ok_or_else(|| anyhow::anyhow!("quota time is outside supported range"))?;
        self.version = 2;
        self.last_attempt_at = Some(now);
        self.next_allowed_at = Some(next);
        Ok(true)
    }

    pub(super) fn confirm(&mut self, key: &str, attempted_at: DateTime<Utc>) -> Result<()> {
        ensure!(
            valid_digest(key) && self.version == 2 && self.last_attempt_at == Some(attempted_at),
            "personal confirmation has no matching reserved request"
        );
        let next = attempted_at
            .checked_add_signed(PERSONAL)
            .ok_or_else(|| anyhow::anyhow!("quota time is outside supported range"))?;
        self.personal_key_fingerprint = Some(key.to_owned());
        self.next_allowed_at = Some(self.retry_after_until.map_or(next, |until| until.max(next)));
        Ok(())
    }

    pub(super) fn postpone(&mut self, now: DateTime<Utc>, seconds: u64) -> Result<()> {
        let delay = Duration::try_seconds(
            i64::try_from(seconds)
                .map_err(|_| anyhow::anyhow!("upstream pause exceeds the supported bound"))?,
        )
        .ok_or_else(|| anyhow::anyhow!("upstream pause exceeds the supported bound"))?;
        let until = now
            .checked_add_signed(delay)
            .ok_or_else(|| anyhow::anyhow!("upstream pause exceeds the supported bound"))?;
        let last = if self.version == 0 {
            Some(now)
        } else {
            self.last_attempt_at
        };
        let minimum = last
            .and_then(|last| {
                last.checked_add_signed(if self.personal_key_fingerprint.is_some() {
                    PERSONAL
                } else {
                    CONSERVATIVE
                })
            })
            .ok_or_else(|| anyhow::anyhow!("quota time is outside supported range"))?;
        // A legacy pause cannot be retrospectively classified as a successful probe.
        if self.version == 0 {
            self.version = 2;
            self.last_attempt_at = last;
        }
        self.retry_after_until = Some(self.retry_after_until.map_or(until, |old| old.max(until)));
        self.next_allowed_at = Some(
            self.next_allowed_at
                .map_or(until, |old| old.max(until))
                .max(minimum),
        );
        Ok(())
    }

    pub(super) fn migrate(
        &mut self,
        receipt: &LegacySuccessReceipt,
        actor: &str,
        key: &str,
        seller: &str,
        now: DateTime<Utc>,
        receipt_hash: String,
    ) -> Result<()> {
        ensure!(
            self.version == 0 && self.last_attempt_at.is_none() && self.retry_after_until.is_none(),
            "only an untouched legacy reservation may be migrated"
        );
        ensure!(
            receipt.version == 1
                && receipt.actor_id == actor
                && receipt.key_fingerprint == key
                && receipt.seller_scope == seller
                && valid_digest(key)
                && valid_digest(seller)
                && valid_digest(&receipt_hash)
                && receipt.endpoint
                    == "POST https://finance-api.wildberries.ru/api/finance/v1/sales-reports/detailed"
                && matches!(receipt.http_status, 200 | 204)
                && self.next_allowed_at == Some(receipt.expected_next_allowed_at)
                && receipt.request_started_at.checked_add_signed(CONSERVATIVE)
                    == Some(receipt.expected_next_allowed_at)
                && receipt.operator_verified_at >= receipt.request_started_at
                && receipt.operator_verified_at <= now
                && !receipt.evidence_ref.is_empty()
                && receipt.evidence_ref.len() <= 256
                && !receipt.evidence_ref.chars().any(char::is_control),
            "legacy success receipt does not match this reservation and key"
        );
        self.version = 2;
        self.last_attempt_at = Some(receipt.request_started_at);
        self.legacy_receipt_sha256 = Some(receipt_hash);
        self.confirm(key, receipt.request_started_at)
    }
}

#[cfg(test)]
#[path = "quota/tests.rs"]
mod tests;
