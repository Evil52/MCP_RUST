//! Bounded, value-free evidence for otherwise opaque Sales publication failures.
use std::collections::BTreeSet;

use super::{
    CollectedFacts, CollectedSalesFact, MAX_COLLECTOR_VERSION_BYTES, MAX_FACT_ROWS,
    PostgresCollectorError, SnapshotSource, SnapshotStatus, fits_i32, fits_i64,
};
use crate::reporting::snapshot::SnapshotError;

pub(super) fn reject_metadata(
    source: SnapshotSource,
    reason: &'static str,
) -> PostgresCollectorError {
    tracing::warn!(source = ?source, reason, "source publication metadata validation failed");
    PostgresCollectorError::InvalidInput
}

pub(super) fn reject_descriptor(
    source: SnapshotSource,
    error: SnapshotError,
) -> PostgresCollectorError {
    let reason = match error {
        SnapshotError::InvalidTimeRange => "invalid_time_range",
        SnapshotError::InvalidAccountScope => "invalid_account_scope",
        _ => "invalid_descriptor",
    };
    reject_metadata(source, reason)
}

pub(super) fn validate_metadata(
    facts: &CollectedFacts,
    version: &str,
    status: SnapshotStatus,
    pagination_complete: bool,
) -> Result<u32, PostgresCollectorError> {
    let reason = if facts.len() > MAX_FACT_ROWS {
        Some("row_limit")
    } else if version.is_empty()
        || version.len() > MAX_COLLECTOR_VERSION_BYTES
        || !version
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        Some("invalid_collector_version")
    } else if status == SnapshotStatus::Succeeded && !pagination_complete {
        Some("incomplete_pagination")
    } else {
        None
    };
    if let Some(reason) = reason {
        return Err(reject_metadata(facts.source(), reason));
    }
    u32::try_from(facts.len()).map_err(|_| reject_metadata(facts.source(), "row_limit"))
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Fingerprint {
    checked_rows: usize,
    distinct_identities: usize,
    duplicate_rows: usize,
    invalid_sku_rows: usize,
    invalid_count_rows: usize,
    invalid_money_rows: usize,
    truncated: bool,
}

impl Fingerprint {
    fn inspect(facts: &[CollectedSalesFact]) -> Self {
        let mut fingerprint = Self {
            truncated: facts.len() > MAX_FACT_ROWS,
            ..Self::default()
        };
        let mut seen = BTreeSet::new();
        for fact in facts.iter().take(MAX_FACT_ROWS) {
            fingerprint.checked_rows += 1;
            fingerprint.duplicate_rows += usize::from(!seen.insert((fact.business_date, fact.sku)));
            fingerprint.invalid_sku_rows += usize::from(fact.sku == 0 || !fits_i64(fact.sku));
            fingerprint.invalid_count_rows += usize::from(
                !fits_i32(fact.ordered_units)
                    || fact.cancelled_units.is_some_and(|value| !fits_i32(value))
                    || fact.returned_units.is_some_and(|value| !fits_i32(value)),
            );
            fingerprint.invalid_money_rows += usize::from(!fits_i64(fact.operational_gmv_minor));
        }
        fingerprint.distinct_identities = seen.len();
        fingerprint
    }

    const fn reason(&self) -> Option<&'static str> {
        if self.truncated {
            Some("row_limit")
        } else if self.invalid_sku_rows + self.invalid_count_rows + self.invalid_money_rows > 0 {
            Some("invalid_numeric_range")
        } else if self.duplicate_rows > 0 {
            Some("duplicate_identity")
        } else {
            None
        }
    }
}

pub(super) fn validate(facts: &[CollectedSalesFact]) -> Result<(), PostgresCollectorError> {
    let fingerprint = Fingerprint::inspect(facts);
    if let Some(reason) = fingerprint.reason() {
        // Never log a fact, date, SKU, monetary value, account name or digest
        // of an identity. The fixed fields survive checkpoint cleanup safely.
        tracing::warn!(
            source = "sales",
            reason,
            checked_rows = fingerprint.checked_rows,
            distinct_identities = fingerprint.distinct_identities,
            duplicate_rows = fingerprint.duplicate_rows,
            invalid_sku_rows = fingerprint.invalid_sku_rows,
            invalid_count_rows = fingerprint.invalid_count_rows,
            invalid_money_rows = fingerprint.invalid_money_rows,
            truncated = fingerprint.truncated,
            "sales publication validation failed"
        );
        return Err(PostgresCollectorError::InvalidInput);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
