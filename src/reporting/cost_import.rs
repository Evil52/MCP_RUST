//! Bounded, offline 1C cost ingestion. This module never connects to 1C or office hosts.

mod postgres;
pub use postgres::PostgresCostRepository;

use std::collections::BTreeSet;

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::snapshot::{AccountScope, Marketplace};

pub const MAX_COST_IMPORT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_COST_IMPORT_ROWS: usize = 10_000;

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum CostImportError {
    #[error("cost import exceeds its size or row limit")]
    LimitExceeded,
    #[error("cost import violates the versioned data contract")]
    InvalidInput,
    #[error("cost import is outside the configured account, source or SKU scope")]
    ScopeDenied,
    #[error("cost import conflicts with an existing export or effective period")]
    Conflict,
    #[error("cost import storage is unavailable or incorrectly restricted")]
    Unavailable,
}

/// Trusted server configuration, never derived from the import payload itself.
#[derive(Debug, Clone)]
pub struct CostImportScope {
    account: AccountScope,
    source_id: String,
    allowed_skus: BTreeSet<u64>,
    imported_by: String,
}

impl CostImportScope {
    pub fn new(
        account: AccountScope,
        source_id: String,
        allowed_skus: BTreeSet<u64>,
        imported_by: String,
    ) -> Result<Self, CostImportError> {
        if !valid_id(&source_id)
            || !valid_id(&imported_by)
            || allowed_skus.is_empty()
            || allowed_skus.len() > MAX_COST_IMPORT_ROWS
            || allowed_skus
                .iter()
                .any(|sku| *sku == 0 || i64::try_from(*sku).is_err())
        {
            return Err(CostImportError::InvalidInput);
        }
        Ok(Self {
            account,
            source_id,
            allowed_skus,
            imported_by,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum CostCurrency {
    RUB,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CostAllocation {
    PerUnit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CostVatTreatment {
    Included,
    Excluded,
    NotApplicable,
}

impl CostVatTreatment {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Included => "included",
            Self::Excluded => "excluded",
            Self::NotApplicable => "not_applicable",
        }
    }
}

/// Amount is exact integer kopecks; zero is an explicit supplied cost.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CostImportRow {
    pub source_row_id: String,
    pub sku: u64,
    pub amount_minor: i64,
    pub currency: CostCurrency,
    pub allocation: CostAllocation,
    pub vat_treatment: CostVatTreatment,
    pub vat_rate_bps: Option<u16>,
    pub effective_from: NaiveDate,
    pub effective_to: NaiveDate,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CostImportEnvelope {
    version: u32,
    account_id: String,
    marketplace: Marketplace,
    source_id: String,
    export_id: String,
    exported_at: DateTime<Utc>,
    rows: Vec<CostImportRow>,
}

/// The only write input accepted by the repository; construction validates scope.
#[derive(Debug, Clone)]
pub struct ValidatedCostBatch {
    envelope: CostImportEnvelope,
    sha256: String,
    imported_by: String,
}

impl ValidatedCostBatch {
    pub fn parse_json(bytes: &[u8], scope: &CostImportScope) -> Result<Self, CostImportError> {
        if bytes.len() > MAX_COST_IMPORT_BYTES {
            return Err(CostImportError::LimitExceeded);
        }
        let mut envelope: CostImportEnvelope =
            serde_json::from_slice(bytes).map_err(|_| CostImportError::InvalidInput)?;
        if envelope.rows.is_empty() || envelope.rows.len() > MAX_COST_IMPORT_ROWS {
            return Err(CostImportError::LimitExceeded);
        }
        if envelope.version != 1
            || !valid_id(&envelope.export_id)
            || !(2000..=2200).contains(&envelope.exported_at.year())
        {
            return Err(CostImportError::InvalidInput);
        }
        if envelope.account_id != scope.account.account_id()
            || envelope.marketplace != scope.account.marketplace()
            || envelope.source_id != scope.source_id
            || envelope
                .rows
                .iter()
                .any(|row| !scope.allowed_skus.contains(&row.sku))
        {
            return Err(CostImportError::ScopeDenied);
        }
        let mut source_rows = BTreeSet::new();
        envelope
            .rows
            .sort_by_key(|row| (row.sku, row.effective_from, row.effective_to));
        for row in &envelope.rows {
            if !valid_id(&row.source_row_id)
                || !source_rows.insert(&row.source_row_id)
                || row.amount_minor < 0
                || !valid_date(row.effective_from)
                || !valid_date(row.effective_to)
                || row.effective_from > row.effective_to
                || !valid_vat(row)
            {
                return Err(CostImportError::InvalidInput);
            }
        }
        if envelope.rows.windows(2).any(|pair| {
            pair[0].sku == pair[1].sku && pair[0].effective_to >= pair[1].effective_from
        }) {
            return Err(CostImportError::Conflict);
        }
        let canonical = serde_json::to_vec(&envelope).map_err(|_| CostImportError::InvalidInput)?;
        let sha256 =
            Sha256::digest(canonical)
                .iter()
                .fold(String::with_capacity(64), |mut output, byte| {
                    use std::fmt::Write as _;
                    write!(output, "{byte:02x}").expect("writing to String cannot fail");
                    output
                });
        Ok(Self {
            envelope,
            sha256,
            imported_by: scope.imported_by.clone(),
        })
    }

    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.envelope.rows.len()
    }

    #[must_use]
    pub fn export_id(&self) -> &str {
        &self.envelope.export_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CostImportReceipt {
    pub batch_id: i64,
    pub account_id: String,
    pub source_id: String,
    pub export_id: String,
    pub sha256: String,
    pub row_count: usize,
    pub imported_at: DateTime<Utc>,
    pub imported_by: String,
    pub already_imported: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredCost {
    pub row: CostImportRow,
    pub batch_id: i64,
    pub source_id: String,
    pub export_id: String,
    pub sha256: String,
    pub imported_at: DateTime<Utc>,
    pub imported_by: String,
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn valid_date(date: NaiveDate) -> bool {
    date.year() >= 2000 && date.year() <= 2200
}

const fn valid_vat(row: &CostImportRow) -> bool {
    match (row.vat_treatment, row.vat_rate_bps) {
        (CostVatTreatment::NotApplicable, None) => true,
        (CostVatTreatment::Included | CostVatTreatment::Excluded, Some(bps)) => bps <= 10_000,
        _ => false,
    }
}

#[cfg(test)]
mod tests;
