//! Offline, deterministic Ozon advertising recommendations.
//!
//! No marketplace clients, credentials, database writes or Control commands
//! belong here.
//! Historical attributed orders are not fulfilled orders or incremental sales.

mod allocation;
pub mod campaign_history;
mod evaluate;
pub mod journal;
mod model;
pub mod prepare;
pub mod published;
pub mod reconciliation;
mod validation;

pub use model::*;

use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerError {
    #[error("optimizer input exceeds its size or row limit")]
    LimitExceeded,
    #[error("optimizer input violates the versioned evidence contract")]
    InvalidInput,
    #[error("optimizer evidence contains duplicate products or dates")]
    DuplicateEvidence,
    #[error("optimizer arithmetic overflowed")]
    Overflow,
}

pub fn parse_input(bytes: &[u8]) -> Result<ShadowInput, OptimizerError> {
    if bytes.len() > MAX_INPUT_BYTES {
        return Err(OptimizerError::LimitExceeded);
    }
    let input = serde_json::from_slice(bytes).map_err(|_| OptimizerError::InvalidInput)?;
    validation::validate(&input)?;
    Ok(input)
}

/// Output is reproducible for equivalent inputs, including permutations of
/// products and daily rows. The digest is provenance, not an execution token.
pub fn recommend(mut input: ShadowInput) -> Result<ShadowReport, OptimizerError> {
    validation::validate(&input)?;
    input.source_refs.sort();
    input.products.sort_by_key(|product| product.sku);
    for product in &mut input.products {
        product.daily.sort_by_key(|day| day.date);
    }
    let bytes = serde_json::to_vec(&input).map_err(|_| OptimizerError::InvalidInput)?;
    let input_sha256 =
        Sha256::digest(bytes)
            .iter()
            .fold(String::with_capacity(64), |mut output, byte| {
                use std::fmt::Write as _;
                write!(output, "{byte:02x}").expect("writing to String cannot fail");
                output
            });
    let mut recommendations = input
        .products
        .iter()
        .map(|product| evaluate::evaluate(&input, product))
        .collect::<Result<Vec<_>, _>>()?;
    allocation::allocate(&input.policy, &mut recommendations)?;
    let allocated_daily_budget_minor = recommendations.iter().try_fold(0u64, |total, row| {
        total
            .checked_add(row.suggested_daily_budget_minor)
            .ok_or(OptimizerError::Overflow)
    })?;
    Ok(ShadowReport {
        version: 1,
        mode: "shadow".to_owned(),
        objective: input.objective,
        currency: "RUB".to_owned(),
        account_id: input.account_id,
        as_of: input.as_of,
        input_sha256,
        total_daily_budget_minor: input.policy.total_daily_budget_minor,
        allocated_daily_budget_minor,
        unallocated_daily_budget_minor: input.policy.total_daily_budget_minor
            - allocated_daily_budget_minor,
        recommendations,
    })
}

fn scaled(value: u64, numerator: u64, denominator: u64) -> Result<u64, OptimizerError> {
    if denominator == 0 {
        return Err(OptimizerError::InvalidInput);
    }
    u64::try_from(u128::from(value) * u128::from(numerator) / u128::from(denominator))
        .map_err(|_| OptimizerError::Overflow)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod advertising_objective_tests;

#[cfg(test)]
mod observation_tests;
