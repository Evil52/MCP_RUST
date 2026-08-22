use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use tokio_postgres::{Config, config::Host, error::SqlState};

use crate::control::wb::WbPreparedBidChange;

use super::model::{PlanStoreError, WbActionQuota};

const PLAN_DIGEST_DOMAIN: &[u8] = b"mcp-ozon/wb-control-plan/v1";
static PLAN_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(super) fn validate_plan_id(plan_id: &str) -> Result<(), PlanStoreError> {
    if is_lower_hex_digest(plan_id) {
        Ok(())
    } else {
        Err(PlanStoreError::NotFound)
    }
}

pub(super) fn validate_digest(digest: &str) -> Result<(), PlanStoreError> {
    if is_lower_hex_digest(digest) {
        Ok(())
    } else {
        Err(PlanStoreError::InvalidPlan)
    }
}

fn is_lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

pub(super) fn validate_actor_or_account(value: &str) -> Result<(), PlanStoreError> {
    if !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
    {
        Ok(())
    } else {
        Err(PlanStoreError::InvalidPlan)
    }
}

pub(super) fn validate_approval_reason(reason: &str) -> Result<(), PlanStoreError> {
    if !reason.is_empty()
        && reason.len() <= 128
        && reason.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b':' | b'/' | b'-')
        })
    {
        Ok(())
    } else {
        Err(PlanStoreError::InvalidPlan)
    }
}

pub(super) fn cumulative_abs_delta(changes: &[WbPreparedBidChange]) -> Result<u64, PlanStoreError> {
    let total = changes.iter().try_fold(0_u64, |total, change| {
        total.checked_add(change.before_bid_kopecks.abs_diff(change.bid_kopecks))
    });
    match total {
        Some(total) if total > 0 => Ok(total),
        _ => Err(PlanStoreError::InvalidPlan),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn make_plan_digest(
    prepare_reservation_id: &str,
    actor_id: &str,
    account_id: &str,
    advert_id: u64,
    schema_version: u32,
    policy_revision: u64,
    policy_digest: &str,
    action_quota: WbActionQuota,
    requested_json: &str,
    changes_json: &str,
    before_json: &str,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> String {
    let mut hasher = Sha256::new();
    update_digest_field(&mut hasher, PLAN_DIGEST_DOMAIN);
    update_digest_field(&mut hasher, prepare_reservation_id.as_bytes());
    update_digest_field(&mut hasher, actor_id.as_bytes());
    update_digest_field(&mut hasher, account_id.as_bytes());
    update_digest_field(&mut hasher, &advert_id.to_be_bytes());
    update_digest_field(&mut hasher, &schema_version.to_be_bytes());
    update_digest_field(&mut hasher, &policy_revision.to_be_bytes());
    update_digest_field(&mut hasher, policy_digest.as_bytes());
    update_digest_field(
        &mut hasher,
        &action_quota.max_actions_per_hour.to_be_bytes(),
    );
    update_digest_field(&mut hasher, &action_quota.max_actions_per_day.to_be_bytes());
    update_digest_field(&mut hasher, &action_quota.cooldown_seconds.to_be_bytes());
    update_digest_field(
        &mut hasher,
        &action_quota
            .max_cumulative_abs_delta_kopecks_per_day
            .to_be_bytes(),
    );
    update_digest_field(&mut hasher, requested_json.as_bytes());
    update_digest_field(&mut hasher, changes_json.as_bytes());
    update_digest_field(&mut hasher, before_json.as_bytes());
    update_digest_field(
        &mut hasher,
        created_at
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
            .as_bytes(),
    );
    update_digest_field(
        &mut hasher,
        expires_at
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
            .as_bytes(),
    );
    hex_digest(hasher.finalize())
}

fn update_digest_field(hasher: &mut Sha256, field: &[u8]) {
    hasher.update((field.len() as u64).to_be_bytes());
    hasher.update(field);
}

pub(super) fn make_plan_id(plan_digest: &str, now: DateTime<Utc>) -> String {
    let sequence = PLAN_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let mut hasher = Sha256::new();
    update_digest_field(&mut hasher, b"mcp-ozon/wb-control-plan-id/v1");
    update_digest_field(&mut hasher, plan_digest.as_bytes());
    update_digest_field(
        &mut hasher,
        &now.timestamp_nanos_opt().unwrap_or_default().to_be_bytes(),
    );
    update_digest_field(&mut hasher, &sequence.to_be_bytes());
    hex_digest(hasher.finalize())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn make_prepare_reservation_id(
    actor_id: &str,
    account_id: &str,
    advert_id: u64,
    schema_version: u32,
    policy_revision: u64,
    policy_digest: &str,
    now: DateTime<Utc>,
) -> String {
    let sequence = PLAN_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let mut hasher = Sha256::new();
    update_digest_field(&mut hasher, b"mcp-ozon/wb-control-prepare-reservation/v1");
    update_digest_field(&mut hasher, actor_id.as_bytes());
    update_digest_field(&mut hasher, account_id.as_bytes());
    update_digest_field(&mut hasher, &advert_id.to_be_bytes());
    update_digest_field(&mut hasher, &schema_version.to_be_bytes());
    update_digest_field(&mut hasher, &policy_revision.to_be_bytes());
    update_digest_field(&mut hasher, policy_digest.as_bytes());
    update_digest_field(
        &mut hasher,
        &now.timestamp_nanos_opt().unwrap_or_default().to_be_bytes(),
    );
    update_digest_field(&mut hasher, &sequence.to_be_bytes());
    hex_digest(hasher.finalize())
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "Result::map_err needs the owned error function to keep coverage complete"
)]
pub(super) fn map_prepare_insert_error(error: tokio_postgres::Error) -> PlanStoreError {
    let Some(database_error) = error.as_db_error() else {
        return PlanStoreError::Unavailable;
    };
    let message = database_error.message();
    if message.contains("unresolved incident") {
        PlanStoreError::CampaignLocked
    } else if message.contains("attempt limit") || message.contains("outstanding prepare limit") {
        PlanStoreError::PrepareLimitExceeded
    } else if message.contains("active policy") {
        PlanStoreError::PolicyChanged
    } else if database_error.code() == &SqlState::UNIQUE_VIOLATION {
        PlanStoreError::InvalidState
    } else {
        PlanStoreError::Unavailable
    }
}

pub(super) fn make_approval_id(
    plan_id: &str,
    plan_digest: &str,
    approver_id: &str,
    reason: &str,
    now: DateTime<Utc>,
) -> String {
    let mut hasher = Sha256::new();
    update_digest_field(&mut hasher, b"mcp-ozon/wb-control-approval/v1");
    update_digest_field(&mut hasher, plan_id.as_bytes());
    update_digest_field(&mut hasher, plan_digest.as_bytes());
    update_digest_field(&mut hasher, approver_id.as_bytes());
    update_digest_field(&mut hasher, reason.as_bytes());
    update_digest_field(
        &mut hasher,
        &now.timestamp_nanos_opt().unwrap_or_default().to_be_bytes(),
    );
    hex_digest(hasher.finalize())
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    use std::fmt::Write as _;

    bytes.as_ref().iter().fold(
        String::with_capacity(bytes.as_ref().len().saturating_mul(2)),
        |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        },
    )
}

pub(in crate::control) fn validate_control_database_url(
    value: &str,
) -> Result<Config, PlanStoreError> {
    let config = value
        .parse::<Config>()
        .map_err(|_| PlanStoreError::InvalidPlan)?;
    let exactly_one_tcp_host = matches!(config.get_hosts(), [Host::Tcp(host)] if !host.is_empty());
    if config.get_user() != Some("control_writer")
        || config.get_password().is_none_or(<[u8]>::is_empty)
        || config.get_dbname().is_none_or(str::is_empty)
        || !exactly_one_tcp_host
        || !config.get_hostaddrs().is_empty()
        || !matches!(config.get_ports(), [port] if *port != 0)
    {
        return Err(PlanStoreError::InvalidPlan);
    }
    Ok(config)
}
