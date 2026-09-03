//! Validation of the operator-authored static Ozon campaign guard file.
//!
//! The file names the campaigns a headless process is allowed to keep inside a
//! spend/DRR guard and a CPC corridor, so it is the document that authorises
//! every write that process can make. Parsing and validation live here rather
//! than in the binary for two reasons: the release lint and coverage gates
//! apply to the library, and the same rules are then reachable from tests that
//! never open a file or a socket.

use serde::Deserialize;
use thiserror::Error;

use super::{guard::evaluate_ozon_campaign_guard, model::OzonCampaignGuard};

/// Upper bound on how many campaigns one file may authorise.
pub const MAX_OZON_STATIC_GUARDS: usize = 50;

/// Largest accepted guard file.
///
/// `MAX_OZON_STATIC_GUARDS` entries are far smaller than this; the byte bound
/// exists so a truncated mount or a wrong file fails before deserialization
/// rather than during it.
pub const MAX_OZON_STATIC_GUARD_FILE_BYTES: usize = 64 * 1024;

/// Default CPC corridor, in microroubles.
///
/// Both bounds are whole roubles, which is the only granularity the corridor
/// check accepts.
pub const DEFAULT_OZON_STATIC_MIN_CPC_BID_MICROROUBLES: u64 = 7_000_000;
pub const DEFAULT_OZON_STATIC_MAX_CPC_BID_MICROROUBLES: u64 = 12_000_000;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum OzonStaticGuardError {
    #[error("static Ozon guard config has an unsupported size")]
    InvalidSize,
    #[error("static Ozon guard config is not valid JSON for this contract")]
    InvalidFormat,
    #[error("static Ozon guard config does not match the runtime scope")]
    ScopeMismatch,
    #[error("static Ozon guard entry has invalid data")]
    InvalidGuard,
}

/// One campaign a static guard run may act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OzonStaticCampaignGuard {
    pub guard: OzonCampaignGuard,
    pub min_cpc_bid_microrubles: u64,
    pub max_cpc_bid_microrubles: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StaticGuardFile {
    account_id: String,
    guards: Vec<StaticGuardEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StaticGuardEntry {
    campaign_id: u64,
    sku: u64,
    date_from: String,
    spend_cap_microrubles: u64,
    target_drr_percent: u8,
    #[serde(default = "default_min_cpc_bid_microrubles")]
    min_cpc_bid_microrubles: u64,
    #[serde(default = "default_max_cpc_bid_microrubles")]
    max_cpc_bid_microrubles: u64,
}

const fn default_min_cpc_bid_microrubles() -> u64 {
    DEFAULT_OZON_STATIC_MIN_CPC_BID_MICROROUBLES
}

const fn default_max_cpc_bid_microrubles() -> u64 {
    DEFAULT_OZON_STATIC_MAX_CPC_BID_MICROROUBLES
}

/// Validates a static guard file against the account the runtime is bound to.
///
/// The account check is what keeps a guard file for one cabinet from
/// authorising writes in another. Campaign IDs and SKUs must both be unique
/// across the file: two entries naming one campaign, or one SKU reached through
/// two campaigns, would let a single run apply two different corridors to the
/// same target.
pub fn parse_ozon_static_guards(
    bytes: &[u8],
    expected_account_id: &str,
) -> Result<Vec<OzonStaticCampaignGuard>, OzonStaticGuardError> {
    if bytes.is_empty() || bytes.len() > MAX_OZON_STATIC_GUARD_FILE_BYTES {
        return Err(OzonStaticGuardError::InvalidSize);
    }
    let config: StaticGuardFile =
        serde_json::from_slice(bytes).map_err(|_| OzonStaticGuardError::InvalidFormat)?;
    if config.account_id != expected_account_id
        || config.guards.is_empty()
        || config.guards.len() > MAX_OZON_STATIC_GUARDS
    {
        return Err(OzonStaticGuardError::ScopeMismatch);
    }
    let mut campaigns = std::collections::BTreeSet::new();
    let mut skus = std::collections::BTreeSet::new();
    config
        .guards
        .into_iter()
        .map(|entry| {
            if entry.campaign_id == 0
                || entry.sku == 0
                || !campaigns.insert(entry.campaign_id)
                || !skus.insert(entry.sku)
                || chrono::NaiveDate::parse_from_str(&entry.date_from, "%Y-%m-%d").is_err()
                || evaluate_ozon_campaign_guard(
                    0,
                    0,
                    entry.spend_cap_microrubles,
                    entry.target_drr_percent,
                )
                .is_err()
                || entry.min_cpc_bid_microrubles == 0
                || entry.min_cpc_bid_microrubles > entry.max_cpc_bid_microrubles
                || !entry.min_cpc_bid_microrubles.is_multiple_of(1_000_000)
                || !entry.max_cpc_bid_microrubles.is_multiple_of(1_000_000)
            {
                return Err(OzonStaticGuardError::InvalidGuard);
            }
            Ok(OzonStaticCampaignGuard {
                guard: OzonCampaignGuard {
                    plan_id: format!("static-{}", entry.campaign_id),
                    account_id: expected_account_id.to_owned(),
                    sku: entry.sku,
                    campaign_id: entry.campaign_id,
                    date_from: entry.date_from,
                    spend_cap_microrubles: entry.spend_cap_microrubles,
                    target_drr_percent: entry.target_drr_percent,
                },
                min_cpc_bid_microrubles: entry.min_cpc_bid_microrubles,
                max_cpc_bid_microrubles: entry.max_cpc_bid_microrubles,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_OZON_STATIC_MAX_CPC_BID_MICROROUBLES, DEFAULT_OZON_STATIC_MIN_CPC_BID_MICROROUBLES,
        MAX_OZON_STATIC_GUARD_FILE_BYTES, MAX_OZON_STATIC_GUARDS, OzonStaticGuardError,
        parse_ozon_static_guards,
    };

    const ACCOUNT: &str = "furnitura_dlya_doma";

    fn entry(campaign_id: u64, sku: u64) -> serde_json::Value {
        serde_json::json!({
            "campaign_id": campaign_id,
            "sku": sku,
            "date_from": "2026-09-02",
            "spend_cap_microrubles": 2_000_000_000_u64,
            "target_drr_percent": 15
        })
    }

    fn file(entries: &[serde_json::Value]) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({"account_id": ACCOUNT, "guards": entries})).unwrap()
    }

    #[test]
    fn the_shipped_guard_file_parses_into_the_documented_corridor() {
        let bytes = include_bytes!("../../../config/ozon-furnitura-cpc7-live.json");
        let guards = parse_ozon_static_guards(bytes, ACCOUNT).unwrap();
        assert_eq!(guards.len(), 5);
        for guard in &guards {
            // Omitting the corridor in the file must mean the reviewed default,
            // never an unbounded one.
            assert_eq!(
                guard.min_cpc_bid_microrubles,
                DEFAULT_OZON_STATIC_MIN_CPC_BID_MICROROUBLES
            );
            assert_eq!(
                guard.max_cpc_bid_microrubles,
                DEFAULT_OZON_STATIC_MAX_CPC_BID_MICROROUBLES
            );
            assert_eq!(guard.guard.account_id, ACCOUNT);
            assert_eq!(
                guard.guard.plan_id,
                format!("static-{}", guard.guard.campaign_id)
            );
        }
        // A file authorises writes only in the cabinet the runtime is bound to.
        assert_eq!(
            parse_ozon_static_guards(bytes, "another_account"),
            Err(OzonStaticGuardError::ScopeMismatch)
        );
    }

    #[test]
    fn file_level_bounds_fail_closed_before_any_entry_is_read() {
        assert_eq!(
            parse_ozon_static_guards(b"", ACCOUNT),
            Err(OzonStaticGuardError::InvalidSize)
        );
        let oversized = vec![b' '; MAX_OZON_STATIC_GUARD_FILE_BYTES + 1];
        assert_eq!(
            parse_ozon_static_guards(&oversized, ACCOUNT),
            Err(OzonStaticGuardError::InvalidSize)
        );
        for malformed in [
            &b"{"[..],
            &b"[]"[..],
            // An unknown field is a config the reviewer did not approve, not a
            // field to ignore.
            br#"{"account_id":"furnitura_dlya_doma","guards":[],"extra":1}"#,
            // A negative or fractional identifier is not a campaign ID.
            br#"{"account_id":"furnitura_dlya_doma","guards":[{"campaign_id":-1,"sku":1,"date_from":"2026-09-02","spend_cap_microrubles":2000000000,"target_drr_percent":15}]}"#,
        ] {
            assert_eq!(
                parse_ozon_static_guards(malformed, ACCOUNT),
                Err(OzonStaticGuardError::InvalidFormat)
            );
        }
        assert_eq!(
            parse_ozon_static_guards(&file(&[]), ACCOUNT),
            Err(OzonStaticGuardError::ScopeMismatch)
        );
        let too_many = (1..=MAX_OZON_STATIC_GUARDS + 1)
            .map(|index| entry(index as u64, 1_000 + index as u64))
            .collect::<Vec<_>>();
        assert_eq!(
            parse_ozon_static_guards(&file(&too_many), ACCOUNT),
            Err(OzonStaticGuardError::ScopeMismatch)
        );
    }

    #[test]
    fn one_campaign_and_one_sku_may_appear_only_once() {
        // Two entries for the same campaign, or the same SKU reached through
        // two campaigns, would let a single run apply two corridors to one
        // target. Both must be refused for the whole file.
        for duplicated in [
            vec![entry(1, 10), entry(1, 11)],
            vec![entry(1, 10), entry(2, 10)],
        ] {
            assert_eq!(
                parse_ozon_static_guards(&file(&duplicated), ACCOUNT),
                Err(OzonStaticGuardError::InvalidGuard)
            );
        }
        let distinct = vec![entry(1, 10), entry(2, 11)];
        assert_eq!(
            parse_ozon_static_guards(&file(&distinct), ACCOUNT)
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn every_entry_field_is_bounded_before_a_campaign_is_authorised() {
        let cases = vec![
            // Zero is not an identifier.
            serde_json::json!({"campaign_id": 0}),
            serde_json::json!({"sku": 0}),
            // The start date is a plain calendar date, not a timestamp.
            serde_json::json!({"date_from": "02.09.2026"}),
            serde_json::json!({"date_from": "2026-09-02T00:00:00Z"}),
            serde_json::json!({"date_from": "2026-02-30"}),
            serde_json::json!({"date_from": ""}),
            // Spend cap and DRR must satisfy the same guard that later stops
            // the campaign, so an unstoppable guard cannot be configured.
            serde_json::json!({"spend_cap_microrubles": 0}),
            serde_json::json!({"spend_cap_microrubles": 1}),
            serde_json::json!({"target_drr_percent": 9}),
            serde_json::json!({"target_drr_percent": 101}),
            // The corridor must be a non-empty whole-rouble range.
            serde_json::json!({"min_cpc_bid_microrubles": 0}),
            serde_json::json!({"min_cpc_bid_microrubles": 13_000_000_u64}),
            serde_json::json!({"min_cpc_bid_microrubles": 7_000_001_u64}),
            serde_json::json!({"max_cpc_bid_microrubles": 12_000_001_u64}),
        ];
        for overrides in cases {
            let mut candidate = entry(37_756_773, 3_457_585_933);
            for (field, value) in overrides.as_object().unwrap() {
                candidate[field] = value.clone();
            }
            assert_eq!(
                parse_ozon_static_guards(&file(std::slice::from_ref(&candidate)), ACCOUNT),
                Err(OzonStaticGuardError::InvalidGuard),
                "{candidate} must not authorise a static campaign guard"
            );
        }

        // An explicit corridor that is valid is carried through unchanged.
        let mut widened = entry(37_756_773, 3_457_585_933);
        widened["min_cpc_bid_microrubles"] = serde_json::json!(8_000_000_u64);
        widened["max_cpc_bid_microrubles"] = serde_json::json!(20_000_000_u64);
        let parsed = parse_ozon_static_guards(&file(&[widened]), ACCOUNT).unwrap();
        assert_eq!(parsed[0].min_cpc_bid_microrubles, 8_000_000);
        assert_eq!(parsed[0].max_cpc_bid_microrubles, 20_000_000);
    }
}
