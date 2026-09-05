use std::collections::BTreeSet;

use chrono::NaiveDate;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{
    OzonCampaignCreateRequest, OzonCampaignProduct, OzonCampaignProductsRequest,
    OzonCampaignStrategy, OzonPlacement,
};

const MANIFEST_DOMAIN: &[u8] = b"mcp-ozon/ozon-campaign-launch/v1";
const MAX_TITLE_BYTES: usize = 128;
const MAX_SKUS: usize = 50;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OzonCampaignLaunchSpec {
    pub account_id: String,
    pub title: String,
    pub from_date: String,
    pub to_date: String,
    pub skus: Vec<u64>,
    pub weekly_budget_microrubles: u64,
    pub per_sku_spend_cap_microrubles: u64,
    pub initial_cpc_bid_microrubles: u64,
    pub max_cpc_bid_microrubles: u64,
    pub target_drr_percent: u8,
    pub target_position: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OzonCampaignLaunchManifest {
    pub manifest_digest: String,
    pub actor_id: String,
    pub policy_schema_version: u32,
    pub policy_revision: u64,
    pub policy_digest: String,
    pub spec: OzonCampaignLaunchSpec,
    pub create_request: OzonCampaignCreateRequest,
    pub products_request: OzonCampaignProductsRequest,
    pub activation_required: bool,
}

impl OzonCampaignLaunchManifest {
    /// Recomputes the signed intent and verifies that every provider request is
    /// an exact mechanical projection of that intent.
    pub(super) fn has_exact_integrity(&self) -> bool {
        self.has_exact_integrity_for_title(&self.spec.title)
    }

    pub(super) fn has_exact_persisted_integrity(&self, plan_id: &str) -> bool {
        let provider_title = provider_title_for_plan_id(plan_id);
        self.has_exact_integrity_for_title(&provider_title)
            // Rows created before migration 025 used the human title. Their
            // stable post-create stages remain safe because campaign_id is
            // already provider-issued; a legacy Approved row is never allowed
            // to start a create write by repository validation below.
            || self.has_exact_integrity()
    }

    fn has_exact_integrity_for_title(&self, expected_title: &str) -> bool {
        validate_spec(&self.spec).is_ok()
            && self.manifest_digest
                == make_manifest_digest(
                    &self.actor_id,
                    self.policy_schema_version,
                    self.policy_revision,
                    &self.policy_digest,
                    &self.spec,
                )
            && self.create_request.title == expected_title
            && self.create_request.from_date == self.spec.from_date
            && self.create_request.to_date == self.spec.to_date
            && self.create_request.weekly_budget == self.spec.weekly_budget_microrubles
            && self.create_request.placement == OzonPlacement::SearchAndCategory
            && self.create_request.product_autopilot_strategy == OzonCampaignStrategy::TargetBids
            && self.products_request.bids.as_slice()
                == [OzonCampaignProduct {
                    sku: self.spec.skus[0],
                    bid: Some(self.spec.initial_cpc_bid_microrubles),
                    target_cir: None,
                    top_position: None,
                }]
            && self.activation_required
    }
}

pub(super) fn provider_title_for_plan_id(plan_id: &str) -> String {
    format!("mcp-ozon-{plan_id}")
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum OzonLaunchPlanError {
    #[error("Ozon campaign launch spec имеет недопустимые данные")]
    InvalidSpec,
    #[error("Ozon campaign launch spec не совпадает с policy target")]
    PolicyMismatch,
}

#[allow(clippy::too_many_arguments)]
pub fn prepare_campaign_launch_manifest(
    actor_id: &str,
    policy_schema_version: u32,
    policy_revision: u64,
    policy_digest: &str,
    allowed_account_id: &str,
    allowed_skus: &[u64],
    allowed_weekly_budget_microrubles: u64,
    allowed_per_sku_spend_cap_microrubles: u64,
    allowed_initial_cpc_bid_microrubles: u64,
    allowed_max_cpc_bid_microrubles: u64,
    allowed_target_drr_percent: u8,
    allowed_target_position: u8,
    spec: OzonCampaignLaunchSpec,
) -> Result<OzonCampaignLaunchManifest, OzonLaunchPlanError> {
    validate_identity(actor_id)?;
    validate_digest(policy_digest)?;
    if policy_schema_version == 0 || policy_revision == 0 {
        return Err(OzonLaunchPlanError::InvalidSpec);
    }
    validate_spec(&spec)?;
    if spec.account_id != allowed_account_id
        || spec.skus != allowed_skus
        || spec.weekly_budget_microrubles != allowed_weekly_budget_microrubles
        || spec.per_sku_spend_cap_microrubles != allowed_per_sku_spend_cap_microrubles
        || spec.initial_cpc_bid_microrubles != allowed_initial_cpc_bid_microrubles
        || spec.max_cpc_bid_microrubles != allowed_max_cpc_bid_microrubles
        || spec.target_drr_percent != allowed_target_drr_percent
        || spec.target_position != allowed_target_position
    {
        return Err(OzonLaunchPlanError::PolicyMismatch);
    }

    // The reviewed CPC range uses TARGET_BIDS. DRR and TOP-30 remain local
    // guard/observation targets because Ozon cannot combine TARGET_CIR,
    // TARGET_BIDS and TOP_PROMOTION in one campaign.
    let create_request = OzonCampaignCreateRequest {
        title: spec.title.clone(),
        from_date: spec.from_date.clone(),
        to_date: spec.to_date.clone(),
        weekly_budget: spec.weekly_budget_microrubles,
        placement: OzonPlacement::SearchAndCategory,
        product_autopilot_strategy: OzonCampaignStrategy::TargetBids,
    };
    let products_request = OzonCampaignProductsRequest {
        bids: spec
            .skus
            .iter()
            .map(|sku| OzonCampaignProduct {
                sku: *sku,
                bid: Some(spec.initial_cpc_bid_microrubles),
                target_cir: None,
                top_position: None,
            })
            .collect(),
    };
    let manifest_digest = make_manifest_digest(
        actor_id,
        policy_schema_version,
        policy_revision,
        policy_digest,
        &spec,
    );
    Ok(OzonCampaignLaunchManifest {
        manifest_digest,
        actor_id: actor_id.to_owned(),
        policy_schema_version,
        policy_revision,
        policy_digest: policy_digest.to_owned(),
        spec,
        create_request,
        products_request,
        activation_required: true,
    })
}

fn validate_spec(spec: &OzonCampaignLaunchSpec) -> Result<(), OzonLaunchPlanError> {
    validate_identity(&spec.account_id)?;
    if spec.title.is_empty()
        || spec.title.len() > MAX_TITLE_BYTES
        || spec.title.trim() != spec.title
        || spec.title.bytes().any(|byte| byte.is_ascii_control())
        || spec.skus.is_empty()
        || spec.skus.len() > MAX_SKUS
        || spec.weekly_budget_microrubles == 0
        || spec.per_sku_spend_cap_microrubles == 0
        || spec.initial_cpc_bid_microrubles == 0
        || spec.initial_cpc_bid_microrubles > spec.max_cpc_bid_microrubles
        || spec
            .per_sku_spend_cap_microrubles
            .checked_mul(spec.skus.len() as u64)
            != Some(spec.weekly_budget_microrubles)
        || !(10..=100).contains(&spec.target_drr_percent)
        || !(1..=30).contains(&spec.target_position)
    {
        return Err(OzonLaunchPlanError::InvalidSpec);
    }
    let mut skus = BTreeSet::new();
    if spec.skus.iter().any(|sku| *sku == 0 || !skus.insert(*sku)) {
        return Err(OzonLaunchPlanError::InvalidSpec);
    }
    let from = NaiveDate::parse_from_str(&spec.from_date, "%Y-%m-%d")
        .map_err(|_| OzonLaunchPlanError::InvalidSpec)?;
    let to = NaiveDate::parse_from_str(&spec.to_date, "%Y-%m-%d")
        .map_err(|_| OzonLaunchPlanError::InvalidSpec)?;
    if to < from || (to - from).num_days() > 31 {
        return Err(OzonLaunchPlanError::InvalidSpec);
    }
    Ok(())
}

fn validate_identity(value: &str) -> Result<(), OzonLaunchPlanError> {
    if value.is_empty()
        || value.len() > 128
        || value.bytes().any(
            |byte| !matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'.'),
        )
    {
        Err(OzonLaunchPlanError::InvalidSpec)
    } else {
        Ok(())
    }
}

fn validate_digest(value: &str) -> Result<(), OzonLaunchPlanError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(OzonLaunchPlanError::InvalidSpec)
    }
}

fn make_manifest_digest(
    actor_id: &str,
    policy_schema_version: u32,
    policy_revision: u64,
    policy_digest: &str,
    spec: &OzonCampaignLaunchSpec,
) -> String {
    let mut hasher = Sha256::new();
    update_field(&mut hasher, MANIFEST_DOMAIN);
    update_field(&mut hasher, actor_id.as_bytes());
    update_field(&mut hasher, &policy_schema_version.to_be_bytes());
    update_field(&mut hasher, &policy_revision.to_be_bytes());
    update_field(&mut hasher, policy_digest.as_bytes());
    update_field(&mut hasher, spec.account_id.as_bytes());
    update_field(&mut hasher, spec.title.as_bytes());
    update_field(&mut hasher, spec.from_date.as_bytes());
    update_field(&mut hasher, spec.to_date.as_bytes());
    update_field(&mut hasher, &spec.weekly_budget_microrubles.to_be_bytes());
    update_field(
        &mut hasher,
        &spec.per_sku_spend_cap_microrubles.to_be_bytes(),
    );
    update_field(&mut hasher, &spec.initial_cpc_bid_microrubles.to_be_bytes());
    update_field(&mut hasher, &spec.max_cpc_bid_microrubles.to_be_bytes());
    update_field(&mut hasher, &[spec.target_drr_percent]);
    update_field(&mut hasher, &[spec.target_position]);
    for sku in &spec.skus {
        update_field(&mut hasher, &sku.to_be_bytes());
    }
    hasher
        .finalize()
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            use std::fmt::Write as _;
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
}

fn update_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> OzonCampaignLaunchSpec {
        OzonCampaignLaunchSpec {
            account_id: "furnitura_dlya_doma".to_owned(),
            title: "Diana potential 5 DRR15 2026-09-02".to_owned(),
            from_date: "2026-09-02".to_owned(),
            to_date: "2026-09-08".to_owned(),
            skus: vec![
                3_457_585_933,
                3_624_640_796,
                3_625_930_192,
                2_978_114_773,
                3_026_611_133,
            ],
            weekly_budget_microrubles: 10_000_000_000,
            per_sku_spend_cap_microrubles: 2_000_000_000,
            initial_cpc_bid_microrubles: 7_000_000,
            max_cpc_bid_microrubles: 12_000_000,
            target_drr_percent: 15,
            target_position: 10,
        }
    }

    #[test]
    fn exact_policy_creates_deterministic_target_bids_manifest() {
        let expected = spec();
        let manifest = prepare_campaign_launch_manifest(
            "rustam_magasumov",
            1,
            9,
            &"a".repeat(64),
            &expected.account_id,
            &expected.skus,
            expected.weekly_budget_microrubles,
            expected.per_sku_spend_cap_microrubles,
            expected.initial_cpc_bid_microrubles,
            expected.max_cpc_bid_microrubles,
            expected.target_drr_percent,
            expected.target_position,
            expected.clone(),
        )
        .unwrap();
        assert_eq!(manifest.manifest_digest.len(), 64);
        assert_eq!(
            manifest.create_request.product_autopilot_strategy,
            OzonCampaignStrategy::TargetBids
        );
        assert_eq!(manifest.products_request.bids.len(), 5);
        assert!(manifest.products_request.bids.iter().all(|product| {
            product.bid == Some(7_000_000)
                && product.target_cir.is_none()
                && product.top_position.is_none()
        }));
        let repeated = prepare_campaign_launch_manifest(
            "rustam_magasumov",
            1,
            9,
            &"a".repeat(64),
            &expected.account_id,
            &expected.skus,
            expected.weekly_budget_microrubles,
            expected.per_sku_spend_cap_microrubles,
            expected.initial_cpc_bid_microrubles,
            expected.max_cpc_bid_microrubles,
            expected.target_drr_percent,
            expected.target_position,
            expected.clone(),
        )
        .unwrap();
        assert_eq!(manifest.manifest_digest, repeated.manifest_digest);
    }

    #[test]
    fn any_budget_sku_or_guard_drift_is_rejected() {
        let expected = spec();
        for changed in [
            OzonCampaignLaunchSpec {
                weekly_budget_microrubles: 9_000_000_000,
                per_sku_spend_cap_microrubles: 1_800_000_000,
                ..expected.clone()
            },
            OzonCampaignLaunchSpec {
                target_drr_percent: 16,
                ..expected.clone()
            },
            OzonCampaignLaunchSpec {
                target_position: 12,
                ..expected.clone()
            },
        ] {
            assert_eq!(
                prepare_campaign_launch_manifest(
                    "rustam_magasumov",
                    1,
                    9,
                    &"a".repeat(64),
                    &expected.account_id,
                    &expected.skus,
                    expected.weekly_budget_microrubles,
                    expected.per_sku_spend_cap_microrubles,
                    expected.initial_cpc_bid_microrubles,
                    expected.max_cpc_bid_microrubles,
                    expected.target_drr_percent,
                    expected.target_position,
                    changed,
                ),
                Err(OzonLaunchPlanError::PolicyMismatch)
            );
        }
    }

    #[test]
    fn malformed_identity_digest_dates_and_spec_are_rejected() {
        let expected = spec();
        for actor_id in ["", "bad actor", "../actor", &"x".repeat(129)] {
            assert_eq!(
                prepare_campaign_launch_manifest(
                    actor_id,
                    1,
                    9,
                    &"a".repeat(64),
                    &expected.account_id,
                    &expected.skus,
                    expected.weekly_budget_microrubles,
                    expected.per_sku_spend_cap_microrubles,
                    expected.initial_cpc_bid_microrubles,
                    expected.max_cpc_bid_microrubles,
                    expected.target_drr_percent,
                    expected.target_position,
                    expected.clone(),
                ),
                Err(OzonLaunchPlanError::InvalidSpec)
            );
        }
        for digest in ["", &"A".repeat(64), &"a".repeat(63)] {
            assert_eq!(
                prepare_campaign_launch_manifest(
                    "actor",
                    1,
                    9,
                    digest,
                    &expected.account_id,
                    &expected.skus,
                    expected.weekly_budget_microrubles,
                    expected.per_sku_spend_cap_microrubles,
                    expected.initial_cpc_bid_microrubles,
                    expected.max_cpc_bid_microrubles,
                    expected.target_drr_percent,
                    expected.target_position,
                    expected.clone(),
                ),
                Err(OzonLaunchPlanError::InvalidSpec)
            );
        }
        for (schema, revision) in [(0, 9), (1, 0)] {
            assert_eq!(
                prepare_campaign_launch_manifest(
                    "actor",
                    schema,
                    revision,
                    &"a".repeat(64),
                    &expected.account_id,
                    &expected.skus,
                    expected.weekly_budget_microrubles,
                    expected.per_sku_spend_cap_microrubles,
                    expected.initial_cpc_bid_microrubles,
                    expected.max_cpc_bid_microrubles,
                    expected.target_drr_percent,
                    expected.target_position,
                    expected.clone(),
                ),
                Err(OzonLaunchPlanError::InvalidSpec)
            );
        }

        let mut invalid_specs = Vec::new();
        invalid_specs.push(OzonCampaignLaunchSpec {
            account_id: "bad account".to_owned(),
            ..expected.clone()
        });
        invalid_specs.push(OzonCampaignLaunchSpec {
            title: String::new(),
            ..expected.clone()
        });
        invalid_specs.push(OzonCampaignLaunchSpec {
            skus: Vec::new(),
            weekly_budget_microrubles: 0,
            ..expected.clone()
        });
        invalid_specs.push(OzonCampaignLaunchSpec {
            skus: vec![1, 1],
            weekly_budget_microrubles: 4_000_000_000,
            ..expected.clone()
        });
        invalid_specs.push(OzonCampaignLaunchSpec {
            from_date: "bad".to_owned(),
            ..expected.clone()
        });
        invalid_specs.push(OzonCampaignLaunchSpec {
            to_date: "2026-09-01".to_owned(),
            ..expected.clone()
        });
        invalid_specs.push(OzonCampaignLaunchSpec {
            to_date: "2026-10-31".to_owned(),
            ..expected.clone()
        });
        for invalid in invalid_specs {
            assert_eq!(
                prepare_campaign_launch_manifest(
                    "actor",
                    1,
                    9,
                    &"a".repeat(64),
                    &expected.account_id,
                    &expected.skus,
                    expected.weekly_budget_microrubles,
                    expected.per_sku_spend_cap_microrubles,
                    expected.initial_cpc_bid_microrubles,
                    expected.max_cpc_bid_microrubles,
                    expected.target_drr_percent,
                    expected.target_position,
                    invalid,
                ),
                Err(OzonLaunchPlanError::InvalidSpec)
            );
        }
    }
}
