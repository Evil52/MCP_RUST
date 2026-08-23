use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

#[cfg(test)]
use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::AccessRegistry;

mod validation;

const CONTROL_POLICY_MAX_BYTES: u64 = 1_048_576;
const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_ACTORS: usize = 256;
const MAX_TARGETS_PER_ACTOR: usize = 1_000;
const MAX_SKUS_PER_TARGET: usize = 1_000;
const MAX_WB_NM_IDS_PER_TARGET: usize = 50;
const MAX_APPROVERS_PER_TARGET: usize = 16;
const MAX_WB_SIGNED_ID: u64 = i64::MAX as u64;
const MAX_ACTIONS_PER_HOUR: u32 = 60;
const MAX_ACTIONS_PER_DAY: u32 = 500;
const MAX_CUMULATIVE_ABS_DELTA_KOPECKS_PER_DAY: u64 = i64::MAX as u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ControlMode {
    /// Policy can be inspected, but plans and writes are unavailable.
    Disabled,
    /// Plans may be prepared and audited, but cannot be applied.
    PlanOnly,
    /// Plans may be prepared and applied when every runtime gate is present.
    Enabled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlPolicy {
    /// Version of the policy document schema. This is not a policy revision.
    pub version: u32,
    /// Monotonic operator-controlled revision of the effective policy.
    pub revision: u64,
    pub mode: ControlMode,
    #[serde(default)]
    pub(super) actors: Vec<ActorControlPolicy>,
    /// Digest of the exact policy bytes loaded by this process.
    #[serde(skip)]
    digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ActorControlPolicy {
    pub(super) actor_id: String,
    #[serde(default)]
    pub(super) targets: Vec<ControlTargetPolicy>,
    #[serde(default)]
    pub(super) wb_promotion_bid_targets: Vec<WbPromotionBidTargetPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ControlTargetPolicy {
    pub(super) account_id: String,
    pub(super) campaign_id: u64,
    pub(super) skus: Vec<u64>,
    pub(super) bid_limits: BidLimits,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BidLimits {
    pub(super) min_minor: u64,
    pub(super) max_minor: u64,
    pub(super) max_delta_percent: u8,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum WbBidPlacement {
    Combined,
    Search,
    Recommendations,
}

impl WbBidPlacement {
    #[must_use]
    pub const fn as_api_str(self) -> &'static str {
        match self {
            Self::Combined => "combined",
            Self::Search => "search",
            Self::Recommendations => "recommendations",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WbPromotionBidTargetPolicy {
    pub(super) account_id: String,
    /// Exact cabinet identity, duplicated intentionally so policy bytes and
    /// every plan digest are invalidated by an account rebind across restart.
    pub(super) seller_sid: String,
    pub(super) advert_id: u64,
    pub(super) nm_ids: Vec<u64>,
    pub(super) placements: Vec<WbBidPlacement>,
    pub(super) bid_limits_kopecks: BidLimits,
    /// Explicit identities allowed to approve a plan prepared by this actor.
    /// Self-approval is rejected both here and by the database transition guard.
    pub(super) approver_actor_ids: Vec<String>,
    pub(super) action_limits: WbActionLimits,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbActionLimits {
    pub max_actions_per_hour: u32,
    pub max_actions_per_day: u32,
    pub cooldown_seconds: u32,
    pub max_cumulative_abs_delta_kopecks_per_day: u64,
}

impl ControlPolicy {
    pub fn load(path: impl Into<PathBuf>, registry: &AccessRegistry) -> Result<Self> {
        let path = path.into();
        let bytes = read_policy_bytes(&path)?;
        Self::from_slice(&bytes, &path, registry)
    }

    fn from_slice(bytes: &[u8], path: &Path, registry: &AccessRegistry) -> Result<Self> {
        let mut policy: Self = serde_json::from_slice(bytes)
            .with_context(|| format!("не удалось разобрать control policy {}", path.display()))?;
        validation::validate_policy(&policy, registry)?;
        policy.digest =
            Sha256::digest(bytes)
                .iter()
                .fold(String::with_capacity(64), |mut output, byte| {
                    use std::fmt::Write as _;
                    write!(output, "{byte:02x}").expect("writing to String cannot fail");
                    output
                });
        Ok(policy)
    }

    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }

    #[must_use]
    pub(super) fn actor_policy(&self, actor_id: &str) -> Option<&ActorControlPolicy> {
        self.actors.iter().find(|actor| actor.actor_id == actor_id)
    }
}

fn read_policy_bytes(path: &Path) -> Result<Vec<u8>> {
    let file = File::open(path)
        .with_context(|| format!("не удалось прочитать control policy {}", path.display()))?;
    let mut bytes = Vec::new();
    file.take(CONTROL_POLICY_MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("не удалось прочитать control policy {}", path.display()))?;
    if bytes.len() as u64 > CONTROL_POLICY_MAX_BYTES {
        bail!("control policy превышает безопасный лимит");
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests;
