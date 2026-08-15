use std::{
    collections::BTreeSet,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::config::{AccessRegistry, Marketplace};

const CONTROL_POLICY_MAX_BYTES: u64 = 1_048_576;
const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_ACTORS: usize = 256;
const MAX_TARGETS_PER_ACTOR: usize = 1_000;
const MAX_SKUS_PER_TARGET: usize = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ControlMode {
    /// The scaffold can describe policy, but cannot create or execute plans.
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlPolicy {
    pub version: u32,
    pub mode: ControlMode,
    #[serde(default)]
    pub actors: Vec<ActorControlPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActorControlPolicy {
    pub actor_id: String,
    #[serde(default)]
    pub targets: Vec<ControlTargetPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlTargetPolicy {
    pub account_id: String,
    pub campaign_id: u64,
    pub skus: Vec<u64>,
    pub bid_limits: BidLimits,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BidLimits {
    pub min_minor: u64,
    pub max_minor: u64,
    pub max_delta_percent: u8,
}

impl ControlPolicy {
    pub fn load(path: impl Into<PathBuf>, registry: &AccessRegistry) -> Result<Self> {
        let path = path.into();
        let bytes = read_policy_bytes(&path)?;
        Self::from_slice(&bytes, &path, registry)
    }

    fn from_slice(bytes: &[u8], path: &Path, registry: &AccessRegistry) -> Result<Self> {
        let policy: Self = serde_json::from_slice(bytes)
            .with_context(|| format!("не удалось разобрать control policy {}", path.display()))?;
        policy.validate(registry)?;
        Ok(policy)
    }

    pub fn actor_policy(&self, actor_id: &str) -> Option<&ActorControlPolicy> {
        self.actors.iter().find(|actor| actor.actor_id == actor_id)
    }

    fn validate(&self, registry: &AccessRegistry) -> Result<()> {
        if self.version != 1 {
            bail!("control policy version должна быть равна 1");
        }
        if self.actors.len() > MAX_ACTORS {
            bail!("control policy содержит слишком много actor bindings");
        }

        let mut actor_ids = BTreeSet::new();
        for actor_policy in &self.actors {
            validate_identifier("actor_id", &actor_policy.actor_id)?;
            if !actor_ids.insert(actor_policy.actor_id.as_str()) {
                bail!("control policy содержит повтор actor_id");
            }
            let actor = registry.actor(&actor_policy.actor_id)?;
            if actor_policy.targets.len() > MAX_TARGETS_PER_ACTOR {
                bail!("control policy содержит слишком много targets для actor");
            }

            let mut targets = BTreeSet::new();
            for target in &actor_policy.targets {
                validate_identifier("account_id", &target.account_id)?;
                if target.campaign_id == 0 {
                    bail!("campaign_id должен быть положительным");
                }
                if !targets.insert((target.account_id.as_str(), target.campaign_id)) {
                    bail!("control policy содержит повтор account_id/campaign_id");
                }
                let account = registry
                    .accounts
                    .iter()
                    .find(|account| account.id == target.account_id)
                    .with_context(|| {
                        format!(
                            "control policy ссылается на неизвестный account_id {}",
                            target.account_id
                        )
                    })?;
                if !matches!(account.marketplace, Marketplace::Ozon)
                    || account
                        .ozon
                        .as_ref()
                        .and_then(|ozon| ozon.performance.as_ref())
                        .is_none()
                {
                    bail!(
                        "control policy target должен ссылаться на Ozon account с Performance binding"
                    );
                }
                if !actor.can_access_account(account) {
                    bail!("actor не имеет базового доступа к account из control policy");
                }
                validate_target(target)?;
            }
        }
        Ok(())
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

fn validate_identifier(field: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_IDENTIFIER_BYTES
        || value.bytes().any(
            |byte| !matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'.'),
        )
    {
        bail!("{field} имеет недопустимый формат");
    }
    Ok(())
}

fn validate_target(target: &ControlTargetPolicy) -> Result<()> {
    if target.skus.is_empty() || target.skus.len() > MAX_SKUS_PER_TARGET {
        bail!("target должен содержать от 1 до {MAX_SKUS_PER_TARGET} SKU");
    }
    let mut skus = BTreeSet::new();
    if target
        .skus
        .iter()
        .any(|sku| *sku == 0 || !skus.insert(*sku))
    {
        bail!("SKU должны быть положительными и уникальными");
    }
    if target.bid_limits.min_minor == 0 || target.bid_limits.max_minor < target.bid_limits.min_minor
    {
        bail!("bid_limits должны задавать положительный диапазон min_minor..=max_minor");
    }
    if !(1..=100).contains(&target.bid_limits.max_delta_percent) {
        bail!("max_delta_percent должен быть от 1 до 100");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::config::{
        Actor, MarketplaceAccount, OidcIdentity, OzonAccount, OzonPerformanceAccount, Role, StoreId,
    };

    static POLICY_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn registry() -> AccessRegistry {
        AccessRegistry {
            version: 1,
            actors: vec![Actor {
                id: "manager".to_owned(),
                name: "Manager".to_owned(),
                role: Role::Manager,
                account_ids: BTreeSet::new(),
                oidc: Some(OidcIdentity {
                    username: Some("manager".to_owned()),
                    ..OidcIdentity::default()
                }),
            }],
            accounts: vec![MarketplaceAccount {
                id: "ozon_one".to_owned(),
                organization: "Example".to_owned(),
                marketplace: Marketplace::Ozon,
                seller_client_id: "seller".to_owned(),
                manager_id: "manager".to_owned(),
                ozon: Some(OzonAccount {
                    store_id: StoreId::from("store_one"),
                    client_id_env: "UNUSED_CLIENT_ID".to_owned(),
                    api_key_env: "UNUSED_API_KEY".to_owned(),
                    performance: Some(OzonPerformanceAccount {
                        client_id_env: "UNUSED_PERF_ID".to_owned(),
                        client_secret_env: "UNUSED_PERF_SECRET".to_owned(),
                    }),
                }),
                wildberries: None,
            }],
        }
    }

    fn valid_policy() -> serde_json::Value {
        serde_json::json!({
            "version": 1,
            "mode": "disabled",
            "actors": [{
                "actor_id": "manager",
                "targets": [{
                    "account_id": "ozon_one",
                    "campaign_id": 42,
                    "skus": [1001, 1002],
                    "bid_limits": {
                        "min_minor": 100,
                        "max_minor": 5000,
                        "max_delta_percent": 5
                    }
                }]
            }]
        })
    }

    fn parse(value: serde_json::Value) -> Result<ControlPolicy> {
        parse_with_registry(value, &registry())
    }

    fn parse_with_registry(
        value: serde_json::Value,
        registry: &AccessRegistry,
    ) -> Result<ControlPolicy> {
        ControlPolicy::from_slice(
            &serde_json::to_vec(&value).expect("test policy serializes"),
            Path::new("test-control-policy.json"),
            registry,
        )
    }

    #[test]
    fn valid_disabled_policy_is_accepted() {
        let policy = parse(valid_policy()).expect("valid policy");
        assert_eq!(policy.mode, ControlMode::Disabled);
        assert_eq!(policy.actor_policy("manager").unwrap().targets.len(), 1);
        assert!(policy.actor_policy("absent").is_none());
    }

    #[test]
    fn policy_rejects_credentials_and_unsafe_or_duplicate_targets() {
        let mut with_secret = valid_policy();
        with_secret["api_token"] = serde_json::json!("must-not-be-accepted");
        assert!(parse(with_secret).is_err());

        let mut duplicate = valid_policy();
        let target = duplicate["actors"][0]["targets"][0].clone();
        duplicate["actors"][0]["targets"]
            .as_array_mut()
            .unwrap()
            .push(target);
        assert!(parse(duplicate).is_err());

        let mut duplicate_sku = valid_policy();
        duplicate_sku["actors"][0]["targets"][0]["skus"] = serde_json::json!([1001, 1001]);
        assert!(parse(duplicate_sku).is_err());
    }

    #[test]
    fn policy_bounds_actor_and_target_collections_and_rejects_duplicate_actors() {
        let actor = serde_json::json!({ "actor_id": "manager", "targets": [] });

        let mut too_many_actors = valid_policy();
        too_many_actors["actors"] =
            serde_json::Value::Array((0..=MAX_ACTORS).map(|_| actor.clone()).collect());
        assert!(parse(too_many_actors).is_err());

        let mut duplicate_actor = valid_policy();
        let repeated_actor = duplicate_actor["actors"][0].clone();
        duplicate_actor["actors"]
            .as_array_mut()
            .unwrap()
            .push(repeated_actor);
        assert!(parse(duplicate_actor).is_err());

        let mut too_many_targets = valid_policy();
        let target = too_many_targets["actors"][0]["targets"][0].clone();
        too_many_targets["actors"][0]["targets"] = serde_json::Value::Array(
            (0..=MAX_TARGETS_PER_ACTOR)
                .map(|_| target.clone())
                .collect(),
        );
        assert!(parse(too_many_targets).is_err());
    }

    #[test]
    fn policy_requires_performance_binding_and_explicit_actor_access() {
        let mut without_performance = registry();
        without_performance.accounts[0]
            .ozon
            .as_mut()
            .unwrap()
            .performance = None;
        assert!(parse_with_registry(valid_policy(), &without_performance).is_err());

        let mut wrong_marketplace = registry();
        wrong_marketplace.accounts[0].marketplace = Marketplace::Wildberries;
        assert!(parse_with_registry(valid_policy(), &wrong_marketplace).is_err());

        let mut denied_registry = registry();
        denied_registry.actors.push(Actor {
            id: "viewer".to_owned(),
            name: "Viewer".to_owned(),
            role: Role::Manager,
            account_ids: BTreeSet::new(),
            oidc: None,
        });
        let mut denied_policy = valid_policy();
        denied_policy["actors"][0]["actor_id"] = serde_json::json!("viewer");
        assert!(parse_with_registry(denied_policy, &denied_registry).is_err());
    }

    #[test]
    fn policy_rejects_invalid_identifiers_and_oversized_files() {
        for invalid_actor in ["", "../manager", &"a".repeat(MAX_IDENTIFIER_BYTES + 1)] {
            let mut value = valid_policy();
            value["actors"][0]["actor_id"] = serde_json::json!(invalid_actor);
            assert!(parse(value).is_err(), "actor {invalid_actor:?} must fail");
        }

        let mut invalid_account = valid_policy();
        invalid_account["actors"][0]["targets"][0]["account_id"] =
            serde_json::json!("ozon account");
        assert!(parse(invalid_account).is_err());

        let id = POLICY_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mcp-control-oversized-policy-{}-{id}.json",
            std::process::id()
        ));
        std::fs::write(&path, vec![b' '; (CONTROL_POLICY_MAX_BYTES + 1) as usize]).unwrap();
        let result = read_policy_bytes(&path);
        std::fs::remove_file(path).unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn policy_file_io_errors_are_reported_without_panicking() {
        let id = POLICY_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let missing = std::env::temp_dir().join(format!(
            "mcp-control-missing-policy-{}-{id}.json",
            std::process::id()
        ));
        assert!(read_policy_bytes(&missing).is_err());

        let directory = std::env::temp_dir().join(format!(
            "mcp-control-policy-directory-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        let result = read_policy_bytes(&directory);
        std::fs::remove_dir(directory).unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn policy_rejects_invalid_version_actor_account_campaign_and_limits() {
        enum Mutation {
            Version,
            Actor,
            Account,
            Campaign,
            Sku,
            Minimum,
            Range,
            Delta,
        }

        let mutations = [
            ("version", Mutation::Version, serde_json::json!(2)),
            ("actor", Mutation::Actor, serde_json::json!("unknown")),
            ("account", Mutation::Account, serde_json::json!("unknown")),
            ("campaign", Mutation::Campaign, serde_json::json!(0)),
            ("sku", Mutation::Sku, serde_json::json!([])),
            ("min", Mutation::Minimum, serde_json::json!(0)),
            ("range", Mutation::Range, serde_json::json!(50)),
            ("delta", Mutation::Delta, serde_json::json!(0)),
        ];
        for (label, kind, replacement) in mutations {
            let mut value = valid_policy();
            match kind {
                Mutation::Version => value["version"] = replacement,
                Mutation::Actor => value["actors"][0]["actor_id"] = replacement,
                Mutation::Account => value["actors"][0]["targets"][0]["account_id"] = replacement,
                Mutation::Campaign => value["actors"][0]["targets"][0]["campaign_id"] = replacement,
                Mutation::Sku => value["actors"][0]["targets"][0]["skus"] = replacement,
                Mutation::Minimum => {
                    value["actors"][0]["targets"][0]["bid_limits"]["min_minor"] = replacement
                }
                Mutation::Range => {
                    value["actors"][0]["targets"][0]["bid_limits"]["max_minor"] = replacement
                }
                Mutation::Delta => {
                    value["actors"][0]["targets"][0]["bid_limits"]["max_delta_percent"] =
                        replacement
                }
            }
            assert!(parse(value).is_err(), "mutation {label} must fail");
        }
    }
}
