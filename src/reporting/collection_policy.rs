//! Collection scope is independent of report recipients and mail credentials.

use std::collections::BTreeSet;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use crate::config::AccessRegistry;

use super::{BUSINESS_TIMEZONE, policy::DailyReportPolicy};

const MAX_POLICY_BYTES: usize = 1024 * 1024;
const MAX_ACCOUNTS: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionPolicy {
    pub version: u32,
    pub enabled: bool,
    pub timezone: String,
    pub account_ids: Vec<String>,
}

impl CollectionPolicy {
    /// Accepts the standalone collection format or a fully validated legacy
    /// delivery policy. Never resolves email addresses or marketplace secrets.
    pub fn from_slice(bytes: &[u8], registry: &AccessRegistry) -> Result<Self> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Input {
            Collection(CollectionPolicy),
            Legacy(DailyReportPolicy),
        }
        ensure!(
            bytes.len() <= MAX_POLICY_BYTES,
            "collection policy exceeds 1 MiB"
        );
        let policy = match serde_json::from_slice(bytes)
            .context("collection policy must be valid strict JSON")?
        {
            Input::Collection(policy) => policy,
            Input::Legacy(policy) => {
                policy.validate(registry)?;
                Self {
                    version: policy.version,
                    enabled: policy.enabled,
                    timezone: policy.timezone,
                    account_ids: policy
                        .audiences
                        .into_iter()
                        .flat_map(|audience| audience.managers)
                        .flat_map(|manager| manager.account_ids)
                        .collect(),
                }
            }
        };
        policy.validate(registry)?;
        Ok(policy)
    }

    pub fn validate(&self, registry: &AccessRegistry) -> Result<()> {
        ensure!(self.version == 1, "unsupported collection policy version");
        ensure!(
            self.timezone == BUSINESS_TIMEZONE,
            "unsupported collection timezone"
        );
        ensure!(
            !self.account_ids.is_empty() && self.account_ids.len() <= MAX_ACCOUNTS,
            "collection account count is outside the supported range"
        );
        let mut seen = BTreeSet::new();
        for id in &self.account_ids {
            ensure!(
                !id.is_empty()
                    && id.len() <= 128
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-')),
                "invalid collection account identifier"
            );
            ensure!(seen.insert(id), "duplicate collection account");
            ensure!(
                registry.accounts.iter().any(|account| account.id == *id),
                "unknown collection account"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn registry() -> AccessRegistry {
        serde_json::from_value(json!({"version":1,"actors":[
            {"id":"manager","name":"Manager","role":"manager"}
        ],"accounts":[
            {"id":"ozon","organization":"Ozon","marketplace":"ozon","seller_client_id":"1","manager_id":"manager"},
            {"id":"wb","organization":"WB","marketplace":"wildberries","seller_client_id":"2","manager_id":"manager"}
        ]})).unwrap()
    }

    fn document() -> Value {
        json!({"version":1,"enabled":true,"timezone":"Asia/Yekaterinburg","account_ids":["ozon","wb"]})
    }

    fn parse(value: &Value) -> Result<CollectionPolicy> {
        CollectionPolicy::from_slice(&serde_json::to_vec(value).unwrap(), &registry())
    }

    #[test]
    fn collection_needs_no_mail_routes_and_preserves_explicit_scope() {
        let policy = parse(&document()).unwrap();
        assert!(policy.enabled);
        assert_eq!(policy.account_ids, ["ozon", "wb"]);
        assert!(
            serde_json::to_value(policy)
                .unwrap()
                .get("audiences")
                .is_none()
        );
    }

    #[test]
    fn rejects_ambiguous_unbounded_or_unknown_scope() {
        for (key, value) in [
            ("version", json!(2)),
            ("timezone", json!("UTC")),
            ("account_ids", json!([])),
            ("account_ids", json!(["ozon", "ozon"])),
            ("account_ids", json!(["missing"])),
            ("account_ids", json!(["bad id"])),
            ("account_ids", json!(vec!["ozon"; 65])),
            ("audiences", json!([])),
            ("sender_email_env", json!("SENDER")),
        ] {
            let mut value_doc = document();
            value_doc[key] = value;
            assert!(parse(&value_doc).is_err(), "accepted {key}");
        }
        assert!(
            CollectionPolicy::from_slice(&vec![b' '; MAX_POLICY_BYTES + 1], &registry()).is_err()
        );
    }

    #[test]
    fn legacy_routes_remain_supported_but_ownership_is_not_bypassed() {
        let mut legacy = json!({"version":1,"enabled":false,"timezone":"Asia/Yekaterinburg",
            "sender_email_env":"SENDER","audiences":[{"id":"owner","email_env":"RECIPIENT",
            "managers":[{"actor_id":"manager","account_ids":["ozon","wb"]}]}]});
        assert_eq!(parse(&legacy).unwrap().account_ids, ["ozon", "wb"]);
        legacy["audiences"][0]["managers"][0]["actor_id"] = json!("other");
        assert!(parse(&legacy).is_err());
    }
}
