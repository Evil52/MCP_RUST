use std::collections::BTreeSet;

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

use crate::config::{AccessRegistry, Role};

const POLICY_VERSION: u32 = 1;
const POLICY_MAX_BYTES: usize = 1024 * 1024;
const MAX_AUDIENCES: usize = 64;
const MAX_MANAGERS_PER_AUDIENCE: usize = 64;
const MAX_ACCOUNTS_PER_MANAGER: usize = 64;
const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_ENV_NAME_BYTES: usize = 128;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DailyReportPolicy {
    pub version: u32,
    pub enabled: bool,
    pub timezone: String,
    pub sender_email_env: String,
    pub audiences: Vec<AudiencePolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AudiencePolicy {
    pub id: String,
    pub email_env: String,
    pub managers: Vec<ManagerScope>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagerScope {
    pub actor_id: String,
    pub account_ids: BTreeSet<String>,
}

impl DailyReportPolicy {
    /// Parses a bounded, credential-free routing policy and validates every
    /// manager/account edge against the authoritative access registry.
    pub fn from_slice(bytes: &[u8], registry: &AccessRegistry) -> Result<Self> {
        ensure!(
            bytes.len() <= POLICY_MAX_BYTES,
            "daily report policy exceeds 1 MiB"
        );
        let policy: Self = serde_json::from_slice(bytes)
            .context("daily report policy must be valid strict JSON")?;
        policy.validate(registry)?;
        Ok(policy)
    }

    pub fn validate(&self, registry: &AccessRegistry) -> Result<()> {
        ensure!(
            self.version == POLICY_VERSION,
            "unsupported daily report policy version: {}",
            self.version
        );
        ensure!(
            self.timezone == "Asia/Yekaterinburg",
            "daily reports must use Asia/Yekaterinburg"
        );
        validate_env_name(&self.sender_email_env)?;
        ensure!(
            !self.audiences.is_empty() && self.audiences.len() <= MAX_AUDIENCES,
            "daily report audiences count is outside the supported range"
        );

        let mut audience_ids = BTreeSet::new();
        let mut email_envs = BTreeSet::new();
        let mut routed_accounts = BTreeSet::new();
        for audience in &self.audiences {
            validate_identifier(&audience.id, "audience id")?;
            ensure!(
                audience_ids.insert(audience.id.as_str()),
                "daily report audience ids must be unique"
            );
            validate_env_name(&audience.email_env)?;
            ensure!(
                audience.email_env != self.sender_email_env,
                "sender and recipient email env names must be different"
            );
            ensure!(
                email_envs.insert(audience.email_env.as_str()),
                "daily report recipient email env names must be unique"
            );
            ensure!(
                !audience.managers.is_empty()
                    && audience.managers.len() <= MAX_MANAGERS_PER_AUDIENCE,
                "manager scope count is outside the supported range"
            );

            let mut manager_ids = BTreeSet::new();
            for manager in &audience.managers {
                validate_identifier(&manager.actor_id, "manager actor id")?;
                ensure!(
                    manager_ids.insert(manager.actor_id.as_str()),
                    "manager actor ids must be unique inside an audience"
                );
                ensure!(
                    !manager.account_ids.is_empty()
                        && manager.account_ids.len() <= MAX_ACCOUNTS_PER_MANAGER,
                    "manager account count is outside the supported range"
                );
                let actor = registry
                    .actor(&manager.actor_id)
                    .with_context(|| format!("unknown report manager {}", manager.actor_id))?;
                ensure!(
                    actor.role == Role::Manager,
                    "report manager {} must have the manager role",
                    manager.actor_id
                );

                for account_id in &manager.account_ids {
                    validate_identifier(account_id, "account id")?;
                    let account = registry
                        .accounts
                        .iter()
                        .find(|account| account.id == *account_id)
                        .with_context(|| format!("unknown report account {account_id}"))?;
                    ensure!(
                        account.manager_id == manager.actor_id,
                        "account {account_id} is not owned by manager {}",
                        manager.actor_id
                    );
                    ensure!(
                        routed_accounts.insert(account_id.as_str()),
                        "report accounts must not be routed more than once"
                    );
                }
            }
        }
        Ok(())
    }
}

fn validate_identifier(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_IDENTIFIER_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        bail!("{label} must be a non-empty bounded identifier");
    }
    Ok(())
}

fn validate_env_name(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_ENV_NAME_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        || value.as_bytes()[0].is_ascii_digit()
    {
        bail!("email environment variable name is invalid");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use crate::config::AccessRegistry;

    use super::{DailyReportPolicy, POLICY_MAX_BYTES};

    fn registry() -> AccessRegistry {
        serde_json::from_value(json!({
            "version": 1,
            "actors": [
                {"id":"diana_serafimovich","name":"Diana","role":"manager","oidc":{"username":"diana"}},
                {"id":"wb6","name":"Vahrusheva / Torsunova","role":"manager","oidc":{"username":"wb6"}},
                {"id":"admin","name":"Admin","role":"admin","oidc":{"username":"admin"}}
            ],
            "accounts": [
                {
                    "id":"furnitura_dlya_doma","organization":"Ozon store","marketplace":"ozon",
                    "seller_client_id":"1","manager_id":"diana_serafimovich",
                    "ozon":{"store_id":"ozon-1","client_id_env":"OZON_ID","api_key_env":"OZON_KEY"}
                },
                {
                    "id":"ip_domnyshev_wb","organization":"WB store","marketplace":"wildberries",
                    "seller_client_id":"2","manager_id":"wb6",
                    "wildberries":{"api_token_env":"WB_TOKEN"}
                }
            ]
        }))
        .unwrap()
    }

    fn policy_json() -> Value {
        json!({
            "version": 1,
            "enabled": false,
            "timezone": "Asia/Yekaterinburg",
            "sender_email_env": "DAILY_REPORT_SENDER_EMAIL",
            "audiences": [{
                "id": "pilot_owner",
                "email_env": "DAILY_REPORT_PILOT_RECIPIENT_EMAIL",
                "managers": [
                    {"actor_id":"diana_serafimovich","account_ids":["furnitura_dlya_doma"]},
                    {"actor_id":"wb6","account_ids":["ip_domnyshev_wb"]}
                ]
            }]
        })
    }

    fn parse(value: &Value) -> anyhow::Result<DailyReportPolicy> {
        DailyReportPolicy::from_slice(&serde_json::to_vec(value).unwrap(), &registry())
    }

    #[test]
    fn pilot_policy_contains_diana_and_vahrusheva_but_stays_disabled() {
        let policy = parse(&policy_json()).unwrap();
        assert!(!policy.enabled);
        assert_eq!(policy.audiences[0].managers.len(), 2);
    }

    #[test]
    fn strict_shape_version_timezone_and_size_fail_closed() {
        let mut unknown = policy_json();
        unknown["unexpected"] = json!(true);
        assert!(parse(&unknown).is_err());

        for (field, value) in [("version", json!(2)), ("timezone", json!("UTC"))] {
            let mut invalid = policy_json();
            invalid[field] = value;
            assert!(parse(&invalid).is_err());
        }
        assert!(
            DailyReportPolicy::from_slice(&vec![b' '; POLICY_MAX_BYTES + 1], &registry()).is_err()
        );
    }

    #[test]
    fn email_env_names_and_audience_bounds_are_validated() {
        for (field, value) in [
            ("sender_email_env", json!("")),
            ("sender_email_env", json!("1SENDER")),
            ("sender_email_env", json!("lowercase")),
            ("sender_email_env", json!("X".repeat(129))),
        ] {
            let mut invalid = policy_json();
            invalid[field] = value;
            assert!(parse(&invalid).is_err());
        }

        let mut empty = policy_json();
        empty["audiences"] = json!([]);
        assert!(parse(&empty).is_err());

        let audience = policy_json()["audiences"][0].clone();
        let mut too_many = policy_json();
        too_many["audiences"] = Value::Array(vec![audience; 65]);
        assert!(parse(&too_many).is_err());
    }

    #[test]
    fn duplicate_or_invalid_delivery_routes_are_rejected() {
        let mut invalid_id = policy_json();
        invalid_id["audiences"][0]["id"] = json!("bad id");
        assert!(parse(&invalid_id).is_err());

        let mut same_env = policy_json();
        same_env["audiences"][0]["email_env"] = json!("DAILY_REPORT_SENDER_EMAIL");
        assert!(parse(&same_env).is_err());

        let audience = policy_json()["audiences"][0].clone();
        let mut duplicate_audience = policy_json();
        duplicate_audience["audiences"] = json!([audience.clone(), audience]);
        assert!(parse(&duplicate_audience).is_err());

        let mut duplicate_env = policy_json();
        let mut second = duplicate_env["audiences"][0].clone();
        second["id"] = json!("second");
        duplicate_env["audiences"]
            .as_array_mut()
            .unwrap()
            .push(second);
        assert!(parse(&duplicate_env).is_err());
    }

    #[test]
    fn manager_and_account_edges_are_authoritative() {
        let mutations = [
            ("actor_id", json!("missing")),
            ("actor_id", json!("admin")),
            ("actor_id", json!("bad actor")),
        ];
        for (field, value) in mutations {
            let mut invalid = policy_json();
            invalid["audiences"][0]["managers"][0][field] = value;
            assert!(parse(&invalid).is_err());
        }

        let mut empty_managers = policy_json();
        empty_managers["audiences"][0]["managers"] = json!([]);
        assert!(parse(&empty_managers).is_err());

        let manager = policy_json()["audiences"][0]["managers"][0].clone();
        let mut duplicate_manager = policy_json();
        duplicate_manager["audiences"][0]["managers"] = json!([manager.clone(), manager]);
        assert!(parse(&duplicate_manager).is_err());

        let mut empty_accounts = policy_json();
        empty_accounts["audiences"][0]["managers"][0]["account_ids"] = json!([]);
        assert!(parse(&empty_accounts).is_err());

        for account in ["missing", "ip_domnyshev_wb", "bad account"] {
            let mut invalid = policy_json();
            invalid["audiences"][0]["managers"][0]["account_ids"] = json!([account]);
            assert!(parse(&invalid).is_err());
        }
    }

    #[test]
    fn accounts_cannot_be_routed_to_multiple_recipients() {
        let mut duplicated = policy_json();
        let mut second = duplicated["audiences"][0].clone();
        second["id"] = json!("second");
        second["email_env"] = json!("SECOND_RECIPIENT_EMAIL");
        duplicated["audiences"].as_array_mut().unwrap().push(second);
        assert!(parse(&duplicated).is_err());
    }
}
