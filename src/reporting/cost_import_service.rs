//! Trusted operator configuration for offline 1C exchange; never payload-derived scope.

use std::collections::BTreeSet;

use serde::Deserialize;

use crate::config::{AccessRegistry, Role};

use super::{
    cost_import::{CostImportError, CostImportScope},
    snapshot::{AccountScope, Marketplace},
};

/// Mounted by the operator separately from an untrusted 1C export.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CostImportPolicy {
    version: u32,
    enabled: bool,
    account_id: String,
    marketplace: Marketplace,
    source_id: String,
    allowed_skus: BTreeSet<u64>,
}

impl CostImportPolicy {
    pub fn from_json(bytes: &[u8]) -> Result<Self, CostImportError> {
        if bytes.len() > 1024 * 1024 {
            return Err(CostImportError::LimitExceeded);
        }
        let policy: Self =
            serde_json::from_slice(bytes).map_err(|_| CostImportError::InvalidInput)?;
        if policy.version != 1 {
            return Err(CostImportError::InvalidInput);
        }
        Ok(policy)
    }

    /// Revalidate the current registry immediately before each import. The actor
    /// is a trusted local service identity, never supplied by the export or MCP.
    pub fn authorize(
        &self,
        registry: &AccessRegistry,
        actor_id: &str,
        writing: bool,
    ) -> Result<CostImportScope, CostImportError> {
        let actor = registry
            .actor(actor_id)
            .map_err(|_| CostImportError::ScopeDenied)?;
        let account = registry
            .accounts
            .iter()
            .find(|account| account.id == self.account_id)
            .ok_or(CostImportError::ScopeDenied)?;
        let marketplace = match account.marketplace {
            crate::config::Marketplace::Ozon => Marketplace::Ozon,
            crate::config::Marketplace::Wildberries => Marketplace::Wildberries,
        };
        if (writing && !self.enabled)
            || !matches!(actor.role, Role::Finance | Role::Admin)
            || !actor.can_access_account(account)
            || marketplace != self.marketplace
        {
            return Err(CostImportError::ScopeDenied);
        }
        let account = AccountScope::new(self.account_id.clone(), marketplace)
            .map_err(|_| CostImportError::InvalidInput)?;
        CostImportScope::new(
            account,
            self.source_id.clone(),
            self.allowed_skus.clone(),
            actor.id.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn registry() -> AccessRegistry {
        serde_json::from_value(json!({"version":1,"actors":[
            {"id":"manager","name":"Manager","role":"manager"},
            {"id":"finance","name":"Finance","role":"finance","account_ids":["wb"]},
            {"id":"outsider","name":"Other finance","role":"finance"},
            {"id":"admin","name":"Admin","role":"admin"}
        ],"accounts":[{"id":"wb","organization":"Fixture","marketplace":"wildberries",
            "seller_client_id":"1","manager_id":"manager"}]}))
        .unwrap()
    }

    fn policy(enabled: bool) -> CostImportPolicy {
        CostImportPolicy::from_json(
            &serde_json::to_vec(&json!({"version":1,"enabled":enabled,
            "account_id":"wb","marketplace":"wildberries","source_id":"one_c",
            "allowed_skus":[1001]}))
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn import_requires_both_finance_access_and_explicit_activation() {
        let registry = registry();
        for actor in ["manager", "outsider", "missing"] {
            assert!(policy(true).authorize(&registry, actor, false).is_err());
        }
        assert!(policy(false).authorize(&registry, "finance", false).is_ok());
        assert!(policy(false).authorize(&registry, "finance", true).is_err());
        assert!(policy(true).authorize(&registry, "finance", true).is_ok());
        assert!(policy(true).authorize(&registry, "admin", true).is_ok());
    }

    #[test]
    fn current_revocation_and_marketplace_mismatch_are_rejected() {
        let mut registry = registry();
        let policy = policy(true);
        assert!(policy.authorize(&registry, "finance", true).is_ok());
        registry.actors.retain(|actor| actor.id != "finance");
        assert!(policy.authorize(&registry, "finance", true).is_err());
        registry.accounts[0].marketplace = crate::config::Marketplace::Ozon;
        assert!(policy.authorize(&registry, "admin", true).is_err());
    }

    #[test]
    fn payload_cannot_expand_the_trusted_policy() {
        assert!(CostImportPolicy::from_json(br#"{"version":1,"enabled":true,"account_id":"wb","marketplace":"wildberries","source_id":"one_c","allowed_skus":[1001],"api_key":"anything"}"#).is_err());
        let mut policy = policy(true);
        policy.allowed_skus.clear();
        assert!(policy.authorize(&registry(), "admin", true).is_err());
    }
}
