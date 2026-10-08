use super::{AccessRegistry, Result, bail};
use crate::identifiers::{MAX_IDENTIFIER_BYTES, is_account_id, is_actor_id};

impl AccessRegistry {
    /// Rejects identifiers that the audit, refresh or reporting boundaries
    /// would otherwise refuse on every call made for that actor or account.
    pub(super) fn validate_identifier_grammar(&self) -> Result<()> {
        if let Some(actor) = self.actors.iter().find(|actor| !is_actor_id(&actor.id)) {
            bail!(
                "идентификатор actor {:?} должен содержать от 1 до {MAX_IDENTIFIER_BYTES} ASCII-символов [A-Za-z0-9._:@-]",
                actor.id
            );
        }
        if let Some(account) = self
            .accounts
            .iter()
            .find(|account| !is_account_id(&account.id))
        {
            bail!(
                "идентификатор кабинета {:?} должен содержать от 1 до {MAX_IDENTIFIER_BYTES} ASCII-символов [A-Za-z0-9_-]",
                account.id
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use crate::config::AccessRegistry;

    fn registry(actor_id: &str, account_id: &str) -> Value {
        json!({
            "version": 1,
            "actors": [{"id": actor_id, "name": "Manager", "role": "manager"}],
            "accounts": [{
                "id": account_id,
                "organization": "Shop",
                "marketplace": "wildberries",
                "seller_client_id": "1",
                "manager_id": actor_id,
                "wildberries": {"api_token_env": "SHOP_WB_TOKEN"}
            }]
        })
    }

    fn validate(value: Value) -> anyhow::Result<()> {
        serde_json::from_value::<AccessRegistry>(value)
            .expect("the fixture has the registry shape")
            .validate()
    }

    #[test]
    fn identifiers_accepted_by_every_downstream_boundary_load() {
        validate(registry("ivan.petrov", "shop_wb-1")).unwrap();
        validate(registry("team:ops@example", "SHOP")).unwrap();
    }

    #[test]
    fn identifiers_a_downstream_boundary_would_refuse_fail_at_load() {
        for (actor_id, account_id, field) in [
            ("Иван", "shop", "actor"),
            ("ivan petrov", "shop", "actor"),
            ("manager", "shop.ru", "кабинета"),
            ("manager", "магазин", "кабинета"),
            ("manager", "shop@wb", "кабинета"),
        ] {
            let error = validate(registry(actor_id, account_id)).unwrap_err();
            assert!(
                error.to_string().contains(field),
                "{actor_id}/{account_id}: {error}"
            );
        }
        let oversized = "a".repeat(129);
        assert!(validate(registry(&oversized, "shop")).is_err());
        assert!(validate(registry("manager", &oversized)).is_err());
    }
}
