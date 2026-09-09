//! Shared marketplace identifiers and redacted credential containers.
//!
//! These are value types, not a credential loader or authorization boundary.
//! Registry validation, environment access and HTTP clients belong to consumers.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct StoreId(pub String);

impl StoreId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for StoreId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<&str> for StoreId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Marketplace {
    Ozon,
    Wildberries,
}

#[derive(Clone)]
pub struct StoreCredentials {
    pub client_id: String,
    pub api_key: String,
}

#[derive(Clone)]
pub struct PerformanceCredentials {
    pub client_id: String,
    pub client_secret: String,
}

impl fmt::Debug for PerformanceCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PerformanceCredentials")
            .field("client_id", &"<redacted>")
            .field("client_secret", &"<redacted>")
            .finish()
    }
}

impl fmt::Debug for StoreCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoreCredentials")
            .field("client_id", &"<redacted>")
            .field("api_key", &"<redacted>")
            .finish()
    }
}

/// API credentials whose debug representation never exposes the token.
#[derive(Clone)]
pub struct WbCredentials {
    pub token: String,
}

impl fmt::Debug for WbCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WbCredentials")
            .field("token", &"<redacted>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn identifiers_and_marketplaces_keep_their_wire_contract() {
        let id = StoreId::new("shop-a");
        assert_eq!(id, StoreId::from("shop-a"));
        assert_eq!(id.to_string(), "shop-a");
        assert_eq!(serde_json::to_value(&id).unwrap(), json!("shop-a"));
        assert_eq!(
            serde_json::from_value::<StoreId>(json!("shop-a")).unwrap(),
            id
        );
        assert_eq!(
            schemars::schema_for!(StoreId).get("type"),
            Some(&json!("string"))
        );
        for (marketplace, value) in [
            (Marketplace::Ozon, "ozon"),
            (Marketplace::Wildberries, "wildberries"),
        ] {
            assert_eq!(serde_json::to_value(marketplace).unwrap(), json!(value));
            assert_eq!(
                serde_json::from_value::<Marketplace>(json!(value)).unwrap(),
                marketplace
            );
        }
        assert!(serde_json::from_value::<Marketplace>(json!("unknown")).is_err());
    }

    #[test]
    fn every_credential_field_is_redacted_even_after_cloning() {
        let store = StoreCredentials {
            client_id: "seller-id-secret".into(),
            api_key: "seller-key-secret".into(),
        };
        let performance = PerformanceCredentials {
            client_id: "performance-id-secret".into(),
            client_secret: "performance-key-secret".into(),
        };
        let wb = WbCredentials {
            token: "wb-token-secret".into(),
        };
        assert_eq!(
            format!("{store:?}"),
            "StoreCredentials { client_id: \"<redacted>\", api_key: \"<redacted>\" }"
        );
        assert_eq!(
            format!("{performance:?}"),
            "PerformanceCredentials { client_id: \"<redacted>\", client_secret: \"<redacted>\" }"
        );
        assert_eq!(format!("{wb:?}"), "WbCredentials { token: \"<redacted>\" }");
        let cloned_store = store.clone();
        let cloned_performance = performance.clone();
        let cloned_wb = wb.clone();
        assert_eq!(cloned_store.api_key, store.api_key);
        assert_eq!(cloned_performance.client_secret, performance.client_secret);
        assert_eq!(cloned_wb.token, wb.token);
        assert!(
            !format!("{cloned_store:?}{cloned_performance:?}{cloned_wb:?}").contains("-secret")
        );
    }
}
