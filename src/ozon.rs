use std::{collections::BTreeMap, sync::Arc, time::Duration};

use reqwest::{Client, StatusCode};
use serde_json::Value;
use thiserror::Error;

use crate::config::{StoreCredentials, StoreId};

#[derive(Debug, Error)]
pub enum OzonError {
    #[error("для магазина {0} не настроены Client-Id и Api-Key")]
    MissingCredentials(StoreId),
    #[error("ошибка HTTP-клиента Ozon: {0}")]
    Request(#[from] reqwest::Error),
    #[error("Ozon API вернул HTTP {status}: {body}")]
    Api { status: StatusCode, body: String },
}

#[derive(Debug, Clone)]
pub struct OzonClient {
    http: Client,
    base_url: String,
    stores: Arc<BTreeMap<StoreId, StoreCredentials>>,
}

impl OzonClient {
    pub fn new(
        base_url: String,
        timeout: Duration,
        stores: BTreeMap<StoreId, StoreCredentials>,
    ) -> Result<Self, reqwest::Error> {
        Self::new_with_user_agent(
            base_url,
            timeout,
            stores,
            concat!("mcp-ozon/", env!("CARGO_PKG_VERSION")),
        )
    }

    fn new_with_user_agent(
        base_url: String,
        timeout: Duration,
        stores: BTreeMap<StoreId, StoreCredentials>,
        user_agent: &str,
    ) -> Result<Self, reqwest::Error> {
        let http = Client::builder()
            .timeout(timeout)
            .user_agent(user_agent)
            .build()?;
        Ok(Self {
            http,
            base_url,
            stores: Arc::new(stores),
        })
    }

    pub fn is_configured(&self, store: &StoreId) -> bool {
        self.stores.contains_key(store)
    }

    pub async fn post(
        &self,
        store: &StoreId,
        path: &'static str,
        payload: Value,
    ) -> Result<Value, OzonError> {
        let credentials = self
            .stores
            .get(store)
            .ok_or_else(|| OzonError::MissingCredentials(store.clone()))?;
        let response = self
            .http
            .post(format!("{}{path}", self.base_url))
            .header("Client-Id", &credentials.client_id)
            .header("Api-Key", &credentials.api_key)
            .json(&payload)
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;

        if !status.is_success() {
            return Err(OzonError::Api {
                status,
                body: truncate(&body, 4_096),
            });
        }

        Ok(serde_json::from_str(&body).unwrap_or_else(|_| {
            serde_json::json!({
                "raw": body,
            })
        }))
    }
}

fn truncate(value: &str, maximum_chars: usize) -> String {
    let mut chars = value.chars();
    let shortened: String = chars.by_ref().take(maximum_chars).collect();
    if chars.next().is_some() {
        format!("{shortened}…")
    } else {
        shortened
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use super::*;
    use crate::test_support::mock_http;

    fn credentials() -> BTreeMap<StoreId, StoreCredentials> {
        BTreeMap::from([(
            StoreId::from("ofk"),
            StoreCredentials {
                client_id: "test-client".to_owned(),
                api_key: "test-key".to_owned(),
            },
        )])
    }

    #[test]
    fn response_bodies_are_bounded_only_when_needed() {
        let value = "x".repeat(5_000);
        let result = truncate(&value, 100);
        assert_eq!(result.chars().count(), 101);
        assert!(result.ends_with('…'));
        assert_eq!(truncate("short", 100), "short");
    }

    #[test]
    fn invalid_http_client_configuration_is_rejected() {
        let error = OzonClient::new_with_user_agent(
            "https://example.invalid".to_owned(),
            Duration::from_secs(1),
            BTreeMap::new(),
            "invalid\nuser-agent",
        )
        .unwrap_err();
        assert!(error.is_builder());
    }

    #[tokio::test]
    async fn post_sends_credentials_and_decodes_json() {
        let (base_url, request) = mock_http(vec![(200, r#"{"result":{"items":[1]}}"#.to_owned())]);
        let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

        let response = client
            .post(
                &StoreId::from("ofk"),
                "/v1/example",
                serde_json::json!({"limit": 5}),
            )
            .await
            .unwrap();

        assert_eq!(response["result"]["items"][0], 1);
        let request = request.recv_timeout(Duration::from_secs(3)).unwrap();
        let request_lowercase = request.to_ascii_lowercase();
        assert!(request.starts_with("POST /v1/example HTTP/1.1"));
        assert!(request_lowercase.contains("client-id: test-client"));
        assert!(request_lowercase.contains("api-key: test-key"));
        assert!(request.contains(r#"{"limit":5}"#));
    }

    #[tokio::test]
    async fn non_json_success_is_returned_as_bounded_raw_data() {
        let (base_url, _) = mock_http(vec![(200, "not-json".to_owned())]);
        let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

        let response = client
            .post(&StoreId::from("ofk"), "/v1/raw", serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(response, serde_json::json!({ "raw": "not-json" }));
    }

    #[tokio::test]
    async fn api_errors_keep_status_and_truncate_large_bodies() {
        let (base_url, _) = mock_http(vec![(409, "x".repeat(5_000))]);
        let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

        let error = client
            .post(&StoreId::from("ofk"), "/v1/fail", serde_json::json!({}))
            .await
            .unwrap_err();
        let message = error.to_string();
        let prefix = "Ozon API вернул HTTP 409 Conflict: ";
        assert!(message.starts_with(prefix));
        assert!(message.ends_with('…'));
        assert_eq!(message.chars().count(), prefix.chars().count() + 4_097);
    }

    #[tokio::test]
    async fn missing_store_credentials_fail_without_network_access() {
        let client = OzonClient::new(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_secs(1),
            BTreeMap::new(),
        )
        .unwrap();
        assert!(!client.is_configured(&StoreId::from("ofk")));

        let error = client
            .post(&StoreId::from("ofk"), "/v1/example", serde_json::json!({}))
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "для магазина ofk не настроены Client-Id и Api-Key"
        );
    }

    #[tokio::test]
    async fn network_errors_are_returned() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let client = OzonClient::new(
            format!("http://{address}"),
            Duration::from_secs(1),
            credentials(),
        )
        .unwrap();

        let error = client
            .post(
                &StoreId::from("ofk"),
                "/v1/unavailable",
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().starts_with("ошибка HTTP-клиента Ozon:"));
    }
}
