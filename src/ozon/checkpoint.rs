//! Single-attempt adapter for durable reporting retries.

use super::{AnalyticsPacingMode, AttemptInput, OzonClient, OzonError, RetryOwner, StoreId, Value};

impl OzonClient {
    /// One guarded read attempt for a durable external retry owner. Vendor
    /// delays must reach PostgreSQL before the short page lease can expire.
    pub(crate) async fn post_checkpoint_page(
        &self,
        store: &StoreId,
        path: &'static str,
        payload: Value,
    ) -> Result<Value, OzonError> {
        if !self.is_endpoint_allowed(path) {
            return Err(OzonError::EndpointNotAllowed(path.to_owned()));
        }
        let credentials = self
            .stores
            .get(store)
            .ok_or_else(|| OzonError::MissingCredentials(store.clone()))?;
        let limiter = self
            .rate_limiters
            .get(store)
            .expect("configured stores always have a rate limiter");
        let outcome = tokio::time::timeout(
            self.request_deadline,
            self.send_attempt(AttemptInput {
                limiter,
                credentials,
                store,
                path,
                payload: &payload,
                attempt: 1,
                pacing_mode: AnalyticsPacingMode::FailFast,
                retry_owner: RetryOwner::Checkpoint,
            }),
        )
        .await
        .map_err(|_| OzonError::DeadlineExceeded)?;
        outcome.map_err(|failure| failure.error)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use axum::{Router, http::StatusCode, routing::post};
    use serde_json::json;

    use super::*;
    use crate::{
        config::StoreCredentials,
        ozon::ANALYTICS_DATA_PATH,
        reporting::{
            ozon_adapter::OzonReportRequest,
            ozon_source::{OzonClientReportTransport, OzonReportSourceError, OzonReportTransport},
        },
    };

    async fn fixture(
        status: StatusCode,
        retry_after: &'static str,
    ) -> (OzonClient, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let router = Router::new().fallback(post(move || {
            let observed = Arc::clone(&observed);
            async move {
                observed.fetch_add(1, Ordering::SeqCst);
                (status, [("Retry-After", retry_after)], "{}")
            }
        }));
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let client = OzonClient::new(
            format!("http://{address}"),
            Duration::from_secs(2),
            BTreeMap::from([(
                StoreId::from("checkpoint"),
                StoreCredentials {
                    client_id: "test-client".to_owned(),
                    api_key: "test-key".to_owned(),
                },
            )]),
        )
        .unwrap();
        (client, calls, server)
    }

    #[tokio::test]
    async fn durable_analytics_reports_installed_cooldown_without_waiting_or_departing() {
        let (client, calls, server) = fixture(StatusCode::TOO_MANY_REQUESTS, "120").await;
        let store = StoreId::from("checkpoint");
        assert!(matches!(
            client.post(&store, ANALYTICS_DATA_PATH, json!({})).await,
            Err(OzonError::RateLimited { .. })
        ));
        let limiter = client.rate_limiters.get(&store).unwrap();
        tokio::time::pause();
        let started = tokio::time::Instant::now();
        let before = limiter.ready_in_for(ANALYTICS_DATA_PATH).await;
        assert!(before > Duration::from_secs(100));
        let scheduled = *limiter.analytics_next_allowed.lock().await;
        let transport = OzonClientReportTransport::new(client.clone(), store).with_durable_retry();
        let error = tokio::time::timeout(
            Duration::from_millis(250),
            transport.post(OzonReportRequest {
                path: ANALYTICS_DATA_PATH,
                payload: json!({}),
            }),
        )
        .await
        .expect("durable retry owner must receive the cooldown immediately")
        .unwrap_err();
        assert!(matches!(
            error,
            OzonReportSourceError::RetryAfter { seconds: 120 }
        ));
        assert_eq!(error.failure().retry_after, Some(120));
        assert_eq!(started.elapsed(), Duration::ZERO);
        assert_eq!(*limiter.analytics_next_allowed.lock().await, scheduled);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the deferred checkpoint must not send HTTP"
        );
        tokio::time::resume();
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn durable_checkpoint_does_not_retry_a_retryable_server_response() {
        let (client, calls, server) = fixture(StatusCode::SERVICE_UNAVAILABLE, "1").await;
        let error = client
            .post_checkpoint_page(
                &StoreId::from("checkpoint"),
                "/v1/rating/summary",
                json!({}),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            OzonError::Server {
                status: StatusCode::SERVICE_UNAVAILABLE,
                ..
            }
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        server.abort();
        let _ = server.await;
    }
}
