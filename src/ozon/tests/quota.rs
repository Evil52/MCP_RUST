use super::*;

#[tokio::test]
#[ignore = "requires the isolated reporting PostgreSQL fixture"]
async fn shared_quota_retains_long_seller_vendor_cooldowns() {
    let database_url = std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL")
        .expect("run through scripts/with-position-test-db.sh");
    for (status, path) in [(429, ANALYTICS_DATA_PATH), (503, "/v3/product/list")] {
        for delay in [Duration::from_hours(2), Duration::from_hours(48)] {
            let (base_url, requests) = mock_server(vec![
                MockResponse::new(status, r#"{"error":"try later"}"#)
                    .header("Retry-After", &delay.as_secs().to_string()),
            ]);
            let mut stores = credentials();
            stores.get_mut(&StoreId::from("ofk")).unwrap().client_id = format!(
                "seller-long-cooldown-{}",
                Utc::now().timestamp_nanos_opt().unwrap()
            );
            let first = OzonClient::new(base_url.clone(), Duration::from_secs(3), stores.clone())
                .unwrap()
                .with_shared_quota(SharedQuota::from_database_url(&database_url));
            let store = StoreId::from("ofk");
            let error = first
                .post(&store, path, serde_json::json!({}))
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                OzonError::RateLimited { .. } | OzonError::Server { .. }
            ));
            drop(first);
            let restarted = OzonClient::new(base_url, Duration::from_secs(3), stores)
                .unwrap()
                .with_shared_quota(SharedQuota::from_database_url(&database_url));
            let error = restarted
                .post(&store, path, serde_json::json!({}))
                .await
                .unwrap_err();
            assert!(
                matches!(error, OzonError::SharedQuota(QuotaError::Limited { retry_after })
                if retry_after > delay.checked_sub(Duration::from_secs(30)).unwrap())
            );
            assert_eq!(requests.try_iter().count(), 1);
        }
    }
}

#[tokio::test]
#[ignore = "requires the isolated reporting PostgreSQL fixture"]
async fn shared_quota_serializes_independent_seller_clients() {
    let database_url = std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL")
        .expect("run through scripts/with-position-test-db.sh");
    let (base_url, requests) = mock_server(vec![MockResponse::new(200, r#"{"ok":true}"#)]);
    let mut stores = credentials();
    stores.get_mut(&StoreId::from("ofk")).unwrap().client_id = format!(
        "seller-concurrent-{}",
        Utc::now().timestamp_nanos_opt().unwrap()
    );
    let first = OzonClient::new(base_url.clone(), Duration::from_secs(3), stores.clone())
        .unwrap()
        .with_shared_quota(SharedQuota::from_database_url(&database_url));
    let second = OzonClient::new(base_url, Duration::from_secs(3), stores)
        .unwrap()
        .with_shared_quota(SharedQuota::from_database_url(&database_url));
    let store = StoreId::from("ofk");
    let (left, right) = tokio::join!(
        first.post(&store, ANALYTICS_DATA_PATH, serde_json::json!({})),
        second.post(&store, ANALYTICS_DATA_PATH, serde_json::json!({})),
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let error = left.err().or_else(|| right.err()).unwrap();
    assert!(matches!(
        error,
        OzonError::SharedQuota(QuotaError::Limited { .. })
    ));
    assert_eq!(requests.try_iter().count(), 1);
}

#[tokio::test]
#[ignore = "requires the isolated reporting PostgreSQL fixture"]
async fn shared_quota_preserves_seller_429_cooldown_for_new_client() {
    let database_url = std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL")
        .expect("run through scripts/with-position-test-db.sh");
    let (base_url, requests) = mock_server(vec![
        MockResponse::new(429, r#"{"error":"slow down"}"#).header("Retry-After", "60"),
    ]);
    let mut stores = credentials();
    stores.get_mut(&StoreId::from("ofk")).unwrap().client_id = format!(
        "seller-cooldown-{}",
        Utc::now().timestamp_nanos_opt().unwrap()
    );
    let first = OzonClient::new(base_url.clone(), Duration::from_secs(3), stores.clone())
        .unwrap()
        .with_shared_quota(SharedQuota::from_database_url(&database_url));
    let store = StoreId::from("ofk");
    let error = first
        .post(&store, "/v3/product/list", serde_json::json!({}))
        .await
        .unwrap_err();
    assert!(matches!(error, OzonError::RateLimited { .. }));
    drop(first);
    let restarted = OzonClient::new(base_url, Duration::from_secs(3), stores)
        .unwrap()
        .with_shared_quota(SharedQuota::from_database_url(&database_url));
    let error = restarted
        .post(&store, "/v3/product/list", serde_json::json!({}))
        .await
        .unwrap_err();
    assert!(
        matches!(error, OzonError::SharedQuota(QuotaError::Limited { retry_after })
        if retry_after > Duration::from_secs(30))
    );
    assert_eq!(requests.try_iter().count(), 1);
}

#[tokio::test]
async fn unavailable_shared_quota_blocks_seller_transport() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let client = OzonClient::new(
        format!("http://{}", listener.local_addr().unwrap()),
        Duration::from_secs(1),
        credentials(),
    )
    .unwrap()
    .with_shared_quota(SharedQuota::from_database_url("invalid-quota-url"));
    let error = client
        .post(
            &StoreId::from("ofk"),
            "/v3/product/list",
            serde_json::json!({}),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        OzonError::SharedQuota(QuotaError::Unavailable)
    ));
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[test]
fn stores_with_the_same_client_id_share_one_rate_limiter() {
    let stores = BTreeMap::from([
        (
            StoreId::from("first"),
            StoreCredentials {
                client_id: "shared-client".to_owned(),
                api_key: "first-key".to_owned(),
            },
        ),
        (
            StoreId::from("second"),
            StoreCredentials {
                client_id: "shared-client".to_owned(),
                api_key: "second-key".to_owned(),
            },
        ),
        (
            StoreId::from("third"),
            StoreCredentials {
                client_id: "other-client".to_owned(),
                api_key: "third-key".to_owned(),
            },
        ),
    ]);
    let client = OzonClient::new(
        "http://127.0.0.1:1".to_owned(),
        Duration::from_secs(1),
        stores,
    )
    .unwrap();

    assert!(Arc::ptr_eq(
        &client.rate_limiters[&StoreId::from("first")],
        &client.rate_limiters[&StoreId::from("second")],
    ));
    assert!(!Arc::ptr_eq(
        &client.rate_limiters[&StoreId::from("first")],
        &client.rate_limiters[&StoreId::from("third")],
    ));
}
