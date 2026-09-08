use super::*;

#[tokio::test]
async fn seller_inventory_uses_only_marketplace_host_and_current_chrt_id_contract() {
    let (marketplace, requests) = mock_http(vec![
        (
            200,
            r#"[{"id":12,"deliveryType":1},{"id":13,"deliveryType":2}]"#.to_owned(),
        ),
        (200, r#"{"stocks":[{"chrtId":101,"amount":0}]}"#.to_owned()),
        (200, r#"{"stocks":[]}"#.to_owned()),
    ]);
    let mut urls = BaseUrls::for_test("http://127.0.0.1:1", "http://127.0.0.1:1");
    urls.marketplace = marketplace;
    let client = WbClient::build(
        Duration::from_secs(2),
        credentials(),
        urls,
        ClientPolicy::immediate_single_attempt(Duration::from_secs(2)),
    );
    assert_eq!(
        client.seller_warehouses("account").await.unwrap()[1]["deliveryType"],
        2
    );
    assert_eq!(
        client
            .seller_warehouse_stocks("account", 12, vec![101])
            .await
            .unwrap()["stocks"][0]["amount"],
        0
    );
    client
        .seller_warehouse_stocks("account", 13, (1..=1_000).collect())
        .await
        .unwrap();
    assert!(
        requests
            .recv()
            .unwrap()
            .starts_with("GET /api/v3/warehouses HTTP/1.1\r\n")
    );
    let stock = requests.recv().unwrap();
    assert!(stock.starts_with("POST /api/v3/stocks/12 HTTP/1.1\r\n"));
    assert!(
        stock
            .to_ascii_lowercase()
            .contains("authorization: bearer test-token")
    );
    assert_eq!(
        serde_json::from_str::<Value>(stock.split_once("\r\n\r\n").unwrap().1).unwrap(),
        json!({"chrtIds":[101]})
    );
    let batch = requests.recv().unwrap();
    assert!(batch.starts_with("POST /api/v3/stocks/13 HTTP/1.1\r\n"));
    assert_eq!(
        serde_json::from_str::<Value>(batch.split_once("\r\n\r\n").unwrap().1).unwrap()["chrtIds"]
            .as_array()
            .unwrap()
            .len(),
        1_000
    );
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn seller_inventory_denies_mutations_and_noncanonical_paths_before_credentials() {
    let client = client("http://127.0.0.1:1");
    for (method, path) in [
        (Method::POST, "/api/v3/warehouses"),
        (Method::PUT, "/api/v3/warehouses/1"),
        (Method::DELETE, "/api/v3/warehouses/1"),
        (Method::GET, "/api/v3/stocks/1"),
        (Method::PUT, "/api/v3/stocks/1"),
        (Method::DELETE, "/api/v3/stocks/1"),
        (Method::POST, "/api/v3/stocks/"),
        (Method::POST, "/api/v3/stocks/{warehouseId}"),
        (Method::POST, "/api/v3/stocks/0"),
        (Method::POST, "/api/v3/stocks/01"),
        (Method::POST, "/api/v3/stocks/-1"),
        (Method::POST, "/api/v3/stocks/+1"),
        (Method::POST, "/api/v3/stocks/9223372036854775808"),
        (Method::POST, "/api/v3/stocks/%31"),
        (Method::POST, "/api/v3/stocks/1/../warehouses"),
        (Method::POST, "/api/v3/stocks/1?warehouseId=2"),
        (Method::POST, "/api/v3/stocks/1#fragment"),
        (Method::POST, "/api/v3/stocks/1/"),
        (Method::POST, "/api/v3/stocks/１"),
    ] {
        assert!(
            matches!(
                client.request_for_test("missing", method, path).await,
                Err(WbError::EndpointNotAllowed { .. })
            ),
            "{path}"
        );
    }
    for path in ["/api/v3/stocks/1", "/api/v3/stocks/9223372036854775807"] {
        assert_eq!(
            RequestClass::for_request(&Method::POST, path),
            Some(RequestClass::SellerInventory)
        );
    }
    assert!(matches!(
        client.seller_warehouses("missing").await,
        Err(WbError::MissingCredentials(_))
    ));
    assert!(matches!(
        client.seller_warehouse_stocks("missing", 1, vec![1]).await,
        Err(WbError::MissingCredentials(_))
    ));
}

#[tokio::test]
async fn seller_inventory_rejects_invalid_batches_before_network() {
    let client = client("http://127.0.0.1:1");
    for (warehouse, ids) in [
        (0, vec![1]),
        (u64::MAX, vec![1]),
        (1, vec![]),
        (1, vec![0]),
        (1, vec![u64::MAX]),
        (1, vec![1, 1]),
        (1, (1..=1_001).collect()),
    ] {
        assert!(matches!(
            client
                .seller_warehouse_stocks("account", warehouse, ids)
                .await,
            Err(WbError::InvalidArguments { .. })
        ));
    }
}

#[tokio::test]
async fn seller_inventory_pacing_is_shared_between_warehouses_and_stock_reads() {
    let (base_url, requests) = mock_http(vec![(200, "[]".to_owned())]);
    let mut policy = ClientPolicy::production(Duration::from_secs(2));
    policy.seller_inventory_interval = Duration::from_secs(30);
    let client = WbClient::new_for_test_with_policy(
        Duration::from_secs(2),
        credentials(),
        &base_url,
        policy,
    );
    client.seller_warehouses("account").await.unwrap();
    assert!(matches!(
        client.seller_warehouse_stocks("account", 1, vec![1]).await,
        Err(WbError::LocalRateLimited { .. })
    ));
    assert!(
        requests
            .recv()
            .unwrap()
            .starts_with("GET /api/v3/warehouses ")
    );
    assert!(requests.try_recv().is_err());
    let production = ClientPolicy::production(Duration::from_secs(2));
    assert_eq!(
        production.interval(RequestClass::SellerInventory),
        Duration::from_millis(250)
    );
    assert!(!RequestClass::SellerInventory.allows_automatic_retry());
}

#[tokio::test]
async fn seller_inventory_rate_limit_is_not_retried_or_replaced_with_fbw() {
    let (base_url, requests) = mock_http(vec![(429, "{}".to_owned())]);
    let client = WbClient::new_for_test_with_policy(
        Duration::from_secs(2),
        credentials(),
        &base_url,
        ClientPolicy::production(Duration::from_secs(2)),
    );
    assert!(matches!(
        client.seller_warehouse_stocks("account", 1, vec![1]).await,
        Err(WbError::RateLimited { .. })
    ));
    assert!(
        requests
            .recv()
            .unwrap()
            .starts_with("POST /api/v3/stocks/1 ")
    );
    assert!(requests.try_recv().is_err());
    assert!(
        client.limiters["account"]
            .ready_in(RequestClass::SellerInventory)
            .await
            > Duration::from_secs(50)
    );
}

#[tokio::test]
async fn seller_inventory_shares_long_vendor_cooldowns_and_weighted_conflicts() {
    for (status, headers, minimum, maximum) in [
        (
            429,
            "Retry-After: 120\r\n",
            Duration::from_secs(110),
            Duration::from_secs(120),
        ),
        (
            503,
            "Retry-After: 172800\r\n",
            Duration::from_secs(86_390),
            Duration::from_hours(24),
        ),
        (
            409,
            "",
            Duration::from_secs(2),
            Duration::from_millis(2_500),
        ),
    ] {
        let (base_url, requests, task) = raw_http(vec![raw_response(status, headers, b"{}")]);
        let client = WbClient::new_for_test_with_policy(
            Duration::from_secs(2),
            credentials(),
            &base_url,
            ClientPolicy::production(Duration::from_secs(2)),
        );
        assert!(
            client
                .seller_warehouse_stocks("account", 1, vec![1])
                .await
                .is_err()
        );
        let delay = client.limiters["account"]
            .ready_in(RequestClass::SellerInventory)
            .await;
        assert!(delay > minimum && delay <= maximum, "{status}: {delay:?}");
        if status == 409 {
            // This short cooldown fits the logical deadline: the reader waits
            // without sending. Cancellation during that wait must be harmless.
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(50),
                    client.seller_warehouses("account")
                )
                .await
                .is_err()
            );
        } else {
            assert!(matches!(
                client.seller_warehouses("account").await,
                Err(WbError::LocalRateLimited { .. })
            ));
        }
        assert!(
            client.limiters["account"]
                .ready_in(RequestClass::ContentReport)
                .await
                .is_zero()
        );
        requests.recv_timeout(Duration::from_secs(2)).unwrap();
        task.join().unwrap();
        assert!(requests.try_recv().is_err());
    }
}
