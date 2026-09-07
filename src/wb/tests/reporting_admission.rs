use super::*;

#[tokio::test]
async fn background_stats_reject_a_local_cooldown_beyond_the_admission_budget() {
    use crate::reporting::wb_source::{
        WbClientReportTransport, WbReportSourceError, WbReportTransport,
    };

    let client = client("http://127.0.0.1:1");
    *client.limiters["account"]
        .promotion_stats
        .next_allowed
        .lock()
        .await = Instant::now() + Duration::from_secs(120);
    let transport = WbClientReportTransport::new(client, "account".to_owned());
    let date = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
    let started = Instant::now();
    assert_eq!(
        transport.promotion_stats(vec![1], date, date).await,
        Err(WbReportSourceError::Upstream(WbErrorKind::RateLimited))
    );
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn queued_background_stats_hold_no_permits_and_cancellation_sends_no_request() {
    use crate::reporting::wb_source::{WbClientReportTransport, WbReportTransport};

    let (base_url, requests) = mock_http(vec![(200, "[]".to_owned())]);
    let mut policy = ClientPolicy::immediate_single_attempt(Duration::from_secs(2));
    policy.promotion_stats_interval = PROMOTION_STATS_MIN_REQUEST_INTERVAL;
    let client = WbClient::new_for_test_with_policy(
        Duration::from_secs(2),
        credentials(),
        &base_url,
        policy,
    );
    let date = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
    client
        .promotion_stats("account", vec![1], date.to_string(), date.to_string())
        .await
        .unwrap();
    assert!(
        requests
            .recv()
            .unwrap()
            .starts_with("GET /adv/v3/fullstats?")
    );

    let transport = WbClientReportTransport::new(client.clone(), "account".to_owned());
    let background =
        tokio::spawn(async move { transport.promotion_stats(vec![2], date, date).await });
    tokio::task::yield_now().await;
    assert!(
        !background.is_finished(),
        "background collection must wait for its local slot"
    );
    assert_eq!(
        client.global_in_flight.available_permits(),
        MAX_GLOBAL_IN_FLIGHT_REQUESTS
    );
    assert_eq!(
        client.limiters["account"].in_flight.available_permits(),
        MAX_IN_FLIGHT_REQUESTS_PER_TOKEN
    );
    assert!(
        matches!(
            client
                .promotion_stats("account", vec![3], date.to_string(), date.to_string())
                .await,
            Err(WbError::LocalRateLimited { .. })
        ),
        "interactive callers must still fail fast"
    );
    background.abort();
    assert!(background.await.unwrap_err().is_cancelled());
    assert!(
        requests.try_recv().is_err(),
        "cancellation must not send another request"
    );
}

#[tokio::test]
async fn background_report_collects_all_campaign_chunks_through_local_quota() {
    use crate::reporting::wb_source::{WbClientReportTransport, WbReportSource};

    let campaigns = json!({"adverts":[{
        "status":9,
        "advert_list": (1_u64..=205)
            .map(|advert_id| json!({"advertId":advert_id}))
            .collect::<Vec<_>>()
    }]});
    let mut responses = vec![(200, campaigns.to_string())];
    responses.extend((0..5).map(|_| (200, "[]".to_owned())));
    responses.extend([
        (
            200,
            r#"{"data":{"currency":"RUB","products":[]}}"#.to_owned(),
        ),
        (200, r#"{"data":{"items":[]}}"#.to_owned()),
        (200, r#"{"data":{"listGoods":[]}}"#.to_owned()),
    ]);
    let (base_url, requests) = mock_http(responses);
    let interval = Duration::from_millis(100);
    let mut policy = ClientPolicy::immediate_single_attempt(Duration::from_secs(2));
    // Exercise the real client admission path with a scaled interval. The
    // production-policy test separately locks the documented 20s quota.
    policy.promotion_stats_interval = interval;
    let client = WbClient::new_for_test_with_policy(
        Duration::from_secs(2),
        credentials(),
        &base_url,
        policy,
    );
    let source = WbReportSource::new(WbClientReportTransport::new(client, "account".to_owned()));
    let started = Instant::now();
    let facts = tokio::time::timeout(
        Duration::from_secs(5),
        source.collect(NaiveDate::from_ymd_opt(2026, 8, 17).unwrap()),
    )
    .await
    .expect("all five background chunks must fit the bounded fixture")
    .expect("local quota must queue instead of aborting the report");
    assert!(started.elapsed() >= interval * 4);
    assert!(facts.sales.is_empty() && facts.advertising.is_empty());
    assert!(facts.stocks.is_empty() && facts.prices.is_empty());

    let requests = requests.try_iter().collect::<Vec<_>>();
    assert_eq!(requests.len(), 9);
    assert!(requests[0].starts_with("GET /adv/v1/promotion/count HTTP/1.1"));
    for (index, request) in requests[1..6].iter().enumerate() {
        let path = request
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap();
        let url = Url::parse(&format!("http://localhost{path}")).unwrap();
        assert_eq!(url.path(), PROMOTION_STATS_PATH);
        let ids = url.query_pairs().find(|(key, _)| key == "ids").unwrap().1;
        let start = u64::try_from(index).unwrap() * 50 + 1;
        let expected = (start..=(start + 49).min(205))
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(ids, expected);
    }
    assert!(requests[6].starts_with("POST /api/analytics/v3/sales-funnel/products "));
    assert!(requests[7].starts_with("POST /api/analytics/v1/stocks-report/wb-warehouses "));
    assert!(requests[8].starts_with("GET /api/v2/list/goods/filter?"));
}

#[tokio::test]
async fn background_report_preserves_vendor_429_after_a_completed_chunk() {
    use crate::reporting::wb_source::{
        WbClientReportTransport, WbReportSource, WbReportSourceError,
    };

    let campaigns = json!({"adverts":[{
        "status":9,
        "advert_list": (1_u64..=51)
            .map(|advert_id| json!({"advertId":advert_id}))
            .collect::<Vec<_>>()
    }]});
    let (base_url, requests, task) = raw_http(vec![
        raw_response(200, "", campaigns.to_string().as_bytes()),
        raw_response(200, "", b"[]"),
        // A vendor cooldown beyond the client's bounded retry policy is
        // terminal. The reporting adapter must not start a fresh request.
        raw_response(429, "Retry-After: 120\r\n", b"{}"),
    ]);
    let mut policy = ClientPolicy::production(Duration::from_secs(2));
    policy.promotion_stats_interval = Duration::from_millis(100);
    let client = WbClient::new_for_test_with_policy(
        Duration::from_secs(2),
        credentials(),
        &base_url,
        policy,
    );
    let source = WbReportSource::new(WbClientReportTransport::new(client, "account".to_owned()));
    assert_eq!(
        source
            .collect(NaiveDate::from_ymd_opt(2026, 8, 17).unwrap())
            .await,
        Err(WbReportSourceError::Upstream(WbErrorKind::RateLimited)),
        "one completed chunk must not produce partial report facts"
    );
    task.join().unwrap();
    let requests = requests.try_iter().collect::<Vec<_>>();
    assert_eq!(
        requests.len(),
        3,
        "no additional chunk or other source is requested"
    );
    assert!(requests[0].starts_with(b"GET /adv/v1/promotion/count "));
    assert!(requests[1].starts_with(b"GET /adv/v3/fullstats?"));
    assert!(requests[2].starts_with(b"GET /adv/v3/fullstats?"));
}
