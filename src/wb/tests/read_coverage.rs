use super::*;
use crate::wb::read_coverage::{
    CardErrorsQuery, ClaimQuery, FeedbackQuery, SuppliesQuery, SupplyGoodsQuery, SupplyIdQuery,
    TrashQuery,
};

fn query<T: serde::de::DeserializeOwned>(value: Value) -> T {
    serde_json::from_value(value).unwrap()
}

#[tokio::test]
async fn supplemental_reads_route_to_fixed_hosts_and_preserve_pagination() {
    let responses = |count| vec![(200, r#"{"data":[],"cursor":{"next":true}}"#.to_owned()); count];
    let (feedbacks, feedback_requests) = mock_http(responses(5));
    let (returns, return_requests) = mock_http(responses(1));
    let (content, content_requests) = mock_http(responses(4));
    let (supplies, supply_requests) = mock_http(responses(4));
    let mut urls = BaseUrls::for_test("http://127.0.0.1:1", "http://127.0.0.1:1");
    urls.feedbacks = feedbacks;
    urls.returns = returns;
    urls.content = content;
    urls.supplies = supplies;
    let client = WbClient::build(
        Duration::from_secs(2),
        credentials(),
        urls,
        ClientPolicy::immediate_single_attempt(Duration::from_secs(2)),
    );
    let feedback = query(
        json!({"is_answered":false,"nm_id":123,"limit":10,"offset":20,"date_from":100,"date_to":200,"order":"dateAsc"}),
    );
    client.reviews("account", &feedback).await.unwrap();
    client.questions("account", &feedback).await.unwrap();
    client.review("account", "review_1").await.unwrap();
    client.question("account", "question-1").await.unwrap();
    client
        .archived_reviews(
            "account",
            &query(json!({"nm_id":123,"limit":10,"offset":30})),
        )
        .await
        .unwrap();
    client
        .return_claims(
            "account",
            &query(json!({"is_archive":false,"nm_id":123,"limit":200,"offset":400})),
        )
        .await
        .unwrap();
    let errors = client.card_errors("account", &query(json!({"limit":20,"updated_at":"2026-09-01T00:00:00Z","batch_uuid":"11111111-1111-1111-1111-111111111111","ascending":true}))).await.unwrap();
    assert_eq!(errors["cursor"]["next"], true);
    client.card_limits("account").await.unwrap();
    client
        .cards_trash(
            "account",
            &query(json!({"limit":10,"trashed_at":"2026-09-01T00:00:00Z","nm_id":123})),
        )
        .await
        .unwrap();
    client.subject_characteristics("account", 42).await.unwrap();
    client.supplies("account", &query(json!({"dates":[{"from":"2026-09-01","till":"2026-09-02","type":"factDate"}],"status_ids":[5,6],"limit":10,"offset":20}))).await.unwrap();
    client
        .supply("account", &query(json!({"id":123,"is_preorder_id":true})))
        .await
        .unwrap();
    client
        .supply_goods(
            "account",
            &query(json!({"id":123,"is_preorder_id":true,"limit":10,"offset":20})),
        )
        .await
        .unwrap();
    client.supply_packages("account", 456).await.unwrap();
    for path in [
        "/api/v1/feedbacks?isAnswered=false&take=10&skip=20&order=dateAsc&nmId=123&dateFrom=100&dateTo=200",
        "/api/v1/questions?isAnswered=false&take=10&skip=20&order=dateAsc&nmId=123&dateFrom=100&dateTo=200",
        "/api/v1/feedback?id=review_1",
        "/api/v1/question?id=question-1",
        "/api/v1/feedbacks/archive?take=10&skip=30&order=dateDesc&nmId=123",
    ] {
        assert_request(&feedback_requests.recv().unwrap(), "GET", path);
    }
    assert_request(
        &return_requests.recv().unwrap(),
        "GET",
        "/api/v1/claims?is_archive=false&limit=200&offset=400&nm_id=123",
    );
    let error = content_requests.recv().unwrap();
    assert_request(&error, "POST", "/content/v2/cards/error/list");
    assert_eq!(
        body(&error),
        json!({"cursor":{"limit":20,"updatedAt":"2026-09-01T00:00:00Z","batchUUID":"11111111-1111-1111-1111-111111111111"},"order":{"ascending":true}})
    );
    assert_request(
        &content_requests.recv().unwrap(),
        "GET",
        "/content/v2/cards/limits",
    );
    let trash = content_requests.recv().unwrap();
    assert_request(&trash, "POST", "/content/v2/get/cards/trash");
    assert_eq!(
        body(&trash)["settings"]["cursor"],
        json!({"limit":10,"trashedAt":"2026-09-01T00:00:00Z","nmID":123})
    );
    assert_request(
        &content_requests.recv().unwrap(),
        "GET",
        "/content/v2/object/charcs/42",
    );
    let supplies = supply_requests.recv().unwrap();
    assert_request(&supplies, "POST", "/api/v1/supplies?limit=10&offset=20");
    assert_eq!(
        body(&supplies),
        json!({"dates":[{"from":"2026-09-01","till":"2026-09-02","type":"factDate"}],"statusIDs":[5,6]})
    );
    for path in [
        "/api/v1/supplies/123?isPreorderID=true",
        "/api/v1/supplies/123/goods?isPreorderID=true&limit=10&offset=20",
        "/api/v1/supplies/456/package",
    ] {
        assert_request(&supply_requests.recv().unwrap(), "GET", path);
    }
    for requests in [
        feedback_requests,
        return_requests,
        content_requests,
        supply_requests,
    ] {
        assert!(requests.try_recv().is_err());
    }
}

fn assert_request(request: &str, method: &str, path: &str) {
    assert!(
        request.starts_with(&format!("{method} {path} HTTP/1.1\r\n")),
        "{}",
        request.lines().next().unwrap()
    );
    assert!(
        request
            .to_ascii_lowercase()
            .contains("authorization: bearer test-token")
    );
}

fn body(request: &str) -> Value {
    serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap()
}

#[tokio::test]
async fn writes_and_noncanonical_dynamic_paths_fail_before_credentials() {
    let client = client("http://127.0.0.1:1");
    for (method, path) in [
        (Method::POST, "/api/v1/feedbacks/answer"),
        (Method::PATCH, "/api/v1/questions"),
        (Method::PATCH, "/api/v1/claim"),
        (Method::POST, "/content/v2/cards/upload"),
        (Method::POST, "/content/v2/cards/update"),
        (Method::POST, "/content/v2/cards/recover"),
        (Method::GET, "/content/v2/cards/error/list"),
        (Method::POST, "/api/v1/supplies/1"),
        (Method::DELETE, "/api/v1/supplies/1"),
        (Method::GET, "/api/v1/supplies/1?url=http://127.0.0.1"),
        (Method::GET, "/api/v1/supplies/01"),
        (Method::GET, "/api/v1/supplies/0/goods"),
        (Method::GET, "/api/v1/supplies/1/package/"),
        (Method::GET, "/api/v1/supplies/%31"),
        (Method::GET, "/content/v2/object/charcs/1/../2"),
        (Method::GET, "/content/v2/object/charcs/9223372036854775808"),
        (Method::GET, "https://example.invalid/api/v1/feedbacks"),
    ] {
        assert!(
            matches!(
                client.request_for_test("missing", method, path).await,
                Err(WbError::EndpointNotAllowed { .. })
            ),
            "{path}"
        );
    }
    for endpoint in crate::wb::coverage_policy::ENDPOINTS {
        let path = endpoint
            .path
            .replace("{ID}", "1")
            .replace("{subjectId}", "1");
        assert!(EndpointPolicy::for_request(&endpoint.method, &path).is_some());
        assert!(EndpointPolicy::for_request(&Method::DELETE, &path).is_none());
    }
}

#[tokio::test]
async fn bounded_arguments_fail_before_wire_dispatch() {
    let client = client("http://127.0.0.1:1");
    for bad in [
        json!({"is_answered":true,"limit":0}),
        json!({"is_answered":true,"date_from":2,"date_to":1}),
        json!({"is_answered":true,"offset":199_991}),
        json!({"is_answered":true,"nm_id":0}),
    ] {
        let q: FeedbackQuery = query(bad);
        assert!(matches!(
            client.reviews("missing", &q).await,
            Err(WbError::InvalidArguments { .. })
        ));
    }
    let q: FeedbackQuery = query(json!({"is_answered":true,"limit":100,"offset":9950}));
    assert!(matches!(
        client.questions("missing", &q).await,
        Err(WbError::InvalidArguments { .. })
    ));
    for id in ["", "a?redirect=x", "a/../b", "https://example.invalid"] {
        assert!(matches!(
            client.review("missing", id).await,
            Err(WbError::InvalidArguments { .. })
        ));
    }
    let claims: ClaimQuery = query(json!({"is_archive":false,"id":"not-a-uuid"}));
    assert!(matches!(
        client.return_claims("missing", &claims).await,
        Err(WbError::InvalidArguments { .. })
    ));
    let errors: CardErrorsQuery = query(json!({"updated_at":"2026-09-01T00:00:00Z"}));
    assert!(matches!(
        client.card_errors("missing", &errors).await,
        Err(WbError::InvalidArguments { .. })
    ));
    let trash: TrashQuery = query(json!({"nm_id":1}));
    assert!(matches!(
        client.cards_trash("missing", &trash).await,
        Err(WbError::InvalidArguments { .. })
    ));
    let supplies: SuppliesQuery = query(json!({"status_ids":[7]}));
    assert!(matches!(
        client.supplies("missing", &supplies).await,
        Err(WbError::InvalidArguments { .. })
    ));
    let supply: SupplyIdQuery = query(json!({"id":0}));
    assert!(matches!(
        client.supply("missing", &supply).await,
        Err(WbError::InvalidArguments { .. })
    ));
    let goods: SupplyGoodsQuery = query(json!({"id":1,"limit":1001}));
    assert!(matches!(
        client.supply_goods("missing", &goods).await,
        Err(WbError::InvalidArguments { .. })
    ));
}

#[tokio::test]
async fn customer_api_errors_are_not_retried_or_converted_to_empty_data() {
    for status in [401, 402, 403, 429, 500] {
        let (base, requests) = mock_http(vec![(status, "{}".into())]);
        let client = WbClient::build(
            Duration::from_secs(2),
            credentials(),
            BaseUrls::for_test(&base, &base),
            retrying_policy(Duration::from_secs(2)),
        );
        let error = client
            .reviews("account", &query(json!({"is_answered":false})))
            .await
            .unwrap_err();
        assert!(
            match status {
                401 => matches!(error, WbError::Unauthorized { .. }),
                402 => matches!(error, WbError::SubscriptionRequired { .. }),
                403 => matches!(error, WbError::Forbidden { .. }),
                429 => matches!(error, WbError::RateLimited { .. }),
                _ =>
                    matches!(error, WbError::Api { status: actual, .. } if actual.as_u16() == status),
            },
            "{error}"
        );
        assert!(
            requests
                .recv()
                .unwrap()
                .starts_with("GET /api/v1/feedbacks?")
        );
        assert!(requests.try_recv().is_err());
    }
}

#[tokio::test]
async fn customer_and_supply_quotas_are_separate_and_fail_fast() {
    let policy = ClientPolicy::production(Duration::from_secs(2));
    let limiter = TokenLimiter::new();
    for (class, seconds) in [
        (RequestClass::FeedbackReport, 720),
        (RequestClass::ReturnClaims, 3600),
        (RequestClass::SupplyReport, 3600),
        (RequestClass::CardErrors, 6),
    ] {
        assert_eq!(policy.interval(class), Duration::from_secs(seconds));
        limiter
            .try_claim(class, policy.interval(class))
            .await
            .unwrap();
        assert!(
            limiter
                .try_claim(class, policy.interval(class))
                .await
                .is_err()
        );
        assert!(!class.allows_automatic_retry());
    }
}
