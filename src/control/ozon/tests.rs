use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

use crate::config::PerformanceCredentials;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use reqwest::StatusCode;
use serde_json::json;

use super::{
    OzonAdsWriteClient, OzonCampaignCreateRequest, OzonCampaignLaunchManifest,
    OzonCampaignLaunchSpec, OzonCampaignProduct, OzonCampaignProductsRequest, OzonCampaignStrategy,
    OzonGuardStopReadback, OzonGuardedWriteError, OzonLaunchStatus, OzonPlacement,
    OzonPlanRepository, OzonPlanStoreError, OzonStaticGuardMutation, OzonStaticGuardWriteIntent,
    OzonWriteError, OzonWriteErrorKind,
    client::{validate_create_request, validate_products_request},
    model::{OzonLaunchAction, OzonLaunchClaimMode},
    plan::provider_title_for_plan_id,
    prepare_campaign_launch_manifest,
    repository::{
        create_identity_preflight_digest_for, digest_fields, validate_digest, validate_error_class,
        validate_identity, validate_manifest, validate_reference,
    },
};
use crate::control::plan::CONTROL_DB_TEST_LOCK;

fn credentials() -> PerformanceCredentials {
    PerformanceCredentials {
        client_id: "write-client".to_owned(),
        client_secret: "write-secret".to_owned(),
    }
}

fn create_request() -> OzonCampaignCreateRequest {
    OzonCampaignCreateRequest {
        title: "Diana 2026-09-02 DRR15".to_owned(),
        from_date: "2026-09-02".to_owned(),
        to_date: "2026-09-08".to_owned(),
        weekly_budget: 10_000_000_000,
        placement: OzonPlacement::SearchAndCategory,
        product_autopilot_strategy: OzonCampaignStrategy::TargetBids,
    }
}

#[test]
fn repository_validators_reject_noncanonical_values_and_manifest_bounds() {
    assert_eq!(validate_digest("bad"), Err(OzonPlanStoreError::InvalidPlan));
    assert_eq!(
        validate_identity("bad identity"),
        Err(OzonPlanStoreError::InvalidPlan)
    );
    assert_eq!(
        validate_reference("bad reference"),
        Err(OzonPlanStoreError::InvalidPlan)
    );
    assert_eq!(
        validate_error_class("BadClass"),
        Err(OzonPlanStoreError::InvalidPlan)
    );

    let spec = OzonCampaignLaunchSpec {
        account_id: "ozon_one".to_owned(),
        title: "Bounded launch".to_owned(),
        from_date: "2026-09-02".to_owned(),
        to_date: "2026-09-08".to_owned(),
        skus: vec![1001],
        weekly_budget_microrubles: 2_000_000_000,
        per_sku_spend_cap_microrubles: 2_000_000_000,
        initial_cpc_bid_microrubles: 7_000_000,
        max_cpc_bid_microrubles: 12_000_000,
        target_drr_percent: 15,
        target_position: 30,
    };
    let mut manifest = prepare_campaign_launch_manifest(
        "launcher",
        1,
        1,
        &"a".repeat(64),
        "ozon_one",
        &[1001],
        2_000_000_000,
        2_000_000_000,
        7_000_000,
        12_000_000,
        15,
        30,
        spec,
    )
    .unwrap();
    manifest.policy_revision = 0;
    assert_eq!(
        validate_manifest(&manifest),
        Err(OzonPlanStoreError::InvalidPlan)
    );
}

fn target_cir_products() -> OzonCampaignProductsRequest {
    OzonCampaignProductsRequest {
        bids: vec![OzonCampaignProduct {
            sku: 3_457_585_933,
            bid: None,
            target_cir: Some(15),
            top_position: None,
        }],
    }
}

fn target_bid_products() -> OzonCampaignProductsRequest {
    OzonCampaignProductsRequest {
        bids: vec![OzonCampaignProduct {
            sku: 3_457_585_933,
            bid: Some(7_000_000),
            target_cir: None,
            top_position: None,
        }],
    }
}

#[test]
fn strategy_and_product_guards_fail_closed() {
    let mut request = create_request();
    request.placement = OzonPlacement::TopPromotion;
    assert!(validate_create_request(&request).is_err());

    let mut products = target_cir_products();
    products.bids[0].top_position = Some(12);
    assert!(validate_products_request(OzonCampaignStrategy::TargetCir, &products).is_err());

    products.bids[0].target_cir = None;
    assert!(validate_products_request(OzonCampaignStrategy::TopPromotion, &products).is_ok());
    products.bids[0].top_position = Some(10);
    assert!(validate_products_request(OzonCampaignStrategy::TopPromotion, &products).is_err());

    assert!(
        validate_products_request(OzonCampaignStrategy::TargetCir, &target_cir_products()).is_ok()
    );

    assert!(
        validate_products_request(OzonCampaignStrategy::TargetBids, &target_bid_products()).is_ok()
    );
}

#[test]
fn all_create_and_product_payload_invariants_are_enforced() {
    let mut valid = create_request();
    assert!(validate_create_request(&valid).is_ok());
    valid.product_autopilot_strategy = OzonCampaignStrategy::TargetCir;
    assert!(validate_create_request(&valid).is_ok());
    valid.placement = OzonPlacement::TopPromotion;
    valid.product_autopilot_strategy = OzonCampaignStrategy::TopPromotion;
    assert!(validate_create_request(&valid).is_ok());

    for mutate in [
        |request: &mut OzonCampaignCreateRequest| request.title.clear(),
        |request: &mut OzonCampaignCreateRequest| request.title = " padded ".to_owned(),
        |request: &mut OzonCampaignCreateRequest| request.title = "bad\nname".to_owned(),
        |request: &mut OzonCampaignCreateRequest| request.title = "x".repeat(129),
        |request: &mut OzonCampaignCreateRequest| request.weekly_budget = 0,
        |request: &mut OzonCampaignCreateRequest| request.from_date = "bad".to_owned(),
        |request: &mut OzonCampaignCreateRequest| request.to_date = "bad".to_owned(),
        |request: &mut OzonCampaignCreateRequest| request.to_date = "2026-09-01".to_owned(),
        |request: &mut OzonCampaignCreateRequest| request.to_date = "2026-10-31".to_owned(),
    ] {
        let mut request = create_request();
        mutate(&mut request);
        assert!(validate_create_request(&request).is_err());
    }

    let empty = OzonCampaignProductsRequest { bids: Vec::new() };
    assert!(validate_products_request(OzonCampaignStrategy::TargetBids, &empty).is_err());
    let too_many = OzonCampaignProductsRequest {
        bids: (1..=51)
            .map(|sku| OzonCampaignProduct {
                sku,
                bid: Some(1),
                target_cir: None,
                top_position: None,
            })
            .collect(),
    };
    assert!(validate_products_request(OzonCampaignStrategy::TargetBids, &too_many).is_err());

    for product in [
        OzonCampaignProduct {
            sku: 0,
            bid: Some(1),
            target_cir: None,
            top_position: None,
        },
        OzonCampaignProduct {
            sku: 1,
            bid: Some(0),
            target_cir: None,
            top_position: None,
        },
        OzonCampaignProduct {
            sku: 1,
            bid: None,
            target_cir: Some(9),
            top_position: None,
        },
    ] {
        let request = OzonCampaignProductsRequest {
            bids: vec![product],
        };
        assert!(
            [
                OzonCampaignStrategy::TargetBids,
                OzonCampaignStrategy::TargetCir,
                OzonCampaignStrategy::TopPromotion,
            ]
            .into_iter()
            .any(|strategy| validate_products_request(strategy, &request).is_err())
        );
    }

    let duplicate = OzonCampaignProductsRequest {
        bids: vec![target_bid_products().bids[0].clone(); 2],
    };
    assert!(validate_products_request(OzonCampaignStrategy::TargetBids, &duplicate).is_err());
}

#[test]
fn production_client_constructor_and_error_classification_fail_closed() {
    assert!(OzonAdsWriteClient::new(Duration::ZERO, credentials(), "http://proxy:3128").is_err());
    assert!(
        OzonAdsWriteClient::new(Duration::from_secs(31), credentials(), "http://proxy:3128")
            .is_err()
    );
    for credentials in [
        PerformanceCredentials {
            client_id: String::new(),
            client_secret: "secret".to_owned(),
        },
        PerformanceCredentials {
            client_id: "client".to_owned(),
            client_secret: " bad ".to_owned(),
        },
        PerformanceCredentials {
            client_id: "не-ascii".to_owned(),
            client_secret: "secret".to_owned(),
        },
    ] {
        assert!(
            OzonAdsWriteClient::new(Duration::from_secs(1), credentials, "http://proxy:3128")
                .is_err()
        );
    }
    assert!(
        OzonAdsWriteClient::new(Duration::from_secs(1), credentials(), "://bad proxy").is_err()
    );
    let client = OzonAdsWriteClient::new(
        Duration::from_secs(1),
        credentials(),
        "http://proxy.example:3128",
    )
    .expect("valid isolated production client");
    let debug = format!("{client:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("write-secret"));

    for error in [
        OzonWriteError::InvalidRequest,
        OzonWriteError::InvalidToken,
        OzonWriteError::Unauthorized,
        OzonWriteError::Forbidden,
        OzonWriteError::Http {
            status: StatusCode::BAD_REQUEST,
        },
        OzonWriteError::TokenResponseTooLarge,
    ] {
        assert_eq!(error.kind(), OzonWriteErrorKind::Definite);
    }
    for error in [
        OzonWriteError::AmbiguousTransport,
        OzonWriteError::ResponseTooLarge,
        OzonWriteError::InvalidCreateResponse,
        OzonWriteError::Http {
            status: StatusCode::SERVICE_UNAVAILABLE,
        },
    ] {
        assert_eq!(error.kind(), OzonWriteErrorKind::Ambiguous);
    }
}

#[test]
fn exact_payload_uses_official_field_names_and_microrubles() {
    let value = serde_json::to_value(create_request()).unwrap();
    assert_eq!(value["weeklyBudget"], 10_000_000_000_u64);
    assert_eq!(value["productAutopilotStrategy"], "TARGET_BIDS");
    assert_eq!(value["placement"], "PLACEMENT_SEARCH_AND_CATEGORY");

    let value = serde_json::to_value(target_cir_products()).unwrap();
    assert_eq!(value["bids"][0]["sku"], 3_457_585_933_u64);
    assert_eq!(value["bids"][0]["targetCir"], 15);
    assert!(value["bids"][0].get("topPosition").is_none());

    let value = serde_json::to_value(target_bid_products()).unwrap();
    assert_eq!(value["bids"][0]["bid"], 7_000_000_u64);
    assert!(value["bids"][0].get("targetCir").is_none());
}

#[tokio::test]
async fn guarded_create_obtains_token_then_runs_one_post() {
    let (base_url, requests) = mock_http(vec![
        (
            200,
            r#"{"access_token":"token","token_type":"Bearer","expires_in":1800}"#,
        ),
        (200, r#"{"campaignId":"12345"}"#),
    ]);
    let client = OzonAdsWriteClient::new_for_test(&base_url, credentials(), Duration::from_secs(3));
    let permit_calls = Arc::new(Mutex::new(0_u8));
    let permit_calls_clone = Arc::clone(&permit_calls);
    let campaign_id = client
        .create_campaign_with_permit(&create_request(), move || async move {
            *permit_calls_clone.lock().unwrap() += 1;
            Ok::<_, ()>(())
        })
        .await
        .unwrap();
    assert_eq!(campaign_id, 12_345);
    assert_eq!(*permit_calls.lock().unwrap(), 1);
    let captured = requests.lock().unwrap();
    assert_eq!(captured.len(), 2);
    assert!(captured[0].starts_with("POST /api/client/token "));
    assert!(captured[1].starts_with("POST /api/client/campaign/cpc/v2/product "));
    assert!(captured[1].contains("\"weeklyBudget\":10000000000"));
    drop(captured);
}

#[tokio::test]
async fn rejected_permit_never_reaches_write_endpoint() {
    let (base_url, requests) = mock_http(vec![(
        200,
        r#"{"access_token":"token","token_type":"Bearer","expires_in":1800}"#,
    )]);
    let client = OzonAdsWriteClient::new_for_test(&base_url, credentials(), Duration::from_secs(3));
    let result = client
        .create_campaign_with_permit(&create_request(), || async { Err::<(), _>("gate") })
        .await;
    assert!(matches!(result, Err(OzonGuardedWriteError::Permit("gate"))));
    let captured = requests.lock().unwrap();
    assert_eq!(captured.len(), 1);
    assert!(captured[0].starts_with("POST /api/client/token "));
    drop(captured);
}

#[tokio::test]
async fn guarded_deactivate_uses_exact_campaign_endpoint() {
    let (base_url, requests) = mock_http(vec![
        (
            200,
            r#"{"access_token":"token","token_type":"Bearer","expires_in":1800}"#,
        ),
        (200, "{}"),
    ]);
    let client = OzonAdsWriteClient::new_for_test(&base_url, credentials(), Duration::from_secs(3));
    client
        .deactivate_campaign_with_permit(12_345, || async { Ok::<_, ()>(()) })
        .await
        .unwrap();
    let captured = requests.lock().unwrap();
    assert_eq!(captured.len(), 2);
    assert!(captured[1].starts_with("POST /api/client/campaign/12345/deactivate "));
    assert!(captured[1].ends_with("\r\n\r\n{}"));
    drop(captured);
}

#[tokio::test]
async fn guarded_product_update_uses_put_and_exact_campaign_endpoint() {
    let (base_url, requests) = mock_http(vec![
        (
            200,
            r#"{"access_token":"token","token_type":"Bearer","expires_in":1800}"#,
        ),
        (200, "{}"),
    ]);
    let client = OzonAdsWriteClient::new_for_test(&base_url, credentials(), Duration::from_secs(3));
    client
        .update_products_with_permit(
            12_345,
            OzonCampaignStrategy::TargetBids,
            &target_bid_products(),
            || async { Ok::<_, ()>(()) },
        )
        .await
        .unwrap();
    let captured = requests.lock().unwrap();
    assert_eq!(captured.len(), 2);
    assert!(captured[1].starts_with("PUT /api/client/campaign/12345/products "));
    assert!(captured[1].ends_with("\r\n\r\n{\"bids\":[{\"sku\":3457585933,\"bid\":7000000}]}"));
    drop(captured);
}

#[tokio::test]
async fn guarded_add_activate_and_cached_token_use_exact_endpoints() {
    let (base_url, requests) = mock_http(vec![
        (
            200,
            r#"{"access_token":"token","token_type":"bearer","expires_in":1800}"#,
        ),
        (200, "{}"),
        (200, "{}"),
    ]);
    let client = OzonAdsWriteClient::new_for_test(&base_url, credentials(), Duration::from_secs(3));
    client
        .add_products_with_permit(
            12_345,
            OzonCampaignStrategy::TargetBids,
            &target_bid_products(),
            || async { Ok::<_, ()>(()) },
        )
        .await
        .unwrap();
    client
        .activate_campaign_with_permit(12_345, || async { Ok::<_, ()>(()) })
        .await
        .unwrap();
    let captured = requests.lock().unwrap();
    assert_eq!(captured.len(), 3);
    assert!(captured[1].starts_with("POST /api/client/campaign/12345/products "));
    assert!(captured[2].starts_with("POST /api/client/campaign/12345/activate "));
    drop(captured);
}

#[tokio::test]
async fn guarded_client_paces_writes_and_bounds_write_responses() {
    let (base_url, _) = mock_http(vec![
        (
            200,
            r#"{"access_token":"token","token_type":"Bearer","expires_in":1800}"#,
        ),
        (200, "{}"),
        (200, "{}"),
    ]);
    let client = OzonAdsWriteClient::new_for_test_with_interval(
        &base_url,
        credentials(),
        Duration::from_secs(2),
        Duration::from_millis(1),
    );
    client
        .activate_campaign_with_permit(1, || async { Ok::<_, ()>(()) })
        .await
        .unwrap();
    client
        .deactivate_campaign_with_permit(1, || async { Ok::<_, ()>(()) })
        .await
        .unwrap();

    let (base_url, _) = mock_http(vec![
        (
            200,
            r#"{"access_token":"token","token_type":"Bearer","expires_in":1800}"#,
        ),
        (403, "{}"),
    ]);
    let client = OzonAdsWriteClient::new_for_test(&base_url, credentials(), Duration::from_secs(2));
    assert!(matches!(
        client
            .activate_campaign_with_permit(1, || async { Ok::<_, ()>(()) })
            .await,
        Err(OzonGuardedWriteError::Write(OzonWriteError::Forbidden))
    ));

    let oversized: &'static str = Box::leak("x".repeat(1_048_577).into_boxed_str());
    let (base_url, _) = mock_http(vec![
        (
            200,
            r#"{"access_token":"token","token_type":"Bearer","expires_in":1800}"#,
        ),
        (200, oversized),
    ]);
    let client = OzonAdsWriteClient::new_for_test(&base_url, credentials(), Duration::from_secs(2));
    let error = client
        .activate_campaign_with_permit(1, || async { Ok::<_, ()>(()) })
        .await
        .unwrap_err();
    let OzonGuardedWriteError::Write(error) = error else {
        panic!("unexpected permit error");
    };
    assert!(matches!(error, OzonWriteError::ResponseTooLarge));
    assert_eq!(error.kind(), OzonWriteErrorKind::Ambiguous);
}

#[tokio::test]
async fn oversized_token_is_definite_because_no_mutation_was_sent() {
    let oversized: &'static str = Box::leak("x".repeat(65_537).into_boxed_str());
    let (base_url, requests) = mock_http(vec![(200, oversized)]);
    let client = OzonAdsWriteClient::new_for_test(&base_url, credentials(), Duration::from_secs(2));
    let error = client
        .activate_campaign_with_permit(1, || async { Ok::<_, ()>(()) })
        .await
        .unwrap_err();
    let OzonGuardedWriteError::Write(error) = error else {
        panic!("unexpected permit error");
    };
    assert!(matches!(error, OzonWriteError::TokenResponseTooLarge));
    assert_eq!(error.kind(), OzonWriteErrorKind::Definite);
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn token_transport_http_and_invalid_header_never_invoke_the_write_permit() {
    let transport_permit = Arc::new(AtomicBool::new(false));
    let transport_client = OzonAdsWriteClient::new_for_test(
        "http://127.0.0.1:1",
        credentials(),
        Duration::from_millis(100),
    );
    let called = Arc::clone(&transport_permit);
    let error = transport_client
        .activate_campaign_with_permit(1, move || async move {
            called.store(true, Ordering::Release);
            Ok::<_, ()>(())
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        OzonGuardedWriteError::Write(OzonWriteError::TokenTransport)
    ));
    assert!(!transport_permit.load(Ordering::Acquire));

    let (http_base, http_requests) = mock_http(vec![(503, "{}")]);
    let http_permit = Arc::new(AtomicBool::new(false));
    let http_client =
        OzonAdsWriteClient::new_for_test(&http_base, credentials(), Duration::from_secs(1));
    let called = Arc::clone(&http_permit);
    let error = http_client
        .activate_campaign_with_permit(1, move || async move {
            called.store(true, Ordering::Release);
            Ok::<_, ()>(())
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        OzonGuardedWriteError::Write(OzonWriteError::TokenHttp {
            status: StatusCode::SERVICE_UNAVAILABLE
        })
    ));
    assert!(!http_permit.load(Ordering::Acquire));
    assert_eq!(http_requests.lock().unwrap().len(), 1);

    let (header_base, header_requests) = mock_http(vec![(
        200,
        r#"{"access_token":"bad\ntoken","token_type":"Bearer","expires_in":1800}"#,
    )]);
    let header_permit = Arc::new(AtomicBool::new(false));
    let header_client =
        OzonAdsWriteClient::new_for_test(&header_base, credentials(), Duration::from_secs(1));
    let called = Arc::clone(&header_permit);
    let error = header_client
        .activate_campaign_with_permit(1, move || async move {
            called.store(true, Ordering::Release);
            Ok::<_, ()>(())
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        OzonGuardedWriteError::Write(OzonWriteError::InvalidToken)
    ));
    assert!(!header_permit.load(Ordering::Acquire));
    assert_eq!(header_requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn guarded_client_rejects_bad_ids_payloads_tokens_statuses_and_create_responses() {
    let client = OzonAdsWriteClient::new_for_test(
        "http://127.0.0.1:1",
        credentials(),
        Duration::from_millis(100),
    );
    assert!(matches!(
        client
            .activate_campaign_with_permit(0, || async { Ok::<_, ()>(()) })
            .await,
        Err(OzonGuardedWriteError::Write(OzonWriteError::InvalidRequest))
    ));
    assert!(matches!(
        client
            .update_products_with_permit(
                1,
                OzonCampaignStrategy::TargetBids,
                &OzonCampaignProductsRequest { bids: Vec::new() },
                || async { Ok::<_, ()>(()) },
            )
            .await,
        Err(OzonGuardedWriteError::Write(OzonWriteError::InvalidRequest))
    ));

    for token_body in [
        "not-json",
        r#"{"access_token":"","token_type":"Bearer","expires_in":1800}"#,
        r#"{"access_token":"token","token_type":"Basic","expires_in":1800}"#,
        r#"{"access_token":"token","token_type":"Bearer","expires_in":0}"#,
        r#"{"access_token":"token","token_type":"Bearer","expires_in":86401}"#,
    ] {
        let leaked: &'static str = Box::leak(token_body.to_owned().into_boxed_str());
        let (base_url, _) = mock_http(vec![(200, leaked)]);
        let client =
            OzonAdsWriteClient::new_for_test(&base_url, credentials(), Duration::from_secs(1));
        assert!(matches!(
            client
                .activate_campaign_with_permit(1, || async { Ok::<_, ()>(()) })
                .await,
            Err(OzonGuardedWriteError::Write(OzonWriteError::InvalidToken))
        ));
    }

    for (status, expected) in [
        (401, OzonWriteErrorKind::Definite),
        (403, OzonWriteErrorKind::Definite),
        (429, OzonWriteErrorKind::Definite),
        (503, OzonWriteErrorKind::Definite),
    ] {
        let (base_url, _) = mock_http(vec![(status, "{}")]);
        let client =
            OzonAdsWriteClient::new_for_test(&base_url, credentials(), Duration::from_secs(1));
        let error = client
            .activate_campaign_with_permit(1, || async { Ok::<_, ()>(()) })
            .await
            .unwrap_err();
        let OzonGuardedWriteError::Write(error) = error else {
            panic!("unexpected permit error");
        };
        assert_eq!(error.kind(), expected);
    }

    for response in [
        "not-json",
        r#"{"campaignId":"0"}"#,
        r#"{"campaignId":"01"}"#,
    ] {
        let leaked: &'static str = Box::leak(response.to_owned().into_boxed_str());
        let (base_url, _) = mock_http(vec![
            (
                200,
                r#"{"access_token":"token","token_type":"Bearer","expires_in":1800}"#,
            ),
            (200, leaked),
        ]);
        let client =
            OzonAdsWriteClient::new_for_test(&base_url, credentials(), Duration::from_secs(1));
        assert!(matches!(
            client
                .create_campaign_with_permit(&create_request(), || async { Ok::<_, ()>(()) })
                .await,
            Err(OzonGuardedWriteError::Write(
                OzonWriteError::InvalidCreateResponse
            ))
        ));
    }
}

fn durable_manifest(sku: u64, title: &str, policy_digest: &str) -> OzonCampaignLaunchManifest {
    let spec = OzonCampaignLaunchSpec {
        account_id: "ozon_one".to_owned(),
        title: title.to_owned(),
        from_date: "2026-09-04".to_owned(),
        to_date: "2026-09-11".to_owned(),
        skus: vec![sku],
        weekly_budget_microrubles: 2_000_000_000,
        per_sku_spend_cap_microrubles: 2_000_000_000,
        initial_cpc_bid_microrubles: 7_000_000,
        max_cpc_bid_microrubles: 12_000_000,
        target_drr_percent: 15,
        target_position: 30,
    };
    prepare_campaign_launch_manifest(
        "launcher",
        1,
        1,
        policy_digest,
        "ozon_one",
        &[sku],
        2_000_000_000,
        2_000_000_000,
        7_000_000,
        12_000_000,
        15,
        30,
        spec,
    )
    .unwrap()
}

fn migration_body(source: &'static str) -> &'static str {
    source
        .strip_prefix("\\set ON_ERROR_STOP on\n\n")
        .expect("migration starts with the psql error-stop directive")
}

async fn create_upgrade_test_database(
    admin_url: &str,
    database_name: &str,
) -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
) {
    assert!(
        database_name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    );
    let (admin, connection) = tokio_postgres::connect(admin_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection_task = tokio::spawn(connection);
    admin
        .batch_execute(&format!(
            "DROP DATABASE IF EXISTS {database_name} WITH (FORCE)"
        ))
        .await
        .unwrap();
    admin
        .batch_execute(&format!("CREATE DATABASE {database_name}"))
        .await
        .unwrap();
    drop(admin);
    connection_task.await.unwrap().unwrap();

    let mut config = admin_url.parse::<tokio_postgres::Config>().unwrap();
    config.dbname(database_name);
    let (database, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
    (database, tokio::spawn(connection))
}

async fn drop_upgrade_test_database(admin_url: &str, database_name: &str) {
    let (admin, connection) = tokio_postgres::connect(admin_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection_task = tokio::spawn(connection);
    admin
        .batch_execute(&format!(
            "DROP DATABASE IF EXISTS {database_name} WITH (FORCE)"
        ))
        .await
        .unwrap();
    drop(admin);
    connection_task.await.unwrap().unwrap();
}

#[derive(Clone, Copy)]
enum LegacyPlanCorruption {
    None,
    Manifest,
    PlanDigest,
    PlanId,
}

async fn seed_legacy_plan(
    database: &tokio_postgres::Client,
    sku: u64,
    status: &str,
    action_event: Option<&str>,
    corruption: LegacyPlanCorruption,
) -> String {
    let policy_digest = "a".repeat(64);
    database
        .execute(
            "INSERT INTO control.ozon_policy_revisions( \
             policy_revision,schema_version,policy_digest,registered_at) \
             SELECT 1,1,$1,clock_timestamp() WHERE NOT EXISTS ( \
                 SELECT 1 FROM control.ozon_policy_revisions \
                 WHERE policy_revision=1)",
            &[&policy_digest],
        )
        .await
        .unwrap();
    let mut manifest = durable_manifest(sku, &format!("Legacy {sku}"), &policy_digest);
    // The 024 repository hashed its first database timestamp, while the 024
    // BEFORE INSERT trigger replaced the persisted timestamps. Keep those
    // values deterministically different so this fixture exercises the real
    // upgrade compatibility path instead of manufacturing a 025-style row.
    let created_at = Utc::now() - ChronoDuration::seconds(1);
    let expires_at = created_at + ChronoDuration::minutes(15);
    let mut plan_digest = digest_fields(&[
        b"mcp-ozon/ozon-plan/v1",
        manifest.manifest_digest.as_bytes(),
        &created_at.timestamp_micros().to_be_bytes(),
        &expires_at.timestamp_micros().to_be_bytes(),
    ]);
    let mut plan_id = digest_fields(&[b"mcp-ozon/ozon-plan-id/v1", plan_digest.as_bytes()]);
    match corruption {
        LegacyPlanCorruption::None => {}
        LegacyPlanCorruption::Manifest => manifest.spec.title.push_str(" forged"),
        LegacyPlanCorruption::PlanDigest => plan_digest = "b".repeat(64),
        LegacyPlanCorruption::PlanId => plan_id = "c".repeat(64),
    }
    let manifest_json = serde_json::to_string(&manifest).unwrap();
    let sku = i64::try_from(sku).unwrap();
    database
        .execute(
            "INSERT INTO control.ozon_campaign_plans( \
             plan_id,plan_digest,actor_id,account_id,sku,schema_version, \
             policy_revision,policy_digest,manifest_json,status,created_at,expires_at) \
             VALUES($1,$2,'launcher','ozon_one',$3,1,1,$4,$5,'prepared',$6,$7)",
            &[
                &plan_id,
                &plan_digest,
                &sku,
                &policy_digest,
                &manifest_json,
                &created_at,
                &expires_at,
            ],
        )
        .await
        .unwrap();
    let persisted_created_at: DateTime<Utc> = database
        .query_one(
            "SELECT created_at FROM control.ozon_campaign_plans WHERE plan_id=$1",
            &[&plan_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_ne!(persisted_created_at, created_at);
    let prepared_payload = serde_json::json!({
        "plan_digest": &plan_digest,
        "manifest_digest": &manifest.manifest_digest,
    })
    .to_string();
    database
        .execute(
            "INSERT INTO control.ozon_campaign_audit_events( \
             plan_id,actor_id,event_type,payload_json,created_at) \
             VALUES($1,'launcher','prepared',$2,$3)",
            &[&plan_id, &prepared_payload, &persisted_created_at],
        )
        .await
        .unwrap();
    if status != "prepared" {
        let approval_id =
            digest_fields(&[b"mcp-ozon/legacy-approval-fixture/v1", plan_id.as_bytes()]);
        database
            .execute(
                "INSERT INTO control.ozon_campaign_plan_approvals( \
                 approval_id,plan_id,plan_digest,approver_id,reference,approved_at,expires_at) \
                 VALUES($1,$2,$3,'approver','test/legacy',$4,$5)",
                &[
                    &approval_id,
                    &plan_id,
                    &plan_digest,
                    &persisted_created_at,
                    &(persisted_created_at + ChronoDuration::minutes(2)),
                ],
            )
            .await
            .unwrap();
    }
    let operation_started_at =
        (!matches!(status, "prepared" | "approved")).then_some(persisted_created_at);
    let finished_at =
        matches!(status, "applied" | "ambiguous" | "failed").then_some(persisted_created_at);
    let error_class = matches!(status, "ambiguous" | "failed").then_some("legacy_http_error");
    let campaign_id = (matches!(
        status,
        "created" | "adding_products" | "products_added" | "activating" | "applied"
    ) || (matches!(status, "ambiguous" | "failed")
        && matches!(action_event, Some("adding_products" | "activating"))))
    .then_some(90_000 + sku);
    let readback_json = (status == "applied").then_some("{}");
    if status != "prepared" {
        database
            .batch_execute("ALTER TABLE control.ozon_campaign_plans DISABLE TRIGGER USER")
            .await
            .unwrap();
        database
            .execute(
                "UPDATE control.ozon_campaign_plans SET status=$2,campaign_id=$3, \
                 operation_started_at=$4,finished_at=$5,last_error_class=$6, \
                 readback_json=$7 WHERE plan_id=$1",
                &[
                    &plan_id,
                    &status,
                    &campaign_id,
                    &operation_started_at,
                    &finished_at,
                    &error_class,
                    &readback_json,
                ],
            )
            .await
            .unwrap();
        database
            .batch_execute("ALTER TABLE control.ozon_campaign_plans ENABLE TRIGGER USER")
            .await
            .unwrap();
    }
    if let Some(event_type) = action_event {
        database
            .execute(
                "INSERT INTO control.ozon_campaign_audit_events( \
                 plan_id,actor_id,event_type,payload_json,created_at) \
                 VALUES($1,'launcher',$2,'{}',$3)",
                &[&plan_id, &event_type, &persisted_created_at],
            )
            .await
            .unwrap();
    }
    plan_id
}

#[tokio::test]
async fn migration_025_validates_legacy_identity_chain_and_uncertain_writes() {
    let Ok(admin_url) = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL") else {
        return;
    };
    let _database_guard = CONTROL_DB_TEST_LOCK.lock().await;
    let migration_024 = migration_body(include_str!(
        "../../../position-monitor/initdb/024_ozon_control_campaign_plans.sql"
    ));
    let migration_025 = migration_body(include_str!(
        "../../../position-monitor/initdb/025_ozon_durable_launch_workflow.sql"
    ));

    let valid_name = format!("ozon_upgrade_valid_{}", std::process::id());
    let (valid, valid_connection) = create_upgrade_test_database(&admin_url, &valid_name).await;
    valid.batch_execute(migration_024).await.unwrap();
    let prepared_id =
        seed_legacy_plan(&valid, 2001, "prepared", None, LegacyPlanCorruption::None).await;
    let legacy_timestamp_chain = valid
        .query_one(
            "SELECT plan_digest,manifest_json,created_at,expires_at \
             FROM control.ozon_campaign_plans WHERE plan_id=$1",
            &[&prepared_id],
        )
        .await
        .unwrap();
    let legacy_manifest: OzonCampaignLaunchManifest =
        serde_json::from_str(legacy_timestamp_chain.get::<_, &str>(1)).unwrap();
    let persisted_created_at: DateTime<Utc> = legacy_timestamp_chain.get(2);
    let persisted_expires_at: DateTime<Utc> = legacy_timestamp_chain.get(3);
    let recomputed_from_replaced_timestamp = digest_fields(&[
        b"mcp-ozon/ozon-plan/v1",
        legacy_manifest.manifest_digest.as_bytes(),
        &persisted_created_at.timestamp_micros().to_be_bytes(),
        &persisted_expires_at.timestamp_micros().to_be_bytes(),
    ]);
    assert_ne!(
        legacy_timestamp_chain.get::<_, &str>(0),
        recomputed_from_replaced_timestamp,
        "the fixture must preserve migration 024's replaced-timestamp identity mismatch"
    );
    let created_id = seed_legacy_plan(
        &valid,
        2002,
        "created",
        Some("creating"),
        LegacyPlanCorruption::None,
    )
    .await;
    let adding_id = seed_legacy_plan(
        &valid,
        2004,
        "adding_products",
        Some("adding_products"),
        LegacyPlanCorruption::None,
    )
    .await;
    let products_added_id = seed_legacy_plan(
        &valid,
        2005,
        "products_added",
        Some("adding_products"),
        LegacyPlanCorruption::None,
    )
    .await;
    let activating_id = seed_legacy_plan(
        &valid,
        2006,
        "activating",
        Some("activating"),
        LegacyPlanCorruption::None,
    )
    .await;
    let ambiguous_products_id = seed_legacy_plan(
        &valid,
        2007,
        "ambiguous",
        Some("adding_products"),
        LegacyPlanCorruption::None,
    )
    .await;
    let ambiguous_activate_id = seed_legacy_plan(
        &valid,
        2008,
        "ambiguous",
        Some("activating"),
        LegacyPlanCorruption::None,
    )
    .await;
    let failed_products_id = seed_legacy_plan(
        &valid,
        2009,
        "failed",
        Some("adding_products"),
        LegacyPlanCorruption::None,
    )
    .await;
    let failed_activate_id = seed_legacy_plan(
        &valid,
        2010,
        "failed",
        Some("activating"),
        LegacyPlanCorruption::None,
    )
    .await;
    let guard_plan_id = seed_legacy_plan(
        &valid,
        2003,
        "applied",
        Some("activating"),
        LegacyPlanCorruption::None,
    )
    .await;
    valid
        .batch_execute("ALTER TABLE control.ozon_campaign_guards DISABLE TRIGGER USER")
        .await
        .unwrap();
    valid
        .execute(
            "INSERT INTO control.ozon_campaign_guards( \
             plan_id,account_id,sku,campaign_id,date_from, \
             spend_cap_microrubles,target_drr_percent,status,stop_reason, \
             last_spend_minor,last_revenue_minor,last_checked_at,created_at) \
             VALUES($1,'ozon_one',2003,92003,'2026-09-04',2000000000,15, \
                    'stopping','spend_cap',11,7,clock_timestamp(),clock_timestamp())",
            &[&guard_plan_id],
        )
        .await
        .unwrap();
    valid
        .batch_execute("ALTER TABLE control.ozon_campaign_guards ENABLE TRIGGER USER")
        .await
        .unwrap();
    valid.batch_execute(migration_025).await.unwrap();
    let prepared = valid
        .query_one(
            "SELECT requested_at,requested_by_actor_id \
             FROM control.ozon_campaign_launch_workflows WHERE plan_id=$1",
            &[&prepared_id],
        )
        .await
        .unwrap();
    assert!(prepared.get::<_, Option<DateTime<Utc>>>(0).is_none());
    assert!(prepared.get::<_, Option<String>>(1).is_none());
    let created = valid
        .query_one(
            "SELECT action,requested_at IS NOT NULL,requested_by_actor_id \
             FROM control.ozon_campaign_launch_workflows WHERE plan_id=$1",
            &[&created_id],
        )
        .await
        .unwrap();
    assert_eq!(created.get::<_, &str>(0), "add_products");
    assert!(created.get::<_, bool>(1));
    assert_eq!(created.get::<_, &str>(2), "launcher");
    for (plan_id, expected_action) in [
        (&adding_id, "add_products"),
        (&products_added_id, "activate_campaign"),
        (&activating_id, "activate_campaign"),
        (&ambiguous_products_id, "add_products"),
        (&ambiguous_activate_id, "activate_campaign"),
        (&failed_products_id, "add_products"),
        (&failed_activate_id, "activate_campaign"),
    ] {
        let recovered = valid
            .query_one(
                "SELECT action,requested_at IS NOT NULL,requested_by_actor_id, \
                        lease_owner_id IS NULL AND lease_token IS NULL \
                 FROM control.ozon_campaign_launch_workflows WHERE plan_id=$1",
                &[plan_id],
            )
            .await
            .unwrap();
        assert_eq!(recovered.get::<_, &str>(0), expected_action);
        assert!(recovered.get::<_, bool>(1));
        assert_eq!(recovered.get::<_, &str>(2), "launcher");
        assert!(recovered.get::<_, bool>(3));
    }
    for plan_id in [&failed_products_id, &failed_activate_id] {
        let reclassified = valid
            .query_one(
                "SELECT plan.status,EXISTS( \
                     SELECT 1 FROM control.ozon_campaign_audit_events event \
                     WHERE event.plan_id=plan.plan_id \
                       AND event.event_type='legacy_failed_reclassified' \
                       AND event.payload_json::jsonb->>'recovery_mode'='readback_only' \
                 ) FROM control.ozon_campaign_plans plan WHERE plan.plan_id=$1",
                &[plan_id],
            )
            .await
            .unwrap();
        assert_eq!(reclassified.get::<_, &str>(0), "ambiguous");
        assert!(reclassified.get::<_, bool>(1));
    }
    let stopping = valid
        .query_one(
            "SELECT stop_write_started_at>=created_at, \
                    stop_write_started_at<stop_lease_expires_at, \
                    last_spend_minor,last_revenue_minor,status \
             FROM control.ozon_campaign_guards WHERE plan_id=$1",
            &[&guard_plan_id],
        )
        .await
        .unwrap();
    assert!(stopping.get::<_, bool>(0));
    assert!(stopping.get::<_, bool>(1));
    assert_eq!(stopping.get::<_, Option<i64>>(2), Some(11));
    assert_eq!(stopping.get::<_, Option<i64>>(3), Some(7));
    assert_eq!(stopping.get::<_, &str>(4), "stopping");
    drop(valid);
    valid_connection.await.unwrap().unwrap();
    drop_upgrade_test_database(&admin_url, &valid_name).await;

    for (case, status, action, corruption) in [
        (
            "creating",
            "creating",
            "creating",
            LegacyPlanCorruption::None,
        ),
        (
            "ambiguous_create",
            "ambiguous",
            "creating",
            LegacyPlanCorruption::None,
        ),
        (
            "failed_create",
            "failed",
            "creating",
            LegacyPlanCorruption::None,
        ),
        (
            "forged_manifest",
            "prepared",
            "creating",
            LegacyPlanCorruption::Manifest,
        ),
        (
            "forged_digest",
            "prepared",
            "creating",
            LegacyPlanCorruption::PlanDigest,
        ),
        (
            "forged_id",
            "prepared",
            "creating",
            LegacyPlanCorruption::PlanId,
        ),
    ] {
        let database_name = format!("ozon_upgrade_{case}_{}", std::process::id());
        let (database, connection) = create_upgrade_test_database(&admin_url, &database_name).await;
        database.batch_execute(migration_024).await.unwrap();
        seed_legacy_plan(&database, 3001, status, Some(action), corruption).await;
        let error = database.batch_execute(migration_025).await.unwrap_err();
        assert!(
            error
                .as_db_error()
                .is_some_and(|error| error.message().contains("manual reconciliation")),
            "{case}: {error:?}"
        );
        database.batch_execute("ROLLBACK").await.unwrap();
        let rolled_back: bool = database
            .query_one(
                "SELECT to_regclass('control.ozon_campaign_launch_workflows') IS NULL",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert!(
            rolled_back,
            "{case} must roll back migration 025 atomically"
        );
        drop(database);
        connection.await.unwrap().unwrap();
        drop_upgrade_test_database(&admin_url, &database_name).await;
    }
}

async fn enable_durable_test_gates(admin: &tokio_postgres::Client, sku: u64) {
    let now = Utc::now();
    let expires_at = now + ChronoDuration::minutes(10);
    let sku_i64 = i64::try_from(sku).unwrap();
    for (gate_key, scope_kind, account_id, gate_sku) in [
        ("global".to_owned(), "global", None, None),
        (
            "account/ozon_one".to_owned(),
            "account",
            Some("ozon_one"),
            None,
        ),
        (
            format!("sku/ozon_one/{sku}"),
            "sku",
            Some("ozon_one"),
            Some(sku_i64),
        ),
    ] {
        admin
            .execute(
                "INSERT INTO control.ozon_runtime_gates( \
                 gate_key,scope_kind,account_id,sku,enabled,lease_expires_at, \
                 disabled_until,revision,reason,updated_by,updated_at) \
                 VALUES($1,$2,$3,$4,true,$5,NULL,1,'test','test',$6) \
                 ON CONFLICT(gate_key) DO UPDATE SET enabled=true, \
                 lease_expires_at=EXCLUDED.lease_expires_at,disabled_until=NULL, \
                 revision=control.ozon_runtime_gates.revision+1, \
                 reason='test',updated_by='test',updated_at=EXCLUDED.updated_at",
                &[
                    &gate_key,
                    &scope_kind,
                    &account_id,
                    &gate_sku,
                    &expires_at,
                    &now,
                ],
            )
            .await
            .unwrap();
    }
}

async fn prepare_approved_enqueued_plan(
    planner: &OzonPlanRepository,
    admin: &tokio_postgres::Client,
    policy_digest: &str,
    sku: u64,
) -> super::model::OzonCampaignPlan {
    enable_durable_test_gates(admin, sku).await;
    let prepared = planner
        .create(&durable_manifest(
            sku,
            &format!("Human title {sku}"),
            policy_digest,
        ))
        .await
        .unwrap();
    assert_eq!(prepared.status, OzonLaunchStatus::Prepared);
    assert_eq!(
        prepared.manifest.create_request.title,
        format!("mcp-ozon-{}", prepared.plan_id)
    );
    let approved = planner
        .approve(
            &prepared.plan_id,
            "approver",
            &prepared.plan_digest,
            &format!("test/approval-{sku}"),
        )
        .await
        .unwrap();
    assert_eq!(approved.status, OzonLaunchStatus::Approved);
    assert!(approved.execution_requested_at.is_none());
    planner
        .enqueue_launch(&approved.plan_id, "launcher", &approved.plan_digest)
        .await
        .unwrap()
}

async fn complete_durable_plan(
    planner: &OzonPlanRepository,
    executor: &OzonPlanRepository,
    admin: &tokio_postgres::Client,
    policy_digest: &str,
    sku: u64,
    campaign_id: u64,
) -> super::model::OzonCampaignPlan {
    let plan = prepare_approved_enqueued_plan(planner, admin, policy_digest, sku).await;
    let create = executor
        .claim_next_launch_action("ozon_one", "worker_create")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(create.action, OzonLaunchAction::CreateCampaign);
    assert_eq!(create.mode, OzonLaunchClaimMode::Execute);
    let commit_attempted = Arc::new(AtomicBool::new(false));
    let commit_attempted_for_callback = Arc::clone(&commit_attempted);
    executor
        .start_launch_write(
            &create,
            Some(&create_identity_preflight_digest_for(&create.plan)),
            move || commit_attempted_for_callback.store(true, Ordering::Release),
        )
        .await
        .unwrap();
    assert!(commit_attempted.load(Ordering::Acquire));
    let create_readback = json!({
        "campaign_id": campaign_id.to_string(),
        "title": plan.manifest.create_request.title,
        "state": "CAMPAIGN_STATE_INACTIVE",
        "action": "create_campaign",
        "verified": true
    });
    let created = executor
        .complete_launch_action(&create, Some(campaign_id), Some(&create_readback))
        .await
        .unwrap();
    assert_eq!(created.status, OzonLaunchStatus::Created);

    let add = executor
        .claim_next_launch_action("ozon_one", "worker_products")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(add.action, OzonLaunchAction::AddProducts);
    executor
        .start_launch_write(&add, None, || {})
        .await
        .unwrap();
    let products_readback = json!({
        "campaign_id": campaign_id,
        "title": created.manifest.create_request.title,
        "state": "CAMPAIGN_STATE_STOPPED",
        "sku": sku,
        "bid_microrubles": 7_000_000,
        "action": "add_products",
        "verified": true
    });
    let products_added = executor
        .complete_launch_action(&add, Some(campaign_id), Some(&products_readback))
        .await
        .unwrap();
    assert_eq!(products_added.status, OzonLaunchStatus::ProductsAdded);

    let activate = executor
        .claim_next_launch_action("ozon_one", "worker_activate")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(activate.action, OzonLaunchAction::ActivateCampaign);
    executor
        .start_launch_write(&activate, None, || {})
        .await
        .unwrap();
    let running = json!({
        "campaign_id": campaign_id,
        "title": products_added.manifest.create_request.title,
        "state": "CAMPAIGN_STATE_RUNNING",
        "sku": sku,
        "bid_microrubles": 7_000_000
    });
    executor
        .complete_launch_action(&activate, Some(campaign_id), Some(&running))
        .await
        .unwrap()
}

#[tokio::test]
async fn postgres_durable_launch_and_guard_workflows_are_fenced_and_recoverable() {
    let (Ok(database_url), Ok(executor_url), Ok(control_url), Ok(admin_url)) = (
        std::env::var("OZON_CONTROL_TEST_DATABASE_URL"),
        std::env::var("OZON_EXECUTOR_TEST_DATABASE_URL"),
        std::env::var("WB_CONTROL_TEST_DATABASE_URL"),
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
    ) else {
        return;
    };
    let _database_guard = CONTROL_DB_TEST_LOCK.lock().await;
    let config = database_url.parse::<tokio_postgres::Config>().unwrap();
    let repository = OzonPlanRepository::connect(&config).await.unwrap();
    repository.verify_runtime_contract().await.unwrap();
    let executor_config = executor_url.parse::<tokio_postgres::Config>().unwrap();
    let executor = OzonPlanRepository::connect(&executor_config).await.unwrap();
    executor.verify_runtime_contract().await.unwrap();
    let (planner_sql, planner_connection) =
        tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
            .await
            .unwrap();
    let planner_task = tokio::spawn(planner_connection);
    let (executor_sql, executor_connection) =
        tokio_postgres::connect(&executor_url, tokio_postgres::NoTls)
            .await
            .unwrap();
    let executor_task = tokio::spawn(executor_connection);
    let (control_sql, control_connection) =
        tokio_postgres::connect(&control_url, tokio_postgres::NoTls)
            .await
            .unwrap();
    let control_task = tokio::spawn(control_connection);
    let (admin, admin_connection) = tokio_postgres::connect(&admin_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let admin_task = tokio::spawn(admin_connection);
    admin
        .batch_execute(
            "TRUNCATE TABLE control.ozon_static_guard_audit_events, \
             control.ozon_campaign_audit_events, \
             control.ozon_campaign_guards,control.ozon_campaign_action_reservations, \
             control.ozon_campaign_plan_approvals,control.ozon_campaign_launch_workflows, \
             control.ozon_campaign_plans,control.ozon_runtime_gates, \
             control.ozon_policy_revisions RESTART IDENTITY CASCADE",
        )
        .await
        .unwrap();
    let policy_digest = "a".repeat(64);
    repository
        .register_policy(1, 1, &policy_digest)
        .await
        .unwrap();
    assert!(
        control_sql
            .query("SELECT plan_id FROM control.ozon_campaign_plans", &[])
            .await
            .is_err()
    );
    assert!(
        planner_sql
            .execute(
                "UPDATE control.ozon_campaign_launch_workflows \
                 SET generation=generation WHERE false",
                &[],
            )
            .await
            .is_err()
    );
    assert!(
        planner_sql
            .execute(
                "INSERT INTO control.ozon_campaign_action_reservations( \
                 plan_id,account_id,sku,reserved_at) \
                 SELECT repeat('a',64),'ozon_one',1,clock_timestamp() WHERE false",
                &[],
            )
            .await
            .is_err()
    );
    assert!(
        executor_sql
            .execute(
                "INSERT INTO control.ozon_policy_revisions( \
                 schema_version,policy_revision,policy_digest,registered_at) \
                 SELECT 1,2,repeat('b',64),clock_timestamp() WHERE false",
                &[],
            )
            .await
            .is_err()
    );
    assert!(
        executor_sql
            .execute(
                "INSERT INTO control.ozon_campaign_plans(plan_id) \
                 VALUES(repeat('a',64))",
                &[],
            )
            .await
            .is_err()
    );
    assert!(
        executor_sql
            .execute(
                "UPDATE control.ozon_campaign_launch_workflows \
                 SET requested_at=requested_at WHERE false",
                &[],
            )
            .await
            .is_err()
    );
    assert!(
        executor_sql
            .execute(
                "INSERT INTO control.ozon_campaign_plan_approvals( \
                 approval_id,plan_id,plan_digest,approver_id,reference, \
                 approved_at,expires_at) \
                 SELECT repeat('a',64),repeat('b',64),repeat('c',64), \
                        'approver','test/ref',clock_timestamp(), \
                        clock_timestamp()+interval '1 minute' WHERE false",
                &[],
            )
            .await
            .is_err()
    );

    enable_durable_test_gates(&admin, 1999).await;
    let static_intent = OzonStaticGuardWriteIntent {
        account_id: "ozon_one".to_owned(),
        sku: 1999,
        campaign_id: 91999,
        mutation: OzonStaticGuardMutation::Deactivate,
        target_bid_microrubles: None,
        config_digest: "d".repeat(64),
    };
    executor_sql.batch_execute("BEGIN").await.unwrap();
    let direct_static_audit = executor_sql
        .execute(
            "INSERT INTO control.ozon_static_guard_audit_events( \
             account_id,sku,campaign_id,mutation,target_bid_microrubles, \
             config_digest,schema_version,policy_revision,policy_digest, \
             worker_id,event_type,occurred_at) VALUES( \
             'ozon_one',1999,91999,'deactivate',NULL,repeat('d',64), \
             1,1,$1,'static_worker','write_authorized',clock_timestamp())",
            &[&policy_digest],
        )
        .await;
    assert!(direct_static_audit.is_ok(), "{direct_static_audit:?}");
    executor_sql.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        executor
            .latest_static_guard_audit_event_id("ozon_one")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        executor
            .initialize_static_guard_state(
                1,
                1,
                &policy_digest,
                "ozon_one",
                &static_intent.config_digest,
                "static_worker",
                None,
                |_audit_event_id| async { Err(OzonPlanStoreError::Unavailable) },
            )
            .await,
        Err(OzonPlanStoreError::Unavailable)
    );
    assert_eq!(
        executor
            .latest_static_guard_audit_event_id("ozon_one")
            .await
            .unwrap(),
        None
    );
    let initialized_event_id = Arc::new(AtomicU64::new(0));
    let initialized_event_id_for_callback = Arc::clone(&initialized_event_id);
    executor
        .initialize_static_guard_state(
            1,
            1,
            &policy_digest,
            "ozon_one",
            &static_intent.config_digest,
            "static_worker",
            None,
            move |audit_event_id| async move {
                initialized_event_id_for_callback.store(audit_event_id, Ordering::Release);
                Ok(())
            },
        )
        .await
        .unwrap();
    let initialized_event_id = initialized_event_id.load(Ordering::Acquire);
    assert!(initialized_event_id > 0);
    assert_eq!(
        executor
            .latest_static_guard_audit_event_id("ozon_one")
            .await
            .unwrap(),
        Some(initialized_event_id)
    );
    let static_marker = Arc::new(AtomicBool::new(false));
    let static_event_id = Arc::new(AtomicU64::new(0));
    let static_marker_for_callback = Arc::clone(&static_marker);
    let static_event_id_for_callback = Arc::clone(&static_event_id);
    executor
        .authorize_static_guard_write(
            1,
            1,
            &policy_digest,
            &static_intent,
            "static_worker",
            Some(initialized_event_id),
            move |audit_event_id| async move {
                static_event_id_for_callback.store(audit_event_id, Ordering::Release);
                static_marker_for_callback.store(true, Ordering::Release);
                Ok(())
            },
        )
        .await
        .unwrap();
    assert!(static_marker.load(Ordering::Acquire));
    let callback_event_id = static_event_id.load(Ordering::Acquire);
    assert!(callback_event_id > 0);
    assert_eq!(
        executor
            .latest_static_guard_audit_event_id("ozon_one")
            .await
            .unwrap(),
        Some(callback_event_id)
    );
    assert_eq!(
        executor
            .authorize_static_guard_write(
                1,
                1,
                &policy_digest,
                &static_intent,
                "static_worker",
                Some(initialized_event_id),
                |_audit_event_id| async { panic!("stale cursor callback must not run") },
            )
            .await,
        Err(OzonPlanStoreError::LeaseLost)
    );
    assert_eq!(
        executor
            .authorize_static_guard_write(
                1,
                1,
                &policy_digest,
                &OzonStaticGuardWriteIntent {
                    config_digest: "e".repeat(64),
                    ..static_intent.clone()
                },
                "static_worker",
                Some(callback_event_id),
                |_audit_event_id| async { Err(OzonPlanStoreError::Unavailable) },
            )
            .await,
        Err(OzonPlanStoreError::Unavailable)
    );
    assert_eq!(
        executor
            .latest_static_guard_audit_event_id("ozon_one")
            .await
            .unwrap(),
        Some(callback_event_id),
        "a failed local marker must roll back the staged database event and preserve the account-wide cursor"
    );
    let static_audit = admin
        .query_one(
            "SELECT account_id,sku,campaign_id,mutation,target_bid_microrubles, \
                    config_digest,policy_revision,policy_digest,worker_id,event_type \
             FROM control.ozon_static_guard_audit_events \
             WHERE event_type='write_authorized'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(static_audit.get::<_, &str>(0), "ozon_one");
    assert_eq!(static_audit.get::<_, i64>(1), 1999);
    assert_eq!(static_audit.get::<_, i64>(2), 91999);
    assert_eq!(static_audit.get::<_, &str>(3), "deactivate");
    assert_eq!(static_audit.get::<_, Option<i64>>(4), None);
    assert_eq!(static_audit.get::<_, &str>(5), "d".repeat(64));
    assert_eq!(static_audit.get::<_, i64>(6), 1);
    assert_eq!(static_audit.get::<_, &str>(7), policy_digest);
    assert_eq!(static_audit.get::<_, &str>(8), "static_worker");
    assert_eq!(static_audit.get::<_, &str>(9), "write_authorized");
    let static_sequence = admin
        .query(
            "SELECT event_id,event_type FROM control.ozon_static_guard_audit_events \
             WHERE account_id='ozon_one' ORDER BY event_id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(static_sequence.len(), 2);
    assert_eq!(
        u64::try_from(static_sequence[0].get::<_, i64>(0)).unwrap(),
        initialized_event_id
    );
    assert_eq!(static_sequence[0].get::<_, &str>(1), "state_initialized");
    assert_eq!(
        u64::try_from(static_sequence[1].get::<_, i64>(0)).unwrap(),
        callback_event_id
    );
    assert_eq!(static_sequence[1].get::<_, &str>(1), "write_authorized");
    assert!(
        admin
            .execute(
                "UPDATE control.ozon_static_guard_audit_events \
                 SET event_type='write_authorized'",
                &[],
            )
            .await
            .is_err()
    );
    admin
        .execute(
            "UPDATE control.ozon_runtime_gates SET enabled=false, \
             lease_expires_at=clock_timestamp(),revision=revision+1, \
             reason='static_revoked',updated_by='test',updated_at=clock_timestamp() \
             WHERE gate_key='sku/ozon_one/1999'",
            &[],
        )
        .await
        .unwrap();
    let revoked_marker = Arc::new(AtomicBool::new(false));
    let revoked_marker_for_callback = Arc::clone(&revoked_marker);
    assert_eq!(
        executor
            .authorize_static_guard_write(
                1,
                1,
                &policy_digest,
                &static_intent,
                "static_worker",
                Some(callback_event_id),
                move |_audit_event_id| async move {
                    revoked_marker_for_callback.store(true, Ordering::Release);
                    Ok(())
                },
            )
            .await,
        Err(OzonPlanStoreError::RuntimeDisabled)
    );
    assert!(!revoked_marker.load(Ordering::Acquire));

    enable_durable_test_gates(&admin, 1001).await;
    let prepared = repository
        .create(&durable_manifest(1001, "Human title", &policy_digest))
        .await
        .unwrap();
    let digest_vector = planner_sql
        .query_one(
            "WITH stored AS ( \
                 SELECT plan_id,plan_digest,manifest_json::jsonb AS manifest, \
                        created_at,expires_at \
                 FROM control.ozon_campaign_plans WHERE plan_id=$1 \
             ), plan_hash AS ( \
                 SELECT stored.*,encode(sha256(decode(string_agg(encode( \
                     int8send(octet_length(field)::bigint)||field,'hex'),'' \
                     ORDER BY ordinal),'hex')),'hex') AS computed_plan_digest \
                 FROM stored CROSS JOIN LATERAL unnest(ARRAY[ \
                     convert_to('mcp-ozon/ozon-plan/v1','UTF8'), \
                     convert_to(manifest->>'manifest_digest','UTF8'), \
                     int8send((extract(epoch FROM created_at)*1000000)::bigint), \
                     int8send((extract(epoch FROM expires_at)*1000000)::bigint) \
                 ]) WITH ORDINALITY AS fields(field,ordinal) \
                 GROUP BY stored.plan_id,stored.plan_digest,stored.manifest, \
                          stored.created_at,stored.expires_at \
             ) \
             SELECT plan_digest=computed_plan_digest,plan_id=encode(sha256( \
                 int8send(octet_length(convert_to( \
                     'mcp-ozon/ozon-plan-id/v1','UTF8'))::bigint) \
                 ||convert_to('mcp-ozon/ozon-plan-id/v1','UTF8') \
                 ||int8send(octet_length(convert_to( \
                     computed_plan_digest,'UTF8'))::bigint) \
                 ||convert_to(computed_plan_digest,'UTF8')),'hex') \
             FROM plan_hash",
            &[&prepared.plan_id],
        )
        .await
        .unwrap();
    assert!(digest_vector.get::<_, bool>(0));
    assert!(digest_vector.get::<_, bool>(1));
    let forged_plan_id = "f".repeat(64);
    let mut forged_manifest = durable_manifest(2001, "Forged title", &policy_digest);
    forged_manifest.create_request.title = provider_title_for_plan_id(&forged_plan_id);
    let forged_manifest_json = serde_json::to_string(&forged_manifest).unwrap();
    let forged_created_at = Utc::now();
    let forged_expires_at = forged_created_at + ChronoDuration::minutes(15);
    assert!(
        planner_sql
            .execute(
                "INSERT INTO control.ozon_campaign_plans( \
                 plan_id,plan_digest,actor_id,account_id,sku,schema_version, \
                 policy_revision,policy_digest,manifest_json,status,created_at,expires_at) \
                 VALUES($1,$2,'launcher','ozon_one',2001,1,1,$3,$4, \
                        'prepared',$5,$6)",
                &[
                    &forged_plan_id,
                    &"e".repeat(64),
                    &policy_digest,
                    &forged_manifest_json,
                    &forged_created_at,
                    &forged_expires_at,
                ],
            )
            .await
            .is_err()
    );
    let approved = repository
        .approve(
            &prepared.plan_id,
            "approver",
            &prepared.plan_digest,
            "test/approval-main",
        )
        .await
        .unwrap();
    assert!(
        executor
            .claim_next_launch_action("ozon_one", "no_apply")
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        repository
            .enqueue_launch(&approved.plan_id, "other_actor", &approved.plan_digest)
            .await,
        Err(OzonPlanStoreError::InvalidState)
    ));
    let queued = repository
        .enqueue_launch(&approved.plan_id, "launcher", &approved.plan_digest)
        .await
        .unwrap();
    assert!(queued.execution_requested_at.is_some());
    assert_eq!(
        repository
            .enqueue_launch(&approved.plan_id, "launcher", &approved.plan_digest)
            .await
            .unwrap()
            .execution_requested_at,
        queued.execution_requested_at
    );
    assert!(
        executor
            .claim_next_launch_action("other_account", "wrong_account")
            .await
            .unwrap()
            .is_none()
    );
    let create = executor
        .claim_next_launch_action("ozon_one", "create_worker")
        .await
        .unwrap()
        .unwrap();
    executor
        .start_launch_write(
            &create,
            Some(&create_identity_preflight_digest_for(&create.plan)),
            || {},
        )
        .await
        .unwrap();
    let create_readback = json!({
        "campaign_id": "9001",
        "title": create.plan.manifest.create_request.title,
        "state": "CAMPAIGN_STATE_INACTIVE",
        "action": "create_campaign",
        "verified": true
    });
    executor
        .complete_launch_action(&create, Some(9001), Some(&create_readback))
        .await
        .unwrap();

    admin
        .batch_execute(
            "ALTER TABLE control.ozon_campaign_plan_approvals \
             DISABLE TRIGGER ozon_approvals_append_only",
        )
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.ozon_campaign_plan_approvals \
             SET approved_at=clock_timestamp()-interval '10 minutes', \
                 expires_at=clock_timestamp()-interval '8 minutes' \
             WHERE plan_id=$1",
            &[&approved.plan_id],
        )
        .await
        .unwrap();
    admin
        .batch_execute(
            "ALTER TABLE control.ozon_campaign_plan_approvals \
             ENABLE TRIGGER ozon_approvals_append_only",
        )
        .await
        .unwrap();
    let add = executor
        .claim_next_launch_action("ozon_one", "products_after_ttl")
        .await
        .unwrap()
        .unwrap();
    executor
        .start_launch_write(&add, None, || {})
        .await
        .unwrap();
    let products = json!({
        "campaign_id": 9001,
        "title": add.plan.manifest.create_request.title,
        "state": "CAMPAIGN_STATE_PLANNED",
        "sku": 1001,
        "bid_microrubles": 7_000_000,
        "action": "add_products",
        "verified": true
    });
    executor
        .complete_launch_action(&add, Some(9001), Some(&products))
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.ozon_runtime_gates SET enabled=false, \
             lease_expires_at=clock_timestamp(),revision=revision+1, \
             reason='revoked',updated_by='test',updated_at=clock_timestamp() \
             WHERE gate_key='sku/ozon_one/1001'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        executor
            .claim_next_launch_action("ozon_one", "revoked_gate")
            .await
            .unwrap()
            .is_none()
    );
    enable_durable_test_gates(&admin, 1001).await;
    let activate = executor
        .claim_next_launch_action("ozon_one", "activate_worker")
        .await
        .unwrap()
        .unwrap();
    executor
        .start_launch_write(&activate, None, || {})
        .await
        .unwrap();
    let running = json!({
        "campaign_id": 9001,
        "title": activate.plan.manifest.create_request.title,
        "state": "CAMPAIGN_STATE_RUNNING",
        "sku": 1001,
        "bid_microrubles": 7_000_000
    });
    let applied = executor
        .complete_launch_action(&activate, Some(9001), Some(&running))
        .await
        .unwrap();
    assert_eq!(applied.status, OzonLaunchStatus::Applied);

    let incident_applied =
        complete_durable_plan(&repository, &executor, &admin, &policy_digest, 1004, 9004).await;
    let incident_guard = executor
        .active_guards_for_account("ozon_one")
        .await
        .unwrap()
        .into_iter()
        .find(|guard| guard.plan_id == incident_applied.plan_id)
        .unwrap();
    let incident_lease = executor
        .claim_guard_stop_leased(&incident_guard, "manual_stop", None, None, "guard_incident")
        .await
        .unwrap();
    executor
        .start_guard_stop_write(&incident_lease)
        .await
        .unwrap();
    executor
        .record_guard_stop_readback(&incident_lease, OzonGuardStopReadback::Running)
        .await
        .unwrap();
    executor
        .mark_guard_incident_leased(&incident_lease, "campaign_still_running", None, None)
        .await
        .unwrap();
    let incident_evidence = admin
        .query_one(
            "SELECT stop_reason,incident_error_class FROM control.ozon_campaign_guards \
             WHERE plan_id=$1",
            &[&incident_applied.plan_id],
        )
        .await
        .unwrap();
    assert_eq!(incident_evidence.get::<_, &str>(0), "manual_stop");
    assert_eq!(
        incident_evidence.get::<_, &str>(1),
        "campaign_still_running"
    );

    drop(prepare_approved_enqueued_plan(&repository, &admin, &policy_digest, 1002).await);
    let conflict = executor
        .claim_next_launch_action("ozon_one", "conflict_worker")
        .await
        .unwrap()
        .unwrap();
    let failed = executor
        .fail_launch_action(&conflict, "ozon_create_precondition_conflict", None)
        .await
        .unwrap();
    assert_eq!(failed.status, OzonLaunchStatus::Failed);
    assert!(failed.operation_started_at.is_some());

    let uncertain = prepare_approved_enqueued_plan(&repository, &admin, &policy_digest, 1003).await;
    let uncertain_lease = executor
        .claim_next_launch_action("ozon_one", "uncertain_worker")
        .await
        .unwrap()
        .unwrap();
    executor
        .start_launch_write(
            &uncertain_lease,
            Some(&create_identity_preflight_digest_for(&uncertain_lease.plan)),
            || {},
        )
        .await
        .unwrap();
    let uncertain_readback = json!({
        "campaign_id": 9003,
        "title": uncertain.manifest.create_request.title,
        "state": "CAMPAIGN_STATE_STOPPED",
        "action": "create_campaign",
        "verified": true
    });
    executor
        .mark_launch_ambiguous(
            &uncertain_lease,
            "ozon_create_ambiguous",
            None,
            Some(&uncertain_readback),
        )
        .await
        .unwrap();
    admin
        .batch_execute(
            "ALTER TABLE control.ozon_campaign_launch_workflows \
             DISABLE TRIGGER ozon_launch_workflow_update_guard",
        )
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.ozon_campaign_launch_workflows \
             SET available_at=clock_timestamp()-interval '1 second' WHERE plan_id=$1",
            &[&uncertain.plan_id],
        )
        .await
        .unwrap();
    admin
        .batch_execute(
            "ALTER TABLE control.ozon_campaign_launch_workflows \
             ENABLE TRIGGER ozon_launch_workflow_update_guard",
        )
        .await
        .unwrap();
    let stale_recovery = executor
        .claim_launch_recovery("ozon_one", "recovery_one")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stale_recovery.mode, OzonLaunchClaimMode::Reconcile);
    assert!(matches!(
        executor
            .fail_launch_action(&stale_recovery, "must_not_fail_recovery", None)
            .await,
        Err(OzonPlanStoreError::InvalidState)
    ));
    admin
        .batch_execute(
            "ALTER TABLE control.ozon_campaign_launch_workflows \
             DISABLE TRIGGER ozon_launch_workflow_update_guard",
        )
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.ozon_campaign_launch_workflows SET \
             lease_claimed_at=clock_timestamp()-interval '5 minutes', \
             lease_expires_at=clock_timestamp()-interval '1 microsecond' \
             WHERE plan_id=$1",
            &[&uncertain.plan_id],
        )
        .await
        .unwrap();
    admin
        .batch_execute(
            "ALTER TABLE control.ozon_campaign_launch_workflows \
             ENABLE TRIGGER ozon_launch_workflow_update_guard",
        )
        .await
        .unwrap();
    let recovery = executor
        .claim_launch_recovery("ozon_one", "recovery_two")
        .await
        .unwrap()
        .unwrap();
    assert!(recovery.generation > stale_recovery.generation);
    assert!(matches!(
        executor
            .complete_launch_action(&stale_recovery, Some(9003), Some(&uncertain_readback))
            .await,
        Err(OzonPlanStoreError::InvalidState)
    ));
    let recovered = executor
        .complete_launch_action(&recovery, Some(9003), Some(&uncertain_readback))
        .await
        .unwrap();
    assert_eq!(recovered.status, OzonLaunchStatus::Created);

    let mut guard = executor
        .active_guards_for_account("ozon_one")
        .await
        .unwrap()
        .into_iter()
        .find(|guard| guard.plan_id == applied.plan_id)
        .unwrap();
    let mut wrong_guard = guard.clone();
    wrong_guard.account_id = "other_account".to_owned();
    assert!(matches!(
        executor
            .record_guard_observation(&wrong_guard, 100, 200)
            .await,
        Err(OzonPlanStoreError::InvalidState)
    ));
    guard.incident_error_class = Some("forged".to_owned());
    assert!(matches!(
        executor.record_guard_observation(&guard, 100, 200).await,
        Err(OzonPlanStoreError::InvalidPlan)
    ));
    guard.incident_error_class = None;
    executor
        .record_guard_observation(&guard, 100, 200)
        .await
        .unwrap();
    assert!(matches!(
        executor
            .claim_guard_stop_leased(&guard, "spend_cap_reached", Some(100), None, "guard_one")
            .await,
        Err(OzonPlanStoreError::InvalidPlan)
    ));
    let stale_guard_lease = executor
        .claim_guard_stop_leased(
            &guard,
            "spend_cap_reached",
            Some(100),
            Some(200),
            "guard_one",
        )
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.ozon_runtime_gates SET enabled=false, \
             lease_expires_at=clock_timestamp(),revision=revision+1, \
             reason='revoked_before_guard_marker',updated_by='test', \
             updated_at=clock_timestamp() \
             WHERE gate_key='sku/ozon_one/1001'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        executor.start_guard_stop_write(&stale_guard_lease).await,
        Err(OzonPlanStoreError::RuntimeDisabled)
    );
    assert!(
        admin
            .execute(
                "UPDATE control.ozon_campaign_guards \
                 SET stop_write_started_at=clock_timestamp() WHERE plan_id=$1",
                &[&guard.plan_id],
            )
            .await
            .is_err()
    );
    let marker: Option<DateTime<Utc>> = admin
        .query_one(
            "SELECT stop_write_started_at FROM control.ozon_campaign_guards \
             WHERE plan_id=$1",
            &[&guard.plan_id],
        )
        .await
        .unwrap()
        .get(0);
    assert!(marker.is_none());
    enable_durable_test_gates(&admin, 1001).await;
    executor
        .start_guard_stop_write(&stale_guard_lease)
        .await
        .unwrap();
    admin
        .batch_execute(
            "ALTER TABLE control.ozon_campaign_guards \
             DISABLE TRIGGER ozon_guards_transition_guard",
        )
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.ozon_campaign_guards SET \
             stop_lease_claimed_at=stop_write_started_at-interval '1 second', \
             stop_lease_expires_at=stop_write_started_at+interval '1 microsecond' \
             WHERE plan_id=$1",
            &[&guard.plan_id],
        )
        .await
        .unwrap();
    admin
        .batch_execute(
            "ALTER TABLE control.ozon_campaign_guards \
             ENABLE TRIGGER ozon_guards_transition_guard",
        )
        .await
        .unwrap();
    assert!(
        executor
            .claim_guard_stop_recovery("other_account", "wrong_guard_worker")
            .await
            .unwrap()
            .is_none()
    );
    let guard_recovery = executor
        .claim_guard_stop_recovery("ozon_one", "guard_two")
        .await
        .unwrap()
        .unwrap();
    assert!(guard_recovery.write_started_at.is_some());
    executor
        .record_guard_stop_readback(&guard_recovery, OzonGuardStopReadback::Unavailable)
        .await
        .unwrap();
    executor
        .record_guard_stop_readback(&guard_recovery, OzonGuardStopReadback::Stopped)
        .await
        .unwrap();
    assert_eq!(
        executor
            .finish_guard_leased(&stale_guard_lease, Some(100), Some(200))
            .await,
        Err(OzonPlanStoreError::InvalidState)
    );
    executor
        .finish_guard_leased(&guard_recovery, Some(100), Some(200))
        .await
        .unwrap();
    let guard_audit: Vec<String> = admin
        .query_one(
            "SELECT array_agg(event_type ORDER BY event_id) \
             FROM control.ozon_campaign_audit_events \
             WHERE plan_id=$1 AND event_type LIKE 'guard_stop_%'",
            &[&guard.plan_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        guard_audit,
        vec![
            "guard_stop_claimed",
            "guard_stop_write_started",
            "guard_stop_reclaimed",
            "guard_stop_readback_unavailable",
            "guard_stop_readback_stopped",
            "guard_stop_stopped",
        ]
    );
    let guard_generations: Vec<Option<String>> = admin
        .query_one(
            "SELECT array_agg(payload_json::jsonb->>'generation' ORDER BY event_id) \
             FROM control.ozon_campaign_audit_events \
             WHERE plan_id=$1 AND event_type LIKE 'guard_stop_%'",
            &[&guard.plan_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        guard_generations,
        vec![
            Some("1".to_owned()),
            Some("1".to_owned()),
            Some("2".to_owned()),
            Some("2".to_owned()),
            Some("2".to_owned()),
            Some("2".to_owned()),
        ]
    );
    assert!(
        admin
            .execute(
                "UPDATE control.ozon_campaign_audit_events SET event_type='forged' \
                 WHERE plan_id=$1 AND event_type='guard_stop_claimed'",
                &[&guard.plan_id],
            )
            .await
            .is_err()
    );

    let illegal = repository
        .create(&durable_manifest(
            1005,
            "Illegal transition",
            &policy_digest,
        ))
        .await
        .unwrap();
    assert!(
        admin
            .execute(
                "UPDATE control.ozon_campaign_launch_workflows SET \
                 requested_at=clock_timestamp(),requested_by_actor_id='spoofed', \
                 available_at=clock_timestamp() WHERE plan_id=$1",
                &[&illegal.plan_id],
            )
            .await
            .is_err()
    );
    assert!(
        admin
            .execute(
                "UPDATE control.ozon_campaign_plans SET status='applied' WHERE plan_id=$1",
                &[&illegal.plan_id],
            )
            .await
            .is_err()
    );

    let audit_count: i64 = admin
        .query_one(
            "SELECT count(*) FROM control.ozon_campaign_audit_events \
             WHERE plan_id=$1 AND event_type LIKE 'workflow_%'",
            &[&queued.plan_id],
        )
        .await
        .unwrap()
        .get(0);
    assert!(audit_count >= 4);
    admin
        .batch_execute(
            "TRUNCATE TABLE control.ozon_static_guard_audit_events, \
             control.ozon_campaign_audit_events, \
             control.ozon_campaign_guards,control.ozon_campaign_action_reservations, \
             control.ozon_campaign_plan_approvals,control.ozon_campaign_launch_workflows, \
             control.ozon_campaign_plans,control.ozon_runtime_gates, \
             control.ozon_policy_revisions RESTART IDENTITY CASCADE",
        )
        .await
        .unwrap();
    repository
        .register_policy(1, 1, &policy_digest)
        .await
        .unwrap();
    let first_contended =
        prepare_approved_enqueued_plan(&repository, &admin, &policy_digest, 1010).await;
    let second_contended =
        prepare_approved_enqueued_plan(&repository, &admin, &policy_digest, 1011).await;
    executor_sql.batch_execute("BEGIN").await.unwrap();
    let locked_plan_id: String = executor_sql
        .query_one(
            "SELECT workflow.plan_id \
             FROM control.ozon_campaign_launch_workflows workflow \
             WHERE workflow.plan_id IN ($1,$2) ORDER BY requested_at,plan_id \
             LIMIT 1 FOR UPDATE",
            &[&first_contended.plan_id, &second_contended.plan_id],
        )
        .await
        .unwrap()
        .get(0);
    let concurrently_claimed = executor
        .claim_next_launch_action("ozon_one", "skip_locked_worker")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(concurrently_claimed.plan.plan_id, locked_plan_id);
    assert!(
        concurrently_claimed.plan.plan_id == first_contended.plan_id
            || concurrently_claimed.plan.plan_id == second_contended.plan_id
    );
    executor_sql.batch_execute("ROLLBACK").await.unwrap();
    admin
        .batch_execute(
            "TRUNCATE TABLE control.ozon_static_guard_audit_events, \
             control.ozon_campaign_audit_events, \
             control.ozon_campaign_guards,control.ozon_campaign_action_reservations, \
             control.ozon_campaign_plan_approvals,control.ozon_campaign_launch_workflows, \
             control.ozon_campaign_plans,control.ozon_runtime_gates, \
             control.ozon_policy_revisions RESTART IDENTITY CASCADE",
        )
        .await
        .unwrap();
    drop(admin);
    drop(planner_sql);
    drop(executor_sql);
    drop(control_sql);
    admin_task.await.unwrap().unwrap();
    planner_task.await.unwrap().unwrap();
    executor_task.await.unwrap().unwrap();
    control_task.await.unwrap().unwrap();
}

fn mock_http(responses: Vec<(u16, &'static str)>) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let captured = Arc::new(Mutex::new(Vec::new()));
    let captured_clone = Arc::clone(&captured);
    thread::spawn(move || {
        for (status, body) in responses {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4096];
            loop {
                let read = stream.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                let Some(headers_end) = request.windows(4).position(|part| part == b"\r\n\r\n")
                else {
                    continue;
                };
                let header_text = String::from_utf8_lossy(&request[..headers_end + 4]);
                let content_length = header_text
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .and_then(|value| value.parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                if request.len() >= headers_end + 4 + content_length {
                    break;
                }
            }
            captured_clone
                .lock()
                .unwrap()
                .push(String::from_utf8_lossy(&request).to_string());
            let reason = if status == 200 { "OK" } else { "ERROR" };
            write!(
                stream,
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        }
    });
    (format!("http://{address}"), captured)
}
