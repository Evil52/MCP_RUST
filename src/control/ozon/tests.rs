use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use crate::config::PerformanceCredentials;
use reqwest::StatusCode;

use super::{
    OzonAdsWriteClient, OzonCampaignCreateRequest, OzonCampaignLaunchSpec, OzonCampaignProduct,
    OzonCampaignProductsRequest, OzonCampaignStrategy, OzonGuardedWriteError, OzonLaunchStatus,
    OzonPlacement, OzonPlanRepository, OzonPlanStoreError, OzonWriteError, OzonWriteErrorKind,
    client::{validate_create_request, validate_products_request},
    prepare_campaign_launch_manifest,
    repository::{
        map_plan_insert, map_policy_insert, validate_digest, validate_error_class,
        validate_identity, validate_manifest, validate_reference,
    },
};

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
        OzonWriteError::ResponseTooLarge,
    ] {
        assert_eq!(error.kind(), OzonWriteErrorKind::Definite);
    }
    for error in [
        OzonWriteError::AmbiguousTransport,
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
    assert!(matches!(
        client
            .activate_campaign_with_permit(1, || async { Ok::<_, ()>(()) })
            .await,
        Err(OzonGuardedWriteError::Write(
            OzonWriteError::ResponseTooLarge
        ))
    ));
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
        (503, OzonWriteErrorKind::Ambiguous),
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

#[tokio::test]
#[allow(clippy::significant_drop_tightening)]
async fn postgres_plan_requires_separate_approval_gates_and_exact_readback() {
    let Ok(database_url) = std::env::var("OZON_CONTROL_TEST_DATABASE_URL") else {
        return;
    };
    let _database_guard = crate::control::plan::CONTROL_DB_TEST_LOCK.lock().await;
    let admin_url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL").unwrap();
    let database_config = database_url.parse().unwrap();
    let repository = OzonPlanRepository::connect(&database_config).await.unwrap();
    repository.probe().await.unwrap();
    repository.verify_runtime_contract().await.unwrap();
    let (admin, connection) = tokio_postgres::connect(&admin_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    admin
        .batch_execute(
            "TRUNCATE TABLE control.ozon_policy_revisions, control.ozon_runtime_gates RESTART IDENTITY CASCADE; \
             INSERT INTO control.ozon_runtime_gates(gate_key,scope_kind,enabled,lease_expires_at,disabled_until,revision,reason,updated_by,updated_at) \
             VALUES('global','global',false,'-infinity','infinity',1,'test_default','test_admin',clock_timestamp())",
        )
        .await
        .unwrap();

    let policy_digest = "b".repeat(64);
    repository
        .register_policy(1, 10_001, &policy_digest)
        .await
        .unwrap();
    repository
        .register_policy(1, 10_001, &policy_digest)
        .await
        .unwrap();
    assert_eq!(
        repository
            .register_policy(1, 10_001, &"c".repeat(64))
            .await
            .unwrap_err(),
        OzonPlanStoreError::PolicyChanged
    );
    assert_eq!(
        repository
            .register_policy(1, 10_000, &policy_digest)
            .await
            .unwrap_err(),
        OzonPlanStoreError::PolicyChanged
    );
    for (schema, revision, digest) in [
        (0, 1, "a".repeat(64)),
        (1, 0, "a".repeat(64)),
        (1, 1, "bad".to_owned()),
    ] {
        assert_eq!(
            repository
                .register_policy(schema, revision, &digest)
                .await
                .unwrap_err(),
            OzonPlanStoreError::InvalidPlan
        );
    }
    let spec = OzonCampaignLaunchSpec {
        account_id: "furnitura_dlya_doma".to_owned(),
        title: "Diana Euphemia DRR15 test".to_owned(),
        from_date: "2026-09-02".to_owned(),
        to_date: "2026-09-08".to_owned(),
        skus: vec![3_457_585_933],
        weekly_budget_microrubles: 2_000_000_000,
        per_sku_spend_cap_microrubles: 2_000_000_000,
        initial_cpc_bid_microrubles: 7_000_000,
        max_cpc_bid_microrubles: 12_000_000,
        target_drr_percent: 15,
        target_position: 10,
    };
    let account_id = spec.account_id.clone();
    let skus = spec.skus.clone();
    let manifest = prepare_campaign_launch_manifest(
        "rustam_magasumov",
        1,
        10_001,
        &policy_digest,
        &account_id,
        &skus,
        spec.weekly_budget_microrubles,
        spec.per_sku_spend_cap_microrubles,
        spec.initial_cpc_bid_microrubles,
        spec.max_cpc_bid_microrubles,
        spec.target_drr_percent,
        spec.target_position,
        spec,
    )
    .unwrap();
    let plan = repository.create(&manifest).await.unwrap();
    assert_eq!(
        repository.create(&manifest).await.unwrap_err(),
        OzonPlanStoreError::SkuLocked
    );
    assert_eq!(
        repository.load("bad").await.unwrap_err(),
        OzonPlanStoreError::InvalidPlan
    );
    assert_eq!(
        repository.load(&"f".repeat(64)).await.unwrap_err(),
        OzonPlanStoreError::NotFound
    );
    assert_eq!(plan.status, OzonLaunchStatus::Prepared);
    assert_eq!(
        repository
            .approve(
                &plan.plan_id,
                "rustam_magasumov",
                &plan.plan_digest,
                "same_actor"
            )
            .await
            .unwrap_err(),
        OzonPlanStoreError::InvalidState
    );
    let plan = repository
        .approve(
            &plan.plan_id,
            "diana_serafimovich",
            &plan.plan_digest,
            "test_approval",
        )
        .await
        .unwrap();
    let same_approval = repository
        .approve(
            &plan.plan_id,
            "diana_serafimovich",
            &plan.plan_digest,
            "test_approval",
        )
        .await
        .unwrap();
    assert_eq!(same_approval.status, OzonLaunchStatus::Approved);
    assert_eq!(
        repository
            .approve(
                &plan.plan_id,
                "diana_serafimovich",
                &plan.plan_digest,
                "different_reference",
            )
            .await
            .unwrap_err(),
        OzonPlanStoreError::InvalidState
    );
    assert_eq!(
        repository
            .approve(&plan.plan_id, "diana_serafimovich", &"d".repeat(64), "ref")
            .await
            .unwrap_err(),
        OzonPlanStoreError::PlanChanged
    );
    assert_eq!(plan.status, OzonLaunchStatus::Approved);
    assert_eq!(
        repository
            .claim_create(&plan.plan_id, &plan.actor_id, &plan.plan_digest)
            .await
            .unwrap_err(),
        OzonPlanStoreError::RuntimeDisabled
    );

    admin
        .execute(
            "UPDATE control.ozon_runtime_gates SET enabled=true,lease_expires_at=clock_timestamp()+interval '10 minutes',disabled_until=NULL,revision=revision+1,reason='integration_test',updated_by='test_admin',updated_at=clock_timestamp() WHERE gate_key='global'",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO control.ozon_runtime_gates(gate_key,scope_kind,account_id,sku,enabled,lease_expires_at,revision,reason,updated_by,updated_at) VALUES('account/furnitura_dlya_doma','account','furnitura_dlya_doma',NULL,true,clock_timestamp()+interval '10 minutes',1,'integration_test','test_admin',clock_timestamp()),('sku/furnitura_dlya_doma/3457585933','sku','furnitura_dlya_doma',3457585933,true,clock_timestamp()+interval '10 minutes',1,'integration_test','test_admin',clock_timestamp())",
            &[],
        )
        .await
        .unwrap();

    let plan = repository
        .claim_create(&plan.plan_id, &plan.actor_id, &plan.plan_digest)
        .await
        .unwrap();
    assert_eq!(plan.status, OzonLaunchStatus::Creating);
    repository
        .revalidate_write_permit(
            &plan.plan_id,
            &plan.actor_id,
            &plan.plan_digest,
            OzonLaunchStatus::Creating,
        )
        .await
        .unwrap();
    let campaign_id = 91_000_001;
    let plan = repository
        .transition(
            &plan.plan_id,
            &plan.actor_id,
            &plan.plan_digest,
            OzonLaunchStatus::Creating,
            OzonLaunchStatus::Created,
            Some(campaign_id),
            None,
            None,
            false,
        )
        .await
        .unwrap();
    let plan = repository
        .transition(
            &plan.plan_id,
            &plan.actor_id,
            &plan.plan_digest,
            OzonLaunchStatus::Created,
            OzonLaunchStatus::AddingProducts,
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();
    let plan = repository
        .transition(
            &plan.plan_id,
            &plan.actor_id,
            &plan.plan_digest,
            OzonLaunchStatus::AddingProducts,
            OzonLaunchStatus::ProductsAdded,
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();
    let plan = repository
        .transition(
            &plan.plan_id,
            &plan.actor_id,
            &plan.plan_digest,
            OzonLaunchStatus::ProductsAdded,
            OzonLaunchStatus::Activating,
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();
    let readback = serde_json::json!({
        "campaign_id": campaign_id.to_string(),
        "sku": plan.sku.to_string(),
        "state": "CAMPAIGN_STATE_RUNNING"
    });
    let plan = repository
        .transition(
            &plan.plan_id,
            &plan.actor_id,
            &plan.plan_digest,
            OzonLaunchStatus::Activating,
            OzonLaunchStatus::Applied,
            None,
            None,
            Some(&readback),
            false,
        )
        .await
        .unwrap();
    assert_eq!(plan.status, OzonLaunchStatus::Applied);
    let guards = repository.active_guards().await.unwrap();
    assert_eq!(guards.len(), 1);
    assert_eq!(guards[0].campaign_id, campaign_id);
    repository
        .record_guard_observation(&plan.plan_id, 100_000, 900_000)
        .await
        .unwrap();
    repository
        .claim_guard_stop(
            &plan.plan_id,
            campaign_id,
            "spend_cap_reached",
            200_000,
            1_000_000,
        )
        .await
        .unwrap();
    repository
        .revalidate_stop_permit(&plan.plan_id, campaign_id)
        .await
        .unwrap();
    repository
        .finish_guard(
            &plan.plan_id,
            campaign_id,
            "spend_cap_reached",
            200_000,
            1_000_000,
        )
        .await
        .unwrap();
    assert!(repository.active_guards().await.unwrap().is_empty());
    admin
        .batch_execute(
            "DELETE FROM control.ozon_campaign_guards; \
             INSERT INTO control.ozon_campaign_guards( \
                 plan_id,account_id,sku,campaign_id,date_from,spend_cap_microrubles, \
                 target_drr_percent,status,created_at) \
             SELECT plan_id,account_id,sku,campaign_id,manifest_json::jsonb->'spec'->>'from_date', \
                    (manifest_json::jsonb->'spec'->>'per_sku_spend_cap_microrubles')::bigint, \
                    (manifest_json::jsonb->'spec'->>'target_drr_percent')::smallint, \
                    'active',clock_timestamp() \
             FROM control.ozon_campaign_plans WHERE status='applied'",
        )
        .await
        .unwrap();
    repository
        .claim_guard_stop(
            &plan.plan_id,
            campaign_id,
            "spend_cap_reached",
            200_000,
            1_000_000,
        )
        .await
        .unwrap();
    repository
        .mark_guard_incident(&plan.plan_id, "readback_failed")
        .await
        .unwrap();
    assert!(repository.active_guards().await.unwrap().is_empty());

    for result in [
        repository
            .record_guard_observation(&plan.plan_id, 1, 1)
            .await,
        repository
            .claim_guard_stop(&plan.plan_id, campaign_id, "reason", 1, 1)
            .await,
        repository
            .revalidate_stop_permit(&plan.plan_id, campaign_id)
            .await,
        repository
            .finish_guard(&plan.plan_id, campaign_id, "reason", 1, 1)
            .await,
        repository
            .mark_guard_incident(&plan.plan_id, "reason")
            .await,
    ] {
        assert_eq!(result.unwrap_err(), OzonPlanStoreError::InvalidState);
    }

    assert_eq!(
        repository
            .approve(
                &plan.plan_id,
                "diana_serafimovich",
                &plan.plan_digest,
                "already_applied",
            )
            .await
            .unwrap_err(),
        OzonPlanStoreError::InvalidState
    );
    assert_eq!(
        repository
            .transition(
                &plan.plan_id,
                "wrong_actor",
                &plan.plan_digest,
                OzonLaunchStatus::Applied,
                OzonLaunchStatus::Failed,
                None,
                Some("wrong_actor"),
                None,
                false,
            )
            .await
            .unwrap_err(),
        OzonPlanStoreError::InvalidState
    );

    let launch_manifest = |sku: u64, title: &str| {
        let mut spec = manifest.spec.clone();
        spec.skus = vec![sku];
        spec.title = title.to_owned();
        let account_id = spec.account_id.clone();
        let skus = spec.skus.clone();
        prepare_campaign_launch_manifest(
            &manifest.actor_id,
            manifest.policy_schema_version,
            manifest.policy_revision,
            &manifest.policy_digest,
            &account_id,
            &skus,
            spec.weekly_budget_microrubles,
            spec.per_sku_spend_cap_microrubles,
            spec.initial_cpc_bid_microrubles,
            spec.max_cpc_bid_microrubles,
            spec.target_drr_percent,
            spec.target_position,
            spec,
        )
        .unwrap()
    };
    let expired = repository
        .create(&launch_manifest(3_457_585_934, "expired plan"))
        .await
        .unwrap();
    admin
        .batch_execute(
            "ALTER TABLE control.ozon_campaign_plans DISABLE TRIGGER ozon_plans_transition_guard",
        )
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.ozon_campaign_plans \
             SET created_at=times.created_at, expires_at=times.created_at+interval '15 minutes' \
             FROM (SELECT clock_timestamp()-interval '20 minutes' AS created_at) AS times \
             WHERE plan_id=$1",
            &[&expired.plan_id],
        )
        .await
        .unwrap();
    admin
        .batch_execute(
            "ALTER TABLE control.ozon_campaign_plans ENABLE TRIGGER ozon_plans_transition_guard",
        )
        .await
        .unwrap();
    assert_eq!(
        repository
            .approve(
                &expired.plan_id,
                "diana_serafimovich",
                &expired.plan_digest,
                "expired_plan",
            )
            .await
            .unwrap_err(),
        OzonPlanStoreError::Expired
    );

    let approval_expired = repository
        .create(&launch_manifest(3_457_585_935, "expired approval"))
        .await
        .unwrap();
    let approval_expired = repository
        .approve(
            &approval_expired.plan_id,
            "diana_serafimovich",
            &approval_expired.plan_digest,
            "expiring_approval",
        )
        .await
        .unwrap();
    admin
        .batch_execute(
            "ALTER TABLE control.ozon_campaign_plan_approvals DISABLE TRIGGER ozon_approvals_append_only",
        )
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.ozon_campaign_plan_approvals \
             SET approved_at=times.approved_at, expires_at=times.approved_at+interval '3 minutes' \
             FROM (SELECT clock_timestamp()-interval '4 minutes' AS approved_at) AS times \
             WHERE plan_id=$1",
            &[&approval_expired.plan_id],
        )
        .await
        .unwrap();
    admin
        .batch_execute(
            "ALTER TABLE control.ozon_campaign_plan_approvals ENABLE TRIGGER ozon_approvals_append_only",
        )
        .await
        .unwrap();
    assert_eq!(
        repository
            .approve(
                &approval_expired.plan_id,
                "diana_serafimovich",
                &approval_expired.plan_digest,
                "expiring_approval",
            )
            .await
            .unwrap_err(),
        OzonPlanStoreError::ApprovalExpired
    );
    admin
        .execute(
            "INSERT INTO control.ozon_runtime_gates(gate_key,scope_kind,account_id,sku,enabled,lease_expires_at,revision,reason,updated_by,updated_at) VALUES('sku/furnitura_dlya_doma/3457585935','sku','furnitura_dlya_doma',3457585935,true,clock_timestamp()+interval '10 minutes',1,'integration_test','test_admin',clock_timestamp())",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        repository
            .revalidate_write_permit(
                &approval_expired.plan_id,
                &approval_expired.actor_id,
                &approval_expired.plan_digest,
                OzonLaunchStatus::Approved,
            )
            .await
            .unwrap_err(),
        OzonPlanStoreError::ApprovalExpired
    );

    let newer_policy_digest = "c".repeat(64);
    repository
        .register_policy(1, 10_002, &newer_policy_digest)
        .await
        .unwrap();
    assert_eq!(
        repository
            .revalidate_write_permit(
                &plan.plan_id,
                &plan.actor_id,
                &plan.plan_digest,
                OzonLaunchStatus::Applied,
            )
            .await
            .unwrap_err(),
        OzonPlanStoreError::PolicyChanged
    );

    admin
        .batch_execute("ALTER ROLE control_writer NOLOGIN")
        .await
        .unwrap();
    admin
        .execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename='control_writer' AND pid<>pg_backend_pid()",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        repository.probe().await.unwrap_err(),
        OzonPlanStoreError::Unavailable
    );
    assert_eq!(
        repository.active_guards().await.unwrap_err(),
        OzonPlanStoreError::Unavailable
    );
    assert_eq!(
        repository.verify_runtime_contract().await.unwrap_err(),
        OzonPlanStoreError::Unavailable
    );
    assert_eq!(
        repository
            .register_policy(1, 10_002, &policy_digest)
            .await
            .unwrap_err(),
        OzonPlanStoreError::Unavailable
    );
    assert_eq!(
        repository.create(&manifest).await.unwrap_err(),
        OzonPlanStoreError::Unavailable
    );
    assert_eq!(
        repository.load(&plan.plan_id).await.unwrap_err(),
        OzonPlanStoreError::Unavailable
    );
    assert_eq!(
        repository
            .approve(&plan.plan_id, "approver", &plan.plan_digest, "reference")
            .await
            .unwrap_err(),
        OzonPlanStoreError::Unavailable
    );
    assert_eq!(
        repository
            .claim_create(&plan.plan_id, &plan.actor_id, &plan.plan_digest)
            .await
            .unwrap_err(),
        OzonPlanStoreError::Unavailable
    );
    assert_eq!(
        repository
            .transition(
                &plan.plan_id,
                &plan.actor_id,
                &plan.plan_digest,
                OzonLaunchStatus::Applied,
                OzonLaunchStatus::Failed,
                None,
                Some("test_failure"),
                None,
                false,
            )
            .await
            .unwrap_err(),
        OzonPlanStoreError::Unavailable
    );
    assert_eq!(
        repository
            .revalidate_write_permit(
                &plan.plan_id,
                &plan.actor_id,
                &plan.plan_digest,
                OzonLaunchStatus::Applied,
            )
            .await
            .unwrap_err(),
        OzonPlanStoreError::Unavailable
    );
    assert_eq!(
        repository
            .record_guard_observation(&plan.plan_id, 1, 1)
            .await
            .unwrap_err(),
        OzonPlanStoreError::Unavailable
    );
    assert_eq!(
        repository
            .mark_guard_incident(&plan.plan_id, "reason")
            .await
            .unwrap_err(),
        OzonPlanStoreError::Unavailable
    );
    admin
        .batch_execute("ALTER ROLE control_writer LOGIN")
        .await
        .unwrap();

    admin
        .batch_execute(
            "CREATE TEMP TABLE ozon_unique_error_probe(id integer PRIMARY KEY); \
             INSERT INTO ozon_unique_error_probe VALUES(1)",
        )
        .await
        .unwrap();
    let duplicate_policy = admin
        .execute("INSERT INTO ozon_unique_error_probe VALUES(1)", &[])
        .await
        .unwrap_err();
    assert_eq!(
        map_policy_insert(&duplicate_policy),
        OzonPlanStoreError::PolicyChanged
    );
    assert_eq!(
        map_plan_insert(&duplicate_policy),
        OzonPlanStoreError::SkuLocked
    );
    let generic = admin
        .query_one("SELECT missing_column", &[])
        .await
        .unwrap_err();
    assert_eq!(map_policy_insert(&generic), OzonPlanStoreError::Unavailable);
    assert_eq!(map_plan_insert(&generic), OzonPlanStoreError::Unavailable);
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
