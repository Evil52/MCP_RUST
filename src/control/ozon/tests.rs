use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use crate::config::PerformanceCredentials;

use super::{
    OzonAdsWriteClient, OzonCampaignCreateRequest, OzonCampaignLaunchSpec, OzonCampaignProduct,
    OzonCampaignProductsRequest, OzonCampaignStrategy, OzonGuardedWriteError, OzonLaunchStatus,
    OzonPlacement, OzonPlanRepository, OzonPlanStoreError,
    client::{validate_create_request, validate_products_request},
    prepare_campaign_launch_manifest,
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
        validate_products_request(OzonCampaignStrategy::TargetBids, &target_bid_products()).is_ok()
    );
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
#[allow(clippy::significant_drop_tightening)]
async fn postgres_plan_requires_separate_approval_gates_and_exact_readback() {
    let Ok(database_url) = std::env::var("OZON_CONTROL_TEST_DATABASE_URL") else {
        return;
    };
    let admin_url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL").unwrap();
    let database_config = database_url.parse().unwrap();
    let repository = OzonPlanRepository::connect(&database_config).await.unwrap();
    repository.verify_runtime_contract().await.unwrap();

    let policy_digest = "b".repeat(64);
    repository
        .register_policy(1, 10_001, &policy_digest)
        .await
        .unwrap();
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
    assert_eq!(plan.status, OzonLaunchStatus::Approved);
    assert_eq!(
        repository
            .claim_create(&plan.plan_id, &plan.actor_id, &plan.plan_digest)
            .await
            .unwrap_err(),
        OzonPlanStoreError::RuntimeDisabled
    );

    let (admin, connection) = tokio_postgres::connect(&admin_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
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
