use super::*;
use crate::{config::PerformanceCredentials, test_support::mock_http};

fn reader(
    responses: Vec<(u16, String)>,
) -> (Arc<PerformanceClient>, std::sync::mpsc::Receiver<String>) {
    let (url, requests) = mock_http(
        std::iter::once((
            200,
            r#"{"access_token":"fixture-token","token_type":"Bearer","expires_in":1800}"#
                .to_owned(),
        ))
        .chain(responses)
        .collect(),
    );
    (
        Arc::new(PerformanceClient::new_for_test(
            url,
            Duration::from_secs(2),
            BTreeMap::from([(
                StoreId::from("store"),
                PerformanceCredentials {
                    client_id: "guard-fixture".to_owned(),
                    client_secret: "guard-fixture-secret".to_owned(),
                },
            )]),
        )),
        requests,
    )
}

fn campaign(campaign_id: u64, state: &str) -> String {
    serde_json::json!({"list":[{"id":campaign_id,"state":state}]}).to_string()
}
fn product(sku: u64, bid: u64) -> String {
    serde_json::json!({"products":[{"sku":sku,"bid":bid}]}).to_string()
}
fn metrics(campaign_id: u64, spend: &str, revenue: &str) -> String {
    serde_json::json!({"rows":[{"id":campaign_id.to_string(),"title":"Guard fixture","date":"2026-09-01","views":"10","clicks":"1","moneySpent":spend,"orders":"1","ordersMoney":revenue}]}).to_string()
}
fn observed_at() -> DateTime<Utc> {
    "2026-09-01T12:00:00Z".parse().unwrap()
}

#[tokio::test]
async fn audit_and_product_validation_use_complete_real_http_snapshots() {
    let guard = test_static_guard(21, 10_000_000);
    let store = StoreId::from("store");
    let (client, requests) = reader(vec![
        (200, campaign(21, "CAMPAIGN_STATE_RUNNING")),
        (200, product(121, 8_000_000)),
        (200, product(121, 8_000_000)),
    ]);
    audit_static_campaigns(std::slice::from_ref(&guard), &client, &store)
        .await
        .unwrap();
    assert_eq!(
        validate_static_campaign_product(&client, &store, &guard)
            .await
            .unwrap(),
        8_000_000
    );
    assert_eq!(requests.try_iter().count(), 4);
    let (client, _) = reader(vec![(200, r#"{"list":[]}"#.to_owned())]);
    assert!(
        running_static_campaigns(&client, &store, std::slice::from_ref(&guard))
            .await
            .is_err()
    );
    let (client, _) = reader(vec![(200, product(999, 8_000_000))]);
    assert!(
        validate_static_campaign_product(&client, &store, &guard)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn telemetry_adapter_aggregates_complete_data_and_fails_closed_on_partial_or_malformed_rows()
{
    let guard = test_static_guard(21, 10_000_000);
    let store = StoreId::from("store");
    let (client, _) = reader(vec![
        (200, metrics(21, "1.00", "100.00")),
        (200, metrics(21, "2000.00", "10000.00")),
    ]);
    let adapter = PerformanceGuardReader {
        client: &client,
        store: &store,
    };
    assert_eq!(
        adapter.metrics(&guard.guard, observed_at()).await.unwrap(),
        OzonGuardMetrics {
            spend_minor: 100,
            attributed_revenue_minor: 10_000
        }
    );
    assert_eq!(
        evaluate_live_guard(&client, &store, &guard.guard, observed_at())
            .await
            .unwrap(),
        (200_000, 1_000_000, Some("spend_cap_reached"))
    );
    for (status, body) in [
        (400, "{}".to_owned()),
        (200, "{}".to_owned()),
        (200, r#"{"rows":[]}"#.to_owned()),
    ] {
        let (client, _) = reader(vec![(status, body)]);
        let adapter = PerformanceGuardReader {
            client: &client,
            store: &store,
        };
        assert!(matches!(
            adapter.metrics(&guard.guard, observed_at()).await,
            Err(OzonGuardReadFailure::Telemetry)
        ));
    }
    for (state, expected) in [
        ("CAMPAIGN_STATE_RUNNING", true),
        ("CAMPAIGN_STATE_STOPPED", false),
    ] {
        let (client, _) = reader(vec![(200, campaign(21, state))]);
        let adapter = PerformanceGuardReader {
            client: &client,
            store: &store,
        };
        assert_eq!(adapter.campaign_is_running(21).await.unwrap(), expected);
    }
    let (client, _) = reader(vec![(200, r#"{"list":[]}"#.to_owned())]);
    let adapter = PerformanceGuardReader {
        client: &client,
        store: &store,
    };
    assert!(matches!(
        adapter.campaign_is_running(21).await,
        Err(OzonGuardReadFailure::CampaignState)
    ));
}

#[tokio::test]
async fn static_metrics_require_every_requested_campaign_day() {
    let guard = test_static_guard(21, 10_000_000);
    let store = StoreId::from("store");
    let running = BTreeSet::from([21]);
    let (client, requests) = reader(vec![(200, metrics(21, "1.00", "100.00"))]);
    assert_eq!(
        static_guard_metrics(
            &client,
            &store,
            std::slice::from_ref(&guard),
            &running,
            observed_at()
        )
        .await
        .unwrap(),
        BTreeMap::from([(21, (100, 10_000))])
    );
    let requests = requests.try_iter().collect::<Vec<_>>();
    assert!(requests[1].contains("dateFrom=2026-09-01"));
    assert!(requests[1].contains("dateTo=2026-09-01"));
    for body in ["{}", r#"{"rows":[]}"#] {
        let (client, _) = reader(vec![(200, body.to_owned())]);
        assert!(
            static_guard_metrics(
                &client,
                &store,
                std::slice::from_ref(&guard),
                &running,
                observed_at()
            )
            .await
            .is_err()
        );
    }
}

#[tokio::test]
async fn pending_bid_recovery_reconciles_exact_http_readback_and_locks_uncertain_state() {
    let directory = TestDirectory::new();
    let state_path = directory.0.join("state.json");
    let guard = test_static_guard(21, 10_000_000);
    let store = StoreId::from("store");
    for case in ["applied", "unavailable", "configuration_changed"] {
        let mut state = StaticGuardState::default();
        let mut pending = test_pending_bid(&guard, observed_at());
        if case == "configuration_changed" {
            pending.sku = Some(999);
        }
        state.pending_bid_changes.insert(21, pending);
        let (client, requests) = reader(match case {
            "applied" => vec![(200, product(121, 8_000_000))],
            "unavailable" => vec![(400, "{}".to_owned())],
            _ => vec![],
        });
        recover_pending_static_bids(
            &mut state,
            &state_path,
            &client,
            &store,
            std::slice::from_ref(&guard),
        )
        .await
        .unwrap();
        if case == "applied" {
            assert!(state.pending_bid_changes.is_empty());
            assert!(state.incident_campaign_ids.is_empty());
            assert_eq!(state.last_bid_change_at.get(&21), Some(&observed_at()));
        } else {
            assert!(state.incident_campaign_ids.contains(&21));
            assert_eq!(
                state.incidents.get(&21).unwrap().error_class,
                if case == "unavailable" {
                    "pending_bid_readback_unavailable"
                } else {
                    "pending_bid_config_mismatch"
                }
            );
        }
        assert_eq!(load_static_state(&state_path).unwrap(), state);
        assert_eq!(
            requests.try_iter().count(),
            if case == "configuration_changed" {
                0
            } else {
                2
            }
        );
    }
    let mut state = StaticGuardState::default();
    state
        .pending_bid_changes
        .insert(21, test_pending_bid(&guard, observed_at()));
    let (client, _) = reader(vec![]);
    assert!(
        recover_pending_static_bids(&mut state, &state_path, &client, &store, &[])
            .await
            .is_err()
    );
}
