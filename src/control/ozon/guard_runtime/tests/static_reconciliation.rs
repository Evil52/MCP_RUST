use super::{static_adapter_fixture::*, *};
use crate::control::{
    ozon::launch_workflow::tests::adapter_fixture::{Database, TOKEN, mock_reader, mock_writer},
    plan::CONTROL_DB_TEST_LOCK,
};

#[tokio::test]
async fn postgres_explicit_reconcile_repairs_both_corridor_edges_before_activation() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    if let Some(database) = Database::connect().await {
        let fixture = StaticFixture::new();
        for (current, target) in [(6_000_000, 7_000_000), (13_000_000, 12_000_000)] {
            let mut state = fixture.initialize(&database).await;
            state.incident_campaign_ids.insert(21);
            state.incidents.insert(21, test_incident(&fixture.guard));
            persist_static_state(&fixture.state_path, &state).unwrap();
            let previous_cursor = state.last_static_audit_event_id.unwrap();
            let (reader, reads) = mock_reader(vec![
                (200, product(current)),
                (200, product(target)),
                (200, product(target)),
                (200, campaign("CAMPAIGN_STATE_STOPPED")),
                (200, campaign("CAMPAIGN_STATE_RUNNING")),
            ]);
            let (writer, requests) = mock_writer(vec![
                (200, TOKEN.to_owned()),
                (200, "{}".to_owned()),
                (200, "{}".to_owned()),
            ]);
            reconcile_static_campaigns(
                std::slice::from_ref(&fixture.guard),
                &mut state,
                &fixture.state_path,
                &reader,
                &writer,
                &fixture.store,
                fixture.write_authorization(&database),
            )
            .await
            .unwrap();
            assert_eq!(reads.try_iter().count(), 6);
            let requests = requests.try_iter().collect::<Vec<_>>();
            assert_eq!(requests.len(), 3);
            assert!(requests[1].starts_with("PUT /api/client/campaign/21/products "));
            assert!(requests[1].contains(&target.to_string()));
            assert!(requests[2].starts_with("POST /api/client/campaign/21/activate "));
            assert_eq!(state.last_static_audit_event_id, Some(previous_cursor + 2));
            assert!(state.incident_campaign_ids.is_empty());
            assert!(state.incidents.is_empty());
            assert!(state.pending_bid_changes.is_empty());
            assert!(state.pending_campaign_mutations.is_empty());
            assert_eq!(load_static_state(&fixture.state_path).unwrap(), state);
        }
    }
}

#[tokio::test]
async fn repaired_corridor_intent_recovers_from_exact_target_and_holds_stale_readback() {
    let fixture = StaticFixture::new();
    for (observed, confirmed) in [(7_000_000, true), (6_000_000, false), (8_000_000, false)] {
        let mut pending = test_pending_bid(&fixture.guard, observed_at());
        pending.from_microrubles = 6_000_000;
        pending.to_microrubles = 7_000_000;
        let mut state = StaticGuardState {
            last_static_audit_event_id: Some(1),
            pending_bid_changes: BTreeMap::from([(21, pending)]),
            ..StaticGuardState::default()
        };
        persist_static_state(&fixture.state_path, &state).unwrap();
        assert!(pending_static_bid_matches_guard(&state, &fixture.guard));
        let (reader, requests) = mock_reader(vec![(200, product(observed))]);
        recover_pending_static_bids(
            &mut state,
            &fixture.state_path,
            &reader,
            &fixture.store,
            std::slice::from_ref(&fixture.guard),
        )
        .await
        .unwrap();
        assert_eq!(requests.try_iter().count(), 2);
        assert_eq!(state.pending_bid_changes.is_empty(), confirmed);
        assert_eq!(state.incident_campaign_ids.is_empty(), confirmed);
        assert_eq!(state.last_static_audit_event_id, Some(1));
        if !confirmed {
            assert_eq!(
                state.incidents[&21].error_class,
                "pending_bid_readback_mismatch"
            );
        }
        assert_eq!(load_static_state(&fixture.state_path).unwrap(), state);
    }
}

#[test]
fn corridor_repair_intent_rejects_nonboundary_targets_and_invalid_observed_bids() {
    let fixture = StaticFixture::new();
    let mut missing_corridor = test_pending_bid(&fixture.guard, observed_at());
    missing_corridor.min_cpc_bid_microrubles = None;
    assert!(!crate::control::ozon::static_state::valid_static_bid_transition(&missing_corridor));
    for (from, to) in [
        (6_000_000, 8_000_000),
        (13_000_000, 11_000_000),
        (6_000_000, 6_000_000),
        (13_000_000, 13_000_000),
        (0, 7_000_000),
        (6_500_000, 7_000_000),
    ] {
        let mut pending = test_pending_bid(&fixture.guard, observed_at());
        pending.from_microrubles = from;
        pending.to_microrubles = to;
        let state = StaticGuardState {
            last_static_audit_event_id: Some(1),
            pending_bid_changes: BTreeMap::from([(21, pending)]),
            ..StaticGuardState::default()
        };
        assert!(persist_static_state(&fixture.state_path, &state).is_err());
        assert!(!pending_static_bid_matches_guard(&state, &fixture.guard));
    }
}

#[tokio::test]
async fn postgres_explicit_reconcile_rejects_missing_actual_bid_without_any_write() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    if let Some(database) = Database::connect().await {
        let fixture = StaticFixture::new();
        for snapshot in [r#"{"products":[]}"#, r#"{"products":[{"sku":1001}]}"#] {
            let mut state = fixture.initialize(&database).await;
            let original = state.clone();
            let (reader, reads) = mock_reader(vec![(200, snapshot.to_owned())]);
            let (writer, requests) = mock_writer(vec![]);
            assert!(
                reconcile_static_campaigns(
                    std::slice::from_ref(&fixture.guard),
                    &mut state,
                    &fixture.state_path,
                    &reader,
                    &writer,
                    &fixture.store,
                    fixture.write_authorization(&database),
                )
                .await
                .is_err()
            );
            assert_eq!(reads.try_iter().count(), 2);
            assert_eq!(requests.try_iter().count(), 0);
            assert_eq!(state, original);
        }
    }
}
