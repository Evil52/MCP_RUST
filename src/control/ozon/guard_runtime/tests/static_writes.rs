use super::{static_adapter_fixture::*, *};
use crate::control::{
    ozon::launch_workflow::tests::adapter_fixture::{Database, TOKEN, mock_reader, mock_writer},
    plan::CONTROL_DB_TEST_LOCK,
};

#[tokio::test]
async fn postgres_static_bid_adapter_journals_before_one_put_and_reconciles_exact_readback() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    if let Some(database) = Database::connect().await {
        let fixture = StaticFixture::new();
        for case in [
            "success",
            "ambiguous_applied",
            "ambiguous_unconfirmed",
            "mismatched",
            "revoked",
        ] {
            let mut state = fixture.initialize(&database).await;
            let marker = state.last_static_audit_event_id;
            if case == "revoked" {
                database.admin.execute("UPDATE control.ozon_runtime_gates SET enabled=false WHERE gate_key='global'",&[]).await.unwrap();
            }
            let (reader, reads) = mock_reader(if case == "revoked" {
                vec![]
            } else {
                vec![(
                    200,
                    product(if matches!(case, "success" | "ambiguous_applied") {
                        8_000_000
                    } else {
                        7_000_000
                    }),
                )]
            });
            let mut responses = vec![(200, TOKEN.to_owned())];
            if case != "revoked" {
                responses.push((
                    if case.starts_with("ambiguous") {
                        400
                    } else {
                        200
                    },
                    "{}".to_owned(),
                ));
            }
            let (writer, requests) = mock_writer(responses);
            let result = change_static_campaign_bid(
                &mut state,
                &fixture.state_path,
                &reader,
                &writer,
                &fixture.store,
                fixture.write_authorization(&database),
                &fixture.guard,
                7_000_000,
                8_000_000,
                observed_at(),
            )
            .await;
            assert_eq!(
                result.is_ok(),
                matches!(case, "success" | "ambiguous_applied")
            );
            if case == "revoked" {
                assert_eq!(state.last_static_audit_event_id, marker);
                assert!(state.pending_bid_changes.is_empty());
                assert_eq!(reads.try_iter().count(), 0);
            } else {
                assert!(state.last_static_audit_event_id > marker);
                assert_eq!(reads.try_iter().count(), 2);
                if result.is_ok() {
                    assert!(state.pending_bid_changes.is_empty());
                    assert_eq!(state.last_bid_change_at.get(&21), Some(&observed_at()));
                } else {
                    assert_eq!(
                        state.incidents.get(&21).unwrap().error_class,
                        "bid_write_unconfirmed"
                    );
                }
            }
            assert_eq!(load_static_state(&fixture.state_path).unwrap(), state);
            let requests = requests.try_iter().collect::<Vec<_>>();
            assert_eq!(requests.len(), if case == "revoked" { 1 } else { 2 });
            if requests.len() == 2 {
                assert!(requests[1].starts_with("PUT /api/client/campaign/21/products "));
            }
        }
    }
}

#[tokio::test]
async fn postgres_static_state_adapters_activate_then_stop_with_durable_markers_and_exact_readback()
{
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    if let Some(database) = Database::connect().await {
        let fixture = StaticFixture::new();
        let mut state = fixture.initialize(&database).await;
        let (reader, reads) = mock_reader(vec![
            (200, campaign("CAMPAIGN_STATE_RUNNING")),
            (200, campaign("CAMPAIGN_STATE_STOPPED")),
        ]);
        let (writer, requests) = mock_writer(vec![
            (200, TOKEN.to_owned()),
            (200, "{}".to_owned()),
            (200, "{}".to_owned()),
        ]);
        activate_static_campaign(
            &mut state,
            &fixture.state_path,
            &reader,
            &writer,
            &fixture.store,
            fixture.write_authorization(&database),
            &fixture.guard,
        )
        .await
        .unwrap();
        assert!(state.pending_campaign_mutations.is_empty());
        guard_campaign_static(
            &mut state,
            &fixture.state_path,
            &reader,
            &writer,
            &fixture.store,
            fixture.write_authorization(&database),
            &fixture.guard,
            Some(200_000),
            Some(1_000_000),
            Some("spend_cap_reached"),
        )
        .await
        .unwrap();
        assert!(state.pending_campaign_mutations.is_empty());
        let requests = requests.try_iter().collect::<Vec<_>>();
        assert_eq!(requests.len(), 3);
        assert!(requests[1].starts_with("POST /api/client/campaign/21/activate "));
        assert!(requests[2].starts_with("POST /api/client/campaign/21/deactivate "));
        assert_eq!(reads.try_iter().count(), 3);
        assert_eq!(load_static_state(&fixture.state_path).unwrap(), state);
        recover_pending_static_campaign_mutations(
            &mut state,
            &fixture.state_path,
            &reader,
            &writer,
            &fixture.store,
            std::slice::from_ref(&fixture.guard),
            fixture.write_authorization(&database),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn postgres_static_cycles_hold_missing_product_and_position_evidence_without_writes() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    if let Some(database) = Database::connect().await {
        let fixture = StaticFixture::new();
        for case in [
            "inactive",
            "valid",
            "products_unavailable",
            "position_unavailable",
        ] {
            let mut state = fixture.initialize(&database).await;
            let responses = if case == "inactive" {
                vec![(200, campaign("CAMPAIGN_STATE_STOPPED"))]
            } else {
                vec![
                    (200, campaign("CAMPAIGN_STATE_RUNNING")),
                    (200, metrics("1.00", "100.00")),
                    (
                        if case == "products_unavailable" {
                            400
                        } else {
                            200
                        },
                        product(8_000_000),
                    ),
                ]
            };
            let (reader, reads) = mock_reader(responses);
            let (writer, requests) = mock_writer(vec![]);
            let dynamic:OzonStaticDynamicBidControl=serde_json::from_value(serde_json::json!({"position_store_id":"account","position_region_name":"region","bid_step_microrubles":1_000_000,"target_position":10,"cooldown_seconds":1800,"max_position_age_seconds":3600})).unwrap();
            guard_once_static(
                std::slice::from_ref(&fixture.guard),
                &mut state,
                &fixture.state_path,
                &reader,
                &writer,
                &fixture.store,
                fixture.write_authorization(&database),
                (case == "position_unavailable").then_some(&dynamic),
                None,
                observed_at(),
            )
            .await
            .unwrap();
            assert!(state.pending_campaign_mutations.is_empty());
            assert!(state.pending_bid_changes.is_empty());
            assert!(state.incident_campaign_ids.is_empty());
            assert_eq!(requests.try_iter().count(), 0);
            assert_eq!(
                reads.try_iter().count(),
                if case == "inactive" { 2 } else { 4 }
            );
        }
    }
}
