use super::{static_adapter_fixture::*, *};
use crate::control::{
    ozon::launch_workflow::tests::adapter_fixture::Database, plan::CONTROL_DB_TEST_LOCK,
};

#[tokio::test]
async fn postgres_static_authorization_reloads_scope_before_durable_marker_and_rolls_back_rejected_marker()
 {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    if let Some(database) = Database::connect().await {
        let fixture = StaticFixture::new();
        let mut state = fixture.initialize(&database).await;
        let previous = state.last_static_audit_event_id;
        let authorization = fixture.write_authorization(&database);
        let state_path = &fixture.state_path;
        authorization
            .persist_marker(
                &fixture.guard,
                OzonStaticGuardMutation::SetBid,
                Some(8_000_000),
                previous,
                |event_id| {
                    let state = &mut state;
                    async move {
                        advance_static_audit_watermark(state, event_id)?;
                        persist_static_state(state_path, state).map_err(|error| error.to_string())
                    }
                },
            )
            .await
            .unwrap();
        assert!(state.last_static_audit_event_id > previous);
        assert_eq!(
            database
                .executor
                .latest_static_guard_audit_event_id("account")
                .await
                .unwrap(),
            state.last_static_audit_event_id
        );
        assert_eq!(load_static_state(&fixture.state_path).unwrap(), state);
        let previous = state.last_static_audit_event_id;
        let failure = authorization
            .persist_marker(
                &fixture.guard,
                OzonStaticGuardMutation::Deactivate,
                None,
                previous,
                |_| std::future::ready(Err("disk full".to_owned())),
            )
            .await;
        assert!(failure.is_err());
        assert_eq!(
            database
                .executor
                .latest_static_guard_audit_event_id("account")
                .await
                .unwrap(),
            previous
        );
        for case in [
            "registry_missing",
            "registry_binding",
            "config_missing",
            "config_changed",
            "corridor",
        ] {
            let marker = AtomicBool::new(false);
            let original = fs::read(&fixture.config_path).unwrap();
            let registry_path = fixture.authorization.path.join("registry.json");
            let hidden = fixture.authorization.path.join("hidden");
            let mut authorization = authorization;
            match case {
                "registry_missing" => fs::rename(&registry_path, &hidden).unwrap(),
                "registry_binding" => authorization.executor_fingerprint = "changed",
                "config_missing" => fs::rename(&fixture.config_path, &hidden).unwrap(),
                "config_changed" => {
                    let mut bytes = original.clone();
                    bytes.push(b' ');
                    fs::write(&fixture.config_path, bytes).unwrap();
                }
                _ => {}
            }
            let result = authorization
                .persist_marker(
                    &fixture.guard,
                    OzonStaticGuardMutation::SetBid,
                    Some(if case == "corridor" {
                        13_000_000
                    } else {
                        8_000_000
                    }),
                    previous,
                    |_| {
                        marker.store(true, Ordering::Relaxed);
                        std::future::ready(Ok(()))
                    },
                )
                .await;
            match case {
                "registry_missing" => fs::rename(&hidden, &registry_path).unwrap(),
                "config_missing" => fs::rename(&hidden, &fixture.config_path).unwrap(),
                "config_changed" => fs::write(&fixture.config_path, original).unwrap(),
                _ => {}
            }
            assert!(result.is_err(), "{case}");
            assert!(!marker.load(Ordering::Relaxed));
            assert_eq!(
                database
                    .executor
                    .latest_static_guard_audit_event_id("account")
                    .await
                    .unwrap(),
                previous
            );
        }
    }
}
