use super::*;

fn state_with_campaign_mutation() -> OzonStaticGuardState {
    let mut state = populated_state();
    state.pending_campaign_mutations.insert(
        14,
        OzonStaticPendingCampaignMutation {
            account_id: "account".to_owned(),
            sku: 114,
            min_cpc_bid_microrubles: 7_000_000,
            max_cpc_bid_microrubles: 10_000_000,
            date_from: "2026-09-01".to_owned(),
            spend_cap_microrubles: 2_000_000_000,
            target_drr_percent: 15,
            kind: OzonStaticCampaignMutationKind::Deactivate,
            stop_reason: Some("telemetry_unavailable".to_owned()),
            spend_minor: None,
            revenue_minor: None,
            started_at: DateTime::UNIX_EPOCH,
        },
    );
    state
}

#[test]
fn noncanonical_binding_dates_cannot_replace_a_recoverable_snapshot() {
    let directory = TestDirectory::new();
    let path = directory.state();
    let original = state_with_campaign_mutation();
    persist_ozon_static_guard_state(&path, &original).unwrap();
    let original_bytes = fs::read(&path).unwrap();
    for date in [" 2026-09-01", "2026-9-01", "\t2026-9-01", "2025-02-29"] {
        let mut incident = original.clone();
        incident.incidents.get_mut(&11).unwrap().date_from = Some(date.to_owned());
        let mut bid = original.clone();
        bid.pending_bid_changes.get_mut(&13).unwrap().date_from = Some(date.to_owned());
        let mut campaign = original.clone();
        campaign
            .pending_campaign_mutations
            .get_mut(&14)
            .unwrap()
            .date_from = date.to_owned();
        for malformed in [incident, bid, campaign] {
            assert!(matches!(
                persist_ozon_static_guard_state(&path, &malformed),
                Err(OzonStaticGuardStateError::InvalidState)
            ));
            assert_eq!(fs::read(&path).unwrap(), original_bytes);
            assert_eq!(load_ozon_static_guard_state(&path).unwrap(), original);
        }
    }
    assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 1);
}

#[test]
fn oversized_date_input_is_invalid_and_disk_input_remains_byte_bounded() {
    let directory = TestDirectory::new();
    let path = directory.state();
    let original = populated_state();
    persist_ozon_static_guard_state(&path, &original).unwrap();
    let original_bytes = fs::read(&path).unwrap();
    let mut malformed = original;
    malformed
        .pending_bid_changes
        .get_mut(&13)
        .unwrap()
        .date_from = Some(format!(
        "{}2026-09-01",
        " ".repeat(usize::try_from(MAX_OZON_STATIC_GUARD_STATE_BYTES).unwrap())
    ));
    assert!(matches!(
        persist_ozon_static_guard_state(&path, &malformed),
        Err(OzonStaticGuardStateError::InvalidState)
    ));
    assert_eq!(fs::read(&path).unwrap(), original_bytes);
    assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 1);

    // The read boundary rejects an oversized serialized file independently
    // of whether its JSON or binding values would otherwise parse.
    let untrusted_path = directory.0.join("oversized.json");
    let bytes = serde_json::to_vec(&malformed).unwrap();
    assert!(u64::try_from(bytes.len()).unwrap() > MAX_OZON_STATIC_GUARD_STATE_BYTES);
    fs::write(&untrusted_path, &bytes).unwrap();
    fs::set_permissions(&untrusted_path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(matches!(
        load_ozon_static_guard_state(&untrusted_path),
        Err(OzonStaticGuardStateError::TooLarge)
    ));
    assert_eq!(fs::read(&untrusted_path).unwrap(), bytes);
}
