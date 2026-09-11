use super::*;

#[test]
fn reviewed_dates_require_the_canonical_calendar_representation() {
    let mut candidate = entry(37_756_773, 3_457_585_933);
    for date in ["2024-02-29", "2026-09-02", "2026-12-31"] {
        candidate["date_from"] = date.into();
        let config = parse_ozon_static_guard_config(&file(&[candidate.clone()]), ACCOUNT).unwrap();
        assert_eq!(config.guards[0].guard.date_from, date);
    }
    for date in [
        " 2026-09-02",
        "2026-9-02",
        "2026-09-2",
        "\t2026-9-02",
        "2026- 9-02",
        "+2026-09-02",
        "2026-09-02 ",
        "2025-02-29",
    ] {
        candidate["date_from"] = date.into();
        assert_eq!(
            parse_ozon_static_guard_config(&file(&[candidate.clone()]), ACCOUNT),
            Err(OzonStaticGuardError::InvalidGuard),
            "noncanonical reviewed date: {date:?}"
        );
    }
    candidate["date_from"] = format!("{}2026-09-02", " ".repeat(32 * 1024)).into();
    let bytes = file(&[candidate]);
    assert!(bytes.len() < MAX_OZON_STATIC_GUARD_FILE_BYTES);
    assert_eq!(
        parse_ozon_static_guard_config(&bytes, ACCOUNT),
        Err(OzonStaticGuardError::InvalidGuard)
    );
}
