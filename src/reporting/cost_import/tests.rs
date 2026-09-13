use serde_json::{Value, json};

use super::*;

fn scope() -> CostImportScope {
    CostImportScope::new(
        AccountScope::new("shop".to_owned(), Marketplace::Ozon).unwrap(),
        "one_c".to_owned(),
        [101, 102].into_iter().collect(),
        "finance_original".to_owned(),
    )
    .unwrap()
}

fn payload() -> Value {
    json!({
        "version":1,"account_id":"shop","marketplace":"ozon","source_id":"one_c",
        "export_id":"batch_1","exported_at":"2026-09-13T10:00:00Z",
        "rows":[{"source_row_id":"row_1","sku":101,"amount_minor":12345,"currency":"RUB",
            "allocation":"per_unit","vat_treatment":"included","vat_rate_bps":2000,
            "effective_from":"2026-09-01","effective_to":"2026-09-30"}]
    })
}

fn parse(value: &Value) -> Result<ValidatedCostBatch, CostImportError> {
    ValidatedCostBatch::parse_json(&serde_json::to_vec(value).unwrap(), &scope())
}

#[test]
fn trusted_actor_is_audited_without_changing_export_identity() {
    let payload = payload();
    let original = parse(&payload).unwrap();
    let mut second_scope = scope();
    second_scope.imported_by = "finance_retry".to_owned();
    let retry =
        ValidatedCostBatch::parse_json(&serde_json::to_vec(&payload).unwrap(), &second_scope)
            .unwrap();
    assert_eq!(original.sha256(), retry.sha256());
    assert_eq!(original.imported_by, "finance_original");
    assert_eq!(retry.imported_by, "finance_retry");
    let mut forged = payload;
    forged["imported_by"] = json!("admin");
    assert_eq!(parse(&forged).unwrap_err(), CostImportError::InvalidInput);
    for actor in ["", "actor@example.test", "actor\nadmin"] {
        assert!(
            CostImportScope::new(
                AccountScope::new("shop".to_owned(), Marketplace::Ozon).unwrap(),
                "one_c".to_owned(),
                std::iter::once(101).collect(),
                actor.to_owned(),
            )
            .is_err()
        );
    }
}

#[test]
fn source_and_sku_scope_are_independent_from_untrusted_payload() {
    for (field, value) in [
        ("account_id", json!("other")),
        ("marketplace", json!("wildberries")),
        ("source_id", json!("untrusted")),
    ] {
        let mut batch = payload();
        batch[field] = value;
        assert_eq!(parse(&batch).unwrap_err(), CostImportError::ScopeDenied);
    }
    let mut batch = payload();
    batch["rows"][0]["sku"] = json!(999);
    assert_eq!(parse(&batch).unwrap_err(), CostImportError::ScopeDenied);
}

#[test]
fn exact_amounts_vat_and_unknown_fields_fail_closed() {
    for (field, value) in [
        ("amount_minor", json!(-1)),
        ("amount_minor", json!(1.25)),
        ("amount_minor", json!("12345")),
        ("currency", json!("USD")),
        ("allocation", json!("absolute")),
        ("vat_rate_bps", json!(null)),
        ("vat_rate_bps", json!(10001)),
        ("buyer_email", json!("private@example.test")),
        ("effective_to", json!("2026-08-31")),
        ("source_row_id", json!("row\nsecret")),
    ] {
        let mut batch = payload();
        batch["rows"][0][field] = value;
        assert_eq!(parse(&batch).unwrap_err(), CostImportError::InvalidInput);
    }
    let mut batch = payload();
    batch["api_key"] = json!("must-not-be-accepted");
    assert_eq!(parse(&batch).unwrap_err(), CostImportError::InvalidInput);
    batch.as_object_mut().unwrap().remove("api_key");
    batch["rows"][0]["vat_treatment"] = json!("not_applicable");
    assert_eq!(parse(&batch).unwrap_err(), CostImportError::InvalidInput);
    batch["rows"][0]["vat_rate_bps"] = json!(null);
    batch["rows"][0]["amount_minor"] = json!(0);
    assert!(parse(&batch).is_ok());
}

#[test]
fn overlapping_periods_are_rejected_but_adjacent_periods_and_row_order_are_stable() {
    let mut batch = payload();
    let mut second = batch["rows"][0].clone();
    second["source_row_id"] = json!("row_2");
    second["effective_from"] = json!("2026-09-30");
    second["effective_to"] = json!("2026-10-31");
    batch["rows"].as_array_mut().unwrap().push(second);
    assert_eq!(parse(&batch).unwrap_err(), CostImportError::Conflict);
    batch["rows"][1]["effective_from"] = json!("2026-10-01");
    let valid = parse(&batch).unwrap();
    batch["rows"].as_array_mut().unwrap().reverse();
    assert_eq!(parse(&batch).unwrap().sha256(), valid.sha256());
    batch["rows"][0]["source_row_id"] = json!("row_1");
    assert_eq!(parse(&batch).unwrap_err(), CostImportError::InvalidInput);
}

#[test]
fn parser_rejects_duplicate_json_keys_unsupported_version_and_unbounded_input() {
    let bytes = serde_json::to_string(&payload()).unwrap().replacen(
        "\"version\":1",
        "\"version\":1,\"version\":1",
        1,
    );
    assert_eq!(
        ValidatedCostBatch::parse_json(bytes.as_bytes(), &scope()).unwrap_err(),
        CostImportError::InvalidInput
    );
    assert_eq!(
        ValidatedCostBatch::parse_json(&vec![b' '; MAX_COST_IMPORT_BYTES + 1], &scope())
            .unwrap_err(),
        CostImportError::LimitExceeded
    );
    let mut batch = payload();
    batch["version"] = json!(2);
    assert_eq!(parse(&batch).unwrap_err(), CostImportError::InvalidInput);
    batch["version"] = json!(1);
    batch["rows"] = json!([]);
    assert_eq!(parse(&batch).unwrap_err(), CostImportError::LimitExceeded);
    assert!(
        CostImportScope::new(
            AccountScope::new("shop".to_owned(), Marketplace::Ozon).unwrap(),
            "one_c".to_owned(),
            BTreeSet::new(),
            "finance_original".to_owned()
        )
        .is_err()
    );
}
