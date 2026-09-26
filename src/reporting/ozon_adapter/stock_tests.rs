use chrono::{Duration, TimeZone, Utc};
use serde_json::{Value, json};

use super::*;
use crate::reporting::{
    postgres_collector::{CollectedFacts, CollectedSnapshot},
    snapshot::{Marketplace, SnapshotStatus},
};

fn page(stock: Value) -> Value {
    json!({"items": [{"product_id": 123, "stocks": Value::Array(vec![stock])}]})
}

#[test]
fn stock_identity_comes_from_each_fulfillment_row_and_reserves_are_removed() {
    let stocks = parse_stock_page(&json!({"items": [{
        "product_id": 123,
        "stocks": [
            {"type": "fbo", "present": 4, "reserved": 1, "sku": 101},
            {"type": "fbs", "present": 7, "reserved": 1, "sku": 102},
            {"type": "rfbs", "present": 7, "reserved": 7, "sku": 103}
        ]
    }]}))
    .unwrap();
    assert_eq!(
        stocks,
        vec![
            CollectedStockFact {
                sku: 101,
                warehouse_id: "sku-fulfillment-v2:fbo".to_owned(),
                sellable_units: 3
            },
            CollectedStockFact {
                sku: 102,
                warehouse_id: "sku-fulfillment-v2:fbs".to_owned(),
                sellable_units: 6
            },
            CollectedStockFact {
                sku: 103,
                warehouse_id: "sku-fulfillment-v2:rfbs".to_owned(),
                sellable_units: 0
            },
        ]
    );
    assert!(stocks.iter().all(|row| row.sku != 123));
}

#[test]
fn stock_fulfillment_type_is_case_normalized_without_changing_sku() {
    for (lower, upper) in [("fbo", "FBO"), ("fbs", "FBS"), ("rfbs", "RFBS")] {
        let lower = parse_stock_page(&page(json!({
            "type": lower, "sku": 999, "present": 7, "reserved": 2
        })))
        .unwrap();
        let upper = parse_stock_page(&page(json!({
            "type": upper, "sku": 999, "present": 7, "reserved": 2
        })))
        .unwrap();
        assert_eq!(lower, upper);
        assert!(stocks::is_sku_fulfillment_dimension(&lower[0].warehouse_id));
    }
}

#[test]
fn same_sku_in_different_schemes_is_preserved_without_merging_provenance() {
    let rows = parse_stock_page(&json!({"items": [{"product_id": 123, "stocks": [
        {"type": "fbs", "sku": 456, "present": 4, "reserved": 1},
        {"type": "rfbs", "sku": 456, "present": 2, "reserved": 0}
    ]}]}))
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].sku, rows[1].sku);
    assert_ne!(rows[0].warehouse_id, rows[1].warehouse_id);
    assert_eq!(rows[0].sellable_units, 3);
    assert_eq!(rows[1].sellable_units, 2);
}

#[test]
fn missing_or_invalid_stock_fields_are_never_inferred_from_product_id_or_zero() {
    let valid = json!({"type": "rfbs", "sku": 456, "present": 7, "reserved": 2});
    for field in ["sku", "type", "present", "reserved"] {
        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(parse_stock_page(&page(missing)).is_err(), "{field}");
        let mut null = valid.clone();
        null[field] = Value::Null;
        assert!(parse_stock_page(&page(null)).is_err(), "{field}");
    }
    for (field, value) in [
        ("sku", json!(0)),
        ("sku", json!(u64::MAX)),
        ("sku", json!(-1)),
        ("type", json!("unverified-scheme")),
        ("reserved", json!(8)),
        ("reserved", json!(-1)),
        ("present", json!(-1)),
        ("present", json!(1.5)),
        ("present", json!("unknown")),
        ("present", json!(u64::MAX)),
    ] {
        let mut invalid = valid.clone();
        invalid[field] = value;
        assert!(parse_stock_page(&page(invalid)).is_err(), "{field}");
    }
    for product_id in [json!(0), json!(-1), json!(null), json!(u64::MAX)] {
        let mut invalid = page(valid.clone());
        invalid["items"][0]["product_id"] = product_id;
        assert!(parse_stock_page(&invalid).is_err());
    }
}

#[test]
fn duplicate_fulfillment_totals_and_product_pages_are_rejected_not_summed() {
    let stock = json!({"type": "fbs", "sku": 456, "present": 3, "reserved": 0});
    let mut same_product = page(stock.clone());
    same_product["items"][0]["stocks"] = json!([stock, stock]);
    assert_eq!(
        parse_stock_page(&same_product),
        Err(OzonReportParseError::Value)
    );
    let mut two_products = page(stock);
    let mut repeated = two_products["items"][0].clone();
    two_products["items"]
        .as_array_mut()
        .unwrap()
        .push(repeated.clone());
    assert_eq!(
        parse_stock_page(&two_products),
        Err(OzonReportParseError::Value)
    );
    repeated["product_id"] = json!(999);
    two_products["items"][1] = repeated;
    assert_eq!(
        parse_stock_page(&two_products),
        Err(OzonReportParseError::Value)
    );
}

#[test]
fn nested_fulfillment_rows_are_bounded() {
    let mut response = page(json!({"type":"fbs","sku":456,"present":1,"reserved":0}));
    response["items"][0]["stocks"] = json!(vec![Value::Null; PRODUCT_PAGE_ROWS + 1]);
    assert_eq!(
        parse_stock_page(&response),
        Err(OzonReportParseError::TooManyRows)
    );
}

#[test]
fn corrected_inventory_satisfies_the_persisted_snapshot_contract() {
    let stocks = parse_stock_page(&json!({"items": [{
        "product_id": 123,
        "stocks": [
            {"type": "fbs", "sku": 456, "present": 2, "reserved": 1},
            {"type": "rfbs", "sku": 456, "present": 7, "reserved": 2}
        ]
    }]}))
    .unwrap();
    let cutoff = Utc.with_ymd_and_hms(2026, 9, 26, 3, 0, 0).unwrap();
    let observed = cutoff - Duration::minutes(10);
    let snapshot = CollectedSnapshot::new(
        "fixture-account".to_owned(),
        Marketplace::Ozon,
        cutoff,
        observed,
        observed,
        observed,
        SnapshotStatus::Succeeded,
        true,
        "test-sku-fulfillment-v2".to_owned(),
        CollectedFacts::Stocks(stocks),
    )
    .unwrap();
    let serialized = serde_json::to_value(&snapshot).unwrap();
    assert_eq!(serialized["facts"]["facts"][0]["sku"], 456);
    assert_eq!(
        serialized["facts"]["facts"][0]["warehouse_id"],
        "sku-fulfillment-v2:fbs"
    );
    assert_eq!(
        serialized["facts"]["facts"][1]["warehouse_id"],
        "sku-fulfillment-v2:rfbs"
    );
    assert_eq!(
        serde_json::from_value::<CollectedSnapshot>(serialized).unwrap(),
        snapshot
    );
}
