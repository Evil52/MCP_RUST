use chrono::{Duration, TimeZone, Utc};
use serde_json::json;

use super::*;
use crate::reporting::{
    postgres_collector::{CollectedFacts, CollectedSnapshot},
    snapshot::{Marketplace, SnapshotStatus},
};

#[test]
fn stock_fulfillment_type_is_case_normalized() {
    let stocks = parse_stock_page(&json!({"items": [{
        "product_id": 123,
        "stocks": [{"type": "fbs", "present": 7}]
    }]}))
    .unwrap();
    assert_eq!(stocks[0].warehouse_id, "FBS");
}

#[test]
fn rfbs_inventory_preserves_fulfillment_provenance() {
    // Synthetic values reproduce the verified response shape: one product
    // may have multiple FBS rows plus a separate rFBS fulfillment row.
    let stocks = parse_stock_page(&json!({"items": [{
        "product_id": 123,
        "stocks": [
            {"type": "fbo", "present": 4, "reserved": 0, "sku": 101},
            {"type": "fbs", "present": 2, "reserved": 0, "sku": 102},
            {"type": "fbs", "present": 3, "reserved": 0, "sku": 103},
            {"type": "rfbs", "present": 7, "reserved": 0, "sku": 104},
            {"type": "RFBS", "present": 11, "reserved": 0, "sku": 105}
        ]
    }, {
        "product_id": 456,
        "stocks": [{"type": "rfbs", "present": 0, "reserved": 0}]
    }]}))
    .unwrap();
    assert_eq!(
        stocks,
        vec![
            CollectedStockFact {
                sku: 123,
                warehouse_id: "FBO".to_owned(),
                sellable_units: 4
            },
            CollectedStockFact {
                sku: 123,
                warehouse_id: "FBS".to_owned(),
                sellable_units: 5
            },
            CollectedStockFact {
                sku: 123,
                warehouse_id: "RFBS".to_owned(),
                sellable_units: 18
            },
            CollectedStockFact {
                sku: 456,
                warehouse_id: "RFBS".to_owned(),
                sellable_units: 0
            },
        ]
    );
}

#[test]
fn rfbs_support_does_not_turn_missing_or_invalid_inventory_into_zero() {
    for stock in [
        json!({"type": "rfbs"}),
        json!({"type": "rfbs", "present": null}),
        json!({"type": "rfbs", "present": -1}),
        json!({"type": "rfbs", "present": 1.5}),
        json!({"type": "rfbs", "present": "unknown"}),
        json!({"present": 1}),
        json!({"type": null, "present": 1}),
        json!({"type": "unverified-scheme", "present": 1}),
    ] {
        assert!(
            parse_stock_page(&json!({"items": [{
                "product_id": 123, "stocks": [stock]
            }]}))
            .is_err()
        );
    }
    assert_eq!(
        parse_stock_page(&json!({"items": [{
            "product_id": 123,
            "stocks": [
                {"type": "rfbs", "present": u64::MAX},
                {"type": "rfbs", "present": 1}
            ]
        }]})),
        Err(OzonReportParseError::Value)
    );
}

#[test]
fn rfbs_inventory_satisfies_the_persisted_snapshot_contract() {
    let stocks = parse_stock_page(&json!({"items": [{
        "product_id": 123,
        "stocks": [
            {"type": "fbs", "present": 2},
            {"type": "rfbs", "present": 7}
        ]
    }]}))
    .unwrap();
    let cutoff = Utc.with_ymd_and_hms(2026, 9, 10, 3, 0, 0).unwrap();
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
        "test-rfbs".to_owned(),
        CollectedFacts::Stocks(stocks),
    )
    .unwrap();
    let serialized = serde_json::to_value(&snapshot).unwrap();
    assert_eq!(serialized["facts"]["facts"][0]["warehouse_id"], "FBS");
    assert_eq!(serialized["facts"]["facts"][1]["warehouse_id"], "RFBS");
    assert_eq!(
        serde_json::from_value::<CollectedSnapshot>(serialized).unwrap(),
        snapshot
    );
}
