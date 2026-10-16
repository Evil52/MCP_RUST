//! Bounded immutable Ozon replenishment evidence for credentialless readers.
use super::{
    Deserialize, PostgresCollectorError, Serialize, Transaction, as_i64, ensure_unique, fits_i64,
};
use crate::ozon_posting_sales::PostingSalesRow;
use serde_json::{Value, json};

const COUNT_FIELDS: &[&str] = &[
    "available_stock_count",
    "transit_stock_count",
    "requested_stock_count",
    "inbound_replenishment",
    "valid_stock_count",
    "outbound_pending_delivery",
    "return_from_customer_stock_count",
    "stock_not_being_sold",
    "other_stock_count",
    "stock_defect_stock_count",
    "transit_defect_stock_count",
    "days_without_sales",
    "days_without_sales_cluster",
    "excess_stock_count",
    "expiring_stock_count",
];
const RATE_FIELDS: &[&str] = &["ads", "ads_cluster", "idc", "idc_cluster"];
const TEXT_FIELDS: &[&str] = &[
    "offer_id",
    "cluster_name",
    "warehouse_name",
    "turnover_grade",
    "turnover_grade_cluster",
];

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CollectedClusterStockFact {
    pub sku: u64,
    pub warehouse_id: Option<i64>,
    pub cluster_id: Option<u64>,
    pub macrolocal_cluster_id: Option<u64>,
    pub data: Value,
}

fn id(value: &Value) -> Option<u64> {
    value.as_u64().filter(|n| *n > 0 && fits_i64(*n))
}

pub(crate) fn parse_cluster_rows(
    response: &Value,
    requested: &[u64],
) -> Result<Vec<CollectedClusterStockFact>, PostgresCollectorError> {
    let invalid = PostgresCollectorError::InvalidInput;
    let items = response
        .get("items")
        .and_then(Value::as_array)
        .ok_or(invalid)?;
    if items.len() > 25_000 {
        return Err(invalid);
    }
    let mut rows = Vec::new();
    let mut observed = std::collections::BTreeSet::new();
    for item in items {
        let sku = id(&item["sku"])
            .filter(|n| requested.contains(n))
            .ok_or(invalid)?;
        let warehouse_id = item["warehouse_id"]
            .as_i64()
            .filter(|n| *n != 0)
            .ok_or(invalid)?;
        let cluster_id = id(&item["cluster_id"]).ok_or(invalid)?;
        let macrolocal_cluster_id = id(&item["macrolocal_cluster_id"]).ok_or(invalid)?;
        let mut data =
            json!({"source_row":true,"data_classification":"untrusted_external_marketplace_data"});
        for field in COUNT_FIELDS.iter().chain(RATE_FIELDS).chain(TEXT_FIELDS) {
            if let Some(value) = item.get(field) {
                data[*field] = value.clone();
            }
        }
        rows.push(CollectedClusterStockFact {
            sku,
            warehouse_id: Some(warehouse_id),
            cluster_id: Some(cluster_id),
            macrolocal_cluster_id: Some(macrolocal_cluster_id),
            data,
        });
        observed.insert(sku);
    }
    for sku in requested.iter().filter(|sku| !observed.contains(sku)) {
        rows.push(CollectedClusterStockFact{sku:*sku,warehouse_id:None,cluster_id:None,macrolocal_cluster_id:None,data:json!({"source_row":false,"data_classification":"untrusted_external_marketplace_data"})});
    }
    validate_clusters(&rows)?;
    Ok(rows)
}

pub(super) fn validate_clusters(
    rows: &[CollectedClusterStockFact],
) -> Result<(), PostgresCollectorError> {
    ensure_unique(
        rows,
        |r| (r.sku, r.warehouse_id, r.cluster_id, r.macrolocal_cluster_id),
        |r| {
            r.sku > 0
                && fits_i64(r.sku)
                && r.warehouse_id.is_none_or(|n| n != 0)
                && [r.cluster_id, r.macrolocal_cluster_id]
                    .iter()
                    .all(|v| v.is_none_or(|n| n > 0 && fits_i64(n)))
                && (r.warehouse_id.is_some() == r.cluster_id.is_some())
                && (r.warehouse_id.is_some() == r.macrolocal_cluster_id.is_some())
                && r.data["source_row"].as_bool() == Some(r.warehouse_id.is_some())
                && r.data["data_classification"] == "untrusted_external_marketplace_data"
                && r.data.as_object().is_some_and(|d| {
                    d.iter().all(|(k, v)| match k.as_str() {
                        "source_row" | "data_classification" => true,
                        key if COUNT_FIELDS.contains(&key) => {
                            v.is_null()
                                || v.as_u64()
                                    .is_some_and(|n| n <= i32::MAX.cast_unsigned().into())
                        }
                        key if RATE_FIELDS.contains(&key) => {
                            v.is_null() || v.as_f64().is_some_and(|n| n.is_finite() && n >= 0.0)
                        }
                        key if TEXT_FIELDS.contains(&key) => {
                            v.is_null()
                                || v.as_str().is_some_and(|s| {
                                    s.len() <= 512 && !s.chars().any(char::is_control)
                                })
                        }
                        _ => false,
                    })
                })
                && serde_json::to_vec(&r.data).is_ok_and(|v| v.len() <= 8192)
        },
    )
}

pub(super) fn validate_postings(rows: &[PostingSalesRow]) -> Result<(), PostgresCollectorError> {
    ensure_unique(
        rows,
        |r| r.sku,
        |r| {
            let statuses = |values: &std::collections::BTreeMap<String, u64>| {
                values.iter().try_fold(0u64, |n, (k, v)| {
                    if k.is_empty()
                        || k.len() > 128
                        || !k
                            .bytes()
                            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
                        || !fits_i64(*v)
                    {
                        None
                    } else {
                        n.checked_add(*v)
                    }
                })
            };
            r.sku > 0
                && fits_i64(r.sku)
                && r.fbo_status_units.len() <= 32
                && r.fbs_status_units.len() <= 32
                && r.fbo_non_cancelled_units
                    .checked_add(r.fbs_non_cancelled_units)
                    == Some(r.total_non_cancelled_units)
                && r.fbo_cancelled_units.checked_add(r.fbs_cancelled_units)
                    == Some(r.total_cancelled_units)
                && statuses(&r.fbo_status_units)
                    == r.fbo_non_cancelled_units.checked_add(r.fbo_cancelled_units)
                && statuses(&r.fbs_status_units)
                    == r.fbs_non_cancelled_units.checked_add(r.fbs_cancelled_units)
                && r.fbo_status_units.get("cancelled").copied().unwrap_or(0)
                    == r.fbo_cancelled_units
                && r.fbs_status_units.get("cancelled").copied().unwrap_or(0)
                    == r.fbs_cancelled_units
                && [r.total_non_cancelled_units, r.total_cancelled_units]
                    .into_iter()
                    .all(fits_i64)
        },
    )
}

pub(super) async fn insert_clusters(
    tx: &Transaction<'_>,
    snapshot: i64,
    rows: &[CollectedClusterStockFact],
) -> Result<(), PostgresCollectorError> {
    let skus = rows
        .iter()
        .map(|r| as_i64(r.sku))
        .collect::<Result<Vec<_>, _>>()?;
    let warehouses = rows.iter().map(|r| r.warehouse_id).collect::<Vec<_>>();
    let clusters = rows
        .iter()
        .map(|r| r.cluster_id.map(as_i64).transpose())
        .collect::<Result<Vec<_>, _>>()?;
    let macroclusters = rows
        .iter()
        .map(|r| r.macrolocal_cluster_id.map(as_i64).transpose())
        .collect::<Result<Vec<_>, _>>()?;
    let data = rows.iter().map(|r| r.data.to_string()).collect::<Vec<_>>();
    tx.execute("INSERT INTO daily_reporting.cluster_stock_facts(snapshot_id,sku,warehouse_id,cluster_id,macrolocal_cluster_id,data) SELECT $1,r.sku,r.warehouse_id,r.cluster_id,r.macrolocal_cluster_id,r.data::jsonb FROM unnest($2::bigint[],$3::bigint[],$4::bigint[],$5::bigint[],$6::text[]) r(sku,warehouse_id,cluster_id,macrolocal_cluster_id,data)", &[&snapshot,&skus,&warehouses,&clusters,&macroclusters,&data]).await.map_err(|_|PostgresCollectorError::Unavailable)?;
    Ok(())
}

pub(super) async fn insert_postings(
    tx: &Transaction<'_>,
    snapshot: i64,
    rows: &[PostingSalesRow],
) -> Result<(), PostgresCollectorError> {
    let skus = rows
        .iter()
        .map(|r| as_i64(r.sku))
        .collect::<Result<Vec<_>, _>>()?;
    let data = rows
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| PostgresCollectorError::InvalidInput)?;
    tx.execute("INSERT INTO daily_reporting.posting_movement_facts(snapshot_id,sku,data) SELECT $1,r.sku,r.data::jsonb FROM unnest($2::bigint[],$3::text[]) r(sku,data)", &[&snapshot,&skus,&data]).await.map_err(|_|PostgresCollectorError::Unavailable)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_cluster_rows_are_unknown_and_foreign_skus_or_nested_contacts_are_refused() {
        let rows = parse_cluster_rows(&json!({"items":[]}), &[1]).unwrap();
        assert_eq!(rows[0].warehouse_id, None);
        assert!(rows[0].data.get("available_stock_count").is_none());
        let row = json!({"sku":1,"warehouse_id":99,"cluster_id":2,"macrolocal_cluster_id":4007,"available_stock_count":0,"ads_cluster":0.5,"return_from_customer_stock_count":2,"customer":{"phone":"123"}});
        let rows = parse_cluster_rows(&json!({"items":[row]}), &[1]).unwrap();
        assert!(rows[0].data.get("customer").is_none());
        assert_eq!(rows[0].data["return_from_customer_stock_count"], 2);
        assert!(parse_cluster_rows(&json!({"items":[row]}), &[2]).is_err());
    }

    #[test]
    fn signed_warehouse_ids_preserve_stock_without_colliding_with_positive_ids() {
        let row = json!({"sku":1,"warehouse_id":-1_036_383,"cluster_id":179,"macrolocal_cluster_id":4069,"available_stock_count":2,"ads_cluster":0.08});
        let mut positive = row.clone();
        positive["warehouse_id"] = json!(1_036_383);
        let rows = parse_cluster_rows(&json!({"items":[row,positive]}), &[1, 2]).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].warehouse_id, Some(-1_036_383));
        assert_eq!(rows[1].warehouse_id, Some(1_036_383));
        assert_eq!(rows[0].data["available_stock_count"], 2);
        assert_eq!(rows[2].warehouse_id, None);
        for invalid in [json!(0), json!(u64::MAX), json!(null), json!(1.5)] {
            let mut bad = row.clone();
            bad["warehouse_id"] = invalid;
            assert!(parse_cluster_rows(&json!({"items":[bad]}), &[1]).is_err());
        }
    }

    #[test]
    fn warehouse_ids_are_scoped_to_cluster_coordinates() {
        let first = json!({"sku":1_426_765_390_u64,"warehouse_id":-1_014_552,"cluster_id":191,"macrolocal_cluster_id":4076,"available_stock_count":0,"outbound_pending_delivery":0});
        let mut second = first.clone();
        second["cluster_id"] = json!(193);
        second["macrolocal_cluster_id"] = json!(4077);
        second["outbound_pending_delivery"] = json!(1);
        let rows = parse_cluster_rows(&json!({"items":[first,second]}), &[1_426_765_390]).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].data["outbound_pending_delivery"], 0);
        assert_eq!(rows[1].data["outbound_pending_delivery"], 1);
        let mut macro_only = first.clone();
        macro_only["macrolocal_cluster_id"] = json!(4077);
        assert!(parse_cluster_rows(&json!({"items":[first,macro_only]}), &[1_426_765_390]).is_ok());
        assert!(parse_cluster_rows(&json!({"items":[first,first]}), &[1_426_765_390]).is_err());
    }
}
