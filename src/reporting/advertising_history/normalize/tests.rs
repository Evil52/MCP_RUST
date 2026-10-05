use super::*;

fn day(spend: &Value) -> Value {
    json!({"date":"2023-10-09T00:00:00Z","sum":spend,"sum_price":1000,"orders":2,"views":20,"clicks":3,
        "apps":[{"nms":[{"nmId":99,"sum":spend,"sum_price":1000,"orders":2,"views":20,"clicks":3}]}]})
}

#[test]
fn old_dates_exact_kopecks_and_parent_child_totals_are_not_added_twice() {
    let date = NaiveDate::from_ymd_opt(2023, 10, 9).unwrap();
    let rows = normalize_statistics(
        &json!([{"advertId":7,"days":[day(&json!("7.01"))]}]),
        &[7, 8],
        date,
        date,
    )
    .unwrap();
    assert_eq!(rows[0]["metrics"]["spend_minor"], 701);
    assert_eq!(rows[0]["products"][0]["metrics"]["spend_minor"], 701);
    assert_eq!(rows[1]["state"], "missing");
    assert!(rows[1]["metrics"].is_null());
}

#[test]
fn mismatched_products_keep_parent_but_report_unallocated_breakdown() {
    let date = NaiveDate::from_ymd_opt(2023, 10, 9).unwrap();
    let mut value = day(&json!(7));
    value["apps"][0]["nms"][0]["sum"] = json!(6);
    let rows =
        normalize_statistics(&json!([{"advertId":7,"days":[value]}]), &[7], date, date).unwrap();
    assert_eq!(rows[0]["metrics"]["spend_minor"], 700);
    assert_eq!(rows[0]["sku_reconciled"], false);
    assert_eq!(rows[0]["products"], json!([]));
}

#[test]
fn null_is_distinct_from_omitted_campaign_and_open_scope_is_rejected() {
    let date = NaiveDate::from_ymd_opt(2023, 10, 9).unwrap();
    assert_eq!(
        normalize_statistics(&Value::Null, &[7], date, date).unwrap()[0]["state"],
        "no_data"
    );
    assert_eq!(
        normalize_statistics(&json!([]), &[7], date, date).unwrap()[0]["state"],
        "missing"
    );
    assert!(normalize_statistics(&json!([{"advertId":8,"days":[]}]), &[7], date, date).is_err());
    assert!(normalize_statistics(&Value::Null, &[7], date, date + Duration::days(31)).is_err());
}

#[test]
fn inventory_is_exhaustive_bounded_and_preserves_unknown_start() {
    let value = json!({"all":2,"adverts":[{"status":7,"count":2,"advert_list":[{"advertId":1,"createTime":"2023-10-09T00:00:00Z"},{"advertId":2}]}]});
    let rows = normalize_inventory(&value).unwrap();
    assert_eq!(rows[0]["created_on"], "2023-10-09");
    assert!(rows[1]["created_on"].is_null());
    let mut truncated = value;
    truncated["all"] = json!(3);
    assert!(normalize_inventory(&truncated).is_err());
}

#[test]
fn discovery_uses_creation_not_last_change_or_restart_and_rejects_duplicate_days() {
    let raw = json!({"adverts":[{"id":7,"timestamps":{"created":"2023-10-09T09:00:00+03:00","started":"2026-10-01T12:00:00+03:00"}}]});
    assert_eq!(
        normalize_details(&raw, &[7]).unwrap()[0]["created_on"],
        "2023-10-09"
    );
    let count = json!({"all":1,"adverts":[{"status":9,"count":1,"advert_list":[{"advertId":7,"changeTime":"2026-10-01T12:00:00+03:00"}]}]});
    assert!(normalize_inventory(&count).unwrap()[0]["created_on"].is_null());
    let date = NaiveDate::from_ymd_opt(2023, 10, 9).unwrap();
    assert!(
        normalize_statistics(
            &json!([{"advertId":7,"days":[day(&json!(7)),day(&json!(7))]}]),
            &[7],
            date,
            date
        )
        .is_err()
    );
}
