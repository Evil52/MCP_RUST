use super::*;

#[test]
fn campaigns_and_both_stats_shapes_are_normalized_without_double_counting() {
    assert!(parse_promotion_stats(&Value::Null).unwrap().is_empty());
    let ids = parse_campaign_ids(&json!({"adverts":[
        {"status":9,"advert_list":[{"advertId":2},{"advertId":1}]},
        {"status":8,"advert_list":[{"advertId":3}]}
    ]}))
    .unwrap();
    assert_eq!(ids, vec![1, 2]);

    let facts = parse_promotion_stats(&json!([{
        "advertId":1,"days":[{"date":"2026-08-17T00:00:00Z","views":99,
            "clicks":9,"sum":10,"orders":2,"sum_price":200,
            "apps":[{"nm":[
                {"nmId":7,"views":20,"clicks":2,"sum":4.25,"orders":1,"sum_price":100},
                {"nmId":7,"views":10,"clicks":1,"sum":2,"orders":0,"sum_price":0}
            ]}]}]
    },{
        "advert_id":2,"stats":[{"date":"2026-08-17","nm_id":8,"views":5,
            "clicks":1,"sum":"1.20","orders":1,"sumPrice":50}]
    }]))
    .unwrap();
    assert_eq!(facts.len(), 2);
    assert_eq!(
        (
            facts[0].campaign_id,
            facts[0].sku,
            facts[0].impressions,
            facts[0].spend_minor
        ),
        (1, 7, 30, 625)
    );
    assert_eq!(
        (
            facts[1].campaign_id,
            facts[1].sku,
            facts[1].attributed_revenue_minor
        ),
        (2, 8, 5000)
    );

    let campaign_only = parse_promotion_stats(&json!([{
        "advertId":3,"days":[{"date":"2026-08-17","views":8,"clicks":1,
            "sum":2,"orders":0,"sum_price":0}]
    }]))
    .unwrap();
    assert_eq!(campaign_only[0].sku, 0);
}

#[test]
fn fullstats_v3_nms_preserves_sku_and_rejects_ambiguous_or_inconsistent_rows() {
    let product = json!({"nmId":7,"views":10,"clicks":2,"sum":1.25,"orders":1,"sum_price":50});
    let mut body = json!([{"advertId":1,"days":[{"date":"2026-09-13T00:00:00Z",
        "views":20,"clicks":4,"sum":2.50,"orders":2,"sum_price":100,
        "apps":[{"appType":1,"nms":[product]},{"appType":64,"nms":[product]}]
    }]}]);
    let facts = parse_promotion_stats(&body).unwrap();
    assert_eq!(facts.len(), 1);
    assert_eq!(
        (
            facts[0].sku,
            facts[0].impressions,
            facts[0].clicks,
            facts[0].spend_minor,
            facts[0].attributed_revenue_minor
        ),
        (7, 20, 4, 250, 10000)
    );
    body[0]["days"][0]["apps"][0]["nm"] = json!([]);
    assert_eq!(parse_promotion_stats(&body), Err(WbReportParseError::Shape));
    body[0]["days"][0]["apps"][0]
        .as_object_mut()
        .unwrap()
        .remove("nm");
    body[0]["days"][0]["apps"][0]["nms"] = Value::Null;
    assert_eq!(parse_promotion_stats(&body), Err(WbReportParseError::Shape));
    body[0]["days"][0]["apps"][0]["nms"] =
        json!([{"nmId":7,"views":0,"clicks":30,"sum":1.25,"orders":1,"sum_price":50}]);
    assert_eq!(
        parse_promotion_stats(&body),
        Err(WbReportParseError::InconsistentAdvertisingCounts)
    );
}
