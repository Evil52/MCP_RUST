//! Exercise durable history against real PostgreSQL functions and restricted roles.
use chrono::{Duration, NaiveDate};
use mcp_ozon::reporting::advertising_history::{
    HistoryGroup, HistoryRepository, normalize_details, normalize_inventory, normalize_statistics,
};
use serde_json::{Value, json};
use tokio_postgres::{Client, NoTls, types::ToSql};

async fn connect(url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    client
}

async fn value(client: &Client, sql: &str, args: &[&(dyn ToSql + Sync)]) -> Value {
    let text: Option<String> = client.query_one(sql, args).await.unwrap().get(0);
    text.map_or(Value::Null, |text| serde_json::from_str(&text).unwrap())
}

async fn expedite(admin: &Client, account: &str) {
    admin.execute("UPDATE daily_reporting.wb_history_jobs SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE account_id=$1 AND status='queued'", &[&account]).await.unwrap();
    admin.execute("UPDATE daily_reporting.source_collection_departures SET next_allowed_at=clock_timestamp()-interval '1 second' WHERE account_id=$1", &[&account]).await.unwrap();
}

async fn claim(collector: &Client, account: &str) -> Value {
    value(
        collector,
        "SELECT daily_reporting.wb_history_claim($1,'history-test')::text",
        &[&vec![account.to_owned()]],
    )
    .await
}

async fn inventory(collector: &Client, claim: &Value, account: &str, ids: &[u64], date: NaiveDate) {
    assert_eq!(claim["account_id"], account);
    let raw = json!({"all":ids.len(),"adverts":[{"status":7,"count":ids.len(),"advert_list":ids.iter().map(|id| json!({"advertId":id,"createTime":date.to_string()})).collect::<Vec<_>>()}]});
    let normalized = normalize_inventory(&raw).unwrap();
    collector
        .execute(
            "SELECT daily_reporting.wb_history_inventory($1,$2,'history-test',$3::text::jsonb)",
            &[
                &claim["job_id"].as_i64().unwrap(),
                &claim["generation"].as_i64().unwrap(),
                &normalized.to_string(),
            ],
        )
        .await
        .unwrap();
}

fn fullstats(ids: &[u64], date: NaiveDate, spend: u64, revenue: u64) -> Value {
    json!(ids.iter().map(|id| json!({"advertId":id,"days":[{"date":date.to_string(),"sum":spend,"sum_price":revenue,"orders":1,"views":100,"clicks":3,
        "apps":[{"nms":[{"nmId":99+id,"sum":spend,"sum_price":revenue,"orders":1,"views":100,"clicks":3}]}]}]})).collect::<Vec<_>>())
}

async fn publish(collector: &Client, claim: &Value, raw: &Value) {
    let from = NaiveDate::parse_from_str(claim["date_from"].as_str().unwrap(), "%Y-%m-%d").unwrap();
    let to = NaiveDate::parse_from_str(claim["date_to"].as_str().unwrap(), "%Y-%m-%d").unwrap();
    let ids: Vec<u64> = serde_json::from_value(claim["campaign_ids"].clone()).unwrap();
    let days = normalize_statistics(raw, &ids, from, to).unwrap();
    collector.execute("SELECT daily_reporting.wb_history_publish($1,$2,'history-test',$3,$4::text::jsonb,$5::text::jsonb)",
        &[&claim["job_id"].as_i64().unwrap(),&claim["generation"].as_i64().unwrap(),&claim["task_id"].as_i64().unwrap(),&raw.to_string(),&days.to_string()]).await.unwrap();
}

#[tokio::test]
async fn history_resumes_revises_deduplicates_and_keeps_gaps_account_scoped() {
    let Ok(admin_url) = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL") else {
        eprintln!("skipped: isolated PostgreSQL URL missing");
        return;
    };
    let collector_url = std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL").unwrap();
    let requester_url = std::env::var("REPORT_REFRESH_TEST_REQUESTER_URL").unwrap();
    let admin = connect(&admin_url).await;
    let collector = connect(&collector_url).await;
    let requester = connect(&requester_url).await;
    let api = HistoryRepository::connect_optional(Some(&requester_url))
        .await
        .unwrap();
    let account = format!("history_{}", std::process::id());
    let date = NaiveDate::from_ymd_opt(2023, 10, 9).unwrap();
    let to = date + Duration::days(62);
    let first = api.request(&account, "finance", None, to).await.unwrap();
    let duplicate = api.request(&account, "finance", None, to).await.unwrap();
    assert_eq!(first["job"]["job_id"], duplicate["job"]["job_id"]);
    let initial = claim(&collector, &account).await;
    inventory(&collector, &initial, &account, &[7, 8], date).await;
    let progress = api.status(&account).await.unwrap();
    assert_eq!(progress["job"]["total_requests"], 3);

    // Restart after leasing a request, before any WB result is published.
    expedite(&admin, &account).await;
    let abandoned = claim(&collector, &account).await;
    assert!(claim(&collector, &account).await.is_null());
    admin.execute("UPDATE daily_reporting.wb_history_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", &[&abandoned["job_id"].as_i64().unwrap()]).await.unwrap();
    expedite(&admin, &account).await;
    let resumed = claim(&collector, &account).await;
    assert_eq!(resumed["task_id"], abandoned["task_id"]);
    assert!(resumed["generation"].as_i64().unwrap() > abandoned["generation"].as_i64().unwrap());
    assert!(collector.execute("SELECT daily_reporting.wb_history_publish($1,$2,'history-test',$3,'null'::jsonb,'[]'::jsonb)",
        &[&abandoned["job_id"].as_i64().unwrap(),&abandoned["generation"].as_i64().unwrap(),&abandoned["task_id"].as_i64().unwrap()]).await.is_err());
    publish(&collector, &resumed, &fullstats(&[7, 8], date, 7, 100)).await;
    for _ in 0..2 {
        expedite(&admin, &account).await;
        let c = claim(&collector, &account).await;
        assert!(
            NaiveDate::parse_from_str(c["date_to"].as_str().unwrap(), "%Y-%m-%d").unwrap()
                - NaiveDate::parse_from_str(c["date_from"].as_str().unwrap(), "%Y-%m-%d").unwrap()
                < Duration::days(31)
        );
        publish(&collector, &c, &Value::Null).await;
    }
    let stats = api
        .stats(&account, None, None, HistoryGroup::Campaign, 100, 0)
        .await
        .unwrap();
    assert_eq!(stats["totals"]["spend_minor"], 1400);
    assert_eq!(stats["totals"]["drr_percent"], "7.0000");
    assert_eq!(stats["coverage"]["expected_campaign_days"], 126);
    assert_eq!(stats["coverage"]["complete_for_known_campaigns"], true);
    assert_eq!(stats["all_time_verified"], false);
    assert_eq!(
        api.status(&account).await.unwrap()["job"]["status"],
        "succeeded"
    );

    // Revised/overlapping response replaces the same calendar day, including
    // SKU projection; it must not append expenditure to the earlier version.
    api.request(&account, "finance", Some(date), date)
        .await
        .unwrap();
    expedite(&admin, &account).await;
    inventory(
        &collector,
        &claim(&collector, &account).await,
        &account,
        &[7, 8],
        date,
    )
    .await;
    expedite(&admin, &account).await;
    let mut revision = fullstats(&[7, 8], date, 12, 200);
    let second = fullstats(&[8], date, 7, 50);
    revision[1] = second[0].clone();
    publish(&collector, &claim(&collector, &account).await, &revision).await;
    let stats = api
        .stats(&account, None, None, HistoryGroup::Campaign, 1, 0)
        .await
        .unwrap();
    assert_eq!(stats["totals"]["spend_minor"], 1900);
    assert_eq!(stats["totals"]["revenue_minor"], 25_000);
    // Weighted account DRR, not the mean of 6% and 14% campaign rates.
    assert_eq!(stats["totals"]["drr_percent"], "7.6000");
    assert_eq!(stats["next_offset"], 1);
    assert_eq!(stats["rows"].as_array().unwrap().len(), 1);
    let sku = api
        .stats(&account, None, None, HistoryGroup::Sku, 100, 0)
        .await
        .unwrap();
    assert_eq!(sku["rows"][0]["spend_minor"], 1200);
    let version_count:i64=admin.query_one("SELECT count(*) FROM daily_reporting.wb_history_days WHERE account_id=$1 AND business_date=$2", &[&account,&date]).await.unwrap().get(0);
    assert_eq!(version_count, 4);
    assert!(
        requester
            .query("SELECT * FROM daily_reporting.wb_history_days", &[])
            .await
            .is_err()
    );
    assert!(
        requester
            .query(
                "SELECT daily_reporting.wb_history_claim(ARRAY[$1],'intruder')",
                &[&account]
            )
            .await
            .is_err()
    );
    assert!(
        collector
            .execute(
                "UPDATE daily_reporting.wb_history_days SET state='missing' WHERE account_id=$1",
                &[&account]
            )
            .await
            .is_err()
    );

    // A non-null response omitting one requested ID must remain incomplete.
    api.request(&account, "finance", Some(date), date)
        .await
        .unwrap();
    expedite(&admin, &account).await;
    inventory(
        &collector,
        &claim(&collector, &account).await,
        &account,
        &[7, 8],
        date,
    )
    .await;
    expedite(&admin, &account).await;
    publish(
        &collector,
        &claim(&collector, &account).await,
        &fullstats(&[7], date, 5, 100),
    )
    .await;
    let stats = api
        .stats(&account, None, None, HistoryGroup::Day, 100, 0)
        .await
        .unwrap();
    assert_eq!(stats["coverage"]["missing_campaign_days"], 1);
    assert_eq!(stats["coverage"]["complete_for_known_campaigns"], false);
    assert_eq!(
        api.status(&account).await.unwrap()["job"]["status"],
        "queued"
    );
    expedite(&admin, &account).await;
    let verification = claim(&collector, &account).await;
    assert_eq!(verification["campaign_ids"], json!([8]));
    publish(&collector, &verification, &Value::Null).await;
    let repaired = api
        .stats(&account, None, None, HistoryGroup::Campaign, 100, 0)
        .await
        .unwrap();
    assert_eq!(repaired["coverage"]["missing_campaign_days"], 0);
    assert_eq!(repaired["coverage"]["complete_for_known_campaigns"], true);
    assert_eq!(repaired["totals"]["spend_minor"], 500);
    assert_eq!(
        api.status(&account).await.unwrap()["job"]["status"],
        "succeeded"
    );
    let other = api
        .stats("history_other", None, None, HistoryGroup::Campaign, 100, 0)
        .await
        .unwrap();
    assert_eq!(other["rows"], json!([]));
    assert_eq!(other["coverage"]["complete_for_known_campaigns"], false);

    // A daily job and the shared persisted gate take precedence over backfill.
    api.request(&account, "finance", Some(date), date)
        .await
        .unwrap();
    expedite(&admin, &account).await;
    admin.execute("INSERT INTO daily_reporting.source_collection_departures VALUES($1,'wildberries','advertising',clock_timestamp()+interval '1 hour') ON CONFLICT(account_id,marketplace,source) DO UPDATE SET next_allowed_at=EXCLUDED.next_allowed_at",&[&account]).await.unwrap();
    assert!(claim(&collector, &account).await.is_null());
    expedite(&admin, &account).await;
    let c = claim(&collector, &account).await;
    collector.execute("SELECT daily_reporting.wb_history_defer($1,$2,'history-test','rate_limited',3600,false)",&[&c["job_id"].as_i64().unwrap(),&c["generation"].as_i64().unwrap()]).await.unwrap();
    assert!(claim(&collector, &account).await.is_null());
    assert_eq!(
        api.status(&account).await.unwrap()["job"]["error_class"],
        "rate_limited"
    );

    // Real count responses contain changeTime, not the creation date. Resolve
    // unknown creation through a durable details task before planning all-time.
    let details_account = format!("history_details_{}", std::process::id());
    api.request(&details_account, "finance", None, date)
        .await
        .unwrap();
    let initial = claim(&collector, &details_account).await;
    let raw = json!({"all":1,"adverts":[{"status":7,"count":1,"advert_list":[{"advertId":17,"changeTime":"2026-10-01T12:00:00+03:00"}]}]});
    let normalized = normalize_inventory(&raw).unwrap();
    collector
        .execute(
            "SELECT daily_reporting.wb_history_inventory($1,$2,'history-test',$3::text::jsonb)",
            &[
                &initial["job_id"].as_i64().unwrap(),
                &initial["generation"].as_i64().unwrap(),
                &normalized.to_string(),
            ],
        )
        .await
        .unwrap();
    expedite(&admin, &details_account).await;
    let details = claim(&collector, &details_account).await;
    assert_eq!(details["kind"], "details");
    let normalized=normalize_details(&json!({"adverts":[{"id":17,"timestamps":{"created":"2023-10-09T12:00:00+03:00","started":"2026-10-01T12:00:00+03:00"}}]}),&[17]).unwrap();
    collector
        .execute(
            "SELECT daily_reporting.wb_history_details($1,$2,'history-test',$3,$4::text::jsonb)",
            &[
                &details["job_id"].as_i64().unwrap(),
                &details["generation"].as_i64().unwrap(),
                &details["task_id"].as_i64().unwrap(),
                &normalized.to_string(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        api.status(&details_account).await.unwrap()["job"]["date_from"],
        "2023-10-09"
    );
    assert_eq!(
        api.status(&details_account).await.unwrap()["job"]["total_requests"],
        2
    );
    expedite(&admin, &details_account).await;
    publish(
        &collector,
        &claim(&collector, &details_account).await,
        &Value::Null,
    )
    .await;
    let no_data = api
        .stats(&details_account, None, None, HistoryGroup::Campaign, 100, 0)
        .await
        .unwrap();
    assert_eq!(no_data["rows"][0]["spend_minor"], 0);
    assert!(no_data["totals"]["drr_percent"].is_null());

    api.request(&details_account, "finance", Some(date), date)
        .await
        .unwrap();
    expedite(&admin, &details_account).await;
    admin.execute("SELECT daily_reporting.enqueue_source_collection($1,'wildberries','advertising',clock_timestamp(),clock_timestamp()-interval '1 day',clock_timestamp())",&[&details_account]).await.unwrap();
    assert!(claim(&collector, &details_account).await.is_null());
    admin
        .execute(
            "UPDATE daily_reporting.source_collection_jobs SET status='failed' WHERE account_id=$1",
            &[&details_account],
        )
        .await
        .unwrap();
    // A disappeared ID remains in the previous manifest and is still planned.
    inventory(
        &collector,
        &claim(&collector, &details_account).await,
        &details_account,
        &[],
        date,
    )
    .await;
    expedite(&admin, &details_account).await;
    let retained = claim(&collector, &details_account).await;
    assert_eq!(retained["campaign_ids"], json!([17]));
    publish(&collector, &retained, &Value::Null).await;

    let unavailable = api
        .stats(
            "history_unknown",
            Some(date),
            Some(date),
            HistoryGroup::Campaign,
            100,
            0,
        )
        .await
        .unwrap();
    assert_eq!(unavailable["state"], "unavailable");
    assert!(unavailable["totals"].is_null());
    assert_eq!(
        unavailable["coverage"]["complete_for_known_campaigns"],
        false
    );
}
