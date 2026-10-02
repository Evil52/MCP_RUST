//! Disposable PostgreSQL only: no WB credentials or marketplace writes.
use std::{fmt::Write as _, str::FromStr};

use chrono::{Duration, Utc};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, Config, NoTls};

const KEY: &str = "87c850113c333f5d64a3e08ecddd26d05f9228093ca2b6faf64d4326c90028d5";
const POLICY: &str = "47f6bd4bf65a854635a1032564012a8bea6166a45397e2753a918627143cfdbc";
const BIDS: &str =
    r#"{"786111901":723,"851035556":635,"868473965":635,"881697128":952,"1216941663":635}"#;
const CALL: &str =
    "SELECT wb_automation.recover_erebor_unconfirmed_bid($1,$2,$3,$4,$5::text::jsonb,$6)::text";

fn cycle_id(index: usize) -> String {
    let mut id = String::with_capacity(64);
    for byte in Sha256::digest(format!("wb_erebor_incident_recovery/cycle/{index}")) {
        write!(id, "{byte:02x}").unwrap();
    }
    id
}

async fn recover(
    client: &Client,
    cycle: &str,
    revision: i64,
    bids: &str,
    authorization: &str,
) -> Result<Value, tokio_postgres::Error> {
    client
        .query_one(
            CALL,
            &[&KEY, &revision, &POLICY, &cycle, &bids, &authorization],
        )
        .await
        .map(|row| serde_json::from_str(&row.get::<_, String>(0)).unwrap())
}

fn observation() -> Value {
    let bids: Value = serde_json::from_str(BIDS).unwrap();
    json!({
        "campaign_status":9, "budget_remaining_minor":82300,
        "daily_spend_complete":true, "daily_spend_minor":10210,
        "paused_by_automation":false,
        "skus": bids.as_object().unwrap().iter().map(|(id, bid)| {
            json!({"nm_id":id.parse::<u64>().unwrap(),"current_bid_kopecks":bid})
        }).collect::<Vec<_>>()
    })
}

async fn cycle(client: &Client, id: &str, observation: &Value, age_seconds: i64) {
    let snapshot = json!({"observation":observation}).to_string();
    let observed_at = Utc::now() - Duration::seconds(age_seconds);
    client.execute(
        "INSERT INTO wb_automation.cycles(cycle_id,account_id,advert_id,policy_digest,observed_at,business_date,state_revision,snapshot_json,decision_json) VALUES($1,'ip_usovik_wb',40508432,$2,$3,($3 AT TIME ZONE 'Europe/Moscow')::date,1,$4,'{}')",
        &[&id, &POLICY, &observed_at, &snapshot],
    ).await.unwrap();
}

#[tokio::test]
async fn recovery_requires_latest_safe_readback_and_preserves_unknown_write() {
    let Ok(url) = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL") else {
        return;
    };
    let (admin, connection) = Config::from_str(&url)
        .unwrap()
        .connect(NoTls)
        .await
        .unwrap();
    let admin_task = tokio::spawn(async move {
        connection.await.unwrap();
    });
    admin.batch_execute(r#"
      INSERT INTO wb_automation.cycles(cycle_id,account_id,advert_id,policy_digest,observed_at,business_date,state_revision,snapshot_json,decision_json)
      VALUES('b2dc910ede0baae73adca510affdbe284f3e28ab0ebfe732da19cc4bb9e1ef0c','ip_usovik_wb',40508432,'47f6bd4bf65a854635a1032564012a8bea6166a45397e2753a918627143cfdbc',clock_timestamp(),(clock_timestamp() AT TIME ZONE 'Europe/Moscow')::date,1,'{}','{}');
      INSERT INTO wb_automation.action_attempts(idempotency_key,account_id,advert_id,cycle_id,policy_digest,request_digest,action_kind,request_json,status)
      VALUES('87c850113c333f5d64a3e08ecddd26d05f9228093ca2b6faf64d4326c90028d5','ip_usovik_wb',40508432,'b2dc910ede0baae73adca510affdbe284f3e28ab0ebfe732da19cc4bb9e1ef0c','47f6bd4bf65a854635a1032564012a8bea6166a45397e2753a918627143cfdbc','28cfbca76dbe8e8b5e01934cd3fa1308e047674839c1935c3eb7b367dfe04d09','change_bids','{"kind":"change_bids","changes":[{"nm_id":868473965,"from_bid_kopecks":931,"to_bid_kopecks":1024}]}','reserved');
      UPDATE wb_automation.action_attempts SET status='write_started' WHERE idempotency_key='87c850113c333f5d64a3e08ecddd26d05f9228093ca2b6faf64d4326c90028d5';
      UPDATE wb_automation.action_attempts SET status='reconciliation_required',last_error_class='write_result_ambiguous' WHERE idempotency_key='87c850113c333f5d64a3e08ecddd26d05f9228093ca2b6faf64d4326c90028d5';
      INSERT INTO wb_automation.execution_state(account_id,advert_id,schema_version,policy_digest,business_date,actions_today,last_action_at,pending_idempotency_key,incident_class,revision)
      SELECT account_id,advert_id,1,policy_digest,(clock_timestamp() AT TIME ZONE 'Europe/Moscow')::date,1,reserved_at,idempotency_key,'write_result_ambiguous',1
      FROM wb_automation.action_attempts WHERE idempotency_key='87c850113c333f5d64a3e08ecddd26d05f9228093ca2b6faf64d4326c90028d5';
    "#).await.unwrap();
    let identity_before: String = admin.query_one(
        "SELECT jsonb_build_array(request_json::jsonb,reserved_at,write_started_at,request_digest,policy_digest)::text FROM wb_automation.action_attempts WHERE idempotency_key=$1", &[&KEY]
    ).await.unwrap().get(0);
    assert!(admin.batch_execute("UPDATE wb_automation.execution_state SET incident_class=NULL,revision=2 WHERE account_id='ip_usovik_wb' AND advert_id=40508432").await.is_err());
    let immediate = cycle_id(10);
    cycle(&admin, &immediate, &observation(), 0).await;
    assert!(
        recover(&admin, &immediate, 1, BIDS, "test/keep-current-bids")
            .await
            .is_err()
    );
    // Do not forge stored write timestamps or disable protective triggers.
    tokio::time::sleep(std::time::Duration::from_secs(31)).await;

    let mut invalid = Vec::new();
    for (field, value) in [
        ("campaign_status", json!(11)),
        ("budget_remaining_minor", json!(0)),
        ("daily_spend_complete", json!(false)),
        ("daily_spend_minor", json!(45000)),
        ("paused_by_automation", json!(true)),
    ] {
        let mut candidate = observation();
        candidate[field] = value;
        invalid.push(candidate);
    }
    let mut missing_sku = observation();
    missing_sku["skus"].as_array_mut().unwrap().pop();
    invalid.push(missing_sku);
    let mut foreign_sku = observation();
    foreign_sku["skus"][0]["nm_id"] = json!(999);
    invalid.push(foreign_sku);
    let mut zero_bid = observation();
    zero_bid["skus"][0]["current_bid_kopecks"] = json!(0);
    invalid.push(zero_bid);
    for bid in [json!(null), json!("635"), json!(6.35)] {
        let mut malformed_bid = observation();
        malformed_bid["skus"][0]["current_bid_kopecks"] = bid;
        invalid.push(malformed_bid);
    }
    let mut target_visible = observation();
    for sku in target_visible["skus"].as_array_mut().unwrap() {
        if sku["nm_id"] == 868_473_965 {
            sku["current_bid_kopecks"] = json!(1024);
        }
    }
    invalid.push(target_visible);
    for (index, value) in invalid.iter().enumerate() {
        let id = cycle_id(20 + index);
        cycle(&admin, &id, value, 0).await;
        let bids = value["skus"]
            .as_array()
            .unwrap()
            .iter()
            .map(|sku| {
                (
                    sku["nm_id"].as_u64().unwrap().to_string(),
                    sku["current_bid_kopecks"].clone(),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        assert!(
            recover(
                &admin,
                &id,
                1,
                &Value::Object(bids).to_string(),
                "test/keep-current-bids"
            )
            .await
            .is_err()
        );
    }
    let stale = cycle_id(40);
    cycle(&admin, &stale, &observation(), 120).await;
    assert!(
        recover(&admin, &stale, 1, BIDS, "test/keep-current-bids")
            .await
            .is_err()
    );
    let older = cycle_id(41);
    cycle(&admin, &older, &observation(), 0).await;
    let latest = cycle_id(42);
    cycle(&admin, &latest, &observation(), 0).await;
    assert!(
        recover(&admin, &older, 1, BIDS, "test/keep-current-bids")
            .await
            .is_err()
    );
    assert!(
        recover(&admin, &latest, 2, BIDS, "test/keep-current-bids")
            .await
            .is_err()
    );
    assert!(
        recover(&admin, &latest, 1, "{}", "test/keep-current-bids")
            .await
            .is_err()
    );
    assert!(recover(&admin, &latest, 1, BIDS, "bad").await.is_err());
    let writer_url = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL").unwrap();
    let (writer, connection) = Config::from_str(&writer_url)
        .unwrap()
        .connect(NoTls)
        .await
        .unwrap();
    let writer_task = tokio::spawn(async move {
        connection.await.unwrap();
    });
    assert!(
        recover(&writer, &latest, 1, BIDS, "test/keep-current-bids")
            .await
            .is_err()
    );
    writer
        .batch_execute("SELECT pg_advisory_lock(hashtextextended('wb/ip_usovik_wb/40508432',0))")
        .await
        .unwrap();
    assert!(
        recover(&admin, &latest, 1, BIDS, "test/keep-current-bids")
            .await
            .is_err()
    );
    writer
        .batch_execute("SELECT pg_advisory_unlock(hashtextextended('wb/ip_usovik_wb/40508432',0))")
        .await
        .unwrap();
    let receipt = recover(&admin, &latest, 1, BIDS, "test/keep-current-bids")
        .await
        .unwrap();
    assert_eq!(receipt["marketplace_write_sent"], false);
    assert_eq!(
        receipt["preserved_bids"],
        serde_json::from_str::<Value>(BIDS).unwrap()
    );
    let row = admin.query_one(
        "SELECT status,write_started_at IS NOT NULL,readback_cycle_id,jsonb_build_array(request_json::jsonb,reserved_at,write_started_at,request_digest,policy_digest)::text FROM wb_automation.action_attempts WHERE idempotency_key=$1", &[&KEY]
    ).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "cancelled");
    assert!(row.get::<_, bool>(1));
    assert_eq!(row.get::<_, String>(2), latest);
    assert_eq!(row.get::<_, String>(3), identity_before);
    let row = admin.query_one(
        "SELECT pending_idempotency_key,incident_class,revision,actions_today,policy_digest FROM wb_automation.execution_state WHERE account_id='ip_usovik_wb' AND advert_id=40508432", &[]
    ).await.unwrap();
    assert_eq!(row.get::<_, Option<String>>(0), None);
    assert_eq!(row.get::<_, Option<String>>(1), None);
    assert_eq!(row.get::<_, i64>(2), 2);
    assert_eq!(row.get::<_, i32>(3), 1);
    assert_eq!(row.get::<_, String>(4), POLICY);
    let audit: String = admin
        .query_one(
            "SELECT payload_json FROM wb_automation.audit_events WHERE event_key=$1",
            &[&KEY],
        )
        .await
        .unwrap()
        .get(0);
    let audit: Value = serde_json::from_str(&audit).unwrap();
    assert_eq!(audit["old_write_applied"], "unknown");
    assert_eq!(audit["retry_old_request"], false);
    assert_eq!(audit["marketplace_write_sent"], false);
    assert!(
        recover(&admin, &latest, 1, BIDS, "test/keep-current-bids")
            .await
            .is_err()
    );
    assert!(admin.batch_execute("UPDATE wb_automation.action_attempts SET status='write_started' WHERE idempotency_key='87c850113c333f5d64a3e08ecddd26d05f9228093ca2b6faf64d4326c90028d5'").await.is_err());
    assert!(
        writer
            .batch_execute("DELETE FROM wb_automation.audit_events")
            .await
            .is_err()
    );
    drop(writer);
    writer_task.await.unwrap();
    drop(admin);
    admin_task.await.unwrap();
}
