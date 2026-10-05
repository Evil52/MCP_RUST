//! Isolated sibling PostgreSQL: no WB credentials or marketplace writes.
use chrono::{Duration, Utc};
use serde_json::{Value, json};
use tokio_postgres::{Client, Config, NoTls};

const POLICY: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const CALL: &str =
    "SELECT wb_automation.recover_nexus_prior_day_cap($1,$2,$3,$4::text::jsonb,$5)::text";
const BIDS: &str =
    r#"{"190904855":700,"207418966":1050,"218972074":602,"455101276":102,"529996417":102}"#;

async fn connect(config: &Config) -> Client {
    let (client, connection) = config.connect(NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    client
}

fn observation() -> Value {
    let bids: Value = serde_json::from_str(BIDS).unwrap();
    json!({"campaign_status":9,"budget_remaining_minor":25700,
        "daily_spend_complete":true,"daily_spend_minor":11664,"paused_by_automation":true,
        "skus":bids.as_object().unwrap().iter().map(|(id,bid)|
            json!({"nm_id":id.parse::<u64>().unwrap(),"current_bid_kopecks":bid})).collect::<Vec<_>>()})
}

async fn cycle(client: &Client, index: u32, observed: &Value, age_seconds: i64) -> String {
    let key = format!("{index:064x}");
    let at = Utc::now() - Duration::seconds(age_seconds);
    client.execute(
        "INSERT INTO wb_automation.cycles(cycle_id,account_id,advert_id,policy_digest,observed_at,business_date,state_revision,snapshot_json,decision_json) VALUES($1,'ofk_region_wb',40141836,$2,$3,($3 AT TIME ZONE 'Europe/Moscow')::date,1,$4,'{}')",
        &[&key,&POLICY,&at,&json!({"observation":observed}).to_string()],
    ).await.unwrap();
    key
}

async fn recover(
    client: &Client,
    revision: i64,
    policy: &str,
    cycle: &str,
    bids: &str,
    auth: &str,
) -> Result<Value, tokio_postgres::Error> {
    client
        .query_one(CALL, &[&revision, &policy, &cycle, &bids, &auth])
        .await
        .map(|r| serde_json::from_str(&r.get::<_, String>(0)).unwrap())
}

#[tokio::test]
async fn stale_cap_recovery_requires_safe_latest_readback_and_preserves_policy_and_history() {
    let (Ok(admin_url), Ok(writer_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("WB_AUTOMATION_TEST_DATABASE_URL"),
    ) else {
        return;
    };
    let mut admin_config = admin_url.parse::<Config>().unwrap();
    let mut writer_config = writer_url.parse::<Config>().unwrap();
    let coordinator = connect(&admin_config).await;
    let database = format!(
        "prior_cap_{}_{}",
        std::process::id(),
        Utc::now().timestamp_micros()
    );
    coordinator
        .batch_execute(&format!("CREATE DATABASE {database}"))
        .await
        .unwrap();
    admin_config.dbname(&database);
    writer_config.dbname(&database);
    let admin = connect(&admin_config).await;
    for sql in [
        include_str!("../position-monitor/initdb/021_wb_automation_state.sql"),
        include_str!("../position-monitor/initdb/031_wb_audited_incident_recovery.sql"),
        include_str!("../position-monitor/initdb/049_wb_nexus_prior_day_cap_recovery.sql"),
    ] {
        let sql = sql
            .lines()
            .filter(|line| !line.starts_with('\\'))
            .collect::<Vec<_>>()
            .join("\n");
        admin.batch_execute(&sql).await.unwrap();
    }
    admin.execute(
        "INSERT INTO wb_automation.execution_state(account_id,advert_id,schema_version,policy_digest,business_date,actions_today,last_action_at,paused_for_daily_cap_on,incident_class,revision) VALUES('ofk_region_wb',40141836,1,$1,(clock_timestamp() AT TIME ZONE 'Europe/Moscow')::date-2,7,clock_timestamp()-interval '2 days',(clock_timestamp() AT TIME ZONE 'Europe/Moscow')::date-2,'daily_spend_cap_breached',1)", &[&POLICY]
    ).await.unwrap();
    assert!(admin.batch_execute("UPDATE wb_automation.execution_state SET incident_class=NULL,paused_for_daily_cap_on=NULL,revision=2 WHERE advert_id=40141836").await.is_err());
    let before: String = admin
        .query_one(
            "SELECT row_to_json(s)::text FROM wb_automation.execution_state s",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let writer = connect(&writer_config).await;
    let key = cycle(&admin, 1, &observation(), 0).await;
    assert!(
        recover(&writer, 1, POLICY, &key, BIDS, "test/repair-old-cap")
            .await
            .is_err()
    );
    for (revision, policy, bids, auth) in [
        (2, POLICY, BIDS, "test/repair-old-cap"),
        (1, "wrong", BIDS, "test/repair-old-cap"),
        (1, POLICY, "{}", "test/repair-old-cap"),
        (1, POLICY, BIDS, ""),
    ] {
        assert!(
            recover(&admin, revision, policy, &key, bids, auth)
                .await
                .is_err()
        );
    }
    for (i, (field, value)) in [
        ("campaign_status", json!(11)),
        ("budget_remaining_minor", json!(0)),
        ("daily_spend_complete", json!(false)),
        ("daily_spend_minor", json!(45000)),
        ("paused_by_automation", json!(false)),
        ("daily_spend_minor", json!(-1)),
    ]
    .into_iter()
    .enumerate()
    {
        let mut invalid = observation();
        invalid[field] = value;
        let key = cycle(&admin, u32::try_from(i + 10).unwrap(), &invalid, 0).await;
        assert!(
            recover(&admin, 1, POLICY, &key, BIDS, "test/repair-old-cap")
                .await
                .is_err()
        );
    }
    let stale = cycle(&admin, 30, &observation(), 91).await;
    assert!(
        recover(&admin, 1, POLICY, &stale, BIDS, "test/repair-old-cap")
            .await
            .is_err()
    );
    let older = cycle(&admin, 31, &observation(), 0).await;
    let latest = cycle(&admin, 32, &observation(), 0).await;
    assert!(
        recover(&admin, 1, POLICY, &older, BIDS, "test/repair-old-cap")
            .await
            .is_err()
    );
    admin.batch_execute("BEGIN; SAVEPOINT current_day; UPDATE wb_automation.execution_state SET business_date=(clock_timestamp() AT TIME ZONE 'Europe/Moscow')::date,actions_today=0,revision=2").await.unwrap();
    assert!(
        recover(&admin, 2, POLICY, &latest, BIDS, "test/repair-old-cap")
            .await
            .is_err()
    );
    admin
        .batch_execute("ROLLBACK TO SAVEPOINT current_day; COMMIT")
        .await
        .unwrap();
    admin.execute("INSERT INTO wb_automation.action_attempts(idempotency_key,account_id,advert_id,cycle_id,policy_digest,request_digest,action_kind,request_json,status) VALUES(repeat('c',64),'ofk_region_wb',40141836,$1,$2,repeat('d',64),'change_bids','{}','reserved')", &[&latest,&POLICY]).await.unwrap();
    assert!(
        recover(&admin, 1, POLICY, &latest, BIDS, "test/repair-old-cap")
            .await
            .is_err()
    );
    admin.batch_execute("UPDATE wb_automation.action_attempts SET status='cancelled',last_error_class='test_cancelled'").await.unwrap();
    let receipt = recover(&admin, 1, POLICY, &latest, BIDS, "test/repair-old-cap")
        .await
        .unwrap();
    assert_eq!(receipt["marketplace_write_sent"], false);
    assert_eq!(receipt["policy_changed"], false);
    assert_eq!(
        receipt["preserved_bids"],
        serde_json::from_str::<Value>(BIDS).unwrap()
    );
    let after: String = admin
        .query_one(
            "SELECT row_to_json(s)::text FROM wb_automation.execution_state s",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let before: Value = serde_json::from_str(&before).unwrap();
    let after: Value = serde_json::from_str(&after).unwrap();
    for field in [
        "account_id",
        "advert_id",
        "policy_digest",
        "last_action_at",
        "created_at",
        "schema_version",
        "imported_legacy_digest",
        "pending_idempotency_key",
    ] {
        assert_eq!(before[field], after[field], "preserve {field}");
    }
    assert_eq!(after["incident_class"], Value::Null);
    assert_eq!(after["paused_for_daily_cap_on"], Value::Null);
    assert_eq!(after["actions_today"], 0);
    assert_eq!(after["revision"], 2);
    let audit:Value=serde_json::from_str(&admin.query_one("SELECT payload_json FROM wb_automation.audit_events WHERE event_type='operator_recovered_prior_day_cap'",&[]).await.unwrap().get::<_,String>(0)).unwrap();
    assert_eq!(audit["previous_state"]["actions_today"], 7);
    assert_eq!(
        audit["previous_state"]["incident_class"],
        "daily_spend_cap_breached"
    );
    assert!(
        admin
            .batch_execute("DELETE FROM wb_automation.audit_events")
            .await
            .is_err()
    );
    assert!(
        recover(&admin, 1, POLICY, &latest, BIDS, "test/repair-old-cap")
            .await
            .is_err()
    );
    drop(writer);
    drop(admin);
    coordinator
        .batch_execute(&format!("DROP DATABASE {database} WITH (FORCE)"))
        .await
        .unwrap();
}
