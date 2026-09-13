//! Disposable PostgreSQL only: no WB credentials, HTTP or production mutations.
use std::str::FromStr;
use tokio_postgres::{Config, NoTls};

const CALL: &str = "SELECT wb_automation.recover_oduvanchik_unconfirmed_bid(repeat('2',64),$1,repeat('3',64),repeat('4',64),$2::text::jsonb,'test/explicit-keep-current-bids')::text";
const BIDS: &str =
    r#"{"38943938":904,"41774347":922,"44081434":922,"44081446":921,"99236811":921}"#;

#[tokio::test]
async fn audited_recovery_preserves_unknown_result_and_never_replays_write() {
    let Ok(url) = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL") else {
        return;
    };
    let (admin, connection) = Config::from_str(&url)
        .unwrap()
        .connect(NoTls)
        .await
        .unwrap();
    let task = tokio::spawn(async move {
        connection.await.unwrap();
    });
    admin.batch_execute(r#"
      INSERT INTO wb_automation.cycles(cycle_id,account_id,advert_id,policy_digest,observed_at,business_date,state_revision,snapshot_json,decision_json)
      VALUES(repeat('1',64),'ofk_region_wb',39807762,repeat('3',64),clock_timestamp(),(clock_timestamp() AT TIME ZONE 'Europe/Moscow')::date,1,'{}','{}');
      INSERT INTO wb_automation.action_attempts(idempotency_key,account_id,advert_id,cycle_id,policy_digest,request_digest,action_kind,request_json,status)
      VALUES(repeat('2',64),'ofk_region_wb',39807762,repeat('1',64),repeat('3',64),repeat('5',64),'change_bids','{"kind":"change_bids","changes":[{"nm_id":38943938,"from_bid_kopecks":904,"to_bid_kopecks":994}]}','reserved');
      UPDATE wb_automation.action_attempts SET status='write_started' WHERE idempotency_key=repeat('2',64);
      UPDATE wb_automation.action_attempts SET status='reconciliation_required',last_error_class='write_result_ambiguous' WHERE idempotency_key=repeat('2',64);
      INSERT INTO wb_automation.execution_state(account_id,advert_id,schema_version,policy_digest,business_date,actions_today,last_action_at,pending_idempotency_key,incident_class,revision)
      SELECT account_id,advert_id,1,policy_digest,(clock_timestamp() AT TIME ZONE 'Europe/Moscow')::date,1,reserved_at,idempotency_key,'write_result_ambiguous',1
      FROM wb_automation.action_attempts WHERE idempotency_key=repeat('2',64);
    "#).await.unwrap();
    assert!(admin.batch_execute("UPDATE wb_automation.execution_state SET incident_class=NULL,revision=2 WHERE advert_id=39807762").await.is_err());
    assert!(admin.batch_execute("UPDATE wb_automation.action_attempts SET status='cancelled',last_error_class='operator_closed_unconfirmed',readback_cycle_id=repeat('1',64) WHERE advert_id=39807762").await.is_err());
    // The procedure refuses in-flight requests. Wait out its bounded grace
    // instead of disabling triggers or forging historical timestamps.
    tokio::time::sleep(std::time::Duration::from_secs(31)).await;
    admin.execute(r"
      INSERT INTO wb_automation.cycles(cycle_id,account_id,advert_id,policy_digest,observed_at,business_date,state_revision,snapshot_json,decision_json)
      VALUES(repeat('4',64),'ofk_region_wb',39807762,repeat('3',64),clock_timestamp(),(clock_timestamp() AT TIME ZONE 'Europe/Moscow')::date,1,
        jsonb_build_object('observation',jsonb_build_object('campaign_status',9,'budget_remaining_minor',99000,'daily_spend_complete',true,'daily_spend_minor',921,'paused_by_automation',false,
        'skus',(SELECT jsonb_agg(jsonb_build_object('nm_id',key::bigint,'current_bid_kopecks',value::bigint)) FROM jsonb_each_text($1::text::jsonb))))::text,'{}')
    ",&[&BIDS]).await.unwrap();
    assert!(admin.query_one(CALL, &[&2_i64, &BIDS]).await.is_err());
    assert!(admin.query_one(CALL, &[&1_i64, &"{}"]).await.is_err());
    let writer_url = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL").unwrap();
    let (writer, conn) = Config::from_str(&writer_url)
        .unwrap()
        .connect(NoTls)
        .await
        .unwrap();
    let writer_task = tokio::spawn(async move {
        conn.await.unwrap();
    });
    assert!(writer.query_one(CALL, &[&1_i64, &BIDS]).await.is_err());
    let receipt = admin.query_one(CALL, &[&1_i64, &BIDS]).await.unwrap();
    let receipt: serde_json::Value = serde_json::from_str(&receipt.get::<_, String>(0)).unwrap();
    assert_eq!(receipt["marketplace_write_sent"], false);
    assert_eq!(
        receipt["preserved_bids"],
        serde_json::from_str::<serde_json::Value>(BIDS).unwrap()
    );
    let row=admin.query_one("SELECT status,write_started_at IS NOT NULL,readback_cycle_id,request_json FROM wb_automation.action_attempts WHERE idempotency_key=repeat('2',64)",&[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "cancelled");
    assert!(row.get::<_, bool>(1));
    assert_eq!(row.get::<_, String>(2), "4".repeat(64));
    assert!(row.get::<_, String>(3).contains("994"));
    let state=admin.query_one("SELECT pending_idempotency_key,incident_class,revision,actions_today FROM wb_automation.execution_state WHERE advert_id=39807762",&[]).await.unwrap();
    assert_eq!(state.get::<_, Option<String>>(0), None);
    assert_eq!(state.get::<_, Option<String>>(1), None);
    assert_eq!(state.get::<_, i64>(2), 2);
    assert_eq!(state.get::<_, i32>(3), 1);
    assert!(admin.query_one(CALL, &[&1_i64, &BIDS]).await.is_err());
    assert!(admin.batch_execute("UPDATE wb_automation.action_attempts SET status='write_started' WHERE idempotency_key=repeat('2',64)").await.is_err());
    assert!(
        writer
            .batch_execute("DELETE FROM wb_automation.audit_events")
            .await
            .is_err()
    );
    drop(writer);
    writer_task.await.unwrap();
    drop(admin);
    task.await.unwrap();
}
