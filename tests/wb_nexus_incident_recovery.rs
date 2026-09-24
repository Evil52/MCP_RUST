//! Disposable PostgreSQL only: no WB credentials, HTTP or production mutations.
use std::str::FromStr;
use tokio_postgres::{Config, NoTls};

const CALL: &str = "SELECT wb_automation.recover_nexus_unconfirmed_bid('a826be916169c46c7d6ec3ef505214f17eaff1965b540d39db692fb45cd567fa',$1,'21fe8d4402af4847e30d0dd791fd59a26e60479e62751f54295e83a20578c51d','4f01a6b6cf614cb928b3372778fb12b498394bcffaf3924d4ca2219438bfe30e',$2::text::jsonb,'test/explicit-keep-current-bids')::text";
const BIDS: &str =
    r#"{"190904855":700,"207418966":1050,"218972074":602,"455101276":102,"529996417":102}"#;

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
    // The all-target coverage run shares this database with a launch unit test
    // that already reserves the actual Nexus campaign. The dedicated DB
    // contract runs this recovery scenario on a fresh database.
    let occupied: bool = admin
        .query_one("SELECT EXISTS(SELECT 1 FROM wb_automation.execution_state WHERE account_id='ofk_region_wb' AND advert_id=40141836)", &[])
        .await
        .unwrap()
        .get(0);
    if occupied {
        drop(admin);
        task.await.unwrap();
        return;
    }
    admin.batch_execute(r#"
      INSERT INTO wb_automation.cycles(cycle_id,account_id,advert_id,policy_digest,observed_at,business_date,state_revision,snapshot_json,decision_json)
      VALUES('b615c5e9c9c02ef37bf0bfbfbbc09b9ced61f95123f754e820a0e9abf4736a84','ofk_region_wb',40141836,'21fe8d4402af4847e30d0dd791fd59a26e60479e62751f54295e83a20578c51d',clock_timestamp(),(clock_timestamp() AT TIME ZONE 'Europe/Moscow')::date,1,'{}','{}');
      INSERT INTO wb_automation.action_attempts(idempotency_key,account_id,advert_id,cycle_id,policy_digest,request_digest,action_kind,request_json,status)
      VALUES('a826be916169c46c7d6ec3ef505214f17eaff1965b540d39db692fb45cd567fa','ofk_region_wb',40141836,'b615c5e9c9c02ef37bf0bfbfbbc09b9ced61f95123f754e820a0e9abf4736a84','21fe8d4402af4847e30d0dd791fd59a26e60479e62751f54295e83a20578c51d','742a3a4d91b6fbb1c9d2707d5a1c949d155cca71121842128c9f86a08ace01bd','change_bids','{"kind":"change_bids","changes":[{"nm_id":218972074,"from_bid_kopecks":602,"to_bid_kopecks":662}]}','reserved');
      UPDATE wb_automation.action_attempts SET status='write_started' WHERE idempotency_key='a826be916169c46c7d6ec3ef505214f17eaff1965b540d39db692fb45cd567fa';
      UPDATE wb_automation.action_attempts SET status='reconciliation_required',last_error_class='write_result_ambiguous' WHERE idempotency_key='a826be916169c46c7d6ec3ef505214f17eaff1965b540d39db692fb45cd567fa';
      INSERT INTO wb_automation.execution_state(account_id,advert_id,schema_version,policy_digest,business_date,actions_today,last_action_at,pending_idempotency_key,incident_class,revision)
      SELECT account_id,advert_id,1,policy_digest,(clock_timestamp() AT TIME ZONE 'Europe/Moscow')::date,1,reserved_at,idempotency_key,'write_result_ambiguous',1
      FROM wb_automation.action_attempts WHERE idempotency_key='a826be916169c46c7d6ec3ef505214f17eaff1965b540d39db692fb45cd567fa';
    "#).await.unwrap();
    assert!(admin.batch_execute("UPDATE wb_automation.execution_state SET incident_class=NULL,revision=2 WHERE advert_id=40141836").await.is_err());
    assert!(admin.batch_execute("UPDATE wb_automation.action_attempts SET status='cancelled',last_error_class='operator_closed_unconfirmed',readback_cycle_id='b615c5e9c9c02ef37bf0bfbfbbc09b9ced61f95123f754e820a0e9abf4736a84' WHERE advert_id=40141836").await.is_err());
    // The procedure refuses in-flight requests. Wait out its bounded grace
    // instead of disabling triggers or forging historical timestamps.
    tokio::time::sleep(std::time::Duration::from_secs(31)).await;
    admin.execute(r"
      INSERT INTO wb_automation.cycles(cycle_id,account_id,advert_id,policy_digest,observed_at,business_date,state_revision,snapshot_json,decision_json)
      VALUES('4f01a6b6cf614cb928b3372778fb12b498394bcffaf3924d4ca2219438bfe30e','ofk_region_wb',40141836,'21fe8d4402af4847e30d0dd791fd59a26e60479e62751f54295e83a20578c51d',clock_timestamp(),(clock_timestamp() AT TIME ZONE 'Europe/Moscow')::date,1,
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
    let row=admin.query_one("SELECT status,write_started_at IS NOT NULL,readback_cycle_id,request_json FROM wb_automation.action_attempts WHERE idempotency_key='a826be916169c46c7d6ec3ef505214f17eaff1965b540d39db692fb45cd567fa'",&[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "cancelled");
    assert!(row.get::<_, bool>(1));
    assert_eq!(
        row.get::<_, String>(2),
        "4f01a6b6cf614cb928b3372778fb12b498394bcffaf3924d4ca2219438bfe30e".to_owned()
    );
    assert!(row.get::<_, String>(3).contains("662"));
    let state=admin.query_one("SELECT pending_idempotency_key,incident_class,revision,actions_today FROM wb_automation.execution_state WHERE advert_id=40141836",&[]).await.unwrap();
    assert_eq!(state.get::<_, Option<String>>(0), None);
    assert_eq!(state.get::<_, Option<String>>(1), None);
    assert_eq!(state.get::<_, i64>(2), 2);
    assert_eq!(state.get::<_, i32>(3), 1);
    assert!(admin.query_one(CALL, &[&1_i64, &BIDS]).await.is_err());
    assert!(admin.batch_execute("UPDATE wb_automation.action_attempts SET status='write_started' WHERE idempotency_key='a826be916169c46c7d6ec3ef505214f17eaff1965b540d39db692fb45cd567fa'").await.is_err());
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
