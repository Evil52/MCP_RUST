use tokio_postgres::{Client, NoTls};
async fn client() -> Option<Client> {
    let url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL").ok()?;
    let (c, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    Some(c)
}
async fn recover(c: &Client, id: i64, account: &str, generation: i64) -> bool {
    c.query_one("SELECT daily_reporting.resume_wb_advertising_reconciliation($2,id,$3,cutoff_at,'test/reconcile-upgrade') FROM daily_reporting.source_collection_jobs WHERE id=$1",&[&id,&account,&generation]).await.unwrap().get(0)
}
#[tokio::test]
async fn recovery_preserves_deadline_quota_audit_and_cannot_be_repeated() {
    let Some(c) = client().await else { return };
    let account = "wb_ads_reconciliation_recovery";
    let id:i64=c.query_one("INSERT INTO daily_reporting.source_collection_jobs(account_id,marketplace,source,cutoff_at,period_start,period_end,deadline_at,status,generation,consecutive_failures,error_class,finished_at) VALUES($1,'wildberries','advertising',clock_timestamp()-interval '1 hour',clock_timestamp()-interval '3 hours',clock_timestamp()-interval '1 hour',clock_timestamp()+interval '1 hour','failed',13,8,'promotion_counts_inconsistent',clock_timestamp()) RETURNING id",&[&account]).await.unwrap().get(0);
    c.execute("INSERT INTO daily_reporting.source_collection_departures VALUES($1,'wildberries','advertising',clock_timestamp()+interval '5 minutes')",&[&account]).await.unwrap();
    assert!(!recover(&c, id, "other_account", 13).await);
    assert!(!recover(&c, id, account, 12).await);
    c.execute("INSERT INTO daily_reporting.source_collection_pages VALUES($1,repeat('a',64),'{}',clock_timestamp())",&[&id]).await.unwrap();
    assert!(!recover(&c, id, account, 13).await);
    c.execute(
        "DELETE FROM daily_reporting.source_collection_pages WHERE job_id=$1",
        &[&id],
    )
    .await
    .unwrap();
    assert!(recover(&c, id, account, 13).await);
    let r=c.query_one("SELECT j.status,j.deadline_at=(r.previous_job->>'deadline_at')::timestamptz,j.next_attempt_at>=d.next_allowed_at,j.consecutive_failures,r.previous_job->>'error_class' FROM daily_reporting.source_collection_jobs j JOIN daily_reporting.advertising_reconciliation_resumes r ON r.job_id=j.id JOIN daily_reporting.source_collection_departures d USING(account_id,marketplace) WHERE j.id=$1",&[&id]).await.unwrap();
    assert_eq!(r.get::<_, String>(0), "ready");
    assert!(r.get::<_, bool>(1));
    assert!(r.get::<_, bool>(2));
    assert_eq!(r.get::<_, i32>(3), 0);
    assert_eq!(r.get::<_, String>(4), "promotion_counts_inconsistent");
    c.execute("UPDATE daily_reporting.source_collection_jobs SET status='failed',consecutive_failures=8,generation=14 WHERE id=$1",&[&id]).await.unwrap();
    assert!(!recover(&c, id, account, 14).await);
    assert!(
        c.execute(
            "DELETE FROM daily_reporting.advertising_reconciliation_resumes WHERE job_id=$1",
            &[&id]
        )
        .await
        .is_err()
    );
    let allowed:bool=c.query_one("SELECT has_function_privilege('report_collector','daily_reporting.resume_wb_advertising_reconciliation(text,bigint,bigint,timestamptz,text)','EXECUTE') OR has_table_privilege('position_reader','daily_reporting.advertising_reconciliation_resumes','UPDATE')",&[]).await.unwrap().get(0);
    assert!(!allowed);
}
