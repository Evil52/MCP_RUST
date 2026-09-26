//! A bounded upgrade recovery; never a general reset of failed collection.
use tokio_postgres::{Client, NoTls};

async fn client() -> Option<Client> {
    let url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL").ok()?;
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    Some(client)
}

async fn job(client: &Client, account: &str, market: &str, deadline_seconds: i32) -> i64 {
    client.query_one("INSERT INTO daily_reporting.source_collection_jobs(account_id,marketplace,source,cutoff_at,period_start,period_end,deadline_at,status,generation,page_restarts,completed_pages,error_class,finished_at,owner_id) VALUES($1,$2,'sales',statement_timestamp()-interval '3 minutes',statement_timestamp()-interval '1 day',statement_timestamp()-interval '3 minutes',clock_timestamp()+make_interval(secs=>$3::int),'failed',5,2,3,'sales_page_overlap_exhausted',clock_timestamp(),'closing-old-owner') RETURNING id",&[&account,&market,&deadline_seconds]).await.unwrap().get(0)
}

async fn initial(client: &Client, id: i64) -> bool {
    client.query_one("SELECT daily_reporting.resume_failed_sales(account_id,marketplace,id,generation,cutoff_at,'test/closing-initial-recovery') FROM daily_reporting.source_collection_jobs WHERE id=$1",&[&id]).await.unwrap().get(0)
}

async fn retry_failed(client: &Client, id: i64) {
    client.execute("UPDATE daily_reporting.source_collection_jobs SET status='failed',generation=generation+3,page_restarts=2,completed_pages=4,finished_at=clock_timestamp() WHERE id=$1",&[&id]).await.unwrap();
}

async fn closing(client: &Client, id: i64, account: &str, generation: i64) -> bool {
    client.query_one("SELECT daily_reporting.resume_failed_wb_sales_closing($2,id,$3,cutoff_at,'test/closing-upgrade-recovery') FROM daily_reporting.source_collection_jobs WHERE id=$1",&[&id,&account,&generation]).await.unwrap().get(0)
}

#[tokio::test]
async fn closing_recovery_is_once_only_wb_scoped_and_keeps_original_deadline_and_quota() {
    let Some(client) = client().await else { return };
    let account = "closing_upgrade_wb";
    let id = job(&client, account, "wildberries", 82_800).await;
    assert!(!closing(&client, id, account, 5).await);
    assert!(initial(&client, id).await);
    assert!(!closing(&client, id, account, 5).await);
    retry_failed(&client, id).await;
    client.execute("INSERT INTO daily_reporting.source_collection_departures VALUES($1,'wildberries','analytics',clock_timestamp()+interval '5 minutes')",&[&account]).await.unwrap();
    assert!(!closing(&client, id, "wrong_closing_account", 8).await);
    assert!(!closing(&client, id, account, 7).await);
    assert!(closing(&client, id, account, 8).await);
    let row=client.query_one("SELECT j.status,j.generation,j.page_restarts,j.owner_id IS NULL AND j.lease_until IS NULL AND j.finished_at IS NULL AND j.first_observed_at IS NULL AND j.last_observed_at IS NULL,j.deadline_at=(initial.previous_job->>'deadline_at')::timestamptz,j.next_attempt_at>=d.next_allowed_at,r.previous_job->>'status',r.failed_generation,j.completed_pages,j.cache_bytes FROM daily_reporting.source_collection_jobs j JOIN daily_reporting.sales_closing_resumes r ON r.job_id=j.id JOIN daily_reporting.sales_collection_resumes initial ON initial.job_id=j.id JOIN daily_reporting.source_collection_departures d USING(account_id,marketplace) WHERE j.id=$1",&[&id]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "ready");
    assert_eq!(row.get::<_, i64>(1), 8);
    assert_eq!(row.get::<_, i32>(2), 0);
    for index in 3..=5 {
        assert!(row.get::<_, bool>(index));
    }
    assert_eq!(row.get::<_, String>(6), "failed");
    assert_eq!(row.get::<_, i64>(7), 8);
    assert_eq!(row.get::<_, i32>(8), 0);
    assert_eq!(row.get::<_, i64>(9), 0);
    assert!(client.execute("UPDATE daily_reporting.sales_closing_resumes SET reason='forged-reason' WHERE job_id=$1",&[&id]).await.is_err());
    assert!(
        client
            .execute(
                "DELETE FROM daily_reporting.sales_closing_resumes WHERE job_id=$1",
                &[&id]
            )
            .await
            .is_err()
    );
    retry_failed(&client, id).await;
    assert!(!closing(&client, id, account, 11).await);
    assert!(!initial(&client, id).await);
    let ozon = job(&client, "closing_upgrade_ozon", "ozon", 82_800).await;
    assert!(initial(&client, ozon).await);
    retry_failed(&client, ozon).await;
    assert!(!closing(&client, ozon, "closing_upgrade_ozon", 8).await);
    let forbidden:bool=client.query_one("SELECT has_function_privilege('report_collector','daily_reporting.resume_failed_wb_sales_closing(text,bigint,bigint,timestamptz,text)','EXECUTE') OR has_function_privilege('position_reader','daily_reporting.resume_failed_wb_sales_closing(text,bigint,bigint,timestamptz,text)','EXECUTE')",&[]).await.unwrap().get(0);
    assert!(!forbidden);
}

#[tokio::test]
async fn closing_recovery_refuses_expired_original_deadline() {
    let Some(client) = client().await else { return };
    let account = "closing_upgrade_expired";
    let id = job(&client, account, "wildberries", 5).await;
    assert!(initial(&client, id).await);
    retry_failed(&client, id).await;
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    assert!(!closing(&client, id, account, 8).await);
}
