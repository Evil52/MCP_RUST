//! Operator recovery contracts on a disposable PostgreSQL database only.
use tokio_postgres::{Client, NoTls};

async fn admin() -> Option<Client> {
    let url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL").ok()?;
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    Some(client)
}

async fn resume(client: &Client, id: i64, account: &str, market: &str, generation: i64) -> bool {
    client.query_one(
        "SELECT daily_reporting.resume_failed_sales($2,$3,id,$4,cutoff_at,'test/operator-sales-recovery') FROM daily_reporting.source_collection_jobs WHERE id=$1",
        &[&id,&account,&market,&generation],
    ).await.unwrap().get(0)
}

#[tokio::test]
async fn sales_recovery_is_scoped_once_only_and_preserves_deadline_quota_and_audit() {
    let Some(client) = admin().await else { return };
    for market in ["ozon", "wildberries"] {
        let account = format!("recovery_contract_{market}");
        let id: i64 = client.query_one(
            "INSERT INTO daily_reporting.source_collection_jobs(account_id,marketplace,source,cutoff_at,period_start,period_end,deadline_at,status,generation,owner_id,page_restarts,completed_pages,error_class,first_observed_at,last_observed_at,finished_at) VALUES($1,$2,'sales',clock_timestamp()-interval '3 minutes',statement_timestamp()-interval '1 day',clock_timestamp()-interval '3 minutes',clock_timestamp()+interval '23 hours','failed',5,'old-owner',2,3,'sales_page_overlap_exhausted',clock_timestamp()-interval '2 minutes',clock_timestamp()-interval '1 minute',clock_timestamp()) RETURNING id",
            &[&account,&market],
        ).await.unwrap().get(0);
        client.execute(
            "INSERT INTO daily_reporting.source_collection_departures VALUES($1,$2,CASE WHEN $2='wildberries' THEN 'analytics' ELSE 'sales' END,clock_timestamp()+interval '5 minutes')",
            &[&account,&market],
        ).await.unwrap();
        assert!(!resume(&client, id, "wrong_account", market, 5).await);
        assert!(
            !resume(
                &client,
                id,
                &account,
                if market == "ozon" {
                    "wildberries"
                } else {
                    "ozon"
                },
                5
            )
            .await
        );
        assert!(!resume(&client, id, &account, market, 4).await);
        assert!(resume(&client, id, &account, market, 5).await);
        let row=client.query_one(
            "SELECT j.status,j.generation,j.page_restarts,j.first_observed_at IS NULL AND j.last_observed_at IS NULL AND j.finished_at IS NULL AND j.owner_id IS NULL AND j.lease_until IS NULL, j.deadline_at=(r.previous_job->>'deadline_at')::timestamptz, j.next_attempt_at>=d.next_allowed_at, r.previous_job->>'status',r.previous_job->>'page_restarts',j.completed_pages,j.cache_bytes FROM daily_reporting.source_collection_jobs j JOIN daily_reporting.sales_collection_resumes r ON r.job_id=j.id JOIN daily_reporting.source_collection_departures d USING(account_id,marketplace) WHERE j.id=$1",
            &[&id],
        ).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "ready");
        assert_eq!(row.get::<_, i64>(1), 5);
        assert_eq!(row.get::<_, i32>(2), 0);
        for index in 3..=5 {
            assert!(row.get::<_, bool>(index));
        }
        assert_eq!(row.get::<_, String>(6), "failed");
        assert_eq!(row.get::<_, String>(7), "2");
        assert_eq!(row.get::<_, i32>(8), 0);
        assert_eq!(row.get::<_, i64>(9), 0);
        assert!(
            !client
                .query_one(
                    "SELECT daily_reporting.restart_overlapping_sales($1,5,'old-owner')",
                    &[&id]
                )
                .await
                .unwrap()
                .get::<_, bool>(0)
        );
        client.execute("UPDATE daily_reporting.source_collection_jobs SET status='failed',page_restarts=2,generation=6 WHERE id=$1",&[&id]).await.unwrap();
        assert!(!resume(&client, id, &account, market, 6).await);
        let audit_count: i64 = client
            .query_one(
                "SELECT count(*) FROM daily_reporting.sales_collection_resumes WHERE job_id=$1",
                &[&id],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(audit_count, 1);
    }
    let allowed:bool=client.query_one("SELECT has_function_privilege('report_collector','daily_reporting.resume_failed_sales(text,text,bigint,bigint,timestamptz,text)','EXECUTE') OR has_function_privilege('position_reader','daily_reporting.resume_failed_sales(text,text,bigint,bigint,timestamptz,text)','EXECUTE')",&[]).await.unwrap().get(0);
    assert!(!allowed);
    let expired:i64=client.query_one("INSERT INTO daily_reporting.source_collection_jobs(account_id,marketplace,source,cutoff_at,period_start,period_end,deadline_at,status,generation,page_restarts,error_class) VALUES('expired_recovery','ozon','sales',statement_timestamp()-interval '2 days',statement_timestamp()-interval '3 days',statement_timestamp()-interval '2 days',statement_timestamp()-interval '1 day','failed',5,2,'sales_page_overlap_exhausted') RETURNING id",&[]).await.unwrap().get(0);
    assert!(!resume(&client, expired, "expired_recovery", "ozon", 5).await);
}

async fn cycle(
    client: &Client,
    key: &str,
    status: i32,
    budget: i64,
    reason: &str,
    age_seconds: i32,
) {
    client.execute(
        "INSERT INTO wb_automation.cycles(cycle_id,account_id,advert_id,policy_digest,observed_at,business_date,state_revision,snapshot_json,decision_json) VALUES($1,'terminal_recovery_contract',987654321,repeat('a',64),clock_timestamp()-make_interval(secs=>$5::int),current_date,1,jsonb_build_object('observation',jsonb_build_object('campaign_status',$2::int,'budget_remaining_minor',$3::bigint,'paused_by_automation',false))::text,jsonb_build_object('action',jsonb_build_object('hold',jsonb_build_object('reason',$4::text)))::text)",
        &[&key,&status,&budget,&reason,&age_seconds],
    ).await.unwrap();
}

async fn archive(client: &Client, key: &str, revision: i64) -> bool {
    client.query_one("SELECT wb_automation.archive_terminal_incident('terminal_recovery_contract',987654321,$1,repeat('a',64),$2,'test/operator-terminal-archive')",&[&revision,&key]).await.unwrap().get(0)
}

async fn open(client: &Client) -> bool {
    client.query_one("SELECT EXISTS(SELECT 1 FROM wb_automation.open_incidents WHERE account_id='terminal_recovery_contract')",&[]).await.unwrap().get(0)
}

#[tokio::test]
async fn terminal_archive_keeps_lock_and_realerts_when_campaign_or_authorization_changes() {
    let Some(client) = admin().await else { return };
    client.batch_execute("INSERT INTO wb_automation.execution_state(account_id,advert_id,schema_version,policy_digest,business_date,actions_today,incident_class,revision) VALUES('terminal_recovery_contract',987654321,1,repeat('a',64),current_date,0,'daily_spend_cap_breached',1)").await.unwrap();
    let old_cycle_id = "1".repeat(64);
    cycle(&client, &old_cycle_id, 7, 0, "authorization_expired", 120).await;
    assert!(!archive(&client, &old_cycle_id, 1).await);
    let active = "2".repeat(64);
    cycle(&client, &active, 9, 0, "authorization_expired", 0).await;
    assert!(!archive(&client, &active, 1).await);
    let funded = "3".repeat(64);
    cycle(&client, &funded, 7, 1, "authorization_expired", 0).await;
    assert!(!archive(&client, &funded, 1).await);
    let authorized = "4".repeat(64);
    cycle(&client, &authorized, 7, 0, "no_material_change", 0).await;
    assert!(!archive(&client, &authorized, 1).await);
    assert!(open(&client).await);
    let terminal = "5".repeat(64);
    cycle(&client, &terminal, 7, 0, "authorization_expired", 0).await;
    assert!(!archive(&client, &terminal, 2).await);
    assert!(archive(&client, &terminal, 1).await);
    assert!(!archive(&client, &terminal, 1).await);
    assert!(!open(&client).await);
    let state=client.query_one("SELECT incident_class,revision FROM wb_automation.execution_state WHERE account_id='terminal_recovery_contract'",&[]).await.unwrap();
    assert_eq!(state.get::<_, String>(0), "daily_spend_cap_breached");
    assert_eq!(state.get::<_, i64>(1), 1);
    let reactivated = "6".repeat(64);
    cycle(&client, &reactivated, 9, 100, "authorization_expired", 0).await;
    assert!(open(&client).await);
    let allowed:bool=client.query_one("SELECT has_function_privilege('wb_automation_writer','wb_automation.archive_terminal_incident(text,bigint,bigint,text,text,text)','EXECUTE') OR has_function_privilege('position_reader','wb_automation.archive_terminal_incident(text,bigint,bigint,text,text,text)','EXECUTE')",&[]).await.unwrap().get(0);
    assert!(!allowed);
}
