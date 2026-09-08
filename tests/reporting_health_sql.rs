use chrono::{DateTime, Utc};
use tokio_postgres::{Client, NoTls};

const HEALTH_SQL: &str = include_str!("../scripts/reporting-health.sql");
const SCOPE: &str = r#"[{"account_id":"ozon_one","marketplace":"ozon"},{"account_id":"wb_one","marketplace":"wildberries"}]"#;

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .unwrap()
        .with_timezone(&Utc)
}

async fn findings(client: &Client, sql: &str, now: &str) -> Vec<String> {
    client
        .query(sql, &[&SCOPE, &Some(timestamp(now))])
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect()
}

async fn complete_cutoff(client: &Client, cutoff: &str) {
    client
        .execute(
            "INSERT INTO source_snapshots \
             SELECT account_id, marketplace, source, $1, 'succeeded', true \
             FROM (VALUES ('ozon_one', 'ozon'), ('wb_one', 'wildberries')) \
                  AS accounts(account_id, marketplace) \
             CROSS JOIN unnest(ARRAY['sales','advertising','stocks','prices','finance']) AS source \
             WHERE marketplace = 'ozon' OR source <> 'finance'",
            &[&timestamp(cutoff)],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn reporting_health_checks_cutoff_boundaries_completeness_and_failed_work() {
    let Ok(url) = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL") else {
        return;
    };
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let driver = tokio::spawn(async move { connection.await.unwrap() });
    // Validate all real schema references before using session-private fixtures.
    client.prepare(HEALTH_SQL).await.unwrap();
    client
        .batch_execute(
            "CREATE TEMP TABLE source_snapshots (
                account_id text, marketplace text, source text, cutoff_at timestamptz,
                status text, pagination_complete boolean
             );
             CREATE TEMP TABLE ozon_sales_refresh_requests (
                id bigint, account_id text, marketplace text, status text,
                requested_at timestamptz, business_date date,
                lease_until timestamptz, error_class text
             );
             CREATE TEMP TABLE source_collection_jobs (
                account_id text,marketplace text,source text,cutoff_at timestamptz,
                status text,lease_until timestamptz,next_attempt_at timestamptz,error_class text
             );
             CREATE TEMP TABLE collection_claims (
                id bigint, account_id text, marketplace text, status text,
                cutoff_at timestamptz, lease_until timestamptz
             );",
        )
        .await
        .unwrap();
    let sql = HEALTH_SQL.replace("daily_reporting.", "pg_temp.");

    let missing = findings(&client, &sql, "2026-09-07T03:30:01Z").await;
    assert_eq!(missing.len(), 2);
    assert!(missing[0].contains("ozon_one|ozon|cutoff_incomplete|2026-09-07T03:00:00Z"));
    assert!(missing[0].contains("missing=advertising,finance,prices,sales,stocks"));
    assert!(missing[1].contains("missing=advertising,prices,sales,stocks"));

    complete_cutoff(&client, "2026-09-06T12:00:00Z").await;
    // The 30-minute completion interval is inclusive; do not alarm early.
    assert!(
        findings(&client, &sql, "2026-09-07T03:30:00Z")
            .await
            .is_empty()
    );
    // Yesterday's complete snapshot must not hide a missed morning cutoff.
    assert_eq!(
        findings(&client, &sql, "2026-09-07T03:30:01Z").await.len(),
        2
    );

    complete_cutoff(&client, "2026-09-07T03:00:00Z").await;
    assert!(
        findings(&client, &sql, "2026-09-07T03:30:01Z")
            .await
            .is_empty()
    );
    client
        .batch_execute(
            "UPDATE source_snapshots SET pagination_complete=false
             WHERE source='finance' AND cutoff_at='2026-09-07T03:00:00Z'",
        )
        .await
        .unwrap();
    let incomplete = findings(&client, &sql, "2026-09-07T04:20:00Z").await;
    assert_eq!(incomplete.len(), 1);
    assert!(incomplete[0].ends_with("missing=finance"));
    client
        .batch_execute(
            "UPDATE source_snapshots SET pagination_complete=true;
             UPDATE source_snapshots SET status='partial'
             WHERE source='sales' AND marketplace='wildberries'
               AND cutoff_at='2026-09-07T03:00:00Z';",
        )
        .await
        .unwrap();
    let partial = findings(&client, &sql, "2026-09-07T04:20:00Z").await;
    assert_eq!(partial.len(), 1);
    assert!(partial[0].contains("wb_one|wildberries|cutoff_incomplete"));
    assert!(partial[0].ends_with("missing=sales"));

    client
        .batch_execute(
            "UPDATE source_snapshots SET status='succeeded';
             INSERT INTO ozon_sales_refresh_requests VALUES
              (1,'ozon_one','ozon','failed','2026-09-07T04:00:00Z','2026-09-07',NULL,'seller_collection_failed'),
              (2,'outside_policy','ozon','failed','2026-09-07T04:00:00Z','2026-09-07',NULL,'seller_collection_failed');",
        )
        .await
        .unwrap();
    assert_eq!(
        findings(&client, &sql, "2026-09-07T04:20:00Z").await,
        ["reporting|ozon_one|ozon|refresh_failed|1|seller_collection_failed"]
    );
    client
        .batch_execute(
            "INSERT INTO ozon_sales_refresh_requests VALUES
              (3,'ozon_one','ozon','succeeded','2026-09-07T04:10:00Z','2026-09-07',NULL,NULL);",
        )
        .await
        .unwrap();
    assert!(
        findings(&client, &sql, "2026-09-07T04:20:00Z")
            .await
            .is_empty()
    );

    client
        .batch_execute(
            "INSERT INTO collection_claims VALUES
              (1,'wb_one','wildberries','active','2026-09-07T04:15:00Z','2026-09-07T04:18:00Z'),
              (2,'wb_one','wildberries','active','2026-09-06T03:00:00Z','2026-09-06T03:15:00Z');
             INSERT INTO ozon_sales_refresh_requests VALUES
              (4,'ozon_one','ozon','running','2026-09-07T04:15:00Z','2026-09-07','2026-09-07T04:18:00Z',NULL);",
        )
        .await
        .unwrap();
    assert_eq!(
        findings(&client, &sql, "2026-09-07T04:20:00Z").await,
        [
            "reporting|ozon_one|ozon|refresh_expired|4",
            "reporting|wb_one|wildberries|collection_lease_expired|1",
        ]
    );
    // A queued refresh crossing the EKB business-day boundary also needs attention.
    client
        .batch_execute(
            "DELETE FROM collection_claims;
             UPDATE ozon_sales_refresh_requests SET status='queued', business_date='2026-09-06'
             WHERE id=4;",
        )
        .await
        .unwrap();
    assert_eq!(
        findings(&client, &sql, "2026-09-07T04:20:00Z").await,
        ["reporting|ozon_one|ozon|refresh_expired|4"]
    );
    // Evening uses the same inclusive grace window and rolls over independently.
    assert_eq!(
        findings(&client, &sql, "2026-09-07T12:30:00Z").await.len(),
        1
    );
    assert_eq!(
        findings(&client, &sql, "2026-09-07T12:30:01Z").await.len(),
        3
    );
    client.batch_execute("DELETE FROM ozon_sales_refresh_requests;
        INSERT INTO source_collection_jobs VALUES
        ('ozon_one','ozon','advertising','2026-09-07T03:00:00Z','failed',NULL,NULL,'invalid_response'),
        ('wb_one','wildberries','stocks','2026-09-07T03:00:00Z','ready',NULL,'2026-09-07T04:00:00Z',NULL),
        ('wb_one','wildberries','sales','2026-09-07T03:00:00Z','ready',NULL,'2026-09-07T04:25:00Z',NULL),
        ('outside_policy','ozon','sales','2026-09-07T03:00:00Z','failed',NULL,NULL,'invalid_response');").await.unwrap();
    assert_eq!(
        findings(&client, &sql, "2026-09-07T04:20:00Z").await,
        [
            "reporting|ozon_one|ozon|source_collection_failed|advertising|invalid_response",
            "reporting|wb_one|wildberries|source_collection_stalled|stocks",
        ]
    );
    drop(client);
    driver.await.unwrap();
}
