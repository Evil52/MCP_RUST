use super::{CONTROL_DB_TEST_LOCK, WbPlanRepository, validate_control_database_url};

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run through scripts/with-position-test-db.sh"]
async fn runtime_contract_allows_only_exact_shared_quota_functions() {
    let _database_guard = CONTROL_DB_TEST_LOCK.lock().await;
    let database_url = std::env::var("WB_CONTROL_TEST_DATABASE_URL")
        .expect("disposable fixture provides the WB Control URL");
    let admin_url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL")
        .expect("disposable fixture provides the admin URL");
    let config = validate_control_database_url(&database_url).unwrap();
    let repository = WbPlanRepository::connect(&config).await.unwrap();
    repository.verify_runtime_contract().await.unwrap();

    let (mut admin, connection) = tokio_postgres::connect(&admin_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    for mutation in [
        "CREATE FUNCTION marketplace_quota.unexpected_control_entry(text,bigint) \
         RETURNS bigint LANGUAGE sql AS 'SELECT 0::bigint'; \
         GRANT EXECUTE ON FUNCTION marketplace_quota.unexpected_control_entry(text,bigint) TO control_writer",
        "CREATE FUNCTION marketplace_quota.try_acquire(text,integer) \
         RETURNS bigint LANGUAGE sql AS 'SELECT 0::bigint'; \
         GRANT EXECUTE ON FUNCTION marketplace_quota.try_acquire(text,integer) TO control_writer",
        "DROP FUNCTION marketplace_quota.try_acquire(text,bigint); \
         CREATE FUNCTION marketplace_quota.try_acquire(text,bigint) \
         RETURNS text LANGUAGE sql AS 'SELECT ''wrong return type''::text'; \
         GRANT EXECUTE ON FUNCTION marketplace_quota.try_acquire(text,bigint) TO control_writer",
        "DROP FUNCTION marketplace_quota.try_acquire(text,bigint); \
         CREATE FUNCTION marketplace_quota.try_acquire(text,bigint) \
         RETURNS SETOF bigint LANGUAGE sql AS 'SELECT 0::bigint'; \
         GRANT EXECUTE ON FUNCTION marketplace_quota.try_acquire(text,bigint) TO control_writer",
        "DROP FUNCTION marketplace_quota.try_acquire(text,bigint); \
         DROP FUNCTION marketplace_quota.extend_cooldown(text,bigint); \
         CREATE FUNCTION marketplace_quota.unexpected_control_entry(text,bigint) \
         RETURNS bigint LANGUAGE sql AS 'SELECT 0::bigint'; \
         GRANT EXECUTE ON FUNCTION marketplace_quota.unexpected_control_entry(text,bigint) TO control_writer",
    ] {
        // Catalog changes are visible only to this transaction. Concurrent
        // coordinator clients keep their committed functions throughout.
        let transaction = admin.transaction().await.unwrap();
        transaction.batch_execute(mutation).await.unwrap();
        transaction
            .batch_execute("SET LOCAL ROLE control_writer")
            .await
            .unwrap();
        let result = transaction
            .query_one(include_str!("verify_runtime_contract.sql"), &[])
            .await;
        transaction.rollback().await.unwrap();
        assert_eq!(
            result.unwrap().get::<_, Option<bool>>(0),
            Some(false),
            "{mutation}"
        );
    }
    repository.verify_runtime_contract().await.unwrap();
    drop(admin);
    connection.await.unwrap().unwrap();
}
