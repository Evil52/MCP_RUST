use super::*;

#[tokio::test]
async fn readiness_rechecks_required_schema_after_startup_without_reading_business_rows() {
    use std::{num::NonZeroUsize, path::PathBuf, time::Duration as StdDuration};

    use axum::{body::Body, http::Request};
    use tower::ServiceExt as _;

    let Ok(admin_url) = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL") else {
        return;
    };
    let (client, connection) = tokio_postgres::connect(&admin_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection_task = tokio::spawn(connection);
    // Session authentication stays with the disposable test DB owner so
    // schema drift can be rolled back. Every readiness query itself runs
    // as the real read-only role, with no ledger or business-table grants.
    client
        .batch_execute(
            "SET ROLE position_reader; \
             SET default_transaction_read_only = on; \
             SET statement_timeout = '2s'; \
             SET idle_in_transaction_session_timeout = '10s'",
        )
        .await
        .unwrap();
    let repository = Arc::new(PostgresReportingRepository::from_client(client));
    repository.verify_runtime_contract().await.unwrap();
    let server = crate::server::OzonMcp::new(
        crate::ozon::OzonClient::new(
            "http://127.0.0.1:1".to_owned(),
            StdDuration::from_secs(1),
            BTreeMap::new(),
        )
        .unwrap(),
        "admin".to_owned(),
        crate::config::RegistrySource::new(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/access.example.json"),
        )
        .unwrap(),
    )
    .with_reporting_reader(ReportingReader::from_repository(repository.clone()));
    let router = crate::http::build_router(server, NonZeroUsize::new(1).unwrap());
    let request = |uri| Request::builder().uri(uri).body(Body::empty()).unwrap();
    assert_eq!(
        router
            .clone()
            .oneshot(request("/readyz"))
            .await
            .unwrap()
            .status(),
        200
    );
    for drift in [
        "ALTER VIEW daily_reporting.mcp_source_collection_jobs RENAME TO hidden_source_jobs",
        "ALTER VIEW daily_reporting.mcp_sales_facts RENAME COLUMN ordered_units TO hidden_units",
        "GRANT SELECT ON daily_reporting.source_snapshots TO position_reader",
        "REVOKE SELECT ON daily_reporting.mcp_source_collection_jobs FROM position_reader",
    ] {
        {
            let client = repository.client.acquire().await.unwrap();
            client
                .batch_execute(&format!(
                    "BEGIN READ WRITE; SET LOCAL ROLE position_admin; {drift}; \
                     SET LOCAL ROLE position_reader; SET LOCAL transaction_read_only = on"
                ))
                .await
                .unwrap();
        }
        repository.client.probe().await.unwrap();
        let status = router
            .clone()
            .oneshot(request("/readyz"))
            .await
            .unwrap()
            .status();
        let liveness = router
            .clone()
            .oneshot(request("/livez"))
            .await
            .unwrap()
            .status();
        // Roll back even before asserting so failed checks restore the
        // fixture. Uncommitted DDL is invisible to other test sessions.
        repository
            .client
            .acquire()
            .await
            .unwrap()
            .batch_execute("ROLLBACK")
            .await
            .unwrap();
        assert_eq!(status, 503, "readiness accepted schema drift: {drift}");
        assert_eq!(liveness, 200);
        assert_eq!(
            router
                .clone()
                .oneshot(request("/readyz"))
                .await
                .unwrap()
                .status(),
            200,
            "readiness did not recover after schema restore"
        );
    }
    drop(router);
    drop(repository);
    connection_task.await.unwrap().unwrap();
}
