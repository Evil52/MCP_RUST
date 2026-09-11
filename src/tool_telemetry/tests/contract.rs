use super::*;

#[tokio::test]
async fn database_contract_drift_and_malformed_rows_fail_closed() {
    let (Ok(admin_url), Ok(reader_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("REPORT_REFRESH_TEST_REQUESTER_URL"),
    ) else {
        return;
    };
    let (admin, connection) = tokio_postgres::connect(&admin_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    admin
        .batch_execute("GRANT SELECT ON daily_reporting.mcp_tool_calls TO report_refresh_requester")
        .await
        .unwrap();
    let invalid_contract = ToolTelemetryService::connect_optional(Some(&reader_url)).await;
    admin
        .batch_execute(
            "REVOKE SELECT ON daily_reporting.mcp_tool_calls FROM report_refresh_requester",
        )
        .await
        .unwrap();
    assert!(
        invalid_contract
            .unwrap_err()
            .to_string()
            .contains("contract is unavailable")
    );
    let service = ToolTelemetryService::connect_optional(Some(&reader_url))
        .await
        .unwrap();
    let original: String = admin.query_one("SELECT pg_get_functiondef('daily_reporting.begin_mcp_tool_call(text,text,text,text)'::regprocedure)", &[]).await.unwrap().get(0);
    admin.batch_execute("CREATE OR REPLACE FUNCTION daily_reporting.begin_mcp_tool_call(requested_actor_id text, requested_tool_name text, requested_account_id text, requested_marketplace text) RETURNS bigint LANGUAGE sql AS $$ SELECT 0::bigint $$").await.unwrap();
    let invalid_id = service.begin("admin", "ofk_test", None, None).await;
    admin.batch_execute(&original).await.unwrap();
    assert_eq!(invalid_id, Err(ToolTelemetryError::InvalidData));
    for (id, duration, outcome) in [
        (0_i64, 1_i32, "failed"),
        (1, -1, "failed"),
        (1, 1, "unknown"),
    ] {
        let row = admin.query_one("SELECT $1::bigint, 'admin'::text, 'ofk_test'::text, NULL::text, NULL::text, now(), NULL::timestamptz, $2::integer, $3::text, NULL::text", &[&id, &duration, &outcome]).await.unwrap();
        assert_eq!(
            tool_call_log_item(&row),
            Err(ToolTelemetryError::InvalidData)
        );
    }
    drop(service);
    drop(admin);
    driver.await.unwrap().unwrap();
}
