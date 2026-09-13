use super::*;
use axum::{
    Router,
    http::{StatusCode, Uri},
    response::IntoResponse as _,
};

#[tokio::test]
async fn all_performance_page_sources_preserve_vendor_retry_after() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new().fallback(|uri: Uri| async move {
        if uri.path() == "/api/client/token" {
            (StatusCode::OK, token()).into_response()
        } else {
            (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "81")], "{}").into_response()
        }
    });
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let date = NaiveDate::from_ymd_opt(2026, 8, 18).unwrap();
    for method in 0..3 {
        let transport = PerformanceClientReportTransport::new(
            PerformanceClient::new_for_test(url.clone(), Duration::from_secs(3), credentials()),
            StoreId::from("shop"),
        );
        let result = match method {
            0 => transport.campaigns(1, 100).await,
            1 => transport.sku_statistics(vec![1], date).await,
            _ => transport.expenses(vec![1], date).await,
        };
        let error = result.unwrap_err();
        assert_eq!(
            error,
            OzonPerformanceReportSourceError::RetryAfter { seconds: 81 }
        );
        assert_eq!(error.code(), "rate_limited");
        assert_eq!(error.failure().retry_after, Some(81));
    }
    server.abort();
}
