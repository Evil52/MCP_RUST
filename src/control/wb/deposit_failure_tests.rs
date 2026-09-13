use super::*;

#[tokio::test]
async fn deposit_http_failure_preserves_request_id_and_is_never_success() {
    let (base_url, server) = response_server(
        http_response(
            "429 Too Many Requests",
            "x-request-id: deposit-rejected\r\n",
            b"{}",
        ),
        Duration::ZERO,
    )
    .await;
    let client = WbBidWriteClient::new_for_test(&base_url, "test-token", Duration::from_secs(1));
    assert!(matches!(
        client.deposit_once_with_permit(42, || async { Ok::<_, ()>(()) }).await,
        Err(WbGuardedWriteError::Write(WbWriteError::HttpStatus {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            request_id: Some(id),
        })) if id == "deposit-rejected"
    ));
    assert!(
        server
            .await
            .unwrap()
            .starts_with("POST /adv/v1/budget/deposit?id=42 ")
    );
}

#[tokio::test]
async fn deposit_truncated_response_is_ambiguous_even_after_http_success() {
    let response = b"HTTP/1.1 200 OK\r\ncontent-length: 100\r\nx-request-id: deposit-truncated\r\nconnection: close\r\n\r\n{\"total\":1000}".to_vec();
    let (base_url, server) = response_server(response, Duration::ZERO).await;
    let client = WbBidWriteClient::new_for_test(&base_url, "test-token", Duration::from_secs(1));
    assert!(matches!(
        client.deposit_once_with_permit(42, || async { Ok::<_, ()>(()) }).await,
        Err(WbGuardedWriteError::Write(WbWriteError::Ambiguous {
            reason: "response_body_error",
            request_id: Some(id),
        })) if id == "deposit-truncated"
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn deposit_operation_deadline_bounds_a_slower_http_client() {
    let (base_url, server) = response_server(
        http_response("200 OK", "", br#"{"total":1000}"#),
        Duration::from_millis(150),
    )
    .await;
    let http = Client::builder()
        .no_proxy()
        .retry(reqwest::retry::never())
        .timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    let client = WbBidWriteClient::from_parts(
        http,
        &base_url,
        "test-token",
        Duration::from_millis(30),
        Duration::ZERO,
    )
    .unwrap();
    assert!(matches!(
        client
            .deposit_once_with_permit(42, || async { Ok::<_, ()>(()) })
            .await,
        Err(WbGuardedWriteError::Write(WbWriteError::Ambiguous {
            reason: "timeout",
            request_id: None,
        }))
    ));
    server.await.unwrap();
}
