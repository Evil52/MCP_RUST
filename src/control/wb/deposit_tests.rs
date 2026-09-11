#[path = "deposit_failure_tests.rs"]
mod failures;
use super::*;

#[tokio::test]
async fn budget_deposit_is_exact_one_thousand_balance_without_bonus() {
    let (base_url, server) = response_server(
        http_response("200 OK", "", br#"{"total":1000}"#),
        Duration::ZERO,
    )
    .await;
    let client = WbBidWriteClient::new_for_test(&base_url, "test-token", Duration::from_secs(1));
    assert_eq!(
        client
            .deposit_once_with_permit(42, || async { Ok::<_, ()>(()) })
            .await
            .unwrap(),
        1000
    );
    let request = server.await.unwrap();
    assert!(request.starts_with("POST /adv/v1/budget/deposit?id=42 HTTP/1.1\r\n"));
    let payload: Value = serde_json::from_str(request.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    assert_eq!(
        payload,
        serde_json::json!({"sum":1000,"type":1,"return":true})
    );
}

#[tokio::test]
async fn budget_deposit_rejected_permit_does_not_send() {
    let client =
        WbBidWriteClient::new_for_test("http://127.0.0.1:1", "test-token", Duration::from_secs(1));
    assert!(matches!(
        client
            .deposit_once_with_permit(42, || async { Err("locked") })
            .await,
        Err(WbGuardedWriteError::Permit("locked"))
    ));
    assert!(matches!(
        client
            .deposit_once_with_permit(0, || async { Ok::<_, ()>(()) })
            .await,
        Err(WbGuardedWriteError::Write(WbWriteError::InvalidRequest(_)))
    ));
}

#[tokio::test]
async fn budget_deposit_invalid_response_remains_ambiguous() {
    for body in [b"{}".as_slice(), br#"{"total":-1}"#, br#"{"total":"1000"}"#] {
        let (base_url, server) =
            response_server(http_response("200 OK", "", body), Duration::ZERO).await;
        let client =
            WbBidWriteClient::new_for_test(&base_url, "test-token", Duration::from_secs(1));
        assert!(matches!(
            client
                .deposit_once_with_permit(42, || async { Ok::<_, ()>(()) })
                .await,
            Err(WbGuardedWriteError::Write(WbWriteError::Ambiguous { .. }))
        ));
        server.await.unwrap();
    }
}

#[tokio::test]
async fn budget_deposit_timeout_does_not_retry() {
    let (base_url, server) = response_server(
        http_response("200 OK", "", br#"{"total":1000}"#),
        Duration::from_millis(150),
    )
    .await;
    let client = WbBidWriteClient::new_for_test(&base_url, "test-token", Duration::from_millis(30));
    assert!(matches!(
        client
            .deposit_once_with_permit(42, || async { Ok::<_, ()>(()) })
            .await,
        Err(WbGuardedWriteError::Write(WbWriteError::Ambiguous { .. }))
    ));
    assert!(
        server
            .await
            .unwrap()
            .starts_with("POST /adv/v1/budget/deposit?id=42 ")
    );
}
