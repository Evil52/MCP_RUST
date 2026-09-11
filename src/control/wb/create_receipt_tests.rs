use super::*;

#[tokio::test]
async fn campaign_creation_refuses_invalid_receipt_as_ambiguous() {
    let (base_url, server) = response_server(
        http_response("200 OK", "x-request-id: create-bad\r\n", b"{}"),
        Duration::ZERO,
    )
    .await;
    let client = WbBidWriteClient::new_for_test(&base_url, "test-token", Duration::from_secs(1));
    let error = client
        .create_campaign_with_permit(&create_campaign_request(), || async { Ok::<_, ()>(()) })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        WbGuardedWriteError::Write(WbWriteError::Ambiguous {
            reason: "invalid_success_advert_id",
            request_id: Some(request_id),
        }) if request_id == "create-bad"
    ));
    server.await.unwrap();
}
