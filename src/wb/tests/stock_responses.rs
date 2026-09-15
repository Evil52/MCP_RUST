use super::*;

#[tokio::test]
async fn fbw_documented_no_content_preserves_evidence_and_does_not_retry() {
    let (base_url, requests, task) = raw_http(vec![raw_response(
        204,
        "x-request-id: fbw-empty-page\r\n",
        b"",
    )]);
    let client = client(&base_url);
    let result = client
        .warehouse_stocks("account", json!({"limit": 1_000, "offset": 0}))
        .await
        .unwrap();
    assert_eq!(
        result,
        json!({
            "data": {"items": []},
            "meta": {
                "upstream_status": 204,
                "data_state": "no_data",
                "source_endpoint": WAREHOUSE_STOCKS_PATH,
                "request_id": "fbw-empty-page",
            },
        })
    );
    let request = String::from_utf8(requests.recv().unwrap()).unwrap();
    assert!(request.starts_with(&format!("POST {WAREHOUSE_STOCKS_PATH} HTTP/1.1\r\n")));
    task.join().unwrap();
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn other_read_endpoints_do_not_accept_no_content() {
    for (method, path) in [
        (Method::GET, PING_PATH),
        (Method::POST, SALES_FUNNEL_PATH),
        (Method::POST, "/api/v3/stocks/1"),
    ] {
        let (base_url, requests, task) = raw_http(vec![raw_response(204, "", b"")]);
        let error = client(&base_url)
            .request_for_test("account", method, path)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), WbErrorKind::InvalidJson, "{path}");
        requests.recv().unwrap();
        task.join().unwrap();
        assert!(requests.try_recv().is_err());
    }
}

#[tokio::test]
async fn fbw_empty_or_malformed_ok_response_remains_invalid_json() {
    for body in [b"".as_slice(), b"not-json", b"<html>upstream error</html>"] {
        let (base_url, requests, task) = raw_http(vec![raw_response(
            200,
            "x-request-id: invalid-fbw-page\r\n",
            body,
        )]);
        let error = client(&base_url)
            .warehouse_stocks("account", json!({}))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), WbErrorKind::InvalidJson);
        assert_eq!(error.request_id(), Some("invalid-fbw-page"));
        requests.recv().unwrap();
        task.join().unwrap();
        assert!(requests.try_recv().is_err());
    }
}

#[tokio::test]
async fn fbw_success_response_still_enforces_declared_and_streamed_body_limits() {
    let declared = MAX_RESPONSE_BODY_BYTES as u64 + 1;
    let declared_oversize =
        format!("HTTP/1.1 200 OK\r\nContent-Length: {declared}\r\nConnection: close\r\n\r\n")
            .into_bytes();
    let mut streamed_oversize = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n".to_vec();
    streamed_oversize.extend(std::iter::repeat_n(b'x', MAX_RESPONSE_BODY_BYTES + 1));
    let (base_url, requests, task) = raw_http(vec![declared_oversize, streamed_oversize]);
    let client = client(&base_url);
    for _ in 0..2 {
        let error = client
            .warehouse_stocks("account", json!({}))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), WbErrorKind::ResponseTooLarge);
        requests.recv().unwrap();
    }
    task.join().unwrap();
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn invalid_json_and_both_response_size_limits_are_enforced() {
    let declared = MAX_RESPONSE_BODY_BYTES as u64 + 1;
    let declared_oversize = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {declared}\r\nConnection: close\r\n\r\n"
    )
    .into_bytes();
    let mut streamed_oversize =
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n".to_vec();
    streamed_oversize.extend(std::iter::repeat_n(b'x', MAX_RESPONSE_BODY_BYTES + 1));
    let (base_url, requests, task) = raw_http(vec![
        raw_response(200, "x-request-id: invalid-json-id\r\n", b"not-json"),
        declared_oversize,
        streamed_oversize,
    ]);
    let client = client(&base_url);
    let invalid_json = client.ping("account").await.unwrap_err();
    assert_eq!(invalid_json.kind(), WbErrorKind::InvalidJson);
    assert_eq!(invalid_json.request_id(), Some("invalid-json-id"));
    for _ in 0..2 {
        assert_eq!(
            client.ping("account").await.unwrap_err().kind(),
            WbErrorKind::ResponseTooLarge
        );
    }
    for _ in 0..3 {
        requests.recv().unwrap();
    }
    task.join().unwrap();
}
