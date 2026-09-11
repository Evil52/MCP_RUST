use super::{
    HeaderMap, MCP_SESSION_ID_HEADER, McpHttpLimits, Method, Response, SessionOwners, StatusCode,
    body_failure_response, unknown_mcp_session_response,
};

pub(super) async fn authorize_request(
    limits: &McpHttpLimits,
    subject: Option<&str>,
    headers: &HeaderMap,
) -> Result<Option<String>, Box<Response>> {
    let session = exact_mcp_session_id(headers).map_err(|()| {
        Box::new(body_failure_response(
            StatusCode::BAD_REQUEST,
            "Bad Request: invalid MCP session identifier",
        ))
    })?;
    if let (Some(owners), Some(subject), Some(session)) =
        (limits.session_owners.as_ref(), subject, session)
        && !owners.authorize(session, subject).await
    {
        // Keep another actor's session indistinguishable from an expired ID.
        return Err(Box::new(unknown_mcp_session_response()));
    }
    Ok(session.map(ToOwned::to_owned))
}

pub(super) async fn reconcile_response(
    limits: &McpHttpLimits,
    subject: Option<&str>,
    method: &Method,
    incoming_session: Option<&str>,
    status: StatusCode,
    headers: &HeaderMap,
) -> Result<(), Box<Response>> {
    let created = exact_mcp_session_id(headers).map_err(|()| {
        Box::new(body_failure_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal Server Error: invalid session identifier",
        ))
    })?;
    reconcile_session_ownership(
        limits.session_owners.as_ref(),
        subject,
        method,
        incoming_session,
        status,
        created,
    )
    .await
}

pub(super) fn exact_mcp_session_id(headers: &HeaderMap) -> Result<Option<&str>, ()> {
    let mut values = headers.get_all(MCP_SESSION_ID_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(());
    }
    value.to_str().map(Some).map_err(|_| ())
}

pub(super) async fn reconcile_session_ownership(
    owners: Option<&SessionOwners>,
    subject: Option<&str>,
    method: &Method,
    incoming_session_id: Option<&str>,
    response_status: StatusCode,
    response_session_id: Option<&str>,
) -> Result<(), Box<Response>> {
    let (Some(owners), Some(subject)) = (owners, subject) else {
        return Ok(());
    };
    if let Some(session_id) = incoming_session_id {
        if (method == Method::DELETE && response_status.is_success())
            || response_status == StatusCode::NOT_FOUND
        {
            owners.remove(session_id).await;
        }
        return Ok(());
    }

    let Some(session_id) = response_session_id else {
        return Ok(());
    };
    if owners.bind_created(session_id, subject).await {
        Ok(())
    } else {
        Err(Box::new(body_failure_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal Server Error: session ownership conflict",
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::{
        Arc, Body, Duration, HeaderValue, LocalSessionManager, Router,
        limit_mcp_request_concurrency, middleware,
    };
    use rmcp::transport::streamable_http_server::session::SessionManager as _;

    #[tokio::test]
    async fn ownership_conflicts_and_transport_expiry_fail_closed() {
        let manager = Arc::new(LocalSessionManager::default());
        let (id, _transport) = manager.create_session().await.unwrap();
        let mut limits = McpHttpLimits::for_test(2, 2, 2, Duration::from_secs(1));
        let owners = SessionOwners::new(manager);
        assert!(owners.bind_created(&id, "alice").await);
        limits.session_owners = Some(owners.clone());
        let mut headers = HeaderMap::new();
        headers.insert(MCP_SESSION_ID_HEADER, HeaderValue::from_str(&id).unwrap());
        assert_eq!(
            authorize_request(&limits, Some("alice"), &headers)
                .await
                .unwrap(),
            Some(id.to_string())
        );
        assert_eq!(
            authorize_request(&limits, Some("bob"), &headers)
                .await
                .unwrap_err()
                .status(),
            StatusCode::NOT_FOUND
        );
        headers.append(MCP_SESSION_ID_HEADER, HeaderValue::from_static("duplicate"));
        assert_eq!(
            authorize_request(&limits, Some("alice"), &headers)
                .await
                .unwrap_err()
                .status(),
            StatusCode::BAD_REQUEST
        );
        headers.clear();
        assert!(
            authorize_request(&limits, Some("alice"), &headers)
                .await
                .unwrap()
                .is_none()
        );
        let created = Response::builder()
            .header(MCP_SESSION_ID_HEADER, id.as_ref())
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            reconcile_response(
                &limits,
                Some("bob"),
                &Method::POST,
                None,
                created.status(),
                created.headers()
            )
            .await
            .unwrap_err()
            .status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert!(owners.authorize(&id, "alice").await);
        let expired = Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();
        reconcile_response(
            &limits,
            Some("alice"),
            &Method::GET,
            Some(&id),
            expired.status(),
            expired.headers(),
        )
        .await
        .unwrap();
        assert!(!owners.authorize(&id, "alice").await);
        reconcile_response(
            &limits,
            Some("alice"),
            &Method::POST,
            None,
            expired.status(),
            expired.headers(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn both_http_paths_reject_ambiguous_response_session_headers() {
        use axum::http::Request as HttpRequest;
        use tower::ServiceExt as _;

        for method in [Method::GET, Method::POST] {
            let router = Router::new()
                .fallback(|| async {
                    Response::builder()
                        .header(MCP_SESSION_ID_HEADER, "first")
                        .header(MCP_SESSION_ID_HEADER, "second")
                        .body(Body::empty())
                        .unwrap()
                })
                .layer(middleware::from_fn_with_state(
                    McpHttpLimits::for_test(2, 2, 2, Duration::from_secs(1)),
                    limit_mcp_request_concurrency,
                ));
            let request = HttpRequest::builder()
                .method(method)
                .uri("/mcp")
                .header("host", "localhost")
                .header("accept", "application/json, text/event-stream")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap();
            let response = router.oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        }
    }
}
