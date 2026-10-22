use super::*;

fn arguments_with_range(range: Value) -> Value {
    let mut arguments = json!({
        "store": "store_a",
        "date_from": "2026-08-01",
        "date_to": "2026-08-02",
        "statuses": ["awaiting_packaging"]
    });
    let Value::Object(range) = range else {
        panic!("date range must be an object");
    };
    arguments.as_object_mut().unwrap().extend(range);
    arguments
}

#[tokio::test]
async fn unfulfilled_requires_exactly_one_complete_date_pair_before_marketplace_io() {
    let (server, requests) = mock_server(0);
    for (range, expected_error) in [
        (json!({}), "ровно одну полную пару"),
        (
            json!({
                "cutoff_from": null, "cutoff_to": null,
                "delivering_date_from": null, "delivering_date_to": null
            }),
            "ровно одну полную пару",
        ),
        (json!({"cutoff_from": "2026-08-03"}), "cutoff"),
        (json!({"cutoff_to": "2026-08-04"}), "cutoff"),
        (
            json!({"cutoff_from": "2026-08-03", "cutoff_to": null}),
            "cutoff",
        ),
        (
            json!({"cutoff_from": null, "cutoff_to": "2026-08-04"}),
            "cutoff",
        ),
        (
            json!({"delivering_date_from": "2026-08-03"}),
            "delivering_date",
        ),
        (
            json!({"delivering_date_to": "2026-08-04"}),
            "delivering_date",
        ),
        (
            json!({"delivering_date_from": "2026-08-03", "delivering_date_to": null}),
            "delivering_date",
        ),
        (
            json!({"delivering_date_from": null, "delivering_date_to": "2026-08-04"}),
            "delivering_date",
        ),
        (
            json!({"cutoff_from": "", "cutoff_to": "2026-08-04"}),
            "cutoff_from",
        ),
        (
            json!({"cutoff_from": "2026-08-03", "cutoff_to": "2026-13-04"}),
            "cutoff_to",
        ),
        (
            json!({"cutoff_from": "2026-08-04", "cutoff_to": "2026-08-03"}),
            "cutoff_to",
        ),
        (
            json!({"delivering_date_from": "", "delivering_date_to": "2026-08-04"}),
            "delivering_date_from",
        ),
        (
            json!({"delivering_date_from": "2026-08-03", "delivering_date_to": "2026-13-04"}),
            "delivering_date_to",
        ),
        (
            json!({"delivering_date_from": "2026-08-04", "delivering_date_to": "2026-08-03"}),
            "delivering_date_to",
        ),
        (
            json!({
                "cutoff_from": "2026-08-03", "cutoff_to": "2026-08-04",
                "delivering_date_from": "2026-08-03", "delivering_date_to": "2026-08-04"
            }),
            "нельзя передавать одновременно",
        ),
    ] {
        let input: FbsUnfulfilledInput =
            serde_json::from_value(arguments_with_range(range)).unwrap();
        assert_validation_error(
            server
                .fbs_unfulfilled(RequestIdentity::dev(), Parameters(input))
                .await,
            expected_error,
        );
    }
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn unfulfilled_missing_range_is_actionable_at_the_mcp_boundary() {
    let (server, requests) = mock_server(0);
    let response: Value = serde_json::from_str(
        &call_tool_over_http(
            server,
            "ozon_fbs_unfulfilled",
            arguments_with_range(json!({})),
        )
        .await,
    )
    .unwrap();
    assert_eq!(response["result"]["isError"], true);
    let text = response["result"]["content"][0]["text"].as_str().unwrap();
    for hint in [
        "cutoff_from/cutoff_to",
        "delivering_date_from/delivering_date_to",
        "date_from/date_to",
        "не подставляйте их автоматически",
        "Запрос к Ozon не выполнен",
    ] {
        assert!(text.contains(hint), "missing {hint}: {text}");
    }
    assert!(!text.contains("OZON_TOOL_CALL_FAILED"), "{text}");
    assert!(!text.contains("Остановите текущую операцию"), "{text}");
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn unfulfilled_forwards_either_date_pair_without_changing_status_dates_or_cursor() {
    let page = json!({
        "count": 1,
        "cursor": "next-unfulfilled-page",
        "has_next": true,
        "postings": [{"posting_number": "test-posting"}]
    });
    let (server, requests) = mock_server_with_responses(vec![(200, page.to_string()); 2]);
    for (selected, other) in [("cutoff", "delivering_date"), ("delivering_date", "cutoff")] {
        let mut range = json!({"cursor": "previous-unfulfilled-page"});
        range[format!("{selected}_from")] = json!("2026-08-03");
        range[format!("{selected}_to")] = json!("2026-08-04");
        // Explicit nulls for the unused pair remain compatible with Option fields.
        range[format!("{other}_from")] = Value::Null;
        range[format!("{other}_to")] = Value::Null;
        let input: FbsUnfulfilledInput =
            serde_json::from_value(arguments_with_range(range)).unwrap();
        let Json(result) = server
            .fbs_unfulfilled(RequestIdentity::dev(), Parameters(input))
            .await
            .unwrap();
        assert_eq!(result.data, page);

        let request = requests.recv_timeout(Duration::from_secs(3)).unwrap();
        let (path, body) = request_path_and_body(&request);
        assert_eq!(path, "/v4/posting/fbs/unfulfilled/list");
        let mut filter = json!({
            "delivery_method_ids": [],
            "last_changed_status_date": {
                "from": "2026-08-01T00:00:00.000Z",
                "to": "2026-08-02T23:59:59.999Z"
            },
            "provider_ids": [],
            "statuses": ["awaiting_packaging"],
            "warehouse_ids": []
        });
        filter[format!("{selected}_from")] = json!("2026-08-03T00:00:00.000Z");
        filter[format!("{selected}_to")] = json!("2026-08-04T23:59:59.999Z");
        assert_eq!(
            body,
            json!({
                "cursor": "previous-unfulfilled-page",
                "filter": filter,
                "limit": 100,
                "sort_dir": "ASC",
                "translit": false,
                "with": {
                    "analytics_data": true,
                    "barcodes": true,
                    "financial_data": false,
                    "legal_info": false
                }
            })
        );
    }
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn unfulfilled_publishes_exclusive_date_pairs_and_export_guidance() {
    let server = server();
    let service: StreamableHttpService<OzonMcp, LocalSessionManager> = StreamableHttpService::new(
        move || Ok(server.clone()),
        Arc::default(),
        StreamableHttpServerConfig::default()
            .with_legacy_session_mode(false)
            .with_json_response(true),
    );
    let router = axum::Router::new().nest_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let response = loopback_client()
        .post(format!("http://{address}/mcp"))
        .header("accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let body: Value = response.json().await.unwrap();
    task.abort();
    let tools = body["result"]["tools"].as_array().unwrap();
    let other_period_tool = tools
        .iter()
        .find(|tool| tool["name"] == "ozon_returns")
        .unwrap();
    assert!(other_period_tool["inputSchema"].get("oneOf").is_none());
    let tool = tools
        .iter()
        .find(|tool| tool["name"] == "ozon_fbs_unfulfilled")
        .unwrap();
    let schema = &tool["inputSchema"];
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["additionalProperties"], false);
    let branches = schema["oneOf"].as_array().unwrap();
    assert_eq!(branches.len(), 2);
    for (selected, other) in [("cutoff", "delivering_date"), ("delivering_date", "cutoff")] {
        let required = json!([format!("{selected}_from"), format!("{selected}_to")]);
        let branch = branches
            .iter()
            .find(|branch| branch["required"] == required)
            .unwrap();
        for suffix in ["from", "to"] {
            assert_eq!(
                branch["properties"][format!("{selected}_{suffix}")]["type"],
                "string"
            );
            assert_eq!(
                branch["properties"][format!("{other}_{suffix}")]["type"],
                "null"
            );
        }
    }
    let properties = &schema["properties"];
    for field in ["date_from", "date_to"] {
        let description = properties[field]["description"].as_str().unwrap();
        assert!(description.contains("изменения статуса"));
        assert!(description.contains("UTC"));
    }
    for (field, partner, time) in [
        ("cutoff_from", "cutoff_to", "00:00:00Z"),
        ("cutoff_to", "cutoff_from", "23:59:59.999Z"),
        ("delivering_date_from", "delivering_date_to", "00:00:00Z"),
        (
            "delivering_date_to",
            "delivering_date_from",
            "23:59:59.999Z",
        ),
    ] {
        let description = properties[field]["description"].as_str().unwrap();
        for hint in ["YYYY-MM-DD", partner, time, "Обязательно вместе"] {
            assert!(
                description.contains(hint),
                "{field} is missing {hint}: {description}"
            );
        }
    }
    assert!(
        properties["statuses"]["description"]
            .as_str()
            .unwrap()
            .contains("awaiting_packaging")
    );
    let description = tool["description"].as_str().unwrap();
    for hint in [
        "ровно одна пара",
        "cutoff_from/cutoff_to",
        "delivering_date_from/delivering_date_to",
        "date_from/date_to",
        "Не придумывайте границы",
        "cursor",
        "has_next=false",
    ] {
        assert!(description.contains(hint), "missing {hint}: {description}");
    }
}
