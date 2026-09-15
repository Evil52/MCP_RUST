use super::*;
use crate::{
    test_support::mock_http,
    wb::{WbCredentials, WbErrorKind},
};
use serde_json::json;
use std::{collections::BTreeMap, time::Duration};

fn transport(base: &str) -> WbClientOfficialReportTransport {
    let client = WbClient::new_for_test(
        Duration::from_secs(2),
        BTreeMap::from([(
            "account".to_owned(),
            WbCredentials {
                token: "test-finance-token".to_owned(),
            },
        )]),
        base,
        base,
    );
    WbClientOfficialReportTransport::new(client, "account".to_owned())
}

fn date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 9, 10).unwrap()
}

fn body(request: &str) -> Value {
    serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap()
}

#[tokio::test]
async fn lists_use_the_bound_account_and_map_each_report_period() {
    let (base, requests) = mock_http(vec![(200, "[]".to_owned()), (204, String::new())]);
    let transport = transport(&base);
    assert_eq!(transport.account_id(), "account");

    assert_eq!(
        transport
            .list_reports(date(), date(), WbFinanceReportPeriod::Daily, 10, 0)
            .await,
        Ok(Some(json!([])))
    );
    // HTTP 204 is the terminal page and must stay distinguishable from JSON.
    assert_eq!(
        transport
            .list_reports(date(), date(), WbFinanceReportPeriod::Weekly, 10, 20)
            .await,
        Ok(None)
    );
    for (period, offset) in [("daily", 0), ("weekly", 20)] {
        let request = requests.recv().unwrap();
        assert!(
            request.starts_with("POST /api/finance/v1/sales-reports/list HTTP/1.1\r\n"),
            "{request}"
        );
        assert_eq!(body(&request)["period"], period);
        assert_eq!(body(&request)["offset"], offset);
    }
}

#[tokio::test]
async fn report_details_are_read_by_id_and_failures_keep_their_kind() {
    let (base, requests) = mock_http(vec![
        (200, "[{\"rrdId\":5}]".to_owned()),
        (500, "{}".to_owned()),
    ]);
    let transport = transport(&base);

    assert_eq!(
        transport.report_details(123, 1000, 5).await,
        Ok(Some(json!([{"rrdId": 5}])))
    );
    assert_eq!(
        transport.report_details(123, 1000, 6).await,
        Err(WbReportSourceError::Upstream(WbErrorKind::Http))
    );
    let request = requests.recv().unwrap();
    assert!(
        request.starts_with("POST /api/finance/v1/sales-reports/detailed/123 HTTP/1.1\r\n"),
        "{request}"
    );
    assert_eq!(body(&request)["rrdId"], 5);

    // Invalid arguments are rejected before any request departs.
    assert_eq!(
        transport.report_details(0, 1000, 0).await,
        Err(WbReportSourceError::Upstream(WbErrorKind::InvalidArguments))
    );
    assert_eq!(
        transport
            .list_reports(date(), date(), WbFinanceReportPeriod::Daily, 0, 0)
            .await,
        Err(WbReportSourceError::Upstream(WbErrorKind::InvalidArguments))
    );
}

#[test]
fn only_rate_limits_with_a_known_delay_become_a_collection_pause() {
    assert_eq!(
        source_error(&WbError::RateLimited {
            request_id: None,
            retry_after: Some(Duration::from_millis(1_500)),
        }),
        WbReportSourceError::RetryAfter { seconds: 2 }
    );
    assert_eq!(
        source_error(&WbError::LocalRateLimited {
            retry_after: Duration::from_secs(7),
        }),
        WbReportSourceError::RetryAfter { seconds: 7 }
    );
    assert_eq!(
        source_error(&WbError::RateLimited {
            request_id: Some("request".to_owned()),
            retry_after: None,
        }),
        WbReportSourceError::Upstream(WbErrorKind::RateLimited)
    );
    assert_eq!(
        source_error(&WbError::Overloaded),
        WbReportSourceError::Upstream(WbErrorKind::Overloaded)
    );
}
