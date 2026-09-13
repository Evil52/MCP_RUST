use std::{collections::VecDeque, sync::Mutex};

use serde_json::json;

use super::*;

type RecordedRequest = (NaiveDate, NaiveDate, u32, u64);

struct FixtureTransport {
    responses: Mutex<VecDeque<Result<Option<Value>, WbReportSourceError>>>,
    requests: Mutex<Vec<RecordedRequest>>,
}

impl FixtureTransport {
    fn new(responses: Vec<Option<Value>>) -> Self {
        Self {
            responses: Mutex::new(responses.into_iter().map(Ok).collect()),
            requests: Mutex::new(Vec::new()),
        }
    }
}

impl WbFinanceTransport for FixtureTransport {
    fn finance_page<'a>(
        &'a self,
        start: NaiveDate,
        end: NaiveDate,
        limit: u32,
        rrd_id: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Value>, WbReportSourceError>> + Send + 'a>> {
        Box::pin(async move {
            self.requests
                .lock()
                .unwrap()
                .push((start, end, limit, rrd_id));
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Err(WbReportSourceError::InvalidResponse))
        })
    }
}

fn date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 9, 10).unwrap()
}

fn row(rrd_id: u64) -> Value {
    json!({
        "rrdId": rrd_id,
        "reportId": 9_007_199_254_740_993_u64,
        "rrDate": "2026-09-10",
        "currency": "RUB",
        "nmId": 112,
        "docTypeName": "Продажа",
        "sellerOperName": "Продажа",
        "quantity": 1,
        "retailAmount": "400.00",
        "forPay": "345.99",
        "rebillLogisticCost": "1.349",
    })
}

#[tokio::test]
async fn short_pages_require_terminal_204_and_exact_last_row_cursor() {
    let transport = FixtureTransport::new(vec![
        Some(json!([row(4), row(6)])),
        Some(json!([row(11)])),
        None,
    ]);
    let collected = collect_finance_details_checkpointed(&transport, date(), date(), &None)
        .await
        .unwrap();
    assert_eq!(collected.len(), 3);
    assert_eq!(collected[0].report_id, 9_007_199_254_740_993);
    assert_eq!(
        collected[0].amounts["rebillLogisticCost"],
        WbFinanceDecimal {
            units: 1349,
            scale: 3
        }
    );
    assert_eq!(
        *transport.requests.lock().unwrap(),
        vec![
            (date(), date(), 1_000, 0),
            (date(), date(), 1_000, 6),
            (date(), date(), 1_000, 11),
        ]
    );
}

#[tokio::test]
async fn empty_200_json_null_and_wrapped_objects_are_not_completion() {
    for body in [json!([]), Value::Null, json!({"data": []})] {
        let transport = FixtureTransport::new(vec![Some(body)]);
        assert_eq!(
            collect_finance_details_checkpointed(&transport, date(), date(), &None).await,
            Err(WbReportSourceError::InvalidResponse)
        );
    }
    let transport = FixtureTransport::new(vec![None]);
    assert!(
        collect_finance_details_checkpointed(&transport, date(), date(), &None)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn rejects_duplicate_and_backwards_rows_including_across_pages() {
    for responses in [
        vec![Some(json!([row(5), row(5)])), None],
        vec![Some(json!([row(6), row(5)])), None],
        vec![Some(json!([row(5)])), Some(json!([row(5)])), None],
        vec![Some(json!([row(5)])), Some(json!([row(4)])), None],
    ] {
        let transport = FixtureTransport::new(responses);
        assert_eq!(
            collect_finance_details_checkpointed(&transport, date(), date(), &None).await,
            Err(WbReportSourceError::InvalidResponse)
        );
    }
}

#[test]
fn decimal_is_exact_signed_bounded_and_never_floating_point() {
    assert_eq!(
        parse_decimal("9007199254740993.01").unwrap(),
        WbFinanceDecimal {
            units: 900_719_925_474_099_301,
            scale: 2
        }
    );
    assert_eq!(
        parse_decimal("-9223372036854775808").unwrap(),
        WbFinanceDecimal {
            units: i64::MIN,
            scale: 0
        }
    );
    assert_eq!(
        parse_decimal("-0.000000001").unwrap(),
        WbFinanceDecimal {
            units: -1,
            scale: 9
        }
    );
    for malformed in [
        "",
        " ",
        "+1",
        "01",
        ".5",
        "1.",
        "1.2.3",
        "1e2",
        "NaN",
        "1,50",
        " 1.2",
        "1.0000000001",
        "9223372036854775808",
        "-9223372036854775809",
        "99999999999999999999999999999999999",
    ] {
        assert_eq!(
            parse_decimal(malformed),
            Err(WbReportSourceError::InvalidResponse)
        );
    }
    let mut numeric = row(1);
    numeric["retailAmount"] = json!(0.1);
    assert_eq!(
        parse_row(&numeric),
        Err(WbReportSourceError::InvalidResponse)
    );
}

#[test]
fn unknown_operations_refunds_missing_amounts_and_unattributed_rows_are_preserved() {
    let mut refund = row(1);
    refund["docTypeName"] = json!("Возврат");
    refund["sellerOperName"] = json!("Новая корректировка");
    refund["forPay"] = json!("-1.20");
    refund["nmId"] = json!(0);
    refund["acquiringFee"] = Value::Null;
    let normalized = parse_row(&refund).unwrap();
    assert_eq!(normalized.sku, None);
    assert_eq!(normalized.document_type.as_deref(), Some("Возврат"));
    assert_eq!(
        normalized.operation_type.as_deref(),
        Some("Новая корректировка")
    );
    assert_eq!(normalized.amounts["retailAmount"].units, 40_000);
    assert_eq!(
        normalized.amounts["forPay"],
        WbFinanceDecimal {
            units: -120,
            scale: 2
        }
    );
    assert!(!normalized.amounts.contains_key("acquiringFee"));
    assert!(!normalized.amounts.contains_key("paidStorage"));
}

#[test]
fn projection_and_normalization_exclude_pii_and_untrusted_extra_payloads() {
    let mut upstream = row(1);
    for field in [
        "b2bCustomerTin",
        "ppvzSupplierInn",
        "ppvzSupplierName",
        "rebillLogisticOrg",
        "ppvzOfficeName",
        "srid",
        "orderUid",
        "title",
        "url",
        "Authorization",
    ] {
        assert!(!WB_FINANCE_FIELDS.contains(&field));
        upstream[field] = json!("sensitive-value-must-not-persist");
    }
    let normalized = parse_row(&upstream).unwrap();
    let serialized = serde_json::to_string(&normalized).unwrap();
    assert!(!serialized.contains("sensitive-value"));
    assert!(
        AMOUNT_FIELDS
            .iter()
            .all(|field| WB_FINANCE_FIELDS.contains(field))
    );
    let mut corrupt = normalized;
    corrupt.amounts.insert(
        "Authorization".to_owned(),
        WbFinanceDecimal { units: 0, scale: 0 },
    );
    assert_eq!(
        corrupt.validate(),
        Err(WbReportSourceError::InvalidResponse)
    );
}

#[test]
fn rejects_malformed_identity_dates_types_and_payload_sizes() {
    for (field, value) in [
        ("rrdId", json!(0)),
        ("rrdId", json!(1.5)),
        ("rrdId", json!("123")),
        ("reportId", json!(0)),
        ("rrdId", json!(u64::MAX)),
        ("reportId", json!(u64::MAX)),
        ("nmId", json!(u64::MAX)),
        ("rrDate", json!("2026-09-10junk")),
        ("rrDate", json!("2026-09-10T00:00:00Z")),
        ("rrDate", json!("2026-02-30")),
        ("currency", json!("rub")),
        ("quantity", json!(1.25)),
        ("docTypeName", json!("a".repeat(MAX_TYPE_BYTES + 1))),
        ("sellerOperName", json!("new\noperation")),
    ] {
        let mut malformed = row(1);
        malformed[field] = value;
        assert_eq!(
            parse_row(&malformed),
            Err(WbReportSourceError::InvalidResponse)
        );
    }
    assert_eq!(
        parse_page(&json!(vec![row(1); WB_FINANCE_PAGE_SIZE as usize + 1])),
        Err(WbReportSourceError::InvalidResponse)
    );
}

#[tokio::test]
async fn date_bounds_fail_before_transport_and_row_dates_remain_unaltered() {
    let transport = FixtureTransport::new(vec![]);
    for end in [
        date().pred_opt().unwrap(),
        date() + chrono::Duration::days(31),
    ] {
        assert_eq!(
            collect_finance_details_checkpointed(&transport, date(), end, &None).await,
            Err(WbReportSourceError::InvalidSnapshotInput)
        );
    }
    assert!(transport.requests.lock().unwrap().is_empty());
    assert_eq!(
        collect_finance_details_checkpointed(
            &transport,
            FIRST_SUPPORTED_DATE.pred_opt().unwrap(),
            FIRST_SUPPORTED_DATE,
            &None,
        )
        .await,
        Err(WbReportSourceError::InvalidSnapshotInput)
    );
    assert!(transport.requests.lock().unwrap().is_empty());
    // Request dates describe the report interval. Preserve a historic rrDate
    // on a correction rather than rewriting it to the requested day.
    let mut historic = row(1);
    historic["rrDate"] = json!("2025-10-20");
    assert_eq!(
        parse_row(&historic).unwrap().business_date.to_string(),
        "2025-10-20"
    );
}

#[tokio::test]
async fn failure_after_data_does_not_return_a_complete_partial_result() {
    let transport = FixtureTransport::new(vec![Some(json!([row(1)]))]);
    transport
        .responses
        .lock()
        .unwrap()
        .push_back(Err(WbReportSourceError::RetryAfter { seconds: 60 }));
    assert_eq!(
        collect_finance_details_checkpointed(&transport, date(), date(), &None).await,
        Err(WbReportSourceError::RetryAfter { seconds: 60 })
    );
    assert_eq!(transport.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn normalized_checkpoint_resume_keeps_privacy_cursor_and_terminal_proof() {
    use crate::reporting::checkpoint::{
        CheckpointError,
        tests::{MemoryPages, journal},
    };

    let pages = MemoryPages::default();
    let mut upstream = row(1);
    upstream["b2bCustomerTin"] = json!("sensitive-checkpoint-marker");
    let transport = FixtureTransport::new(vec![Some(json!([upstream])), None]);
    assert_eq!(
        collect_finance_details_checkpointed(&transport, date(), date(), &journal(&pages)).await,
        Err(WbReportSourceError::Checkpoint(CheckpointError::Deferred))
    );
    assert!(
        !serde_json::to_string(&*pages.lock().unwrap())
            .unwrap()
            .contains("sensitive-checkpoint-marker")
    );
    let result = collect_finance_details_checkpointed(&transport, date(), date(), &journal(&pages))
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(
        *transport.requests.lock().unwrap(),
        vec![(date(), date(), 1_000, 0), (date(), date(), 1_000, 1)]
    );
}

#[tokio::test]
async fn total_row_bound_fails_closed_and_exact_bound_requires_terminal_page() {
    let responses = (0..25)
        .map(|page| {
            Some(Value::Array(
                (1..=1_000).map(|i| row(page * 1_000 + i)).collect(),
            ))
        })
        .chain([None])
        .collect();
    let transport = FixtureTransport::new(responses);
    assert_eq!(
        collect_finance_details_checkpointed(&transport, date(), date(), &None)
            .await
            .unwrap()
            .len(),
        WB_FINANCE_MAX_ROWS
    );
    assert_eq!(transport.requests.lock().unwrap().len(), 26);

    let responses = (0..26)
        .map(|page| {
            Some(Value::Array(
                (1..=1_000).map(|i| row(page * 1_000 + i)).collect(),
            ))
        })
        .chain([None])
        .collect();
    let transport = FixtureTransport::new(responses);
    assert_eq!(
        collect_finance_details_checkpointed(&transport, date(), date(), &None).await,
        Err(WbReportSourceError::PaginationLimit)
    );
}

#[tokio::test]
async fn independent_page_bound_rejects_never_ending_tiny_pages() {
    let responses = (1..=MAX_PAGES)
        .map(|id| Some(json!([row(u64::try_from(id).unwrap())])))
        .collect();
    let transport = FixtureTransport::new(responses);
    assert_eq!(
        collect_finance_details_checkpointed(&transport, date(), date(), &None).await,
        Err(WbReportSourceError::PaginationLimit)
    );
}

#[tokio::test]
async fn checkpoint_replay_rejects_identifiers_outside_database_and_cursor_bounds() {
    use crate::reporting::checkpoint::{
        CheckpointError,
        tests::{MemoryPages, journal},
    };

    for field in ["rrd_id", "report_id", "sku"] {
        let pages = MemoryPages::default();
        let transport = FixtureTransport::new(vec![Some(json!([row(1)])), None]);
        assert_eq!(
            collect_finance_details_checkpointed(&transport, date(), date(), &journal(&pages))
                .await,
            Err(WbReportSourceError::Checkpoint(CheckpointError::Deferred))
        );
        pages.lock().unwrap().values_mut().next().unwrap()[0][field] = json!(u64::MAX);
        assert_eq!(
            collect_finance_details_checkpointed(&transport, date(), date(), &journal(&pages))
                .await,
            Err(WbReportSourceError::InvalidResponse)
        );
        assert_eq!(transport.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn invalid_cursor_pages_are_not_saved_to_the_checkpoint() {
    use crate::reporting::checkpoint::tests::{MemoryPages, journal};

    let pages = MemoryPages::default();
    let transport = FixtureTransport::new(vec![Some(json!([row(5), row(4)]))]);
    assert_eq!(
        collect_finance_details_checkpointed(&transport, date(), date(), &journal(&pages)).await,
        Err(WbReportSourceError::InvalidResponse)
    );
    assert!(pages.lock().unwrap().is_empty());
}
