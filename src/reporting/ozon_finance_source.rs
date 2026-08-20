//! Strict, bounded normalization of Ozon's accrual ledger.

use std::collections::{BTreeMap, BTreeSet};

use chrono::NaiveDate;
use serde_json::{Value, json};
use thiserror::Error;

use super::{
    ozon_adapter::OzonReportRequest,
    ozon_source::{OzonReportSourceError, OzonReportTransport},
    postgres_collector::{CollectedFinanceFact, FinanceCategory},
};

const MAX_PAGES_PER_DAY: usize = 100;
const MAX_ROWS_PER_PAGE: usize = 10_000;
const MAX_CURSOR_BYTES: usize = 4_096;
const MAX_TYPE_TEXT_BYTES: usize = 2_048;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum OzonFinanceParseError {
    #[error("Ozon finance response has an unsupported shape")]
    Shape,
    #[error("Ozon finance response has an invalid value")]
    Value,
    #[error("Ozon finance response exceeds a fixed bound")]
    Limit,
}

#[derive(Debug, Clone)]
struct AccrualType {
    category: FinanceCategory,
    known: bool,
}

pub async fn collect_finance_facts(
    transport: &dyn OzonReportTransport,
    date_from: NaiveDate,
    date_to: NaiveDate,
) -> Result<Vec<CollectedFinanceFact>, OzonReportSourceError> {
    if date_from > date_to {
        return Err(OzonReportSourceError::InvalidSnapshotInput);
    }
    let types_response = transport
        .post(OzonReportRequest {
            path: "/v1/finance/accrual/types",
            payload: json!({}),
        })
        .await?;
    let types =
        parse_types(&types_response).map_err(|_| OzonReportSourceError::InvalidFinanceResponse)?;
    let mut aggregate = BTreeMap::new();
    let mut date = date_from;
    loop {
        collect_day(transport, date, &types, &mut aggregate).await?;
        if date == date_to {
            break;
        }
        date = date
            .succ_opt()
            .ok_or(OzonReportSourceError::InvalidSnapshotInput)?;
    }
    Ok(aggregate
        .into_iter()
        .map(
            |((business_date, sku, category), (amount_minor, line_count, unknown_type_count))| {
                CollectedFinanceFact {
                    business_date,
                    sku,
                    category,
                    amount_minor,
                    line_count,
                    unknown_type_count,
                }
            },
        )
        .collect())
}

type FinanceAggregate = BTreeMap<(NaiveDate, Option<u64>, FinanceCategory), (i64, u32, u32)>;

async fn collect_day(
    transport: &dyn OzonReportTransport,
    date: NaiveDate,
    types: &BTreeMap<u64, AccrualType>,
    aggregate: &mut FinanceAggregate,
) -> Result<(), OzonReportSourceError> {
    let mut last_id = String::new();
    let mut seen = BTreeSet::new();
    for _ in 0..MAX_PAGES_PER_DAY {
        let response = transport
            .post(OzonReportRequest {
                path: "/v1/finance/accrual/by-day",
                payload: json!({"date": date.format("%Y-%m-%d").to_string(), "last_id": last_id}),
            })
            .await?;
        let (rows, next) = parse_page(&response, date, types, aggregate)
            .map_err(|_| OzonReportSourceError::InvalidFinanceResponse)?;
        if next.is_empty() {
            return Ok(());
        }
        if rows == 0 || next.len() > MAX_CURSOR_BYTES || !seen.insert(next.clone()) {
            return Err(OzonReportSourceError::InvalidFinanceResponse);
        }
        last_id = next;
    }
    Err(OzonReportSourceError::PaginationLimit)
}

fn parse_types(response: &Value) -> Result<BTreeMap<u64, AccrualType>, OzonFinanceParseError> {
    let rows = response
        .get("accrual_types")
        .and_then(Value::as_array)
        .ok_or(OzonFinanceParseError::Shape)?;
    if rows.len() > MAX_ROWS_PER_PAGE {
        return Err(OzonFinanceParseError::Limit);
    }
    let mut result = BTreeMap::new();
    for row in rows {
        let row = row.as_object().ok_or(OzonFinanceParseError::Shape)?;
        let id = parse_u64(row.get("id").ok_or(OzonFinanceParseError::Shape)?)?;
        let name = bounded_text(row.get("name"))?;
        let description = bounded_text(row.get("description"))?;
        let (category, known) = classify_type(&format!("{name} {description}"));
        if id == 0 || result.insert(id, AccrualType { category, known }).is_some() {
            return Err(OzonFinanceParseError::Value);
        }
    }
    Ok(result)
}

fn parse_page(
    response: &Value,
    requested_date: NaiveDate,
    types: &BTreeMap<u64, AccrualType>,
    aggregate: &mut FinanceAggregate,
) -> Result<(usize, String), OzonFinanceParseError> {
    let rows = response
        .get("accruals")
        .and_then(Value::as_array)
        .ok_or(OzonFinanceParseError::Shape)?;
    if rows.len() > MAX_ROWS_PER_PAGE {
        return Err(OzonFinanceParseError::Limit);
    }
    for row in rows {
        let row = row.as_object().ok_or(OzonFinanceParseError::Shape)?;
        let date = parse_date(row.get("date").ok_or(OzonFinanceParseError::Shape)?)?;
        if date != requested_date {
            return Err(OzonFinanceParseError::Value);
        }
        let type_id = parse_u64(
            row.get("accrual_id")
                .or_else(|| row.get("type_id"))
                .ok_or(OzonFinanceParseError::Shape)?,
        )?;
        let kind = types.get(&type_id);
        let category = kind.map_or(FinanceCategory::Other, |value| value.category);
        let unknown = u32::from(kind.is_none_or(|value| !value.known));
        let amount = parse_money(
            row.get("total_amount")
                .ok_or(OzonFinanceParseError::Shape)?,
        )?;
        let sku = unique_posting_sku(row.get("posting"))?;
        let entry = aggregate.entry((date, sku, category)).or_default();
        entry.0 = entry
            .0
            .checked_add(amount)
            .ok_or(OzonFinanceParseError::Value)?;
        entry.1 = entry.1.checked_add(1).ok_or(OzonFinanceParseError::Value)?;
        entry.2 = entry
            .2
            .checked_add(unknown)
            .ok_or(OzonFinanceParseError::Value)?;
    }
    let last_id = response
        .get("last_id")
        .and_then(Value::as_str)
        .ok_or(OzonFinanceParseError::Shape)?
        .to_owned();
    Ok((rows.len(), last_id))
}

fn unique_posting_sku(posting: Option<&Value>) -> Result<Option<u64>, OzonFinanceParseError> {
    let Some(products) = posting
        .and_then(|v| v.get("products"))
        .and_then(Value::as_array)
    else {
        return Ok(None);
    };
    let mut sku = None;
    for product in products {
        let value = parse_u64(product.get("sku").ok_or(OzonFinanceParseError::Shape)?)?;
        if value == 0 {
            return Err(OzonFinanceParseError::Value);
        }
        if sku.is_some_and(|old| old != value) {
            return Ok(None);
        }
        sku = Some(value);
    }
    Ok(sku)
}

fn classify_type(text: &str) -> (FinanceCategory, bool) {
    let text = text.to_lowercase();
    let tests = [
        (FinanceCategory::Acquiring, &["эквайр", "acquir"][..]),
        (FinanceCategory::Storage, &["хранен", "storage"][..]),
        (
            FinanceCategory::PaidAcceptance,
            &["платн", "прием", "acceptance"][..],
        ),
        (
            FinanceCategory::Logistics,
            &["логист", "достав", "перевоз", "logistic", "delivery"][..],
        ),
        (FinanceCategory::Commission, &["комис", "commission"][..]),
        (
            FinanceCategory::Compensation,
            &["компенсац", "возмещ", "compensation"][..],
        ),
        (FinanceCategory::Advertising, &["реклам", "advert"][..]),
        (
            FinanceCategory::MarketplaceDiscount,
            &["скидк", "балл", "discount", "bonus"][..],
        ),
        (FinanceCategory::Sale, &["продаж", "выруч", "sale"][..]),
    ];
    tests
        .into_iter()
        .find(|(_, needles)| needles.iter().any(|needle| text.contains(needle)))
        .map_or((FinanceCategory::Other, false), |(category, _)| {
            (category, true)
        })
}

fn bounded_text(value: Option<&Value>) -> Result<&str, OzonFinanceParseError> {
    let text = value
        .and_then(Value::as_str)
        .ok_or(OzonFinanceParseError::Shape)?;
    if text.len() > MAX_TYPE_TEXT_BYTES {
        return Err(OzonFinanceParseError::Limit);
    }
    Ok(text)
}

fn parse_u64(value: &Value) -> Result<u64, OzonFinanceParseError> {
    value
        .as_u64()
        .or_else(|| value.as_str()?.parse().ok())
        .ok_or(OzonFinanceParseError::Value)
}

fn parse_date(value: &Value) -> Result<NaiveDate, OzonFinanceParseError> {
    let raw = value.as_str().ok_or(OzonFinanceParseError::Shape)?;
    NaiveDate::parse_from_str(
        raw.get(..10).ok_or(OzonFinanceParseError::Value)?,
        "%Y-%m-%d",
    )
    .map_err(|_| OzonFinanceParseError::Value)
}

fn parse_money(value: &Value) -> Result<i64, OzonFinanceParseError> {
    let object = value.as_object().ok_or(OzonFinanceParseError::Shape)?;
    if object.get("currency").and_then(Value::as_str) != Some("RUB") {
        return Err(OzonFinanceParseError::Value);
    }
    decimal_minor(
        object
            .get("amount")
            .and_then(Value::as_str)
            .ok_or(OzonFinanceParseError::Shape)?,
    )
}

fn decimal_minor(raw: &str) -> Result<i64, OzonFinanceParseError> {
    let (negative, raw) = raw.strip_prefix('-').map_or((false, raw), |v| (true, v));
    let (whole, fraction) = raw.split_once('.').unwrap_or((raw, ""));
    if whole.is_empty()
        || fraction.len() > 2
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(OzonFinanceParseError::Value);
    }
    let whole: i64 = whole.parse().map_err(|_| OzonFinanceParseError::Value)?;
    let fraction = match fraction.len() {
        0 => 0,
        1 => {
            fraction
                .parse::<i64>()
                .map_err(|_| OzonFinanceParseError::Value)?
                * 10
        }
        _ => fraction.parse().map_err(|_| OzonFinanceParseError::Value)?,
    };
    let amount = whole
        .checked_mul(100)
        .and_then(|v| v.checked_add(fraction))
        .ok_or(OzonFinanceParseError::Value)?;
    if negative {
        amount.checked_neg().ok_or(OzonFinanceParseError::Value)
    } else {
        Ok(amount)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, future::Future, pin::Pin, sync::Mutex};

    use super::*;

    struct FixtureTransport {
        responses: Mutex<VecDeque<Result<Value, OzonReportSourceError>>>,
        requests: Mutex<Vec<OzonReportRequest>>,
    }

    impl FixtureTransport {
        fn new(responses: Vec<Value>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().map(Ok).collect()),
                requests: Mutex::new(Vec::new()),
            }
        }
    }

    impl OzonReportTransport for FixtureTransport {
        fn post<'a>(
            &'a self,
            request: OzonReportRequest,
        ) -> Pin<Box<dyn Future<Output = Result<Value, OzonReportSourceError>> + Send + 'a>>
        {
            Box::pin(async move {
                self.requests.lock().unwrap().push(request);
                self.responses
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(Err(OzonReportSourceError::Transport))
            })
        }
    }

    fn date() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 8, 19).unwrap()
    }

    fn amount(raw: &str) -> Value {
        json!({"currency":"RUB", "amount":raw})
    }

    #[tokio::test]
    async fn collection_uses_exact_routes_paginates_days_and_aggregates() {
        let transport = FixtureTransport::new(vec![
            json!({"accrual_types":[
                {"id":"1", "name":"Продажа", "description":"товара"},
                {"id":2, "name":"new", "description":"unknown fee"}
            ]}),
            json!({"accruals":[
                {"date":"2026-08-19T10:00:00Z", "accrual_id":"1", "total_amount":amount("10.00"),
                 "posting":{"products":[{"sku":"55"},{"sku":55}]}},
                {"date":"2026-08-19", "type_id":999, "total_amount":amount("-1.25")}
            ], "last_id":"next"}),
            json!({"accruals":[
                {"date":"2026-08-19", "accrual_id":1, "total_amount":amount("2.50"),
                 "posting":{"products":[{"sku":55}]}}
            ], "last_id":""}),
            json!({"accruals":[
                {"date":"2026-08-20", "accrual_id":2, "total_amount":amount("1"),
                 "posting":{"products":[{"sku":55},{"sku":56}]}}
            ], "last_id":""}),
        ]);

        let facts = collect_finance_facts(&transport, date(), date().succ_opt().unwrap())
            .await
            .unwrap();
        assert_eq!(facts.len(), 3);
        assert!(facts.iter().any(|fact| {
            fact.business_date == date()
                && fact.sku == Some(55)
                && fact.category == FinanceCategory::Sale
                && fact.amount_minor == 1_250
                && fact.line_count == 2
                && fact.unknown_type_count == 0
        }));
        assert!(facts.iter().any(|fact| {
            fact.business_date == date()
                && fact.sku.is_none()
                && fact.category == FinanceCategory::Other
                && fact.amount_minor == -125
                && fact.line_count == 1
                && fact.unknown_type_count == 1
        }));
        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        assert_eq!(requests[0].path, "/v1/finance/accrual/types");
        assert_eq!(requests[1].path, "/v1/finance/accrual/by-day");
        assert_eq!(requests[1].payload["date"], "2026-08-19");
        assert_eq!(requests[1].payload["last_id"], "");
        assert_eq!(requests[2].payload["last_id"], "next");
        assert_eq!(requests[3].payload["date"], "2026-08-20");
    }

    #[tokio::test]
    async fn collection_rejects_invalid_input_types_and_pagination() {
        assert_eq!(
            collect_finance_facts(
                &FixtureTransport::new(vec![]),
                date(),
                date().pred_opt().unwrap()
            )
            .await,
            Err(OzonReportSourceError::InvalidSnapshotInput)
        );
        assert_eq!(
            collect_finance_facts(&FixtureTransport::new(vec![json!({})]), date(), date()).await,
            Err(OzonReportSourceError::InvalidFinanceResponse)
        );
        assert_eq!(
            collect_finance_facts(
                &FixtureTransport::new(vec![
                    json!({"accrual_types":[]}),
                    json!({"accruals":[], "last_id":"non-terminal"})
                ]),
                date(),
                date()
            )
            .await,
            Err(OzonReportSourceError::InvalidFinanceResponse)
        );

        let row = json!({"date":"2026-08-19", "accrual_id":7, "total_amount":amount("1")});
        let mut responses = vec![json!({"accrual_types":[]})];
        responses.extend(
            (0..MAX_PAGES_PER_DAY)
                .map(|page| json!({"accruals":[row.clone()], "last_id":format!("cursor-{page}")})),
        );
        assert_eq!(
            collect_finance_facts(&FixtureTransport::new(responses), date(), date()).await,
            Err(OzonReportSourceError::PaginationLimit)
        );

        let too_long = "x".repeat(MAX_CURSOR_BYTES + 1);
        assert_eq!(
            collect_finance_facts(
                &FixtureTransport::new(vec![
                    json!({"accrual_types":[]}),
                    json!({"accruals":[row.clone()], "last_id":too_long})
                ]),
                date(),
                date()
            )
            .await,
            Err(OzonReportSourceError::InvalidFinanceResponse)
        );
        assert_eq!(
            collect_finance_facts(
                &FixtureTransport::new(vec![
                    json!({"accrual_types":[]}),
                    json!({"accruals":[row.clone()], "last_id":"same"}),
                    json!({"accruals":[row], "last_id":"same"})
                ]),
                date(),
                date()
            )
            .await,
            Err(OzonReportSourceError::InvalidFinanceResponse)
        );
    }

    #[test]
    fn type_and_page_parsers_are_strict_and_bounded() {
        let types = parse_types(&json!({"accrual_types":[
            {"id":1, "name":"Комиссия", "description":""},
            {"id":"2", "name":"unknown", "description":"fee"}
        ]}))
        .unwrap();
        assert_eq!(types[&1].category, FinanceCategory::Commission);
        assert!(types[&1].known);
        assert!(!types[&2].known);

        for invalid in [
            json!({}),
            json!({"accrual_types":[false]}),
            json!({"accrual_types":[{"name":"a", "description":"b"}]}),
            json!({"accrual_types":[{"id":0, "name":"a", "description":"b"}]}),
            json!({"accrual_types":[{"id":"bad", "name":"a", "description":"b"}]}),
            json!({"accrual_types":[{"id":1, "name":"a"}]}),
            json!({"accrual_types":[
                {"id":1, "name":"a", "description":"b"},
                {"id":1, "name":"c", "description":"d"}
            ]}),
            json!({"accrual_types":[{
                "id":1, "name":"x".repeat(MAX_TYPE_TEXT_BYTES + 1), "description":"b"
            }]}),
        ] {
            assert!(parse_types(&invalid).is_err(), "accepted {invalid}");
        }
        assert!(matches!(
            parse_types(&json!({"accrual_types":vec![Value::Null; MAX_ROWS_PER_PAGE + 1]})),
            Err(OzonFinanceParseError::Limit)
        ));

        let mut aggregate = FinanceAggregate::new();
        assert_eq!(
            parse_page(
                &json!({"accruals":[{
                    "date":"2026-08-19", "type_id":99, "total_amount":amount("3.25"),
                    "posting":{"products":[]}
                }], "last_id":"done"}),
                date(),
                &types,
                &mut aggregate
            ),
            Ok((1, "done".to_owned()))
        );
        assert_eq!(
            aggregate[&(date(), None, FinanceCategory::Other)],
            (325, 1, 1)
        );

        let invalid_pages = [
            json!({}),
            json!({"accruals":[false], "last_id":""}),
            json!({"accruals":[{"type_id":1, "total_amount":amount("1")}], "last_id":""}),
            json!({"accruals":[{"date":"2026-08-18", "type_id":1, "total_amount":amount("1")}], "last_id":""}),
            json!({"accruals":[{"date":"2026-08-19", "total_amount":amount("1")}], "last_id":""}),
            json!({"accruals":[{"date":"2026-08-19", "type_id":"bad", "total_amount":amount("1")}], "last_id":""}),
            json!({"accruals":[{"date":"2026-08-19", "type_id":1}], "last_id":""}),
            json!({"accruals":[{"date":"2026-08-19", "type_id":1, "total_amount":{"currency":"USD", "amount":"1"}}], "last_id":""}),
            json!({"accruals":[], "last_id":7}),
        ];
        for invalid in invalid_pages {
            assert!(
                parse_page(&invalid, date(), &types, &mut FinanceAggregate::new()).is_err(),
                "accepted {invalid}"
            );
        }
        assert_eq!(
            parse_page(
                &json!({"accruals":vec![Value::Null; MAX_ROWS_PER_PAGE + 1], "last_id":""}),
                date(),
                &types,
                &mut FinanceAggregate::new()
            ),
            Err(OzonFinanceParseError::Limit)
        );
    }

    #[test]
    fn aggregation_overflows_fail_closed() {
        let types = parse_types(&json!({"accrual_types":[]})).unwrap();
        let row = json!({"accruals":[{
            "date":"2026-08-19", "type_id":99, "total_amount":amount("1")
        }], "last_id":""});
        for existing in [(i64::MAX, 0, 0), (0, u32::MAX, 0), (0, 0, u32::MAX)] {
            let mut aggregate =
                FinanceAggregate::from([((date(), None, FinanceCategory::Other), existing)]);
            assert_eq!(
                parse_page(&row, date(), &types, &mut aggregate),
                Err(OzonFinanceParseError::Value)
            );
        }
    }

    #[test]
    fn posting_dates_money_and_integer_helpers_reject_ambiguous_values() {
        assert_eq!(unique_posting_sku(None), Ok(None));
        assert_eq!(unique_posting_sku(Some(&json!({}))), Ok(None));
        assert_eq!(
            unique_posting_sku(Some(&json!({"products":[{"sku":4},{"sku":"4"}]}))),
            Ok(Some(4))
        );
        assert_eq!(
            unique_posting_sku(Some(&json!({"products":[{"sku":4},{"sku":5}]}))),
            Ok(None)
        );
        for invalid in [json!({"products":[{}]}), json!({"products":[{"sku":0}]})] {
            assert!(unique_posting_sku(Some(&invalid)).is_err());
        }
        assert_eq!(parse_u64(&json!(7)), Ok(7));
        assert_eq!(parse_u64(&json!("8")), Ok(8));
        assert!(parse_u64(&json!(-1)).is_err());
        assert_eq!(parse_date(&json!("2026-08-19T12:00:00Z")), Ok(date()));
        for invalid in [json!(7), json!("x"), json!("2026")] {
            assert!(parse_date(&invalid).is_err());
        }
        assert_eq!(parse_money(&amount("1.23")), Ok(123));
        for invalid in [
            json!("1"),
            json!({"currency":"USD", "amount":"1"}),
            json!({"currency":"RUB", "amount":1}),
        ] {
            assert!(parse_money(&invalid).is_err());
        }
        assert_eq!(bounded_text(Some(&json!("ok"))), Ok("ok"));
        assert!(bounded_text(None).is_err());
    }

    #[test]
    fn parses_signed_money_without_floats() {
        assert_eq!(decimal_minor("12"), Ok(1_200));
        assert_eq!(decimal_minor("12.3"), Ok(1_230));
        assert_eq!(decimal_minor("12.34"), Ok(1_234));
        assert_eq!(decimal_minor("-0.01"), Ok(-1));
        for invalid in ["", ".1", "a", "1.a", "1.001", "922337203685477580"] {
            assert!(decimal_minor(invalid).is_err(), "accepted {invalid}");
        }
    }

    #[test]
    fn classifies_core_finance_families() {
        for (text, expected) in [
            ("Услуга эквайринга", FinanceCategory::Acquiring),
            ("storage fee", FinanceCategory::Storage),
            ("paid acceptance", FinanceCategory::PaidAcceptance),
            ("delivery", FinanceCategory::Logistics),
            ("commission", FinanceCategory::Commission),
            ("compensation", FinanceCategory::Compensation),
            ("advertising", FinanceCategory::Advertising),
            ("bonus", FinanceCategory::MarketplaceDiscount),
            ("sale", FinanceCategory::Sale),
        ] {
            assert_eq!(classify_type(text), (expected, true));
        }
        assert_eq!(
            classify_type("new unknown fee"),
            (FinanceCategory::Other, false)
        );
    }
}
