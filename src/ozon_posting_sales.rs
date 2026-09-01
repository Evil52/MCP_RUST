//! Fail-closed normalization for the explicit FBO/FBS posting-sales fallback.
//!
//! Posting lists are not a drop-in replacement for Seller Analytics: they
//! expose current shipment status and product quantities, but not the exact
//! `ordered_units`/GMV attribution contract. This module therefore keeps the
//! fallback metric explicit and never turns it into daily-report sales facts.

use std::collections::{BTreeMap, BTreeSet};

use rmcp::schemars::JsonSchema;
use serde::Serialize;
use serde_json::{Map, Value, json};
use thiserror::Error;

pub const FBO_POSTINGS_PATH: &str = "/v3/posting/fbo/list";
pub const FBS_POSTINGS_PATH: &str = "/v4/posting/fbs/list";
pub const POSTING_PAGE_LIMIT: u32 = 100;
pub const MAX_POSTING_SALES_PAGES: usize = 1_000;

const MAX_CURSOR_BYTES: usize = 4_096;
const MAX_POSTING_NUMBER_BYTES: usize = 256;
const MAX_STATUS_BYTES: usize = 128;
const MAX_POSTINGS_PER_PAGE: usize = 100;
const MAX_PRODUCTS_PER_PAGE: usize = 10_000;
// Keep the serialized MCP result comfortably below the structured-result
// ceiling even when every SKU has several distinct status buckets.
const MAX_AGGREGATED_SKUS: usize = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PostingScheme {
    Fbo,
    Fbs,
}

impl PostingScheme {
    #[must_use]
    pub const fn endpoint(self) -> &'static str {
        match self {
            Self::Fbo => FBO_POSTINGS_PATH,
            Self::Fbs => FBS_POSTINGS_PATH,
        }
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum PostingSalesError {
    #[error("Ozon posting response has an unsupported shape")]
    Shape,
    #[error("Ozon posting response has an invalid value")]
    Value,
    #[error("Ozon posting response exceeds the fallback bound")]
    TooManyRows,
    #[error("Ozon posting response repeats an already counted posting")]
    DuplicatePosting,
    #[error("Ozon posting aggregate overflowed")]
    Overflow,
}

impl PostingSalesError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Shape => "invalid_shape",
            Self::Value => "invalid_value",
            Self::TooManyRows => "too_many_rows",
            Self::DuplicatePosting => "duplicate_posting",
            Self::Overflow => "overflow",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct PostingSalesRow {
    pub sku: u64,
    pub fbo_non_cancelled_units: u64,
    pub fbs_non_cancelled_units: u64,
    pub total_non_cancelled_units: u64,
    pub fbo_cancelled_units: u64,
    pub fbs_cancelled_units: u64,
    pub total_cancelled_units: u64,
    pub fbo_status_units: BTreeMap<String, u64>,
    pub fbs_status_units: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, JsonSchema)]
pub struct PostingSalesTotals {
    pub fbo_postings: u64,
    pub fbs_postings: u64,
    pub fbo_non_cancelled_units: u64,
    pub fbs_non_cancelled_units: u64,
    pub total_non_cancelled_units: u64,
    pub fbo_cancelled_units: u64,
    pub fbs_cancelled_units: u64,
    pub total_cancelled_units: u64,
}

#[derive(Debug, Default)]
struct MutablePostingSalesRow {
    fbo_non_cancelled: u64,
    fbs_non_cancelled: u64,
    fbo_cancelled: u64,
    fbs_cancelled: u64,
    fbo_by_status: BTreeMap<String, u64>,
    fbs_by_status: BTreeMap<String, u64>,
}

#[derive(Debug, Default)]
pub struct PostingSalesAccumulator {
    rows: BTreeMap<u64, MutablePostingSalesRow>,
    seen_postings: BTreeSet<(PostingScheme, String)>,
    totals: PostingSalesTotals,
}

impl PostingSalesAccumulator {
    pub fn absorb_page(
        &mut self,
        scheme: PostingScheme,
        response: &Value,
    ) -> Result<Option<String>, PostingSalesError> {
        let root = response.as_object().ok_or(PostingSalesError::Shape)?;
        let postings = array_field(root, "postings")?;
        if postings.len() > MAX_POSTINGS_PER_PAGE {
            return Err(PostingSalesError::TooManyRows);
        }

        let mut product_rows = 0_usize;
        for posting in postings {
            let posting = posting.as_object().ok_or(PostingSalesError::Shape)?;
            let posting_number = bounded_string_field(
                posting,
                "posting_number",
                MAX_POSTING_NUMBER_BYTES,
                valid_identifier,
            )?;
            if !self
                .seen_postings
                .insert((scheme, posting_number.to_owned()))
            {
                return Err(PostingSalesError::DuplicatePosting);
            }
            let status = bounded_string_field(posting, "status", MAX_STATUS_BYTES, valid_status)?;
            let products = array_field(posting, "products")?;
            product_rows = product_rows
                .checked_add(products.len())
                .ok_or(PostingSalesError::Overflow)?;
            if product_rows > MAX_PRODUCTS_PER_PAGE {
                return Err(PostingSalesError::TooManyRows);
            }

            match scheme {
                PostingScheme::Fbo => checked_add(&mut self.totals.fbo_postings, 1)?,
                PostingScheme::Fbs => checked_add(&mut self.totals.fbs_postings, 1)?,
            }
            for product in products {
                let product = product.as_object().ok_or(PostingSalesError::Shape)?;
                let sku = positive_u64_field(product, "sku")?;
                let quantity = positive_u64_field(product, "quantity")?;
                if !self.rows.contains_key(&sku) && self.rows.len() >= MAX_AGGREGATED_SKUS {
                    return Err(PostingSalesError::TooManyRows);
                }
                let row = self.rows.entry(sku).or_default();
                let cancelled = status == "cancelled";
                match (scheme, cancelled) {
                    (PostingScheme::Fbo, false) => {
                        checked_add(&mut row.fbo_non_cancelled, quantity)?;
                        checked_add(&mut self.totals.fbo_non_cancelled_units, quantity)?;
                    }
                    (PostingScheme::Fbs, false) => {
                        checked_add(&mut row.fbs_non_cancelled, quantity)?;
                        checked_add(&mut self.totals.fbs_non_cancelled_units, quantity)?;
                    }
                    (PostingScheme::Fbo, true) => {
                        checked_add(&mut row.fbo_cancelled, quantity)?;
                        checked_add(&mut self.totals.fbo_cancelled_units, quantity)?;
                    }
                    (PostingScheme::Fbs, true) => {
                        checked_add(&mut row.fbs_cancelled, quantity)?;
                        checked_add(&mut self.totals.fbs_cancelled_units, quantity)?;
                    }
                }
                let status_units = match scheme {
                    PostingScheme::Fbo => &mut row.fbo_by_status,
                    PostingScheme::Fbs => &mut row.fbs_by_status,
                };
                checked_add(status_units.entry(status.to_owned()).or_default(), quantity)?;
            }
        }

        next_cursor(root)
    }

    pub fn finish(
        mut self,
    ) -> Result<(PostingSalesTotals, Vec<PostingSalesRow>), PostingSalesError> {
        self.totals.total_non_cancelled_units = self
            .totals
            .fbo_non_cancelled_units
            .checked_add(self.totals.fbs_non_cancelled_units)
            .ok_or(PostingSalesError::Overflow)?;
        self.totals.total_cancelled_units = self
            .totals
            .fbo_cancelled_units
            .checked_add(self.totals.fbs_cancelled_units)
            .ok_or(PostingSalesError::Overflow)?;

        let rows = self
            .rows
            .into_iter()
            .map(|(sku, row)| {
                Ok(PostingSalesRow {
                    sku,
                    fbo_non_cancelled_units: row.fbo_non_cancelled,
                    fbs_non_cancelled_units: row.fbs_non_cancelled,
                    total_non_cancelled_units: row
                        .fbo_non_cancelled
                        .checked_add(row.fbs_non_cancelled)
                        .ok_or(PostingSalesError::Overflow)?,
                    fbo_cancelled_units: row.fbo_cancelled,
                    fbs_cancelled_units: row.fbs_cancelled,
                    total_cancelled_units: row
                        .fbo_cancelled
                        .checked_add(row.fbs_cancelled)
                        .ok_or(PostingSalesError::Overflow)?,
                    fbo_status_units: row.fbo_by_status,
                    fbs_status_units: row.fbs_by_status,
                })
            })
            .collect::<Result<Vec<_>, PostingSalesError>>()?;
        Ok((self.totals, rows))
    }
}

#[must_use]
pub fn posting_page_request(
    scheme: PostingScheme,
    from: &str,
    to: &str,
    cursor: Option<&str>,
) -> Value {
    let mut with = Map::new();
    with.insert("analytics_data".to_owned(), Value::Bool(false));
    with.insert("financial_data".to_owned(), Value::Bool(false));
    with.insert("legal_info".to_owned(), Value::Bool(false));
    if scheme == PostingScheme::Fbs {
        with.insert("barcodes".to_owned(), Value::Bool(false));
    }

    let mut object = Map::new();
    object.insert(
        "filter".to_owned(),
        json!({"since": from, "to": to, "statuses": []}),
    );
    object.insert("limit".to_owned(), json!(POSTING_PAGE_LIMIT));
    object.insert("sort_dir".to_owned(), Value::String("ASC".to_owned()));
    object.insert("translit".to_owned(), Value::Bool(false));
    object.insert("with".to_owned(), Value::Object(with));
    if let Some(cursor) = cursor {
        object.insert("cursor".to_owned(), Value::String(cursor.to_owned()));
    }
    Value::Object(object)
}

fn next_cursor(root: &Map<String, Value>) -> Result<Option<String>, PostingSalesError> {
    let has_next = root
        .get("has_next")
        .and_then(Value::as_bool)
        .ok_or(PostingSalesError::Shape)?;
    let cursor = root
        .get("cursor")
        .and_then(Value::as_str)
        .ok_or(PostingSalesError::Shape)?;
    let cursor_is_safe =
        cursor.len() <= MAX_CURSOR_BYTES && !cursor.bytes().any(|byte| byte.is_ascii_control());
    if has_next {
        if cursor.is_empty() || !cursor_is_safe {
            return Err(PostingSalesError::Value);
        }
        Ok(Some(cursor.to_owned()))
    } else if cursor_is_safe {
        Ok(None)
    } else {
        Err(PostingSalesError::Value)
    }
}

fn array_field<'a>(
    object: &'a Map<String, Value>,
    name: &str,
) -> Result<&'a Vec<Value>, PostingSalesError> {
    object
        .get(name)
        .and_then(Value::as_array)
        .ok_or(PostingSalesError::Shape)
}

fn positive_u64_field(object: &Map<String, Value>, name: &str) -> Result<u64, PostingSalesError> {
    let value = object.get(name).ok_or(PostingSalesError::Shape)?;
    let parsed = value.as_u64().or_else(|| value.as_str()?.parse().ok());
    parsed
        .filter(|value| *value > 0)
        .ok_or(PostingSalesError::Value)
}

fn bounded_string_field<'a>(
    object: &'a Map<String, Value>,
    name: &str,
    max_bytes: usize,
    validate: fn(u8) -> bool,
) -> Result<&'a str, PostingSalesError> {
    object
        .get(name)
        .and_then(Value::as_str)
        .filter(|value| {
            !value.is_empty() && value.len() <= max_bytes && value.bytes().all(validate)
        })
        .ok_or(PostingSalesError::Value)
}

const fn valid_identifier(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/')
}

const fn valid_status(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')
}

fn checked_add(target: &mut u64, value: u64) -> Result<(), PostingSalesError> {
    *target = target
        .checked_add(value)
        .ok_or(PostingSalesError::Overflow)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_pin_the_current_read_only_fbo_and_fbs_contracts() {
        assert_eq!(
            posting_page_request(PostingScheme::Fbo, "from", "to", None),
            json!({
                "filter":{"since":"from","to":"to","statuses":[]},
                "limit":100,
                "sort_dir":"ASC",
                "translit":false,
                "with":{"analytics_data":false,"financial_data":false,"legal_info":false},
            })
        );
        assert_eq!(
            posting_page_request(PostingScheme::Fbs, "from", "to", Some("next")),
            json!({
                "cursor":"next",
                "filter":{"since":"from","to":"to","statuses":[]},
                "limit":100,
                "sort_dir":"ASC",
                "translit":false,
                "with":{
                    "analytics_data":false,
                    "barcodes":false,
                    "financial_data":false,
                    "legal_info":false,
                },
            })
        );
        assert_eq!(PostingScheme::Fbo.endpoint(), FBO_POSTINGS_PATH);
        assert_eq!(PostingScheme::Fbs.endpoint(), FBS_POSTINGS_PATH);
    }

    #[test]
    fn pages_aggregate_non_cancelled_and_cancelled_units_without_gmv_inference() {
        let mut aggregate = PostingSalesAccumulator::default();
        assert_eq!(
            aggregate
                .absorb_page(
                    PostingScheme::Fbo,
                    &json!({
                        "postings":[
                            {"posting_number":"fbo-1","status":"delivered","products":[
                                {"sku":7,"quantity":2},{"sku":"8","quantity":"3"}
                            ]},
                            {"posting_number":"fbo-2","status":"cancelled","products":[
                                {"sku":7,"quantity":1}
                            ]}
                        ],
                        "has_next":true,
                        "cursor":"next-fbo"
                    }),
                )
                .unwrap(),
            Some("next-fbo".to_owned())
        );
        assert_eq!(
            aggregate
                .absorb_page(
                    PostingScheme::Fbs,
                    &json!({
                        "postings":[
                            {"posting_number":"fbs-1","status":"awaiting_deliver","products":[
                                {"sku":7,"quantity":4}
                            ]}
                        ],
                        "has_next":false,
                        "cursor":""
                    }),
                )
                .unwrap(),
            None
        );

        let (totals, rows) = aggregate.finish().unwrap();
        assert_eq!(totals.fbo_postings, 2);
        assert_eq!(totals.fbs_postings, 1);
        assert_eq!(totals.total_non_cancelled_units, 9);
        assert_eq!(totals.total_cancelled_units, 1);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].sku, 7);
        assert_eq!(rows[0].fbo_non_cancelled_units, 2);
        assert_eq!(rows[0].fbs_non_cancelled_units, 4);
        assert_eq!(rows[0].total_cancelled_units, 1);
        assert_eq!(rows[0].fbo_status_units["delivered"], 2);
        assert_eq!(rows[0].fbo_status_units["cancelled"], 1);
    }

    #[test]
    fn repeated_postings_and_untrustworthy_pagination_fail_closed() {
        let page = json!({
            "postings":[{"posting_number":"posting-1","status":"delivered","products":[
                {"sku":1,"quantity":1}
            ]}],
            "has_next":false,
            "cursor":""
        });
        let mut aggregate = PostingSalesAccumulator::default();
        aggregate.absorb_page(PostingScheme::Fbo, &page).unwrap();
        assert_eq!(
            aggregate.absorb_page(PostingScheme::Fbo, &page),
            Err(PostingSalesError::DuplicatePosting)
        );
        assert_eq!(
            PostingSalesAccumulator::default().absorb_page(
                PostingScheme::Fbs,
                &json!({"postings":[],"has_next":true,"cursor":""}),
            ),
            Err(PostingSalesError::Value)
        );
    }

    #[test]
    fn malformed_identifiers_statuses_and_quantities_are_rejected() {
        for posting in [
            json!({"posting_number":"","status":"delivered","products":[]}),
            json!({"posting_number":"posting","status":"bad status","products":[]}),
            json!({"posting_number":"posting","status":"delivered","products":[{"sku":0,"quantity":1}]}),
            json!({"posting_number":"posting","status":"delivered","products":[{"sku":1,"quantity":0}]}),
        ] {
            assert!(
                PostingSalesAccumulator::default()
                    .absorb_page(
                        PostingScheme::Fbo,
                        &json!({"postings":[posting],"has_next":false,"cursor":""}),
                    )
                    .is_err()
            );
        }
    }

    #[test]
    fn every_safe_error_code_and_terminal_cursor_validation_are_stable() {
        for (error, code) in [
            (PostingSalesError::Shape, "invalid_shape"),
            (PostingSalesError::Value, "invalid_value"),
            (PostingSalesError::TooManyRows, "too_many_rows"),
            (PostingSalesError::DuplicatePosting, "duplicate_posting"),
            (PostingSalesError::Overflow, "overflow"),
        ] {
            assert_eq!(error.code(), code);
        }
        assert_eq!(
            PostingSalesAccumulator::default().absorb_page(
                PostingScheme::Fbo,
                &json!({"postings":[],"has_next":false,"cursor":"bad\nvalue"}),
            ),
            Err(PostingSalesError::Value)
        );
    }

    #[test]
    fn posting_product_and_aggregate_bounds_fail_closed() {
        let too_many_postings = (0..=MAX_POSTINGS_PER_PAGE)
            .map(|index| {
                json!({
                    "posting_number": format!("posting-{index}"),
                    "status": "delivered",
                    "products": []
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            PostingSalesAccumulator::default().absorb_page(
                PostingScheme::Fbo,
                &json!({"postings":too_many_postings,"has_next":false,"cursor":""}),
            ),
            Err(PostingSalesError::TooManyRows)
        );

        let too_many_products = (0..=MAX_PRODUCTS_PER_PAGE)
            .map(|_| json!({"sku": 1, "quantity": 1}))
            .collect::<Vec<_>>();
        assert_eq!(
            PostingSalesAccumulator::default().absorb_page(
                PostingScheme::Fbo,
                &json!({
                    "postings":[{
                        "posting_number":"posting-products",
                        "status":"delivered",
                        "products":too_many_products
                    }],
                    "has_next":false,
                    "cursor":""
                }),
            ),
            Err(PostingSalesError::TooManyRows)
        );

        let too_many_skus = (1..=u64::try_from(MAX_AGGREGATED_SKUS + 1).unwrap())
            .map(|sku| json!({"sku": sku, "quantity": 1}))
            .collect::<Vec<_>>();
        assert_eq!(
            PostingSalesAccumulator::default().absorb_page(
                PostingScheme::Fbo,
                &json!({
                    "postings":[{
                        "posting_number":"posting-skus",
                        "status":"delivered",
                        "products":too_many_skus
                    }],
                    "has_next":false,
                    "cursor":""
                }),
            ),
            Err(PostingSalesError::TooManyRows)
        );
    }

    #[test]
    fn fbs_cancelled_units_remain_separate_from_non_cancelled_units() {
        let mut aggregate = PostingSalesAccumulator::default();
        aggregate
            .absorb_page(
                PostingScheme::Fbs,
                &json!({
                    "postings":[{
                        "posting_number":"fbs-cancelled",
                        "status":"cancelled",
                        "products":[{"sku":9,"quantity":2}]
                    }],
                    "has_next":false,
                    "cursor":""
                }),
            )
            .unwrap();

        let (totals, rows) = aggregate.finish().unwrap();
        assert_eq!(totals.fbs_cancelled_units, 2);
        assert_eq!(totals.total_non_cancelled_units, 0);
        assert_eq!(rows[0].fbs_cancelled_units, 2);
        assert_eq!(rows[0].fbs_status_units["cancelled"], 2);
    }
}
