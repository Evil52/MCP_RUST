//! Structured customer data redaction, preserving analytical fields.
use super::{REDACTED_VALUE, Value};

/// Field-name fragments that mark a value as identifying wherever they appear.
///
/// These are matched as substrings so that composite names — `recipient_name`,
/// `customer_full_name`, `delivery_phone` — are covered too. Person-denoting
/// tokens belong here rather than in [`SENSITIVE_EXACT_FIELDS`] precisely
/// because vendors attach suffixes freely and the schema can change without
/// notice; over-redacting an aggregate such as `customers_count` is the correct
/// trade for a release gate.
const SENSITIVE_FIELD_FRAGMENTS: &[&str] = &[
    "address",
    "birth",
    "buyer",
    "contact",
    "coordinate",
    "customer",
    "email",
    "latitude",
    "longitude",
    "passport",
    "phone",
    "postal",
    "postcode",
    "recipient",
    "snils",
    "zip",
];

/// Field names that are identifying only as a whole.
///
/// Each of these is too short or too common to match as a substring: `inn`
/// occurs inside `winner`, `rid` inside `period` and `grid`, `lat` inside
/// `translate`, and `card` inside the `cards` array that `wb_product_cards`
/// returns as its entire payload.
const SENSITIVE_EXACT_FIELDS: &[&str] = &[
    "card_number",
    "cardnumber",
    "fio",
    "gnumber",
    "inn",
    "kpp",
    "lat",
    "lon",
    "odid",
    "ogrn",
    "pan",
    "payment_card",
    "rid",
    "srid",
    "ssn",
    "tin",
    "username",
    "user_name",
    "lastordershkid",
];

pub(super) fn is_sensitive_marketplace_field(field: &str) -> bool {
    SENSITIVE_FIELD_FRAGMENTS.iter().any(|fragment| {
        field
            .as_bytes()
            .windows(fragment.len())
            .any(|window| window.eq_ignore_ascii_case(fragment.as_bytes()))
    }) || SENSITIVE_EXACT_FIELDS
        .iter()
        .any(|candidate| field.eq_ignore_ascii_case(candidate))
}

pub(super) fn redact_marketplace_pii(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for (field, value) in object {
                if is_sensitive_marketplace_field(field) {
                    *value = Value::String(REDACTED_VALUE.to_owned());
                } else {
                    redact_marketplace_pii(value);
                }
            }
        }
        Value::Array(values) => values.iter_mut().for_each(redact_marketplace_pii),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}
