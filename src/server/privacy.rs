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

/// For each lowercase ASCII letter, the bit set of fragments that start with it.
///
/// Redaction checks every key of every marketplace response, and keys repeat
/// thousands of times per page. Starting a comparison only where the key's
/// byte can begin a fragment skips most of the 16 fragment scans per position.
const FRAGMENTS_BY_FIRST_LETTER: [u32; 26] = {
    assert!(SENSITIVE_FIELD_FRAGMENTS.len() <= 32);
    let mut index = [0; 26];
    let mut position = 0;
    while position < SENSITIVE_FIELD_FRAGMENTS.len() {
        let first = SENSITIVE_FIELD_FRAGMENTS[position].as_bytes()[0];
        assert!(first.is_ascii_lowercase());
        index[(first - b'a') as usize] |= 1 << position;
        position += 1;
    }
    index
};

fn contains_sensitive_fragment(field: &[u8]) -> bool {
    field.iter().enumerate().any(|(start, byte)| {
        let byte = byte.to_ascii_lowercase();
        if !byte.is_ascii_lowercase() {
            return false;
        }
        let mut candidates = FRAGMENTS_BY_FIRST_LETTER[usize::from(byte - b'a')];
        while candidates != 0 {
            let fragment = SENSITIVE_FIELD_FRAGMENTS[candidates.trailing_zeros() as usize];
            if field
                .get(start..start + fragment.len())
                .is_some_and(|window| window.eq_ignore_ascii_case(fragment.as_bytes()))
            {
                return true;
            }
            candidates &= candidates - 1;
        }
        false
    })
}

pub(super) fn is_sensitive_marketplace_field(field: &str) -> bool {
    contains_sensitive_fragment(field.as_bytes())
        || SENSITIVE_EXACT_FIELDS
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The scan every fragment at every position that the index replaced.
    fn reference(field: &str) -> bool {
        SENSITIVE_FIELD_FRAGMENTS.iter().any(|fragment| {
            field
                .as_bytes()
                .windows(fragment.len())
                .any(|window| window.eq_ignore_ascii_case(fragment.as_bytes()))
        }) || SENSITIVE_EXACT_FIELDS
            .iter()
            .any(|candidate| field.eq_ignore_ascii_case(candidate))
    }

    #[test]
    fn indexed_matching_decides_exactly_like_a_full_scan() {
        let mut fields: Vec<String> = SENSITIVE_FIELD_FRAGMENTS
            .iter()
            .chain(SENSITIVE_EXACT_FIELDS)
            .flat_map(|word| {
                [
                    (*word).to_owned(),
                    word.to_ascii_uppercase(),
                    format!("x_{word}_y"),
                    format!("{word}s"),
                    word[1..].to_owned(),
                    word[..word.len() - 1].to_owned(),
                ]
            })
            .collect();
        fields.extend(
            [
                "",
                "_",
                "9zip",
                "ZIP_CODE",
                "Zi",
                "offer_id",
                "sku",
                "winner",
                "period",
                "translate",
                "cards",
                "customers_count",
                "ЛАТ",
                "адрес_zip",
                "İnn",
                "posting_number",
                "delivery_method",
                "moderate_status",
                "commissions",
            ]
            .map(str::to_owned),
        );
        fields.push("customer".repeat(20));
        fields.push("Ф".repeat(40));
        for field in fields {
            assert_eq!(
                is_sensitive_marketplace_field(&field),
                reference(&field),
                "{field:?}"
            );
        }
    }
}
