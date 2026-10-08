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

/// Fields whose string values are prose written by buyers or sellers: review
/// and question texts, their pros/cons and answers. Contact details typed into
/// them are masked in place; the rest of the text stays analyzable.
const FREE_TEXT_FIELDS: &[&str] = &[
    "text", "comment", "pros", "cons", "answer", "question", "message", "body",
];

pub(super) fn redact_marketplace_pii(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for (field, value) in object {
                if is_sensitive_marketplace_field(field) {
                    *value = Value::String(REDACTED_VALUE.to_owned());
                } else if let Value::String(text) = value
                    && FREE_TEXT_FIELDS
                        .iter()
                        .any(|candidate| field.eq_ignore_ascii_case(candidate))
                {
                    if let Some(masked) = mask_contacts(text) {
                        *text = masked;
                    }
                } else {
                    redact_marketplace_pii(value);
                }
            }
        }
        Value::Array(values) => values.iter_mut().for_each(redact_marketplace_pii),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

/// Masks e-mail addresses and phone numbers inside free text, or returns
/// `None` when there is nothing to mask.
///
/// Phone matching is deliberately narrow so that order, posting and article
/// numbers survive: an international `+` number with 10..=15 digits, or an
/// 11-digit Russian number that starts with 7 or 8. Every boundary is an ASCII
/// byte, so the byte offsets below always split valid UTF-8.
fn mask_contacts(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut spans = email_spans(text);
    spans.extend(phone_spans(bytes));
    if spans.is_empty() {
        return None;
    }
    spans.sort_unstable();
    let mut masked = String::with_capacity(text.len());
    let mut written = 0;
    for (start, end) in spans {
        if start < written {
            // Overlapping matches, such as a phone used as an e-mail local
            // part, extend the mask already written instead of leaking a tail.
            written = written.max(end);
            continue;
        }
        masked.push_str(&text[written..start]);
        masked.push_str(REDACTED_VALUE);
        written = end;
    }
    masked.push_str(&text[written..]);
    Some(masked)
}

fn email_spans(text: &str) -> Vec<(usize, usize)> {
    let bytes = text.as_bytes();
    let local = |byte: u8| byte.is_ascii_alphanumeric() || b"._%+-".contains(&byte);
    let domain = |byte: u8| byte.is_ascii_alphanumeric() || b".-".contains(&byte);
    let mut spans = Vec::new();
    for (at, _) in text.match_indices('@') {
        let start = bytes[..at]
            .iter()
            .rposition(|byte| !local(*byte))
            .map_or(0, |position| position + 1);
        let mut end = bytes[at + 1..]
            .iter()
            .position(|byte| !domain(*byte))
            .map_or(bytes.len(), |position| at + 1 + position);
        while end > at + 1 && bytes[end - 1] == b'.' {
            end -= 1;
        }
        let host = &text[at + 1..end];
        let top_level = host.rsplit('.').next().unwrap_or_default();
        if start < at
            && host.contains('.')
            && host.split('.').all(|label| !label.is_empty())
            && top_level.len() >= 2
            && top_level.bytes().all(|byte| byte.is_ascii_alphabetic())
        {
            spans.push((start, end));
        }
    }
    spans
}

fn phone_spans(bytes: &[u8]) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        let at_boundary = index == 0 || !bytes[index - 1].is_ascii_alphanumeric();
        if !(at_boundary && (byte == b'+' || byte.is_ascii_digit())) {
            index += 1;
            continue;
        }
        let international = byte == b'+';
        let mut cursor = index + usize::from(international);
        let (mut digits, mut first_digit, mut end) = (0_usize, None, cursor);
        while cursor < bytes.len() {
            let current = bytes[cursor];
            if current.is_ascii_digit() {
                digits += 1;
                first_digit.get_or_insert(current);
                end = cursor + 1;
            } else if !b" -()".contains(&current) || cursor - end >= 2 {
                break;
            }
            cursor += 1;
        }
        let bounded = end == bytes.len() || !bytes[end].is_ascii_alphanumeric();
        let phone = if international {
            (10..=15).contains(&digits)
        } else {
            digits == 11 && matches!(first_digit, Some(b'7' | b'8'))
        };
        if bounded && phone {
            spans.push((index, end));
        }
        index = end.max(index + 1);
    }
    spans
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contacts_inside_free_text_are_masked_and_identifiers_survive() {
        for (text, expected) in [
            (
                "Пишите на ivan.petrov+shop@mail.ru.",
                "Пишите на [REDACTED].",
            ),
            (
                "звоните +7 (916) 123-45-67 вечером",
                "звоните [REDACTED] вечером",
            ),
            ("тел. 8 916 123 45 67", "тел. [REDACTED]"),
            ("89161234567, спасибо", "[REDACTED], спасибо"),
            ("+44 20 7946 0958", "[REDACTED]"),
            ("a@b.co и c@d.org", "[REDACTED] и [REDACTED]"),
            ("79161234567@mail.ru!", "[REDACTED]!"),
        ] {
            assert_eq!(mask_contacts(text).as_deref(), Some(expected), "{text}");
        }
        for text in [
            "заказ 12345678-0012-1 пришёл",
            "артикул 123456789, цена 1 234 567 ₽",
            "SKU 1234567890 в 2026 году",
            "77777777777777 слишком длинный номер",
            "почта @ не указана, user@localhost",
            "a7916123456789",
            "",
        ] {
            assert_eq!(mask_contacts(text), None, "{text}");
        }
    }

    #[test]
    fn only_free_text_fields_are_scanned_inside_marketplace_payloads() {
        let mut value = serde_json::json!({
            "text": "Мой номер +79161234567",
            "pros": "ответьте на mail@example.ru",
            "answer": {"text": "Звоните 8-916-123-45-67"},
            "offer_id": "79161234567",
            "rating": 5,
            "items": [{"comment": "a@b.co"}]
        });
        redact_marketplace_pii(&mut value);
        assert_eq!(value["text"], "Мой номер [REDACTED]");
        assert_eq!(value["pros"], "ответьте на [REDACTED]");
        assert_eq!(value["answer"]["text"], "Звоните [REDACTED]");
        assert_eq!(value["offer_id"], "79161234567");
        assert_eq!(value["rating"], 5);
        assert_eq!(value["items"][0]["comment"], "[REDACTED]");
    }

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
