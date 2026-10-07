//! One grammar for the local actor and account identifiers.
//!
//! These identifiers leave the access registry for audit rows, refresh queues,
//! snapshot keys and report object keys. Each of those boundaries used to
//! restate its own byte check, while the registry itself accepted any
//! non-empty string. A registry entry such as `shop.ru` therefore loaded
//! successfully and then made every tool call for that account fail at the
//! first downstream check, with an error that named the wrong component.
//! Validating the registry against the same grammar the boundaries enforce
//! turns that into one explicit startup error.

/// Longest accepted actor or account identifier, in bytes.
pub const MAX_IDENTIFIER_BYTES: usize = 128;

/// Account identifiers become database keys and report object-key segments,
/// so they stay within the narrow ASCII set every consumer accepts.
#[must_use]
pub fn is_account_id(value: &str) -> bool {
    is_bounded_ascii(value, b"_-")
}

/// Actor identifiers may additionally carry the punctuation of e-mail-like
/// and namespaced names (`.`, `:`, `@`). The audit and refresh boundaries
/// accept exactly this set.
#[must_use]
pub fn is_actor_id(value: &str) -> bool {
    is_bounded_ascii(value, b"._:@-")
}

fn is_bounded_ascii(value: &str, punctuation: &[u8]) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || punctuation.contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::{MAX_IDENTIFIER_BYTES, is_account_id, is_actor_id};

    #[test]
    fn account_ids_use_the_storage_key_grammar() {
        for valid in ["example_ozon", "ofk-region-wb", "A1", &"a".repeat(128)] {
            assert!(is_account_id(valid), "{valid}");
        }
        for invalid in [
            "",
            "shop.ru",
            "shop ru",
            "магазин",
            "a@b",
            "a:b",
            &"a".repeat(MAX_IDENTIFIER_BYTES + 1),
        ] {
            assert!(!is_account_id(invalid), "{invalid}");
        }
    }

    #[test]
    fn actor_ids_also_allow_namespaced_punctuation() {
        for valid in ["admin", "ivan.petrov", "team:ops", "user@example", "a_b-c"] {
            assert!(is_actor_id(valid), "{valid}");
        }
        for invalid in [
            "",
            "Иван",
            "ivan petrov",
            "a/b",
            "a\nb",
            &"a".repeat(MAX_IDENTIFIER_BYTES + 1),
        ] {
            assert!(!is_actor_id(invalid), "{invalid}");
        }
    }
}
