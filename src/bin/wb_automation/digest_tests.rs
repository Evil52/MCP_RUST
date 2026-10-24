use super::{is_lower_sha256, sha256_domain};

#[test]
fn domain_separated_digests_are_stable_lower_hex() {
    let digest = sha256_domain("wb-automation-policy", br#"{"campaign_id":1}"#);
    // Independently computed: SHA-256("wb-automation-policy" || 0x00 || payload).
    assert_eq!(
        digest,
        "2bebf3194d5d1ed55ab6f27b186f461c47bf83732c89d0bd1c458ad1e1eb313e"
    );
    assert!(is_lower_sha256(&digest));
    // The separator keeps a domain/payload boundary from moving unnoticed.
    assert_ne!(sha256_domain("ab", b"c"), sha256_domain("a", b"bc"));
}

#[test]
fn only_exact_lowercase_sha256_hex_is_accepted() {
    let digest = sha256_domain("wb", b"value");
    for invalid in [
        digest.to_uppercase(),
        digest[..63].to_owned(),
        format!("{digest}0"),
        format!("{}g", &digest[..63]),
        String::new(),
    ] {
        assert!(!is_lower_sha256(&invalid), "{invalid}");
    }
}
