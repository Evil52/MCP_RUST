//! Parsing of the `Authorization` request header before any JWT work.

use super::*;

#[tokio::test]
async fn malformed_authorization_headers_are_rejected_before_key_lookup() {
    // An unroutable JWKS URL proves that none of these headers reaches it.
    let auth = JwtAuthenticator::new(config("http://127.0.0.1:1".to_owned()), registry()).unwrap();
    assert_eq!(
        auth.authenticate(&HeaderMap::new()).await.unwrap_err(),
        JwtAuthenticationFailure::MissingCredentials
    );

    for value in [
        "Basic abc",
        "Bearer ",
        "bearer   ",
        "Bearer not-a-jwt",
        "Bearertoken",
        "Bear",
    ] {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_str(value).unwrap());
        assert_eq!(
            auth.authenticate(&headers).await.unwrap_err(),
            JwtAuthenticationFailure::InvalidToken,
            "{value:?}"
        );
    }
    let mut invalid_text = HeaderMap::new();
    invalid_text.insert(
        AUTHORIZATION,
        HeaderValue::from_bytes(b"Bearer \xff").unwrap(),
    );
    assert_eq!(
        auth.authenticate(&invalid_text).await.unwrap_err(),
        JwtAuthenticationFailure::InvalidToken
    );
}

/// RFC 9110 section 11.1: clients may send the scheme in any letter case.
#[tokio::test]
async fn the_bearer_scheme_is_case_insensitive() {
    let (base_url, _) = mock_http(vec![(200, jwks())]);
    let auth = JwtAuthenticator::new(config(base_url), registry()).unwrap();
    let valid = token(Some(KID), "ozonofk-mcp", "admin");
    for scheme in ["Bearer", "bearer", "BEARER"] {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("{scheme} {valid}")).unwrap(),
        );
        assert_eq!(
            auth.authenticate(&headers).await.unwrap().actor_id,
            "admin",
            "{scheme}"
        );
    }
}
