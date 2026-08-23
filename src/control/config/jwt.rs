use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::config::JwtConfig;

use super::validation::value_or;

pub(super) const CONTROL_REQUIRED_SCOPE: &str = "mcp:ads-control";
pub(super) const CONTROL_INTERNAL_JWKS_URL: &str = "http://control-auth-egress:8080/jwks";

pub(super) fn load_jwt_config(lookup: &mut dyn FnMut(&str) -> Option<String>) -> Result<JwtConfig> {
    let issuer = required(lookup, "CONTROL_MCP_JWT_ISSUER")?
        .trim_end_matches('/')
        .to_owned();
    validate_https_url("CONTROL_MCP_JWT_ISSUER", &issuer)?;
    let resource_url = normalize_url(
        "CONTROL_MCP_PUBLIC_URL",
        &required(lookup, "CONTROL_MCP_PUBLIC_URL")?,
    )?;
    let audience = normalize_url(
        "CONTROL_MCP_JWT_AUDIENCE",
        &required(lookup, "CONTROL_MCP_JWT_AUDIENCE")?,
    )?;
    if audience != resource_url {
        bail!("CONTROL_MCP_JWT_AUDIENCE должен точно совпадать с CONTROL_MCP_PUBLIC_URL");
    }
    let jwks_url = lookup("CONTROL_MCP_JWT_JWKS_URL")
        .unwrap_or_else(|| format!("{issuer}/protocol/openid-connect/certs"));
    validate_jwks_url(&jwks_url)?;
    let scopes = value_or(
        lookup,
        "CONTROL_MCP_JWT_REQUIRED_SCOPES",
        CONTROL_REQUIRED_SCOPE,
    );
    if scopes != CONTROL_REQUIRED_SCOPE {
        bail!(
            "CONTROL_MCP_JWT_REQUIRED_SCOPES должен быть ровно {CONTROL_REQUIRED_SCOPE}; analytics scope не подходит"
        );
    }
    let ttl = value_or(lookup, "CONTROL_MCP_JWKS_CACHE_TTL_SECONDS", "300")
        .parse::<u64>()
        .context("CONTROL_MCP_JWKS_CACHE_TTL_SECONDS должен быть целым числом")?;
    if !(30..=86_400).contains(&ttl) {
        bail!("CONTROL_MCP_JWKS_CACHE_TTL_SECONDS должен быть от 30 до 86400");
    }
    let mut metadata_url =
        reqwest::Url::parse(&resource_url).expect("normalized control public URL remains valid");
    metadata_url.set_path("/.well-known/oauth-protected-resource");
    metadata_url.set_query(None);
    metadata_url.set_fragment(None);
    Ok(JwtConfig {
        issuer,
        audience,
        jwks_url,
        resource_url,
        resource_metadata_url: metadata_url.to_string(),
        required_scopes: vec![CONTROL_REQUIRED_SCOPE.to_owned()],
        jwks_cache_ttl: Duration::from_secs(ttl),
    })
}

fn required(lookup: &mut dyn FnMut(&str) -> Option<String>, key: &str) -> Result<String> {
    lookup(key).with_context(|| format!("{key} обязателен в JWT-режиме"))
}

fn validate_https_url(name: &str, value: &str) -> Result<()> {
    let parsed = reqwest::Url::parse(value).with_context(|| format!("{name} должен быть URL"))?;
    if parsed.scheme() != "https"
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        bail!("{name} должен быть абсолютным HTTPS URL без credentials/query/fragment");
    }
    Ok(())
}

fn validate_jwks_url(value: &str) -> Result<()> {
    if value == CONTROL_INTERNAL_JWKS_URL {
        return Ok(());
    }
    validate_https_url("CONTROL_MCP_JWT_JWKS_URL", value).with_context(|| {
        format!(
            "CONTROL_MCP_JWT_JWKS_URL должен использовать HTTPS или точный внутренний адрес {CONTROL_INTERNAL_JWKS_URL}"
        )
    })
}

fn normalize_url(name: &str, value: &str) -> Result<String> {
    validate_https_url(name, value)?;
    Ok(reqwest::Url::parse(value)
        .expect("validated URL parses")
        .to_string())
}
