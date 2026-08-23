use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use jsonwebtoken::dangerous::insecure_decode;
use serde::Deserialize;

use crate::{
    config::{AccessRegistry, Marketplace, is_canonical_uuid},
    control::{
        plan::validate_control_database_url,
        policy::{ControlMode, ControlPolicy},
    },
};

use super::{
    ControlAuthConfig, ControlPolicyDatabaseConfig, ControlWbRuntimeConfig,
    validation::{parse_strict_bool, value_or},
};

pub(super) const MAX_CONTROL_CREDENTIAL_BYTES: u64 = 16_384;
pub(super) const WB_PROMOTION_BIT: u64 = 1 << 6;
pub(super) const WB_READ_ONLY_BIT: u64 = 1 << 30;

#[derive(Deserialize)]
struct WbControlTokenClaims {
    acc: u8,
    #[serde(rename = "for")]
    token_for: Option<String>,
    t: Option<bool>,
    s: u64,
    exp: u64,
    sid: String,
}

pub(super) fn load_wb_runtime(
    lookup: &mut dyn FnMut(&str) -> Option<String>,
    auth: &ControlAuthConfig,
    policy: &ControlPolicy,
    registry: &AccessRegistry,
    policy_database: Option<&ControlPolicyDatabaseConfig>,
) -> Result<Option<ControlWbRuntimeConfig>> {
    let writes_enabled = parse_strict_bool(
        &value_or(lookup, "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED", "false"),
        "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED",
    )?;
    if policy.mode == ControlMode::Disabled {
        return Ok(None);
    }
    if !matches!(auth, ControlAuthConfig::Jwt(_)) {
        bail!("WB Control runtime разрешён только в JWT-режиме Control MCP");
    }
    let account_id = required_nonempty(lookup, "CONTROL_MCP_WB_ACCOUNT_ID")?;
    let account = registry
        .accounts
        .iter()
        .find(|account| account.id == account_id)
        .context("CONTROL_MCP_WB_ACCOUNT_ID отсутствует в access registry")?;
    if !matches!(account.marketplace, Marketplace::Wildberries) || account.wildberries.is_none() {
        bail!("CONTROL_MCP_WB_ACCOUNT_ID должен ссылаться на Wildberries account");
    }
    let expected_seller_sid = account
        .wildberries
        .as_ref()
        .and_then(|wildberries| wildberries.seller_sid.as_deref())
        .context("WB Control требует reviewed wildberries.seller_sid в access registry")?;
    if !policy.actors.iter().any(|actor| {
        actor
            .wb_promotion_bid_targets
            .iter()
            .any(|target| target.account_id == account_id)
    }) {
        bail!("CONTROL_MCP_WB_ACCOUNT_ID не имеет явных targets в control policy");
    }
    let reader_token_path = PathBuf::from(required_nonempty(
        lookup,
        "CONTROL_MCP_WB_PROMOTION_READ_TOKEN_FILE",
    )?);
    let reader_token = read_control_token(
        &reader_token_path,
        "CONTROL_MCP_WB_PROMOTION_READ_TOKEN_FILE",
    )?;
    validate_wb_reader_token(&reader_token, expected_seller_sid)?;
    let writer_token = if policy.mode == ControlMode::Enabled && writes_enabled {
        let writer_token_path = PathBuf::from(required_nonempty(
            lookup,
            "CONTROL_MCP_WB_PROMOTION_WRITE_TOKEN_FILE",
        )?);
        let token = read_control_token(
            &writer_token_path,
            "CONTROL_MCP_WB_PROMOTION_WRITE_TOKEN_FILE",
        )?;
        validate_wb_writer_token(&token, expected_seller_sid)?;
        Some(token)
    } else {
        None
    };
    let database = policy_database
        .context("CONTROL_MCP_DATABASE_URL обязателен для WB Control runtime")?
        .database
        .clone();
    let proxy_url = required_nonempty(lookup, "CONTROL_MCP_WB_PROXY")?;
    validate_proxy_url(&proxy_url)?;
    let timeout_seconds = value_or(lookup, "CONTROL_MCP_WB_TIMEOUT_SECONDS", "20")
        .parse::<u64>()
        .context("CONTROL_MCP_WB_TIMEOUT_SECONDS должен быть целым числом")?;
    if !(1..=30).contains(&timeout_seconds) {
        bail!("CONTROL_MCP_WB_TIMEOUT_SECONDS должен быть от 1 до 30");
    }
    Ok(Some(ControlWbRuntimeConfig {
        account_id,
        seller_sid: expected_seller_sid.to_owned(),
        reader_token,
        writer_token,
        database,
        proxy_url,
        request_timeout: Duration::from_secs(timeout_seconds),
    }))
}

pub(super) fn load_policy_database(
    lookup: &mut dyn FnMut(&str) -> Option<String>,
) -> Result<Option<ControlPolicyDatabaseConfig>> {
    let Some(database_url) = lookup("CONTROL_MCP_DATABASE_URL") else {
        return Ok(None);
    };
    if database_url.is_empty() || database_url.trim() != database_url {
        bail!("CONTROL_MCP_DATABASE_URL должен быть непустым URL без внешнего whitespace");
    }
    let database = validate_control_database_url(&database_url).map_err(|_| {
        anyhow::anyhow!(
            "CONTROL_MCP_DATABASE_URL должен использовать restricted role control_writer и один TCP host"
        )
    })?;
    Ok(Some(ControlPolicyDatabaseConfig { database }))
}

fn required_nonempty(lookup: &mut dyn FnMut(&str) -> Option<String>, key: &str) -> Result<String> {
    lookup(key)
        .filter(|value| !value.is_empty() && value.trim() == value)
        .with_context(|| format!("{key} обязателен для WB Control runtime"))
}

pub(super) fn read_control_token(path: &Path, variable_name: &str) -> Result<String> {
    let file = File::open(path)
        .with_context(|| format!("не удалось прочитать {variable_name} {}", path.display()))?;
    let metadata = file
        .metadata()
        .context("не удалось проверить WB token file")?;
    if !metadata.is_file() || metadata.len() > MAX_CONTROL_CREDENTIAL_BYTES {
        bail!("WB token file должен быть обычным файлом безопасного размера");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.mode() & 0o077 != 0 {
            bail!("WB token file не должен быть доступен group/other (ожидается chmod 600/400)");
        }
    }
    let mut bytes = Vec::new();
    file.take(MAX_CONTROL_CREDENTIAL_BYTES + 1)
        .read_to_end(&mut bytes)
        .context("не удалось прочитать WB token file")?;
    normalize_control_token_bytes(bytes)
}

pub(super) fn normalize_control_token_bytes(mut bytes: Vec<u8>) -> Result<String> {
    if bytes.len() as u64 > MAX_CONTROL_CREDENTIAL_BYTES {
        bail!("WB token file превышает безопасный лимит");
    }
    if bytes.ends_with(b"\r\n") {
        bytes.truncate(bytes.len() - 2);
    } else if bytes.ends_with(b"\n") {
        bytes.pop();
    }
    let token = String::from_utf8(bytes).context("WB token file должен быть UTF-8")?;
    if token.is_empty()
        || !token.is_ascii()
        || token
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        bail!("WB promotion token имеет недопустимый формат");
    }
    Ok(token)
}

fn decode_wb_control_token(token: &str, purpose: &str) -> Result<WbControlTokenClaims> {
    let claims = insecure_decode::<WbControlTokenClaims>(token)
        .with_context(|| format!("WB promotion {purpose} token должен быть корректным JWT"))?
        .claims;
    if claims.acc != 3 || claims.token_for.as_deref() != Some("self") || claims.t != Some(false) {
        bail!("WB promotion {purpose} token должен быть Personal production token");
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("системное время находится до Unix epoch")?
        .as_secs();
    if claims.exp <= now.saturating_add(300) {
        bail!("WB promotion {purpose} token истёк или истекает менее чем через 5 минут");
    }
    Ok(claims)
}

pub(super) fn validate_wb_reader_token(token: &str, expected_seller_sid: &str) -> Result<()> {
    let claims = decode_wb_control_token(token, "read")?;
    validate_wb_token_seller(&claims, expected_seller_sid, "read")?;
    if claims.s != (WB_PROMOTION_BIT | WB_READ_ONLY_BIT) {
        bail!(
            "WB promotion read token должен быть узким: только категория Продвижение в режиме чтения"
        );
    }
    Ok(())
}

pub(super) fn validate_wb_writer_token(token: &str, expected_seller_sid: &str) -> Result<()> {
    let claims = decode_wb_control_token(token, "write")?;
    validate_wb_token_seller(&claims, expected_seller_sid, "write")?;
    if claims.s != WB_PROMOTION_BIT {
        bail!(
            "WB promotion write token должен быть узким: только категория Продвижение с чтением и записью"
        );
    }
    Ok(())
}

fn validate_wb_token_seller(
    claims: &WbControlTokenClaims,
    expected_seller_sid: &str,
    purpose: &str,
) -> Result<()> {
    if !is_canonical_uuid(&claims.sid)
        || !is_canonical_uuid(expected_seller_sid)
        || claims.sid != expected_seller_sid
    {
        bail!("WB promotion {purpose} token принадлежит другому seller sid");
    }
    Ok(())
}

pub(super) fn validate_proxy_url(value: &str) -> Result<()> {
    let url = reqwest::Url::parse(value).context("CONTROL_MCP_WB_PROXY должен быть URL")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        bail!("CONTROL_MCP_WB_PROXY должен быть origin URL без credentials/path/query/fragment");
    }
    Ok(())
}
