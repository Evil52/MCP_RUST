#![expect(
    clippy::verbose_bit_mask,
    reason = "explicit octal Unix permission masks make the privacy boundary auditable"
)]

use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::Path,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use jsonwebtoken::dangerous::insecure_decode;
use mcp_ozon::{
    config::{Marketplace, RegistrySource, Role},
    wb::{WbClient, WbCredentials},
};
use serde::Deserialize;

use super::{Arguments, Egress, checkpoint::sha256};

const MAX_SECRET_BYTES: u64 = 16_384;

#[derive(Deserialize)]
struct WbCapabilities {
    acc: u8,
    s: u64,
    exp: i64,
    sid: String,
}

#[derive(PartialEq, Eq)]
pub struct CredentialIdentity {
    pub seller_scope: String,
    binding: String,
    fingerprint: String,
}

impl CredentialIdentity {
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
}

pub struct ScopedClient {
    pub client: WbClient,
    pub identity: CredentialIdentity,
}

pub fn resolve(arguments: &Arguments) -> Result<ScopedClient> {
    let registry = RegistrySource::new(&arguments.registry)
        .map_err(|_| anyhow::anyhow!("access registry is invalid"))?
        .load()
        .map_err(|_| anyhow::anyhow!("access registry is unavailable"))?;
    let actor = registry
        .actor(&arguments.actor)
        .map_err(|_| anyhow::anyhow!("operator identity is unavailable"))?;
    ensure!(
        matches!(actor.role, Role::Finance | Role::Admin),
        "finance or admin role is required"
    );
    let account = registry
        .accounts
        .iter()
        .find(|account| account.id == arguments.account)
        .context("account is unavailable")?;
    ensure!(
        account.marketplace == Marketplace::Wildberries && actor.can_access_account(account),
        "operator is not authorized for this WB account"
    );
    let binding = account
        .wildberries
        .as_ref()
        .context("WB credential binding is unavailable")?;
    let token = read_secret(&arguments.credentials_dir, &binding.api_token_env)?;
    let claims = insecure_decode::<WbCapabilities>(&token)
        .map_err(|_| anyhow::anyhow!("WB key capability claims are invalid"))?
        .claims;
    // Claims are local capability hints; only the official method call proves
    // current access. This operator deliberately accepts read-only Personal keys.
    ensure!(
        claims.acc == 3 && claims.s & (1 << 30) != 0 && claims.s & (1 << 13) != 0,
        "a Personal read-only WB key with Finance scope is required"
    );
    ensure!(
        claims.exp > chrono::Utc::now().timestamp(),
        "WB key is expired"
    );
    ensure!(
        claims.sid.len() == 36
            && claims
                .sid
                .bytes()
                .all(|b| b.is_ascii_hexdigit() || b == b'-'),
        "WB seller identity is invalid"
    );
    ensure!(
        binding.seller_sid.as_deref() == Some(claims.sid.as_str()),
        "a registered WB seller identity matching this key is required"
    );
    let identity = CredentialIdentity {
        seller_scope: sha256(claims.sid.as_bytes()),
        binding: binding.api_token_env.clone(),
        fingerprint: sha256(token.as_bytes()),
    };
    let credentials = BTreeMap::from([(arguments.account.clone(), WbCredentials { token })]);
    let client = match arguments.egress {
        Egress::Direct => WbClient::new(Duration::from_secs(45), credentials),
        Egress::CollectorProxy => WbClient::new_with_https_proxy(
            Duration::from_secs(45),
            credentials,
            "http://ozon-egress:3128",
        )
        .map_err(|_| anyhow::anyhow!("collector proxy configuration is unavailable"))?,
    };
    Ok(ScopedClient { client, identity })
}

fn read_secret(directory: &Path, name: &str) -> Result<String> {
    ensure!(
        !name.is_empty()
            && name.len() <= 128
            && name
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_'),
        "credential binding is invalid"
    );
    let root_meta =
        fs::symlink_metadata(directory).context("credential directory is unavailable")?;
    ensure!(
        root_meta.is_dir() && !root_meta.file_type().is_symlink(),
        "credential directory must be a real directory"
    );
    ensure!(
        root_meta.permissions().mode() & 0o077 == 0,
        "credential directory must be private (0700)"
    );
    let root = directory
        .canonicalize()
        .context("credential directory is unavailable")?;
    let path = root.join(name);
    let metadata =
        fs::symlink_metadata(&path).context("scoped WB credential file is unavailable")?;
    ensure!(
        metadata.is_file()
            && !metadata.file_type().is_symlink()
            && metadata.nlink() == 1
            && metadata.len() <= MAX_SECRET_BYTES
            && metadata.permissions().mode() & 0o077 == 0,
        "scoped WB credential must be a bounded private regular file"
    );
    let canonical = path
        .canonicalize()
        .context("scoped WB credential is unavailable")?;
    ensure!(
        canonical.parent() == Some(root.as_path()),
        "credential escaped its directory"
    );
    let mut bytes = Vec::new();
    fs::File::open(canonical)
        .context("scoped WB credential is unavailable")?
        .take(MAX_SECRET_BYTES + 1)
        .read_to_end(&mut bytes)
        .context("scoped WB credential cannot be read")?;
    ensure!(
        bytes.len() as u64 <= MAX_SECRET_BYTES,
        "scoped WB credential exceeds its bound"
    );
    let token = String::from_utf8(bytes).context("WB credential must be ASCII")?;
    let token = token.trim_end_matches(['\n', '\r']);
    ensure!(
        !token.is_empty() && token.bytes().all(|b| (0x21..=0x7e).contains(&b)),
        "WB credential contains invalid characters"
    );
    Ok(token.to_owned())
}
