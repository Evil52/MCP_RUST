use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail, ensure};

use crate::{
    config::{AccessRegistry, Marketplace, Role},
    control::{
        policy::{ControlMode, ControlPolicy},
        wb_launch::wb_campaign_profile_runtime,
    },
};

use super::{
    ControlAuthConfig, ControlWbCampaignRuntimeConfig,
    validation::{parse_strict_bool, value_or},
    wb_runtime::{read_control_token, validate_wb_reader_token, validate_wb_writer_token},
};

pub(super) fn load_wb_campaign_runtime(
    lookup: &mut dyn FnMut(&str) -> Option<String>,
    auth: &ControlAuthConfig,
    policy: &ControlPolicy,
    registry: &AccessRegistry,
    registry_path: &str,
) -> Result<Option<ControlWbCampaignRuntimeConfig>> {
    let root = lookup("CONTROL_MCP_WB_CAMPAIGN_ROOT");
    let account_id = lookup("CONTROL_MCP_WB_CAMPAIGN_ACCOUNT_ID");
    if root.is_none() && account_id.is_none() {
        return Ok(None);
    }
    if !matches!(auth, ControlAuthConfig::Jwt(_)) {
        bail!("WB campaign MCP требует JWT authentication");
    }
    let root = PathBuf::from(root.context("CONTROL_MCP_WB_CAMPAIGN_ROOT обязателен")?);
    ensure!(
        root.is_absolute(),
        "WB campaign root должен быть абсолютным путём"
    );
    let metadata = fs::symlink_metadata(&root)?;
    ensure!(
        metadata.is_dir() && metadata.permissions().mode().trailing_zeros() >= 6,
        "WB campaign root должен быть приватным каталогом 0700"
    );
    let account_id = account_id
        .filter(|value| !value.is_empty() && value.trim() == value)
        .context("CONTROL_MCP_WB_CAMPAIGN_ACCOUNT_ID обязателен")?;
    let account = registry
        .accounts
        .iter()
        .find(|account| account.id == account_id)
        .context("WB campaign account отсутствует в access registry")?;
    ensure!(
        matches!(account.marketplace, Marketplace::Wildberries),
        "нужен WB account"
    );
    let seller_sid = account
        .wildberries
        .as_ref()
        .and_then(|wb| wb.seller_sid.as_deref())
        .context("WB campaign требует seller SID в access registry")?;
    let profile_path = root.join("profile.json");
    let profile = wb_campaign_profile_runtime(&profile_path)?;
    ensure!(
        profile.account_id == account_id,
        "WB campaign profile account не совпадает с runtime"
    );
    let actor = registry.actor(&profile.actor_id)?;
    ensure!(
        actor.role == Role::Admin && actor.can_access_account(account),
        "WB campaign profile actor должен быть администратором этого account"
    );
    ensure!(
        profile.registry == Path::new(registry_path)
            && profile.robot_template == root.join("robot-template.json")
            && profile.journal_directory == root.join("journal")
            && profile.campaigns_directory == root.join("campaigns"),
        "WB campaign profile должен использовать фиксированные runtime пути"
    );
    for path in [&profile.journal_directory, &profile.campaigns_directory] {
        let metadata = fs::symlink_metadata(path)?;
        ensure!(
            metadata.is_dir() && metadata.permissions().mode().trailing_zeros() >= 6,
            "WB campaign journal/campaigns должны быть приватными каталогами 0700"
        );
    }
    let reader = read_control_token(&profile.reader_token, "WB_CAMPAIGN_READER")?;
    validate_wb_reader_token(&reader, seller_sid, profile.allow_broad_reader)?;
    let writes_enabled = parse_strict_bool(
        &value_or(lookup, "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED", "false"),
        "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED",
    )? && policy.mode == ControlMode::Enabled;
    if writes_enabled {
        let writer = read_control_token(&profile.writer_token, "WB_CAMPAIGN_WRITER")?;
        validate_wb_writer_token(&writer, seller_sid)?;
    }
    Ok(Some(ControlWbCampaignRuntimeConfig {
        account_id,
        actor_id: profile.actor_id,
        root,
        writes_enabled,
    }))
}
