use std::path::{Path, PathBuf};

use chrono::Utc;
use serde_json::{Value, json};

use crate::{
    config::{Marketplace, Role},
    control::{
        config::ControlWbCampaignRuntimeConfig,
        policy::ControlMode,
        wb_launch::{
            export_wb_campaign, prepare_wb_campaign_from_request, run_wb_campaign_launch_scoped,
            wb_campaign_handle_for_name, wb_campaign_manifest_scope, wb_campaign_profile_runtime,
        },
    },
};

use super::{
    ACCESS_DENIED, ControlMcp,
    authorization::ControlIdentity,
    contract::{
        PrepareWbCampaignInput, WbCampaignHandleInput, WbCampaignNameInput, WbCampaignToolResult,
    },
};

impl ControlMcp {
    fn campaign_authorized<'a>(
        &'a self,
        identity: &ControlIdentity,
        account_id: &str,
        write_required: bool,
        allow_disabled: bool,
    ) -> Result<&'a ControlWbCampaignRuntimeConfig, String> {
        let runtime = self
            .wb_campaign
            .as_ref()
            .ok_or_else(|| "CONTROL_DISABLED: WB campaign runtime не настроен".to_owned())?;
        let (registry, actor) = self.access_context(identity)?;
        let account = registry
            .accounts
            .iter()
            .find(|account| account.id == account_id)
            .ok_or_else(|| format!("{ACCESS_DENIED}: WB account отсутствует"))?;
        if runtime.account_id != account_id
            || actor.id != runtime.actor_id
            || actor.role != Role::Admin
            || !matches!(account.marketplace, Marketplace::Wildberries)
            || !actor.can_access_account(account)
        {
            return Err(format!(
                "{ACCESS_DENIED}: WB campaign вне actor/runtime scope"
            ));
        }
        if self.policy.mode == ControlMode::Disabled && !allow_disabled {
            return Err("CONTROL_DISABLED: WB campaign policy выключена".to_owned());
        }
        if write_required && (self.policy.mode != ControlMode::Enabled || !runtime.writes_enabled) {
            return Err("CONTROL_DISABLED: WB campaign marketplace writes выключены".to_owned());
        }
        Ok(runtime)
    }

    pub(super) fn prepare_campaign_request(
        &self,
        identity: &ControlIdentity,
        input: &PrepareWbCampaignInput,
    ) -> Result<WbCampaignToolResult, String> {
        let runtime = self.campaign_authorized(identity, &input.account_id, false, false)?;
        let profile_path = runtime.root.join("profile.json");
        let profile = wb_campaign_profile_runtime(&profile_path)
            .map_err(|error| format!("CONTROL_WB_CAMPAIGN_PROFILE: {error}"))?;
        if profile.account_id != runtime.account_id || profile.actor_id != runtime.actor_id {
            return Err("CONTROL_POLICY_CHANGED: WB campaign profile scope изменился".to_owned());
        }
        let expected_handle = wb_campaign_handle_for_name(&input.account_id, &input.campaign_name)
            .map_err(|error| format!("CONTROL_WB_CAMPAIGN_IDENTITY: {error}"))?;
        let request = json!({
            "campaign_name":input.campaign_name,
            "bids_kopecks":input.bids_kopecks,
            "budget_rubles":input.budget_rubles,
            "authorization_reference":input.authorization_reference,
            "authorized_at":Utc::now(),
            "expires_at":input.expires_at,
            "robot_authorization_expires_at":input.robot_authorization_expires_at,
        });
        let mut result = prepare_wb_campaign_from_request(&profile_path, &request)
            .map_err(|error| format!("CONTROL_WB_CAMPAIGN_PREPARE_FAILED: {error}"))?;
        let manifest = Path::new(
            result
                .get("manifest")
                .and_then(Value::as_str)
                .ok_or_else(|| "CONTROL_WB_CAMPAIGN: отсутствует manifest".to_owned())?,
        );
        let campaign_handle = handle_from_manifest(&runtime.root, manifest)?;
        if campaign_handle != expected_handle {
            return Err("CONTROL_WB_CAMPAIGN: manifest identity mismatch".to_owned());
        }
        if let Value::Object(ref mut object) = result {
            object.remove("manifest");
        }
        Ok(WbCampaignToolResult {
            account_id: runtime.account_id.clone(),
            campaign_handle,
            result,
        })
    }

    pub(super) async fn campaign_stage(
        &self,
        identity: &ControlIdentity,
        input: WbCampaignHandleInput,
        stage: &'static str,
        write_required: bool,
    ) -> Result<WbCampaignToolResult, String> {
        let runtime = self.campaign_authorized(
            identity,
            &input.account_id,
            write_required,
            stage == "reconcile",
        )?;
        let manifest = manifest_for_handle(&runtime.root, &input.campaign_handle)?;
        let result =
            run_wb_campaign_launch_scoped(stage, &manifest, &runtime.account_id, &runtime.actor_id)
                .await
                .map_err(|error| format!("CONTROL_WB_CAMPAIGN_{stage}_FAILED: {error}"))?;
        Ok(WbCampaignToolResult {
            account_id: runtime.account_id.clone(),
            campaign_handle: input.campaign_handle,
            result,
        })
    }

    pub(super) fn campaign_lookup(
        &self,
        identity: &ControlIdentity,
        input: &WbCampaignNameInput,
    ) -> Result<WbCampaignToolResult, String> {
        let runtime = self.campaign_authorized(identity, &input.account_id, false, true)?;
        let campaign_handle = wb_campaign_handle_for_name(&input.account_id, &input.campaign_name)
            .map_err(|error| format!("CONTROL_WB_CAMPAIGN_IDENTITY: {error}"))?;
        let manifest = manifest_for_handle(&runtime.root, &campaign_handle)?;
        let scope = wb_campaign_manifest_scope(&manifest)
            .map_err(|error| format!("CONTROL_WB_CAMPAIGN_LOOKUP_FAILED: {error}"))?;
        if scope.account_id != runtime.account_id
            || scope.actor_id != runtime.actor_id
            || scope.campaign_name != input.campaign_name
        {
            return Err(format!(
                "{ACCESS_DENIED}: WB campaign вне actor/runtime scope"
            ));
        }
        Ok(WbCampaignToolResult {
            account_id: runtime.account_id.clone(),
            campaign_handle,
            result: json!({"outcome":"found","campaign_name":scope.campaign_name}),
        })
    }

    pub(super) fn campaign_export(
        &self,
        identity: &ControlIdentity,
        input: WbCampaignHandleInput,
    ) -> Result<WbCampaignToolResult, String> {
        let runtime = self.campaign_authorized(identity, &input.account_id, false, true)?;
        let manifest = manifest_for_handle(&runtime.root, &input.campaign_handle)?;
        let scope = wb_campaign_manifest_scope(&manifest)
            .map_err(|error| format!("CONTROL_WB_CAMPAIGN_SCOPE: {error}"))?;
        if scope.account_id != runtime.account_id || scope.actor_id != runtime.actor_id {
            return Err(format!(
                "{ACCESS_DENIED}: WB campaign manifest вне actor/runtime scope"
            ));
        }
        let result = export_wb_campaign(&manifest)
            .map_err(|error| format!("CONTROL_WB_CAMPAIGN_EXPORT_FAILED: {error}"))?;
        Ok(WbCampaignToolResult {
            account_id: runtime.account_id.clone(),
            campaign_handle: input.campaign_handle,
            result,
        })
    }
}

fn manifest_for_handle(root: &Path, handle: &str) -> Result<PathBuf, String> {
    if handle.len() != 64
        || !handle
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err("CONTROL_WB_CAMPAIGN: недопустимый campaign_handle".to_owned());
    }
    Ok(root
        .join("campaigns")
        .join(format!("campaign-{handle}"))
        .join("manifest.json"))
}

fn handle_from_manifest(root: &Path, manifest: &Path) -> Result<String, String> {
    let directory = manifest
        .parent()
        .ok_or_else(|| "CONTROL_WB_CAMPAIGN: manifest path".to_owned())?;
    if manifest.file_name().and_then(|name| name.to_str()) != Some("manifest.json")
        || directory.parent() != Some(root.join("campaigns").as_path())
    {
        return Err("CONTROL_WB_CAMPAIGN: manifest вне фиксированного каталога".to_owned());
    }
    let name = directory
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "CONTROL_WB_CAMPAIGN: campaign directory".to_owned())?;
    let handle = name
        .strip_prefix("campaign-")
        .ok_or_else(|| "CONTROL_WB_CAMPAIGN: campaign handle".to_owned())?;
    manifest_for_handle(root, handle)?;
    Ok(handle.to_owned())
}

#[cfg(test)]
mod tests {
    use super::{handle_from_manifest, manifest_for_handle};
    use crate::control::wb_launch::wb_campaign_handle_for_name;
    use std::path::Path;

    #[test]
    fn mcp_prepare_schema_accepts_string_encoded_wb_article_ids() {
        let input: crate::control::server::contract::PrepareWbCampaignInput =
            serde_json::from_value(serde_json::json!({
                "account_id":"wb_one","campaign_name":"Erebor",
                "bids_kopecks":{"881697128":700},"budget_rubles":0,
                "authorization_reference":"test",
                "expires_at":"2026-09-26T00:00:00Z",
                "robot_authorization_expires_at":"2026-09-27T00:00:00Z"
            }))
            .unwrap();
        assert_eq!(input.bids_kopecks.get(&881_697_128), Some(&700));
    }

    #[test]
    fn handle_is_confined_to_fixed_campaign_directory() {
        let root = Path::new("/var/lib/wb-campaigns");
        let handle = "a".repeat(64);
        let path = manifest_for_handle(root, &handle).unwrap();
        assert_eq!(handle_from_manifest(root, &path).unwrap(), handle);
        assert!(manifest_for_handle(root, "../other").is_err());
        assert!(manifest_for_handle(root, &"A".repeat(64)).is_err());
        assert!(handle_from_manifest(root, Path::new("/tmp/campaign-abc/manifest.json")).is_err());
        assert!(wb_campaign_handle_for_name("ofk_region_wb", "Nexus").is_err());
    }
}
