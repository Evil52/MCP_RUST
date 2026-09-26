use chrono::Utc;
use rmcp::{
    Json,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{JsonObject, MetaObject},
    tool, tool_router,
};
use serde_json::Value;

use crate::{
    auth::JwtAuthenticator,
    control::{
        ozon::{OzonCampaignLaunchManifest, prepare_campaign_launch_manifest},
        plan::{WbActionQuota, WbApplyContext, WbPlanFinish, WbPlanStatus},
        policy::ControlMode,
        wb::{
            WbGuardedWriteError, campaign_snapshot, prepare_changes, snapshot_matches_plan_state,
        },
    },
};

#[cfg(test)]
pub(super) use crate::control::ozon::{
    OzonFinalPermitError, ensure_ozon_sku_not_running, exact_ozon_launch_readback,
    find_ozon_campaign_by_title, positive_json_u64,
};

use super::{
    ACCESS_DENIED, ControlMcp,
    authorization::{
        ControlIdentity, authorize_ozon_plan_apply, authorize_ozon_plan_approval,
        authorize_plan_account_access, authorize_plan_apply, authorize_plan_approval,
    },
    contract::{
        ApplyOzonCampaignLaunchInput, ApplyWbBidPlanInput, ApproveOzonCampaignLaunchInput,
        ApproveWbBidPlanInput, ControlScopeResult, ControlStatusResult, EmptyInput,
        OzonCampaignPlanInput, OzonCampaignPlanResult, PrepareOzonCampaignLaunchInput,
        PrepareWbBidPlanInput, PrepareWbCampaignInput, PreviewOzonCampaignLaunchInput,
        WbCampaignHandleInput, WbCampaignNameInput, WbCampaignToolResult, WbPlanInput,
        WbPlanResult,
    },
    plan_readback::{load_plan_result, read_plan_snapshot},
    presentation::{
        WritePermitFailure, guarded_write_permit_error_class, ozon_plan_result,
        ozon_plan_store_error, plan_result, plan_store_error, write_failure_finish,
    },
};

impl ControlMcp {
    pub(super) fn configured_tool_router(
        authenticator: Option<&JwtAuthenticator>,
    ) -> ToolRouter<Self> {
        let mut router = Self::tool_router();
        let mut security_scheme = JsonObject::new();
        match authenticator {
            Some(authenticator) => {
                security_scheme.insert("type".to_owned(), Value::String("oauth2".to_owned()));
                security_scheme.insert(
                    "scopes".to_owned(),
                    Value::Array(
                        authenticator
                            .required_scopes()
                            .iter()
                            .cloned()
                            .map(Value::String)
                            .collect(),
                    ),
                );
            }
            None => {
                security_scheme.insert("type".to_owned(), Value::String("noauth".to_owned()));
            }
        }
        let schemes = vec![security_scheme];
        let schemes_value = Value::Array(schemes.iter().cloned().map(Value::Object).collect());
        for route in router.map.values_mut() {
            route.attr.security_schemes = Some(schemes.clone());
            route
                .attr
                .meta
                .get_or_insert_with(MetaObject::new)
                .0
                .insert("securitySchemes".to_owned(), schemes_value.clone());
        }
        router
    }
}

#[tool_router]
impl ControlMcp {
    /// Показывает фактическое состояние fail-closed Control MCP.
    #[tool(
        name = "ozon_ads_control_status",
        annotations(
            title = "Статус Control MCP",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(super) async fn control_status(
        &self,
        identity: ControlIdentity,
        Parameters(_input): Parameters<EmptyInput>,
    ) -> Result<Json<ControlStatusResult>, String> {
        self.status_result(&identity).map(Json)
    }

    /// Возвращает только явно перечисленные в локальной policy кампании, SKU и лимиты текущего actor. Сетевых запросов нет.
    #[tool(
        name = "ozon_ads_control_scope",
        annotations(
            title = "Разрешённый scope рекламы",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(super) async fn control_scope(
        &self,
        identity: ControlIdentity,
        Parameters(_input): Parameters<EmptyInput>,
    ) -> Result<Json<ControlScopeResult>, String> {
        self.scope_result(&identity).map(Json)
    }

    /// Creates a deterministic, policy-bound preview. It performs no network
    /// request, stores no plan and cannot be approved or applied.
    #[tool(
        name = "ozon_performance_preview_campaign_launch",
        annotations(
            title = "Проверить план запуска Ozon Performance",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(super) async fn preview_ozon_campaign_launch(
        &self,
        identity: ControlIdentity,
        Parameters(input): Parameters<PreviewOzonCampaignLaunchInput>,
    ) -> Result<Json<OzonCampaignLaunchManifest>, String> {
        if self.policy.mode == ControlMode::Disabled {
            return Err("CONTROL_DISABLED: preview launch выключен policy".to_owned());
        }
        let (registry, actor) = self.access_context(&identity)?;
        let Some(actor_policy) = self.policy.actor_policy(&actor.id) else {
            return Err(format!(
                "{ACCESS_DENIED}: отсутствует явная control policy binding"
            ));
        };
        let Some(target) = actor_policy
            .ozon_campaign_launch_targets
            .iter()
            .find(|target| {
                target.account_id == input.spec.account_id && target.skus == input.spec.skus
            })
        else {
            return Err(format!(
                "{ACCESS_DENIED}: Ozon launch target отсутствует в control policy"
            ));
        };
        let Some(account) = registry
            .accounts
            .iter()
            .find(|account| account.id == target.account_id)
        else {
            return Err(format!(
                "{ACCESS_DENIED}: Ozon account отсутствует в registry"
            ));
        };
        if !actor.can_access_account(account) {
            return Err(format!(
                "{ACCESS_DENIED}: actor не имеет доступа к Ozon account"
            ));
        }
        let manifest = prepare_campaign_launch_manifest(
            &actor.id,
            self.policy.version,
            self.policy.revision,
            self.policy.digest(),
            &target.account_id,
            &target.skus,
            target.weekly_budget_microrubles,
            target.per_sku_spend_cap_microrubles,
            target.initial_cpc_bid_microrubles,
            target.max_cpc_bid_microrubles,
            target.target_drr_percent,
            target.target_position,
            input.spec,
        );
        match manifest {
            Ok(manifest) => Ok(Json(manifest)),
            Err(error) => Err(format!("CONTROL_POLICY_DENIED: {error}")),
        }
    }

    /// Persists one immutable, single-SKU Ozon launch plan. The credentialless
    /// planner performs no marketplace I/O; the executor owns the final live
    /// preflight immediately before the durable write marker.
    #[tool(
        name = "ozon_performance_prepare_campaign_launch",
        annotations(
            title = "Подготовить запуск кампании Ozon",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    pub(super) async fn prepare_ozon_campaign_launch(
        &self,
        identity: ControlIdentity,
        Parameters(input): Parameters<PrepareOzonCampaignLaunchInput>,
    ) -> Result<Json<OzonCampaignPlanResult>, String> {
        if self.policy.mode == ControlMode::Disabled {
            return Err("CONTROL_DISABLED: создание Ozon plan выключено policy".to_owned());
        }
        let (registry, actor) = self.access_context(&identity)?;
        let Some(actor_policy) = self.policy.actor_policy(&actor.id) else {
            return Err(format!("{ACCESS_DENIED}: отсутствует Ozon policy binding"));
        };
        let Some(target) = actor_policy
            .ozon_campaign_launch_targets
            .iter()
            .find(|target| {
                target.account_id == input.spec.account_id && target.skus == input.spec.skus
            })
        else {
            return Err(format!("{ACCESS_DENIED}: Ozon launch target отсутствует"));
        };
        if input.spec.skus.len() != 1 {
            return Err("CONTROL_POLICY_DENIED: Ozon plan должен содержать один SKU".to_owned());
        }
        let Some(account) = registry
            .accounts
            .iter()
            .find(|account| account.id == target.account_id)
        else {
            return Err(format!("{ACCESS_DENIED}: Ozon account отсутствует"));
        };
        if !actor.can_access_account(account) {
            return Err(format!(
                "{ACCESS_DENIED}: actor не имеет доступа к Ozon account"
            ));
        }
        let services = self.ozon_services(&input.spec.account_id)?;
        let manifest = match prepare_campaign_launch_manifest(
            &actor.id,
            self.policy.version,
            self.policy.revision,
            self.policy.digest(),
            &target.account_id,
            &target.skus,
            target.weekly_budget_microrubles,
            target.per_sku_spend_cap_microrubles,
            target.initial_cpc_bid_microrubles,
            target.max_cpc_bid_microrubles,
            target.target_drr_percent,
            target.target_position,
            input.spec,
        ) {
            Ok(manifest) => manifest,
            Err(error) => return Err(format!("CONTROL_POLICY_DENIED: {error}")),
        };
        let plan = services
            .plans
            .create(&manifest)
            .await
            .map_err(ozon_plan_store_error)?;
        Ok(Json(ozon_plan_result(&plan)))
    }

    /// Persists Diana's short-lived approval of the exact Ozon plan digest.
    #[tool(
        name = "ozon_performance_approve_campaign_launch",
        annotations(
            title = "Подтвердить запуск кампании Ozon",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(super) async fn approve_ozon_campaign_launch(
        &self,
        identity: ControlIdentity,
        Parameters(input): Parameters<ApproveOzonCampaignLaunchInput>,
    ) -> Result<Json<OzonCampaignPlanResult>, String> {
        if self.policy.mode == ControlMode::Disabled {
            return Err("CONTROL_DISABLED: Ozon approval выключен policy".to_owned());
        }
        let (registry, approver) = self.access_context(&identity)?;
        let Some(services) = self.ozon.as_ref() else {
            return Err("CONTROL_DISABLED: Ozon plan store не настроен".to_owned());
        };
        let plan = services
            .plans
            .load(&input.plan_id)
            .await
            .map_err(ozon_plan_store_error)?;
        authorize_ozon_plan_approval(&self.policy, &registry, &approver, &plan)?;
        let plan = services
            .plans
            .approve(
                &input.plan_id,
                &approver.id,
                &input.plan_digest,
                &input.approval_reference,
            )
            .await
            .map_err(ozon_plan_store_error)?;
        Ok(Json(ozon_plan_result(&plan)))
    }

    /// Durably requests execution of an approved Ozon launch plan. The
    /// independent consumer owns every marketplace POST and recovery readback.
    #[tool(
        name = "ozon_performance_apply_campaign_launch",
        annotations(
            title = "Запустить кампанию Ozon",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(super) async fn apply_ozon_campaign_launch(
        &self,
        identity: ControlIdentity,
        Parameters(input): Parameters<ApplyOzonCampaignLaunchInput>,
    ) -> Result<Json<OzonCampaignPlanResult>, String> {
        if self.policy.mode != ControlMode::Enabled {
            return Err("CONTROL_DISABLED: Ozon apply выключен policy".to_owned());
        }
        let (registry, actor) = self.access_context(&identity)?;
        let Some(services) = self.ozon.as_ref() else {
            return Err("CONTROL_DISABLED: Ozon runtime не настроен".to_owned());
        };
        let plan = services
            .plans
            .load(&input.plan_id)
            .await
            .map_err(ozon_plan_store_error)?;
        authorize_ozon_plan_apply(&self.policy, &registry, &actor, &services.account_id, &plan)?;
        if plan.plan_digest != input.plan_digest {
            return Err("CONTROL_PLAN_CHANGED".to_owned());
        }
        let queued = services
            .plans
            .enqueue_launch(&input.plan_id, &actor.id, &input.plan_digest)
            .await
            .map_err(ozon_plan_store_error)?;
        Ok(Json(ozon_plan_result(&queued)))
    }

    /// Reads the durable recovery status. The independent consumer performs
    /// every readback; this tool never claims workflow ownership.
    #[tool(
        name = "ozon_performance_reconcile_campaign_launch",
        annotations(
            title = "Сверить запуск кампании Ozon",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(super) async fn reconcile_ozon_campaign_launch(
        &self,
        identity: ControlIdentity,
        Parameters(input): Parameters<OzonCampaignPlanInput>,
    ) -> Result<Json<OzonCampaignPlanResult>, String> {
        let (registry, actor) = self.access_context(&identity)?;
        let Some(services) = self.ozon.as_ref() else {
            return Err("CONTROL_DISABLED: Ozon runtime не настроен".to_owned());
        };
        let plan = services
            .plans
            .load(&input.plan_id)
            .await
            .map_err(ozon_plan_store_error)?;
        let Some(account) = registry
            .accounts
            .iter()
            .find(|account| account.id == plan.account_id)
        else {
            return Err(format!("{ACCESS_DENIED}: Ozon account отсутствует"));
        };
        if actor.id != plan.actor_id || !actor.can_access_account(account) {
            return Err(format!("{ACCESS_DENIED}: Ozon plan вне actor scope"));
        }
        Ok(Json(ozon_plan_result(&plan)))
    }

    /// Reads the current WB campaign state and creates an immutable five-minute plan.
    #[tool(
        name = "wb_promotion_prepare_bid_update",
        annotations(
            title = "Подготовить изменение ставок WB",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    pub(super) async fn prepare_wb_bid_update(
        &self,
        identity: ControlIdentity,
        Parameters(input): Parameters<PrepareWbBidPlanInput>,
    ) -> Result<Json<WbPlanResult>, String> {
        if self.policy.mode == ControlMode::Disabled {
            return Err("CONTROL_DISABLED: создание планов выключено policy".to_owned());
        }
        let (registry, actor) = self.access_context(&identity)?;
        let actor_policy = self
            .policy
            .actor_policy(&actor.id)
            .ok_or_else(|| format!("{ACCESS_DENIED}: отсутствует явная control policy binding"))?;
        let target = actor_policy
            .wb_promotion_bid_targets
            .iter()
            .find(|target| {
                target.account_id == input.account_id && target.advert_id == input.advert_id
            })
            .cloned()
            .ok_or_else(|| format!("{ACCESS_DENIED}: WB campaign отсутствует в control policy"))?;
        let account = registry
            .accounts
            .iter()
            .find(|account| account.id == input.account_id)
            .ok_or_else(|| format!("{ACCESS_DENIED}: WB account отсутствует в registry"))?;
        if !actor.can_access_account(account) {
            return Err(format!(
                "{ACCESS_DENIED}: actor не имеет доступа к WB account"
            ));
        }
        let services = self.wb_services(&input.account_id)?;
        if services.seller_sid != target.seller_sid {
            return Err(format!(
                "{ACCESS_DENIED}: WB seller sid находится вне runtime scope"
            ));
        }
        let action_quota = WbActionQuota {
            max_actions_per_hour: target.action_limits.max_actions_per_hour,
            max_actions_per_day: target.action_limits.max_actions_per_day,
            cooldown_seconds: u64::from(target.action_limits.cooldown_seconds),
            max_cumulative_abs_delta_kopecks_per_day: target
                .action_limits
                .max_cumulative_abs_delta_kopecks_per_day,
        };
        let prepare_reservation = services
            .plans
            .reserve_prepare_attempt(
                &actor.id,
                &input.account_id,
                input.advert_id,
                self.policy.version,
                self.policy.revision,
                self.policy.digest(),
                action_quota,
                Utc::now(),
            )
            .await
            .map_err(plan_store_error)?;
        let details = services
            .reader
            .promotion_campaign_details(&input.account_id, vec![input.advert_id], vec![], None)
            .await
            .map_err(|error| format!("CONTROL_PREFLIGHT_FAILED: {error}"))?;
        let before = campaign_snapshot(
            &details,
            &services.seller_sid,
            input.advert_id,
            &input.changes,
        )
        .map_err(|error| format!("CONTROL_PREFLIGHT_FAILED: {error}"))?;
        let changes = prepare_changes(&target, &input.changes, &before)
            .map_err(|error| format!("CONTROL_POLICY_DENIED: {error}"))?;
        let plan = services
            .plans
            .create(
                &actor.id,
                &input.account_id,
                input.advert_id,
                self.policy.version,
                self.policy.revision,
                self.policy.digest(),
                action_quota,
                &prepare_reservation.reservation_id,
                &input.changes,
                &changes,
                &before,
                Utc::now(),
            )
            .await
            .map_err(plan_store_error)?;
        Ok(Json(plan_result(&plan)))
    }

    /// Persists a short-lived two-person approval bound to the exact plan digest.
    #[tool(
        name = "wb_promotion_approve_bid_plan",
        annotations(
            title = "Подтвердить точный план ставок WB",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(super) async fn approve_wb_bid_plan(
        &self,
        identity: ControlIdentity,
        Parameters(input): Parameters<ApproveWbBidPlanInput>,
    ) -> Result<Json<WbPlanResult>, String> {
        if self.policy.mode == ControlMode::Disabled {
            return Err("CONTROL_DISABLED: approval планов выключен policy".to_owned());
        }
        let (registry, approver) = self.access_context(&identity)?;
        let services = self
            .wb
            .as_ref()
            .ok_or_else(|| "CONTROL_DISABLED: WB plan store не настроен".to_owned())?;
        let plan = services
            .plans
            .load_by_id_for_approval(&input.plan_id)
            .await
            .map_err(plan_store_error)?;
        if plan.account_id != services.account_id {
            return Err(format!(
                "{ACCESS_DENIED}: WB account находится вне runtime scope"
            ));
        }
        authorize_plan_approval(&self.policy, &registry, &approver, &plan)?;
        if input.plan_digest != plan.plan_digest {
            return Err("CONTROL_PLAN_CHANGED".to_owned());
        }
        let plan = services
            .plans
            .approve(
                &input.plan_id,
                &approver.id,
                &input.plan_digest,
                &input.approval_reference,
                Utc::now(),
            )
            .await
            .map_err(plan_store_error)?;
        Ok(Json(plan_result(&plan)))
    }

    /// Applies one previously prepared plan exactly once. The HTTP write is never retried.
    #[tool(
        name = "wb_promotion_apply_bid_plan",
        annotations(
            title = "Применить план ставок WB",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    pub(super) async fn apply_wb_bid_plan(
        &self,
        identity: ControlIdentity,
        Parameters(input): Parameters<ApplyWbBidPlanInput>,
    ) -> Result<Json<WbPlanResult>, String> {
        if self.policy.mode != ControlMode::Enabled {
            return Err("CONTROL_DISABLED: применение планов выключено policy".to_owned());
        }
        let (registry, actor) = self.access_context(&identity)?;
        let services = self
            .wb
            .as_ref()
            .ok_or_else(|| "CONTROL_DISABLED: WB runtime не настроен".to_owned())?;
        let writer = services
            .writer
            .as_ref()
            .ok_or_else(|| "CONTROL_DISABLED: WB write executor не настроен".to_owned())?;
        let pending = services
            .plans
            .load_for_actor(&input.plan_id, &actor.id)
            .await
            .map_err(plan_store_error)?;
        if pending.plan_digest != input.plan_digest {
            return Err("CONTROL_PLAN_CHANGED".to_owned());
        }
        authorize_plan_apply(&self.policy, &registry, &actor, services, &pending)?;
        let plan = services
            .plans
            .claim_for_apply(WbApplyContext {
                plan_id: &input.plan_id,
                actor_id: &actor.id,
                expected_plan_digest: &input.plan_digest,
                expected_schema_version: self.policy.version,
                expected_policy_revision: self.policy.revision,
                expected_policy_digest: self.policy.digest(),
                now: Utc::now(),
            })
            .await
            .map_err(plan_store_error)?;

        let write_response = match writer
            .change_bids_with_permit(plan.advert_id, &plan.changes, || async {
                let current = read_plan_snapshot(services, &plan)
                    .await
                    .map_err(|_| WritePermitFailure::PreflightRead)?;
                if !snapshot_matches_plan_state(&current, &plan.before, &plan.changes, false) {
                    return Err(WritePermitFailure::PreconditionChanged(Box::new(current)));
                }
                let latest_registry = self
                    .registry
                    .load()
                    .map_err(|_| WritePermitFailure::Authorization)?;
                let latest_actor = latest_registry
                    .actor(&actor.id)
                    .map_err(|_| WritePermitFailure::Authorization)?;
                authorize_plan_apply(
                    &self.policy,
                    &latest_registry,
                    latest_actor,
                    services,
                    &plan,
                )
                .map_err(|_| WritePermitFailure::Authorization)?;
                services
                    .plans
                    .revalidate_before_write(WbApplyContext {
                        plan_id: &plan.plan_id,
                        actor_id: &actor.id,
                        expected_plan_digest: &plan.plan_digest,
                        expected_schema_version: self.policy.version,
                        expected_policy_revision: self.policy.revision,
                        expected_policy_digest: self.policy.digest(),
                        now: Utc::now(),
                    })
                    .await
                    .map_err(WritePermitFailure::Store)
            })
            .await
        {
            Ok(response) => response,
            Err(WbGuardedWriteError::Permit(error)) => {
                let error_class = guarded_write_permit_error_class(&error);
                let readback = match &error {
                    WritePermitFailure::PreconditionChanged(snapshot) => Some(snapshot.as_ref()),
                    _ => None,
                };
                services
                    .plans
                    .finish(
                        &plan.plan_id,
                        &actor.id,
                        WbPlanFinish {
                            status: WbPlanStatus::Rejected,
                            error_class: Some(error_class),
                            write_response: None,
                            readback,
                            now: Utc::now(),
                        },
                    )
                    .await
                    .map_err(plan_store_error)?;
                return load_plan_result(services, &plan.plan_id, &actor.id).await;
            }
            Err(WbGuardedWriteError::Write(error)) => {
                let (status, class) = write_failure_finish(&error);
                services
                    .plans
                    .finish(
                        &plan.plan_id,
                        &actor.id,
                        WbPlanFinish {
                            status,
                            error_class: Some(class),
                            write_response: None,
                            readback: None,
                            now: Utc::now(),
                        },
                    )
                    .await
                    .map_err(plan_store_error)?;
                return load_plan_result(services, &plan.plan_id, &actor.id).await;
            }
        };

        let (status, error_class, readback) = match read_plan_snapshot(services, &plan).await {
            Ok(readback)
                if snapshot_matches_plan_state(&readback, &plan.before, &plan.changes, true) =>
            {
                (WbPlanStatus::Applied, None, Some(readback))
            }
            Ok(readback) => (
                WbPlanStatus::ReconciliationRequired,
                Some("readback_mismatch"),
                Some(readback),
            ),
            Err(_) => (
                WbPlanStatus::ReconciliationRequired,
                Some("readback_unavailable"),
                None,
            ),
        };
        services
            .plans
            .finish(
                &plan.plan_id,
                &actor.id,
                WbPlanFinish {
                    status,
                    error_class,
                    write_response: Some(&write_response),
                    readback: readback.as_ref(),
                    now: Utc::now(),
                },
            )
            .await
            .map_err(plan_store_error)?;
        load_plan_result(services, &plan.plan_id, &actor.id).await
    }

    /// Returns durable plan state without contacting Wildberries.
    #[tool(
        name = "wb_promotion_bid_plan_status",
        annotations(
            title = "Статус плана ставок WB",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(super) async fn wb_bid_plan_status(
        &self,
        identity: ControlIdentity,
        Parameters(input): Parameters<WbPlanInput>,
    ) -> Result<Json<WbPlanResult>, String> {
        let (registry, actor) = self.access_context(&identity)?;
        let services = self
            .wb
            .as_ref()
            .ok_or_else(|| "CONTROL_DISABLED: WB plan store не настроен".to_owned())?;
        let plan = services
            .plans
            .load_for_actor(&input.plan_id, &actor.id)
            .await
            .map_err(plan_store_error)?;
        authorize_plan_account_access(&registry, &actor, services, &plan)?;
        Ok(Json(plan_result(&plan)))
    }

    /// Re-reads WB after an accepted or ambiguous write; it never repeats the mutation.
    #[tool(
        name = "wb_promotion_reconcile_bid_plan",
        annotations(
            title = "Сверить результат плана ставок WB",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    pub(super) async fn reconcile_wb_bid_plan(
        &self,
        identity: ControlIdentity,
        Parameters(input): Parameters<WbPlanInput>,
    ) -> Result<Json<WbPlanResult>, String> {
        let (registry, actor) = self.access_context(&identity)?;
        let services = self
            .wb
            .as_ref()
            .ok_or_else(|| "CONTROL_DISABLED: WB runtime не настроен".to_owned())?;
        let mut plan = services
            .plans
            .load_for_actor(&input.plan_id, &actor.id)
            .await
            .map_err(plan_store_error)?;
        authorize_plan_account_access(&registry, &actor, services, &plan)?;
        if plan.status == WbPlanStatus::Applying {
            services
                .plans
                .mark_stale_applying_ambiguous(&plan.plan_id, &actor.id, Utc::now())
                .await
                .map_err(plan_store_error)?;
            plan = services
                .plans
                .load_for_actor(&input.plan_id, &actor.id)
                .await
                .map_err(plan_store_error)?;
        }
        match plan.status {
            WbPlanStatus::Applied => return Ok(Json(plan_result(&plan))),
            WbPlanStatus::ReconciliationRequired | WbPlanStatus::Ambiguous => {}
            _ => {
                return Err("CONTROL_PLAN_STATE: план не требует reconciliation".to_owned());
            }
        }
        let readback = read_plan_snapshot(services, &plan)
            .await
            .map_err(|error| format!("CONTROL_RECONCILIATION_FAILED: {error}"))?;
        if snapshot_matches_plan_state(&readback, &plan.before, &plan.changes, true) {
            services
                .plans
                .confirm_reconciled(&plan.plan_id, &actor.id, &readback, Utc::now())
                .await
                .map_err(plan_store_error)?;
        }
        load_plan_result(services, &plan.plan_id, &actor.id).await
    }

    /// Builds a bounded, immutable WB launch manifest from a private account profile.
    #[tool(
        name = "wb_promotion_prepare_campaign",
        annotations(
            title = "Подготовить новую кампанию WB",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    pub(super) async fn prepare_wb_campaign_tool(
        &self,
        identity: ControlIdentity,
        Parameters(input): Parameters<PrepareWbCampaignInput>,
    ) -> Result<Json<WbCampaignToolResult>, String> {
        self.prepare_campaign_request(&identity, &input).map(Json)
    }

    /// Recovers the immutable handle by account and name after a lost prepare response.
    #[tool(
        name = "wb_promotion_find_campaign",
        annotations(
            title = "Найти подготовленную кампанию WB",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(super) async fn wb_campaign_find(
        &self,
        identity: ControlIdentity,
        Parameters(input): Parameters<WbCampaignNameInput>,
    ) -> Result<Json<WbCampaignToolResult>, String> {
        self.campaign_lookup(&identity, &input).map(Json)
    }

    /// Rechecks products, category, overlap, stock and balance before creation.
    #[tool(
        name = "wb_promotion_campaign_preflight",
        annotations(
            title = "Проверить запуск кампании WB",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    pub(super) async fn wb_campaign_preflight(
        &self,
        identity: ControlIdentity,
        Parameters(input): Parameters<WbCampaignHandleInput>,
    ) -> Result<Json<WbCampaignToolResult>, String> {
        self.campaign_stage(&identity, input, "preflight", false)
            .await
            .map(Json)
    }

    /// Sends at most one journaled WB create attempt for the exact manifest.
    #[tool(
        name = "wb_promotion_create_campaign",
        annotations(
            title = "Создать кампанию WB",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    pub(super) async fn wb_campaign_create(
        &self,
        identity: ControlIdentity,
        Parameters(input): Parameters<WbCampaignHandleInput>,
    ) -> Result<Json<WbCampaignToolResult>, String> {
        self.campaign_stage(&identity, input, "create", true)
            .await
            .map(Json)
    }

    /// Applies the exact initial manual search bids after confirmed creation.
    #[tool(
        name = "wb_promotion_set_initial_campaign_bids",
        annotations(
            title = "Установить начальные ставки WB",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    pub(super) async fn wb_campaign_bids(
        &self,
        identity: ControlIdentity,
        Parameters(input): Parameters<WbCampaignHandleInput>,
    ) -> Result<Json<WbCampaignToolResult>, String> {
        self.campaign_stage(&identity, input, "bids", true)
            .await
            .map(Json)
    }

    /// Exports robot policy and initial state only after confirmed create/bids.
    #[tool(
        name = "wb_promotion_export_campaign_robot",
        annotations(
            title = "Подготовить защитного робота WB",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(super) async fn wb_campaign_export_tool(
        &self,
        identity: ControlIdentity,
        Parameters(input): Parameters<WbCampaignHandleInput>,
    ) -> Result<Json<WbCampaignToolResult>, String> {
        self.campaign_export(&identity, input).map(Json)
    }

    /// Transfers the exact authorized initial amount once, after journal checks.
    #[tool(
        name = "wb_promotion_fund_campaign",
        annotations(
            title = "Пополнить бюджет кампании WB",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    pub(super) async fn wb_campaign_fund(
        &self,
        identity: ControlIdentity,
        Parameters(input): Parameters<WbCampaignHandleInput>,
    ) -> Result<Json<WbCampaignToolResult>, String> {
        self.campaign_stage(&identity, input, "fund", true)
            .await
            .map(Json)
    }

    /// Starts only after two fresh protective robot cycles and no incident.
    #[tool(
        name = "wb_promotion_start_campaign",
        annotations(
            title = "Запустить кампанию WB",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    pub(super) async fn wb_campaign_start(
        &self,
        identity: ControlIdentity,
        Parameters(input): Parameters<WbCampaignHandleInput>,
    ) -> Result<Json<WbCampaignToolResult>, String> {
        self.campaign_stage(&identity, input, "start", true)
            .await
            .map(Json)
    }

    /// Read-back only after any uncertain create, bids, fund or start attempt.
    #[tool(
        name = "wb_promotion_reconcile_campaign",
        annotations(
            title = "Сверить создание кампании WB",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    pub(super) async fn wb_campaign_reconcile(
        &self,
        identity: ControlIdentity,
        Parameters(input): Parameters<WbCampaignHandleInput>,
    ) -> Result<Json<WbCampaignToolResult>, String> {
        self.campaign_stage(&identity, input, "reconcile", false)
            .await
            .map(Json)
    }
}
