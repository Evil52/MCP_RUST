use std::sync::Arc;

use chrono::{DateTime, Utc};
use rmcp::{
    Json, ServerHandler,
    handler::server::{
        common::{AsRequestContext, FromContextPart},
        router::tool::ToolRouter,
        wrapper::Parameters,
    },
    model::{Implementation, JsonObject, MetaObject, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router,
};
use serde_json::Value;

use crate::{
    auth::{AuthenticatedActor, JwtAuthenticator, ProtectedResourceMetadata},
    config::{AccessRegistry, Actor, MarketplaceAccount, RegistrySource},
    control::{
        plan::{
            PlanStoreError, WbActionQuota, WbApplyContext, WbControlPlan, WbPlanFinish,
            WbPlanRepository, WbPlanStatus,
        },
        policy::{ControlMode, ControlPolicy, WbPromotionBidTargetPolicy},
        wb::{
            WbBidWriteClient, WbCampaignBidSnapshot, WbGuardedWriteError, WbWriteError,
            WbWriteOutcomeKind, campaign_snapshot, prepare_changes, snapshot_matches_plan_state,
        },
    },
    http::HttpMcpServer,
    wb::WbClient,
};

pub use contract::{
    ApplyWbBidPlanInput, ApproveWbBidPlanInput, BidLimitsResult, ControlScopeResult,
    ControlStatusResult, ControlTargetResult, EmptyInput, PrepareWbBidPlanInput,
    WbPlanApprovalResult, WbPlanInput, WbPlanResult, WbPromotionBidTargetResult,
};

mod contract;

const ACCESS_DENIED: &str = "CONTROL_ACCESS_DENIED";

#[derive(Debug, Clone)]
pub struct ControlMcp {
    policy: Arc<ControlPolicy>,
    registry: RegistrySource,
    default_actor_id: Option<String>,
    authenticator: Option<JwtAuthenticator>,
    wb: Option<WbControlServices>,
    tool_router: ToolRouter<Self>,
}

#[derive(Clone)]
pub struct WbControlServices {
    pub account_id: String,
    pub seller_sid: String,
    pub reader: Arc<WbClient>,
    pub writer: Option<Arc<WbBidWriteClient>>,
    pub plans: Arc<WbPlanRepository>,
}

impl std::fmt::Debug for WbControlServices {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WbControlServices")
            .field("account_id", &self.account_id)
            .field("seller_sid", &self.seller_sid)
            .field("reader", &"<configured>")
            .field("writer_configured", &self.writer.is_some())
            .field("plans", &"<configured>")
            .finish()
    }
}

#[derive(Debug, Clone, Default)]
struct ControlIdentity {
    actor_id: Option<String>,
    registry: Option<Arc<AccessRegistry>>,
}

#[derive(Debug)]
enum WritePermitFailure {
    Authorization,
    PreflightRead,
    PreconditionChanged(Box<WbCampaignBidSnapshot>),
    Store(PlanStoreError),
}

impl<C> FromContextPart<C> for ControlIdentity
where
    C: AsRequestContext,
{
    fn from_context_part(context: &mut C) -> Result<Self, rmcp::ErrorData> {
        let context = context.as_request_context();
        let actor_id = context
            .extensions
            .get::<AuthenticatedActor>()
            .or_else(|| {
                context
                    .extensions
                    .get::<axum::http::request::Parts>()
                    .and_then(|parts| parts.extensions.get::<AuthenticatedActor>())
            })
            .map(|actor| actor.actor_id.clone());
        let registry = context
            .extensions
            .get::<Arc<AccessRegistry>>()
            .cloned()
            .or_else(|| {
                context
                    .extensions
                    .get::<axum::http::request::Parts>()
                    .and_then(|parts| parts.extensions.get::<Arc<AccessRegistry>>())
                    .cloned()
            });
        Ok(Self { actor_id, registry })
    }
}

impl ControlMcp {
    #[must_use]
    pub fn new_disabled(actor_id: String, registry: RegistrySource, policy: ControlPolicy) -> Self {
        Self {
            policy: Arc::new(policy),
            registry,
            default_actor_id: Some(actor_id),
            authenticator: None,
            wb: None,
            tool_router: Self::configured_tool_router(None),
        }
    }

    #[must_use]
    pub fn new_authenticated_disabled(
        registry: RegistrySource,
        policy: ControlPolicy,
        authenticator: JwtAuthenticator,
    ) -> Self {
        let tool_router = Self::configured_tool_router(Some(&authenticator));
        Self {
            policy: Arc::new(policy),
            registry,
            default_actor_id: None,
            authenticator: Some(authenticator),
            wb: None,
            tool_router,
        }
    }

    #[must_use]
    pub fn with_wb_control_services(mut self, services: WbControlServices) -> Self {
        if self.authenticator.is_some() {
            self.wb = Some(services);
        } else {
            tracing::error!("refusing to attach WB write services to dev/no-auth Control MCP");
        }
        self
    }

    fn configured_tool_router(authenticator: Option<&JwtAuthenticator>) -> ToolRouter<Self> {
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

    fn access_context(
        &self,
        identity: &ControlIdentity,
    ) -> Result<(Arc<AccessRegistry>, Actor), String> {
        let registry = match &identity.registry {
            Some(registry) => Arc::clone(registry),
            None => self
                .registry
                .load()
                .map_err(|error| format!("CONTROL_POLICY_ERROR: {error}"))?,
        };
        let actor_id = identity
            .actor_id
            .as_deref()
            .or(self.default_actor_id.as_deref())
            .ok_or_else(|| format!("{ACCESS_DENIED}: отсутствует проверенная идентичность"))?;
        let actor = registry
            .actor(actor_id)
            .map_err(|_| format!("{ACCESS_DENIED}: actor отсутствует в access registry"))?
            .clone();
        Ok((registry, actor))
    }

    pub fn protected_resource_metadata(&self) -> Option<ProtectedResourceMetadata> {
        self.authenticator
            .as_ref()
            .map(JwtAuthenticator::protected_resource_metadata)
    }

    #[must_use]
    pub const fn transport_authenticator(&self) -> Option<&JwtAuthenticator> {
        self.authenticator.as_ref()
    }

    fn wb_services(&self, account_id: &str) -> Result<&WbControlServices, String> {
        let services = self
            .wb
            .as_ref()
            .ok_or_else(|| "CONTROL_DISABLED: WB runtime не настроен".to_owned())?;
        if services.account_id != account_id {
            return Err(format!(
                "{ACCESS_DENIED}: WB account находится вне runtime scope"
            ));
        }
        Ok(services)
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
    async fn control_status(
        &self,
        identity: ControlIdentity,
        Parameters(_input): Parameters<EmptyInput>,
    ) -> Result<Json<ControlStatusResult>, String> {
        let (_registry, actor) = self.access_context(&identity)?;
        let actor_id = actor.id;
        let runtime_ready = self.wb.is_some();
        let writer_ready = self
            .wb
            .as_ref()
            .is_some_and(|services| services.writer.is_some());
        Ok(Json(ControlStatusResult {
            explicit_policy_binding: self.policy.actor_policy(&actor_id).is_some(),
            actor_id,
            policy_schema_version: self.policy.version,
            policy_revision: self.policy.revision,
            policy_digest: self.policy.digest().to_owned(),
            mode: self.policy.mode,
            write_executor_configured: writer_ready && self.policy.mode == ControlMode::Enabled,
            runtime_gates_required: true,
            credentials_loaded: runtime_ready,
            marketplace_egress_enabled: runtime_ready,
            persistence_configured: runtime_ready,
        }))
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
    async fn control_scope(
        &self,
        identity: ControlIdentity,
        Parameters(_input): Parameters<EmptyInput>,
    ) -> Result<Json<ControlScopeResult>, String> {
        let (registry, actor) = self.access_context(&identity)?;
        let actor_id = actor.id.clone();
        let targets = self
            .policy
            .actor_policy(&actor_id)
            .into_iter()
            .flat_map(|policy| &policy.targets)
            .filter(|target| {
                registry
                    .accounts
                    .iter()
                    .find(|account| account.id == target.account_id)
                    .is_some_and(|account| actor.can_access_account(account))
            })
            .map(|target| ControlTargetResult {
                account_id: target.account_id.clone(),
                campaign_id: target.campaign_id,
                skus: target.skus.clone(),
                bid_limits: BidLimitsResult {
                    min_minor: target.bid_limits.min_minor,
                    max_minor: target.bid_limits.max_minor,
                    max_delta_percent: target.bid_limits.max_delta_percent,
                },
            })
            .collect();
        let wb_promotion_bid_targets = self
            .policy
            .actor_policy(&actor_id)
            .into_iter()
            .flat_map(|policy| &policy.wb_promotion_bid_targets)
            .filter(|target| {
                registry
                    .accounts
                    .iter()
                    .find(|account| account.id == target.account_id)
                    .is_some_and(|account| actor.can_access_account(account))
            })
            .map(|target| WbPromotionBidTargetResult {
                account_id: target.account_id.clone(),
                seller_sid: target.seller_sid.clone(),
                advert_id: target.advert_id,
                nm_ids: target.nm_ids.clone(),
                placements: target.placements.clone(),
                bid_limits_kopecks: BidLimitsResult {
                    min_minor: target.bid_limits_kopecks.min_minor,
                    max_minor: target.bid_limits_kopecks.max_minor,
                    max_delta_percent: target.bid_limits_kopecks.max_delta_percent,
                },
                approver_actor_ids: target.approver_actor_ids.clone(),
                action_limits: target.action_limits,
            })
            .collect();
        Ok(Json(ControlScopeResult {
            actor_id,
            policy_schema_version: self.policy.version,
            policy_revision: self.policy.revision,
            policy_digest: self.policy.digest().to_owned(),
            mode: self.policy.mode,
            targets,
            wb_promotion_bid_targets,
        }))
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
    async fn prepare_wb_bid_update(
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
    async fn approve_wb_bid_plan(
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
    async fn apply_wb_bid_plan(
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
    async fn wb_bid_plan_status(
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
    async fn reconcile_wb_bid_plan(
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
}

fn plan_target<'a>(
    policy: &'a ControlPolicy,
    plan: &WbControlPlan,
) -> Option<&'a WbPromotionBidTargetPolicy> {
    policy
        .actor_policy(&plan.actor_id)
        .into_iter()
        .flat_map(|actor| &actor.wb_promotion_bid_targets)
        .find(|target| target.account_id == plan.account_id && target.advert_id == plan.advert_id)
}

fn allowed_plan_target<'a>(
    policy: &'a ControlPolicy,
    plan: &WbControlPlan,
) -> Option<&'a WbPromotionBidTargetPolicy> {
    if plan.schema_version != policy.version
        || plan.policy_revision != policy.revision
        || plan.policy_digest != policy.digest()
    {
        return None;
    }
    let target = plan_target(policy, plan)?;
    let expected_quota = WbActionQuota {
        max_actions_per_hour: target.action_limits.max_actions_per_hour,
        max_actions_per_day: target.action_limits.max_actions_per_day,
        cooldown_seconds: u64::from(target.action_limits.cooldown_seconds),
        max_cumulative_abs_delta_kopecks_per_day: target
            .action_limits
            .max_cumulative_abs_delta_kopecks_per_day,
    };
    (prepare_changes(target, &plan.requested, &plan.before)
        .is_ok_and(|changes| changes == plan.changes)
        && plan.action_quota == expected_quota)
        .then_some(target)
}

fn authorize_plan_approval(
    policy: &ControlPolicy,
    registry: &AccessRegistry,
    approver: &Actor,
    plan: &WbControlPlan,
) -> Result<(), String> {
    let target = allowed_plan_target(policy, plan)
        .ok_or_else(|| "CONTROL_POLICY_CHANGED: plan больше не соответствует policy".to_owned())?;
    let account = registry
        .accounts
        .iter()
        .find(|account| account.id == plan.account_id)
        .ok_or_else(|| format!("{ACCESS_DENIED}: WB account отсутствует в registry"))?;
    let plan_actor = registry
        .actor(&plan.actor_id)
        .map_err(|_| format!("{ACCESS_DENIED}: plan actor отсутствует в registry"))?;
    if !plan_actor.can_access_account(account) || !approver.can_access_account(account) {
        return Err(format!(
            "{ACCESS_DENIED}: актуальный доступ к WB account отозван"
        ));
    }
    if approver.id == plan.actor_id
        || !target
            .approver_actor_ids
            .iter()
            .any(|actor_id| actor_id == &approver.id)
    {
        return Err(format!(
            "{ACCESS_DENIED}: actor не делегирован как отдельный approver"
        ));
    }
    Ok(())
}

#[expect(
    clippy::suspicious_operation_groupings,
    reason = "all comparisons independently bind the plan to its actor and runtime scope"
)]
fn authorize_plan_account_access<'a>(
    registry: &'a AccessRegistry,
    actor: &Actor,
    services: &WbControlServices,
    plan: &WbControlPlan,
) -> Result<&'a MarketplaceAccount, String> {
    if actor.id != plan.actor_id
        || plan.account_id != services.account_id
        || plan.before.seller_sid != services.seller_sid
    {
        return Err(format!(
            "{ACCESS_DENIED}: plan находится вне runtime/actor scope"
        ));
    }
    let account = registry
        .accounts
        .iter()
        .find(|account| account.id == plan.account_id)
        .ok_or_else(|| format!("{ACCESS_DENIED}: WB account отсутствует в registry"))?;
    if !actor.can_access_account(account) {
        return Err(format!(
            "{ACCESS_DENIED}: актуальный доступ к WB account отозван"
        ));
    }
    if account
        .wildberries
        .as_ref()
        .and_then(|wildberries| wildberries.seller_sid.as_deref())
        != Some(plan.before.seller_sid.as_str())
    {
        return Err(format!(
            "{ACCESS_DENIED}: WB cabinet binding изменился после создания plan"
        ));
    }
    Ok(account)
}

fn authorize_plan_apply(
    policy: &ControlPolicy,
    registry: &AccessRegistry,
    actor: &Actor,
    services: &WbControlServices,
    plan: &WbControlPlan,
) -> Result<(), String> {
    let account = authorize_plan_account_access(registry, actor, services, plan)?;
    let target = allowed_plan_target(policy, plan)
        .ok_or_else(|| "CONTROL_POLICY_CHANGED: plan больше не соответствует policy".to_owned())?;
    let approval = plan
        .approval
        .as_ref()
        .ok_or_else(|| "CONTROL_PLAN_APPROVAL_REQUIRED".to_owned())?;
    let approver = registry
        .actor(&approval.approver_id)
        .map_err(|_| format!("{ACCESS_DENIED}: approver отсутствует в registry"))?;
    if approval.approver_id == plan.actor_id
        || !approver.can_access_account(account)
        || !target
            .approver_actor_ids
            .iter()
            .any(|actor_id| actor_id == &approval.approver_id)
    {
        return Err(format!(
            "{ACCESS_DENIED}: актуальная approval delegation отозвана"
        ));
    }
    Ok(())
}

async fn read_plan_snapshot(
    services: &WbControlServices,
    plan: &WbControlPlan,
) -> Result<crate::control::wb::WbCampaignBidSnapshot, String> {
    let details = services
        .reader
        .promotion_campaign_details(&plan.account_id, vec![plan.advert_id], vec![], None)
        .await
        .map_err(|error| error.to_string())?;
    campaign_snapshot(
        &details,
        &services.seller_sid,
        plan.advert_id,
        &plan.requested,
    )
    .map_err(|error| error.to_string())
}

async fn load_plan_result(
    services: &WbControlServices,
    plan_id: &str,
    actor_id: &str,
) -> Result<Json<WbPlanResult>, String> {
    let plan = services
        .plans
        .load_for_actor(plan_id, actor_id)
        .await
        .map_err(plan_store_error)?;
    Ok(Json(plan_result(&plan)))
}

fn plan_result(plan: &WbControlPlan) -> WbPlanResult {
    WbPlanResult {
        plan_id: plan.plan_id.clone(),
        plan_digest: plan.plan_digest.clone(),
        actor_id: plan.actor_id.clone(),
        account_id: plan.account_id.clone(),
        seller_sid: plan.before.seller_sid.clone(),
        advert_id: plan.advert_id,
        policy_schema_version: plan.schema_version,
        policy_revision: plan.policy_revision,
        policy_digest: plan.policy_digest.clone(),
        action_quota: plan.action_quota,
        status: plan.status,
        approval: plan.approval.as_ref().map(|approval| WbPlanApprovalResult {
            approval_id: approval.approval_id.clone(),
            approver_id: approval.approver_id.clone(),
            approved_at: format_timestamp(approval.approved_at),
            expires_at: format_timestamp(approval.expires_at),
        }),
        changes: plan.changes.clone(),
        created_at: format_timestamp(plan.created_at),
        expires_at: format_timestamp(plan.expires_at),
        last_error_class: plan.last_error_class.clone(),
        requires_reconciliation: matches!(
            plan.status,
            WbPlanStatus::ReconciliationRequired | WbPlanStatus::Ambiguous
        ),
    }
}

fn format_timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn plan_store_error(error: PlanStoreError) -> String {
    match error {
        PlanStoreError::NotFound => "CONTROL_PLAN_NOT_FOUND".to_owned(),
        PlanStoreError::InvalidState => "CONTROL_PLAN_ALREADY_USED".to_owned(),
        PlanStoreError::Expired => "CONTROL_PLAN_EXPIRED".to_owned(),
        PlanStoreError::ApprovalRequired => "CONTROL_PLAN_APPROVAL_REQUIRED".to_owned(),
        PlanStoreError::ApprovalExpired => "CONTROL_PLAN_APPROVAL_EXPIRED".to_owned(),
        PlanStoreError::PlanChanged => "CONTROL_PLAN_CHANGED".to_owned(),
        PlanStoreError::PolicyChanged => "CONTROL_POLICY_CHANGED".to_owned(),
        PlanStoreError::CampaignLocked => "CONTROL_CAMPAIGN_INCIDENT_LOCKED".to_owned(),
        PlanStoreError::RuntimeDisabled => "CONTROL_RUNTIME_DISABLED".to_owned(),
        PlanStoreError::QuotaExceeded => "CONTROL_ACTION_LIMIT_REACHED".to_owned(),
        PlanStoreError::PrepareLimitExceeded => "CONTROL_PREPARE_LIMIT_REACHED".to_owned(),
        PlanStoreError::Busy => "CONTROL_CAMPAIGN_BUSY".to_owned(),
        PlanStoreError::ApplyInProgress => "CONTROL_PLAN_APPLY_IN_PROGRESS".to_owned(),
        PlanStoreError::InvalidPlan => "CONTROL_PLAN_INVALID".to_owned(),
        PlanStoreError::Unavailable => "CONTROL_PERSISTENCE_UNAVAILABLE".to_owned(),
    }
}

const fn guarded_write_permit_error_class(error: &WritePermitFailure) -> &'static str {
    match error {
        WritePermitFailure::Authorization => "access_revoked",
        WritePermitFailure::PreflightRead => "preflight_read_failed",
        WritePermitFailure::PreconditionChanged(_) => "precondition_changed",
        WritePermitFailure::Store(error) => match error {
            PlanStoreError::ApprovalRequired | PlanStoreError::ApprovalExpired => {
                "approval_revoked"
            }
            PlanStoreError::PlanChanged | PlanStoreError::PolicyChanged => "policy_changed",
            PlanStoreError::CampaignLocked => "incident_lock",
            PlanStoreError::RuntimeDisabled => "runtime_gate_revoked",
            PlanStoreError::QuotaExceeded => "quota_revoked",
            PlanStoreError::NotFound
            | PlanStoreError::InvalidState
            | PlanStoreError::Expired
            | PlanStoreError::PrepareLimitExceeded
            | PlanStoreError::Busy
            | PlanStoreError::ApplyInProgress
            | PlanStoreError::InvalidPlan
            | PlanStoreError::Unavailable => "write_permit_unavailable",
        },
    }
}

const fn write_failure_finish(error: &WbWriteError) -> (WbPlanStatus, &'static str) {
    match error.outcome_kind() {
        WbWriteOutcomeKind::DefiniteFailure => (WbPlanStatus::Failed, "wb_write_rejected"),
        WbWriteOutcomeKind::Ambiguous => (WbPlanStatus::Ambiguous, "wb_write_ambiguous"),
    }
}

#[allow(clippy::unused_async_trait_impl)]
#[tool_handler(router = self.tool_router)]
impl ServerHandler for ControlMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new("mcp-ozon-control", env!("CARGO_PKG_VERSION"))
                    .with_title("OzonOFK Advertising Control"),
            )
            .with_instructions(
                "Отдельный fail-closed контур управления рекламой. По умолчанию marketplace credentials, egress, persistence и writes отключены. WB-план живёт пять минут, требует short-lived approval другого явно делегированного actor, точный plan_digest и три runtime lease-gate. Apply исполняет не более одного PATCH и перед ним повторно проверяет approval/gates/incident lock. Никогда не повторяйте apply после ambiguous/reconciliation_required: вызывайте reconcile, который выполняет только read-back. Не просите API-ключи через чат и не заявляйте об успехе, пока status не равен applied.",
            )
    }
}

impl HttpMcpServer for ControlMcp {
    fn protected_resource_metadata(&self) -> Option<ProtectedResourceMetadata> {
        self.protected_resource_metadata()
    }

    fn transport_authenticator(&self) -> Option<&JwtAuthenticator> {
        self.transport_authenticator()
    }
}

#[cfg(test)]
mod tests;
