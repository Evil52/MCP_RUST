use std::sync::Arc;

use rmcp::{
    ServerHandler,
    handler::server::router::tool::ToolRouter,
    model::{Implementation, ServerCapabilities, ServerInfo},
    tool_handler,
};

use crate::{
    auth::{JwtAuthenticator, ProtectedResourceMetadata},
    config::{AccessRegistry, Actor, RegistrySource},
    control::{plan::WbPlanRepository, policy::ControlPolicy, wb::WbBidWriteClient},
    http::HttpMcpServer,
    wb::WbClient,
};

use authorization::ControlIdentity;
mod authorization;
mod contract;
mod presentation;
mod tools;

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

    /// Verifies the hot-reloaded registry and, when enabled, the durable plan
    /// store. The WB read/write APIs are deliberately outside readiness.
    pub(crate) async fn readiness(&self) -> Result<(), ()> {
        if let Err(error) = self.registry.load_async().await {
            tracing::warn!(%error, "Control readiness failed: access registry is invalid");
            return Err(());
        }
        if let Some(services) = &self.wb
            && let Err(error) = services.plans.probe().await
        {
            tracing::warn!(%error, "Control readiness failed: plan store is unavailable");
            return Err(());
        }
        Ok(())
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

    fn readiness(&self) -> crate::http::ReadinessFuture<'_> {
        Box::pin(self.readiness())
    }
}

#[cfg(test)]
mod tests;
