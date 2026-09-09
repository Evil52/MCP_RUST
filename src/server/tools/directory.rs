//! Authorized directory MCP tools.

use super::super::OzonMcp;
use super::super::{
    AccountStatus, AccountsResult, EmptyInput, Json, MemberAccountStatus, MemberStatus,
    MembersResult, Parameters, RequestIdentity, Role, StoreStatus, StoresResult, WbStoreStatus,
    WbStoresResult, tool,
};
use rmcp::tool_router;

#[tool_router(router = directory_router, vis = "pub(in crate::server)")]
impl OzonMcp {
    /// Показывает локально настроенные магазины и наличие ключей, не раскрывая секреты. Не проверяет сеть или авторизацию Ozon API.
    #[tool(
        name = "ozon_stores_status",
        annotations(title = "Статус магазинов Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn stores_status(
        &self,
        identity: RequestIdentity,
        Parameters(_input): Parameters<EmptyInput>,
    ) -> Result<Json<StoresResult>, String> {
        let (registry, actor) = self.access_context(&identity)?;
        let accessible_stores: Vec<_> = registry
            .accounts
            .iter()
            .filter_map(|account| account.ozon.as_ref().map(|ozon| (account, ozon)))
            .filter(|(account, _)| actor.can_access_account(account))
            .collect();
        Ok(Json(StoresResult {
            actor: Self::actor_status(&actor),
            default_store: (accessible_stores.len() == 1)
                .then(|| accessible_stores[0].1.store_id.clone()),
            access_mode: "server-side RBAC, read-only allowlist",
            stores: accessible_stores
                .into_iter()
                .map(|(account, ozon)| {
                    let manager = registry
                        .actor(&account.manager_id)
                        .expect("validated manager");
                    StoreStatus {
                        id: ozon.store_id.clone(),
                        account_id: account.id.clone(),
                        store_id: ozon.store_id.clone(),
                        name: account.organization.clone(),
                        seller_client_id: account.seller_client_id.clone(),
                        manager: manager.name.clone(),
                        configured: self.client.is_configured(&ozon.store_id),
                        performance_configured: self
                            .performance_client
                            .is_configured(&ozon.store_id),
                    }
                })
                .collect(),
        }))
    }

    /// Показывает доступные кабинеты Wildberries и наличие токенов, не раскрывая секреты и не выполняя сетевые запросы.
    #[tool(
        name = "wb_stores_status",
        annotations(title = "Статус кабинетов Wildberries", read_only_hint = true)
    )]
    pub(in crate::server) async fn wb_stores_status(
        &self,
        identity: RequestIdentity,
        Parameters(_input): Parameters<EmptyInput>,
    ) -> Result<Json<WbStoresResult>, String> {
        let (registry, actor) = self.access_context(&identity)?;
        let accessible_accounts: Vec<_> = registry
            .accounts
            .iter()
            .filter(|account| account.wildberries.is_some() && actor.can_access_account(account))
            .collect();
        Ok(Json(WbStoresResult {
            actor: Self::actor_status(&actor),
            default_account: (accessible_accounts.len() == 1)
                .then(|| accessible_accounts[0].id.clone()),
            access_mode: "server-side RBAC, explicit read-only WB methods",
            accounts: accessible_accounts
                .into_iter()
                .map(|account| {
                    let manager = registry
                        .actor(&account.manager_id)
                        .expect("validated manager");
                    WbStoreStatus {
                        account_id: account.id.clone(),
                        organization: account.organization.clone(),
                        seller_client_id: account.seller_client_id.clone(),
                        manager: manager.name.clone(),
                        configured: self.wb_client.is_configured(&account.id),
                    }
                })
                .collect(),
        }))
    }

    /// Показывает доступные текущему пользователю кабинеты Ozon и Wildberries и состояние их read-only интеграций.
    #[tool(
        name = "marketplace_accounts",
        annotations(title = "Доступные кабинеты маркетплейсов", read_only_hint = true)
    )]
    pub(in crate::server) async fn marketplace_accounts(
        &self,
        identity: RequestIdentity,
        Parameters(_input): Parameters<EmptyInput>,
    ) -> Result<Json<AccountsResult>, String> {
        let (registry, actor) = self.access_context(&identity)?;
        Ok(Json(AccountsResult {
            actor: Self::actor_status(&actor),
            accounts: registry
                .accounts
                .iter()
                .filter(|account| actor.can_access_account(account))
                .map(|account| {
                    let (integration_status, configured) = account.ozon.as_ref().map_or_else(
                        || {
                            if account.wildberries.is_some() {
                                (
                                    "read_only_wildberries_api",
                                    self.wb_client.is_configured(&account.id),
                                )
                            } else {
                                ("directory_only", false)
                            }
                        },
                        |ozon| {
                            (
                                "read_only_ozon_api",
                                self.client.is_configured(&ozon.store_id),
                            )
                        },
                    );
                    let manager = registry
                        .actor(&account.manager_id)
                        .expect("validated manager");
                    AccountStatus {
                        id: account.id.clone(),
                        account_id: account.id.clone(),
                        store_id: account.ozon.as_ref().map(|ozon| ozon.store_id.clone()),
                        organization: account.organization.clone(),
                        marketplace: account.marketplace,
                        seller_client_id: account.seller_client_id.clone(),
                        manager: manager.name.clone(),
                        integration_status,
                        configured,
                    }
                })
                .collect(),
        }))
    }

    /// Показывает сотрудников, их роли и доступные им кабинеты. Администратор видит весь реестр; остальные пользователи видят только собственную запись.
    #[tool(
        name = "list_members",
        annotations(title = "Сотрудники и роли OzonOFK", read_only_hint = true)
    )]
    pub(in crate::server) async fn list_members(
        &self,
        identity: RequestIdentity,
        Parameters(_input): Parameters<EmptyInput>,
    ) -> Result<Json<MembersResult>, String> {
        let (registry, actor) = self.access_context(&identity)?;
        let members = registry
            .actors
            .iter()
            .filter(|member| actor.role == Role::Admin || member.id == actor.id)
            .map(|member| {
                let mut account_ids: Vec<_> = registry
                    .accounts
                    .iter()
                    .filter(|account| member.can_access_account(account))
                    .map(|account| account.id.clone())
                    .collect();
                account_ids.sort();
                let mut accounts: Vec<_> = registry
                    .accounts
                    .iter()
                    .filter(|account| member.can_access_account(account))
                    .map(|account| MemberAccountStatus {
                        account_id: account.id.clone(),
                        store_id: account.ozon.as_ref().map(|ozon| ozon.store_id.clone()),
                        organization: account.organization.clone(),
                        marketplace: account.marketplace,
                    })
                    .collect();
                accounts.sort_by(|left, right| left.account_id.cmp(&right.account_id));
                MemberStatus {
                    id: member.id.clone(),
                    name: member.name.clone(),
                    role: member.role,
                    account_ids,
                    accounts,
                }
            })
            .collect();
        Ok(Json(MembersResult {
            actor: Self::actor_status(&actor),
            members,
        }))
    }
}
