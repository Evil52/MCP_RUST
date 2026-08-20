//! Credential-free preflight plan for daily-report source collection.
//!
//! It validates only registry metadata and report policy. It deliberately
//! never resolves credential environment variables or performs I/O.

use std::collections::BTreeSet;

use thiserror::Error;

use crate::config::{AccessRegistry, Marketplace as RegistryMarketplace};

use super::{
    policy::DailyReportPolicy,
    snapshot::{Marketplace, SnapshotSource},
};

const MAX_COLLECTION_TARGETS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionTarget {
    pub account_id: String,
    pub marketplace: Marketplace,
    pub sources: Vec<SnapshotSource>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CollectionPlanError {
    #[error("daily report collection target count exceeds the supported limit")]
    TooManyTargets,
    #[error("report policy references an unknown account")]
    UnknownAccount,
    #[error("report account has no read-only marketplace binding")]
    MissingMarketplaceBinding,
    #[error("Ozon report account has no Performance read binding for advertising")]
    MissingAdvertisingBinding,
}

/// Builds the exact account/source inventory that a future collector must
/// complete before a report can be generated. Every marketplace-specific
/// source is mandatory: missing data is a preflight error, not a silently
/// zero-valued KPI.
pub fn build_collection_plan(
    policy: &DailyReportPolicy,
    registry: &AccessRegistry,
) -> Result<Vec<CollectionTarget>, CollectionPlanError> {
    let mut account_ids = BTreeSet::new();
    for audience in &policy.audiences {
        for manager in &audience.managers {
            account_ids.extend(manager.account_ids.iter().cloned());
        }
    }
    if account_ids.len() > MAX_COLLECTION_TARGETS {
        return Err(CollectionPlanError::TooManyTargets);
    }
    account_ids
        .into_iter()
        .map(|account_id| {
            let account = registry
                .accounts
                .iter()
                .find(|account| account.id == account_id)
                .ok_or(CollectionPlanError::UnknownAccount)?;
            let marketplace = match account.marketplace {
                RegistryMarketplace::Ozon => {
                    let ozon = account
                        .ozon
                        .as_ref()
                        .ok_or(CollectionPlanError::MissingMarketplaceBinding)?;
                    if ozon.performance.is_none() {
                        return Err(CollectionPlanError::MissingAdvertisingBinding);
                    }
                    Marketplace::Ozon
                }
                RegistryMarketplace::Wildberries => {
                    if account.wildberries.is_none() {
                        return Err(CollectionPlanError::MissingMarketplaceBinding);
                    }
                    Marketplace::Wildberries
                }
            };
            Ok(CollectionTarget {
                account_id,
                marketplace,
                sources: SnapshotSource::required_for(marketplace).to_vec(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        config::AccessRegistry,
        reporting::policy::{AudiencePolicy, DailyReportPolicy, ManagerScope},
    };

    use super::{CollectionPlanError, build_collection_plan};

    fn registry() -> AccessRegistry {
        serde_json::from_value(json!({"version":1,"actors":[
          {"id":"diana","name":"Diana","role":"manager","oidc":{"username":"diana"}},
          {"id":"anna","name":"Anna","role":"manager","oidc":{"username":"anna"}}
        ],"accounts":[
          {"id":"ozon","organization":"Ozon","marketplace":"ozon","seller_client_id":"1","manager_id":"diana","ozon":{"store_id":"1","client_id_env":"ID","api_key_env":"KEY","performance":{"client_id_env":"PERF_ID","client_secret_env":"PERF_SECRET"}}},
          {"id":"wb","organization":"WB","marketplace":"wildberries","seller_client_id":"2","manager_id":"anna","wildberries":{"api_token_env":"WB_TOKEN"}}
        ]})).unwrap()
    }

    fn policy(value: serde_json::Value) -> DailyReportPolicy {
        DailyReportPolicy::from_slice(&serde_json::to_vec(&value).unwrap(), &registry()).unwrap()
    }

    #[test]
    fn plan_is_unique_sorted_and_requires_all_report_sources() {
        let policy = policy(
            json!({"version":1,"enabled":false,"timezone":"Asia/Yekaterinburg","sender_email_env":"SENDER","audiences":[{"id":"owner","email_env":"OWNER","managers":[{"actor_id":"diana","account_ids":["ozon"]},{"actor_id":"anna","account_ids":["wb"]}]}]}),
        );
        let plan = build_collection_plan(&policy, &registry()).unwrap();
        assert_eq!(plan.len(), 2);
        assert_eq!(plan[0].account_id, "ozon");
        assert_eq!(plan[0].sources.len(), 5);
        assert_eq!(plan[1].sources.len(), 4);
    }

    #[test]
    fn absent_bindings_fail_closed() {
        let mut ozon_registry = registry();
        ozon_registry.accounts[0].ozon.as_mut().unwrap().performance = None;
        let ozon_policy = policy(
            json!({"version":1,"enabled":false,"timezone":"Asia/Yekaterinburg","sender_email_env":"SENDER","audiences":[{"id":"owner","email_env":"OWNER","managers":[{"actor_id":"diana","account_ids":["ozon"]}]}]}),
        );
        assert_eq!(
            build_collection_plan(&ozon_policy, &ozon_registry),
            Err(CollectionPlanError::MissingAdvertisingBinding)
        );
        ozon_registry.accounts[0].ozon = None;
        assert_eq!(
            build_collection_plan(&ozon_policy, &ozon_registry),
            Err(CollectionPlanError::MissingMarketplaceBinding)
        );

        let mut wb_registry = registry();
        wb_registry.accounts[1].wildberries = None;
        let wb_policy = policy(
            json!({"version":1,"enabled":false,"timezone":"Asia/Yekaterinburg","sender_email_env":"SENDER","audiences":[{"id":"owner","email_env":"OWNER","managers":[{"actor_id":"anna","account_ids":["wb"]}]}]}),
        );
        assert_eq!(
            build_collection_plan(&wb_policy, &wb_registry),
            Err(CollectionPlanError::MissingMarketplaceBinding)
        );
    }

    #[test]
    fn unvalidated_unknown_account_still_fails_closed() {
        let policy = DailyReportPolicy {
            version: 1,
            enabled: false,
            timezone: "Asia/Yekaterinburg".to_owned(),
            sender_email_env: "SENDER".to_owned(),
            audiences: vec![AudiencePolicy {
                id: "owner".to_owned(),
                email_env: "OWNER".to_owned(),
                managers: vec![ManagerScope {
                    actor_id: "diana".to_owned(),
                    account_ids: ["missing".to_owned()].into_iter().collect(),
                }],
            }],
        };
        assert_eq!(
            build_collection_plan(&policy, &registry()),
            Err(CollectionPlanError::UnknownAccount)
        );
    }

    #[test]
    fn collection_plan_is_bounded_before_account_resolution() {
        let policy = DailyReportPolicy {
            version: 1,
            enabled: true,
            timezone: "Asia/Yekaterinburg".to_owned(),
            sender_email_env: "SENDER".to_owned(),
            audiences: vec![AudiencePolicy {
                id: "owner".to_owned(),
                email_env: "OWNER".to_owned(),
                managers: vec![ManagerScope {
                    actor_id: "diana".to_owned(),
                    account_ids: (0..65).map(|index| format!("account_{index}")).collect(),
                }],
            }],
        };
        assert_eq!(
            build_collection_plan(&policy, &registry()),
            Err(CollectionPlanError::TooManyTargets)
        );
    }
}
