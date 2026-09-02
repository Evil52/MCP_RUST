use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio_postgres::{Config, Row, Transaction, error::SqlState};

use crate::postgres::SupervisedClient;

use super::{
    OzonCampaignLaunchManifest,
    model::{
        OzonCampaignGuard, OzonCampaignPlan, OzonLaunchStatus, OzonPlanApproval, OzonPlanStoreError,
    },
};

const COMPONENT: &str = "mcp-ozon-control-ozon-writer";
const PLAN_TTL: Duration = Duration::minutes(15);
const APPROVAL_TTL: Duration = Duration::minutes(3);
const VERIFY_RUNTIME_CONTRACT_SQL: &str = include_str!("verify_runtime_contract.sql");
const PLAN_SELECT: &str = "SELECT p.plan_id,p.plan_digest,p.actor_id,p.account_id,p.sku,\
 p.schema_version,p.policy_revision,p.policy_digest,p.manifest_json,p.status,\
 p.campaign_id,p.created_at,p.expires_at,p.operation_started_at,p.finished_at,\
 p.last_error_class,p.readback_json,a.approval_id,a.approver_id,a.reference,\
 a.approved_at,a.expires_at FROM control.ozon_campaign_plans p LEFT JOIN \
 control.ozon_campaign_plan_approvals a ON a.plan_id=p.plan_id";

#[derive(Clone)]
pub struct OzonPlanRepository {
    client: Arc<SupervisedClient>,
}

impl OzonPlanRepository {
    pub async fn connect(config: &Config) -> Result<Self, OzonPlanStoreError> {
        let client = SupervisedClient::connect(config, COMPONENT)
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        Ok(Self {
            client: Arc::new(client),
        })
    }

    pub async fn probe(&self) -> Result<(), OzonPlanStoreError> {
        self.client
            .probe()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)
    }

    pub async fn verify_runtime_contract(&self) -> Result<(), OzonPlanStoreError> {
        self.client
            .verify_session_bounds()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let row = client
            .query_one(VERIFY_RUNTIME_CONTRACT_SQL, &[])
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        row.get::<_, bool>(0)
            .then_some(())
            .ok_or(OzonPlanStoreError::Unavailable)
    }

    pub async fn register_policy(
        &self,
        schema_version: u32,
        policy_revision: u64,
        policy_digest: &str,
    ) -> Result<(), OzonPlanStoreError> {
        validate_digest(policy_digest)?;
        let schema_version =
            i32::try_from(schema_version).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let policy_revision =
            i64::try_from(policy_revision).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        if schema_version == 0 || policy_revision == 0 {
            return Err(OzonPlanStoreError::InvalidPlan);
        }
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let tx = client
            .transaction()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        lock_policy(&tx).await?;
        if let Some(row) = tx
            .query_opt("SELECT schema_version,policy_revision,policy_digest FROM control.ozon_policy_revisions ORDER BY policy_revision DESC LIMIT 1", &[])
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?
        {
            let current_revision: i64 = row.get(1);
            if policy_revision < current_revision {
                return Err(OzonPlanStoreError::PolicyChanged);
            }
            if policy_revision == current_revision {
                return if row.get::<_, i32>(0) == schema_version && row.get::<_, &str>(2) == policy_digest {
                    tx.commit().await.map_err(|_| OzonPlanStoreError::Unavailable)?;
                    Ok(())
                } else {
                    Err(OzonPlanStoreError::PolicyChanged)
                };
            }
        }
        tx.execute(
            "INSERT INTO control.ozon_policy_revisions(schema_version,policy_revision,policy_digest,registered_at) VALUES($1,$2,$3,clock_timestamp())",
            &[&schema_version, &policy_revision, &policy_digest],
        )
        .await
        .map_err(|error| map_policy_insert(&error))?;
        let committed = tx
            .commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable);
        drop(client);
        committed
    }

    pub(in crate::control) async fn create(
        &self,
        manifest: &OzonCampaignLaunchManifest,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
        validate_manifest(manifest)?;
        let sku = manifest.spec.skus[0];
        let sku_i64 = i64::try_from(sku).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let schema_version = i32::try_from(manifest.policy_schema_version)
            .map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let policy_revision =
            i64::try_from(manifest.policy_revision).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let manifest_json =
            serde_json::to_string(manifest).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let tx = client
            .transaction()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        require_policy(
            &tx,
            schema_version,
            policy_revision,
            &manifest.policy_digest,
        )
        .await?;
        lock_sku(&tx, &manifest.spec.account_id, sku).await?;
        if has_incident_or_open_plan(&tx, &manifest.spec.account_id, sku, None).await? {
            return Err(OzonPlanStoreError::SkuLocked);
        }
        let now = database_now(&tx).await?;
        let expires_at = now + PLAN_TTL;
        let plan_digest = digest_fields(&[
            b"mcp-ozon/ozon-plan/v1",
            manifest.manifest_digest.as_bytes(),
            &now.timestamp_micros().to_be_bytes(),
            &expires_at.timestamp_micros().to_be_bytes(),
        ]);
        let plan_id = digest_fields(&[b"mcp-ozon/ozon-plan-id/v1", plan_digest.as_bytes()]);
        tx.execute(
            "INSERT INTO control.ozon_campaign_plans(plan_id,plan_digest,actor_id,account_id,sku,schema_version,policy_revision,policy_digest,manifest_json,status,created_at,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'prepared',$10,$11)",
            &[&plan_id,&plan_digest,&manifest.actor_id,&manifest.spec.account_id,&sku_i64,&schema_version,&policy_revision,&manifest.policy_digest,&manifest_json,&now,&expires_at],
        )
        .await
        .map_err(|error| map_plan_insert(&error))?;
        insert_audit(&tx, &plan_id, &manifest.actor_id, "prepared", &serde_json::json!({"plan_digest":plan_digest,"manifest_digest":manifest.manifest_digest})).await?;
        tx.commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        self.load(&plan_id).await
    }

    pub(in crate::control) async fn load(
        &self,
        plan_id: &str,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
        validate_digest(plan_id)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let query = format!("{PLAN_SELECT} WHERE p.plan_id=$1");
        let row = client
            .query_opt(&query, &[&plan_id])
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?
            .ok_or(OzonPlanStoreError::NotFound)?;
        drop(client);
        plan_from_row(&row)
    }

    pub(in crate::control) async fn approve(
        &self,
        plan_id: &str,
        approver_id: &str,
        expected_digest: &str,
        reference: &str,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
        validate_digest(plan_id)?;
        validate_digest(expected_digest)?;
        validate_identity(approver_id)?;
        validate_reference(reference)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let tx = client
            .transaction()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let query = format!("{PLAN_SELECT} WHERE p.plan_id=$1 FOR UPDATE OF p");
        let row = tx
            .query_opt(&query, &[&plan_id])
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?
            .ok_or(OzonPlanStoreError::NotFound)?;
        let plan = plan_from_row(&row)?;
        if plan.plan_digest != expected_digest {
            return Err(OzonPlanStoreError::PlanChanged);
        }
        if plan.actor_id == approver_id {
            return Err(OzonPlanStoreError::InvalidState);
        }
        require_policy(
            &tx,
            i32::try_from(plan.schema_version).map_err(|_| OzonPlanStoreError::InvalidPlan)?,
            i64::try_from(plan.policy_revision).map_err(|_| OzonPlanStoreError::InvalidPlan)?,
            &plan.policy_digest,
        )
        .await?;
        let now = database_now(&tx).await?;
        if plan.expires_at <= now {
            expire(&tx, plan_id, now).await?;
            tx.commit()
                .await
                .map_err(|_| OzonPlanStoreError::Unavailable)?;
            return Err(OzonPlanStoreError::Expired);
        }
        if plan.status == OzonLaunchStatus::Approved {
            let approval = plan
                .approval
                .as_ref()
                .ok_or(OzonPlanStoreError::Unavailable)?;
            if approval.expires_at <= now {
                return Err(OzonPlanStoreError::ApprovalExpired);
            }
            return if approval.approver_id == approver_id && approval.reference == reference {
                Ok(plan)
            } else {
                Err(OzonPlanStoreError::InvalidState)
            };
        }
        if plan.status != OzonLaunchStatus::Prepared {
            return Err(OzonPlanStoreError::InvalidState);
        }
        let expires_at = std::cmp::min(plan.expires_at, now + APPROVAL_TTL);
        let approval_id = digest_fields(&[
            b"mcp-ozon/ozon-approval/v1",
            plan_id.as_bytes(),
            expected_digest.as_bytes(),
            approver_id.as_bytes(),
            reference.as_bytes(),
            &now.timestamp_micros().to_be_bytes(),
        ]);
        tx.execute("INSERT INTO control.ozon_campaign_plan_approvals(approval_id,plan_id,plan_digest,approver_id,reference,approved_at,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7)", &[&approval_id,&plan_id,&expected_digest,&approver_id,&reference,&now,&expires_at]).await.map_err(|_| OzonPlanStoreError::Unavailable)?;
        let updated = tx.execute("UPDATE control.ozon_campaign_plans SET status='approved' WHERE plan_id=$1 AND status='prepared'", &[&plan_id]).await.map_err(|_| OzonPlanStoreError::Unavailable)?;
        if updated != 1 {
            return Err(OzonPlanStoreError::InvalidState);
        }
        insert_audit(
            &tx,
            plan_id,
            approver_id,
            "approved",
            &serde_json::json!({"approval_id":approval_id,"plan_digest":expected_digest}),
        )
        .await?;
        tx.commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        self.load(plan_id).await
    }

    pub(in crate::control) async fn claim_create(
        &self,
        plan_id: &str,
        actor_id: &str,
        expected_digest: &str,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
        self.transition(
            plan_id,
            actor_id,
            expected_digest,
            OzonLaunchStatus::Approved,
            OzonLaunchStatus::Creating,
            None,
            None,
            None,
            true,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::control) async fn transition(
        &self,
        plan_id: &str,
        actor_id: &str,
        expected_digest: &str,
        from: OzonLaunchStatus,
        to: OzonLaunchStatus,
        campaign_id: Option<u64>,
        error_class: Option<&str>,
        readback: Option<&Value>,
        reserve: bool,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
        validate_digest(plan_id)?;
        validate_digest(expected_digest)?;
        validate_identity(actor_id)?;
        if let Some(error) = error_class {
            validate_error_class(error)?;
        }
        let campaign_id_i64 = campaign_id
            .map(i64::try_from)
            .transpose()
            .map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let readback_json = readback
            .map(serde_json::to_string)
            .transpose()
            .map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let tx = client
            .transaction()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let query = format!("{PLAN_SELECT} WHERE p.plan_id=$1 FOR UPDATE OF p");
        let row = tx
            .query_opt(&query, &[&plan_id])
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?
            .ok_or(OzonPlanStoreError::NotFound)?;
        let plan = plan_from_row(&row)?;
        if plan.actor_id != actor_id || plan.plan_digest != expected_digest || plan.status != from {
            return Err(OzonPlanStoreError::InvalidState);
        }
        lock_sku(&tx, &plan.account_id, plan.sku).await?;
        let begins_marketplace_write = reserve
            || matches!(
                to,
                OzonLaunchStatus::AddingProducts | OzonLaunchStatus::Activating
            );
        if begins_marketplace_write {
            require_policy(
                &tx,
                i32::try_from(plan.schema_version).map_err(|_| OzonPlanStoreError::InvalidPlan)?,
                i64::try_from(plan.policy_revision).map_err(|_| OzonPlanStoreError::InvalidPlan)?,
                &plan.policy_digest,
            )
            .await?;
            require_gates(&tx, &plan.account_id, plan.sku).await?;
        }
        if reserve {
            tx.execute("INSERT INTO control.ozon_campaign_action_reservations(plan_id,account_id,sku,reserved_at) VALUES($1,$2,$3,clock_timestamp())", &[&plan_id,&plan.account_id,&i64::try_from(plan.sku).map_err(|_| OzonPlanStoreError::InvalidPlan)?]).await.map_err(|error| if error.as_db_error().is_some_and(|db| db.code()==&SqlState::UNIQUE_VIOLATION) { OzonPlanStoreError::InvalidState } else { OzonPlanStoreError::Unavailable })?;
        }
        let updated = tx.execute("UPDATE control.ozon_campaign_plans SET status=$4,campaign_id=COALESCE($5,campaign_id),last_error_class=$6,readback_json=COALESCE($7,readback_json) WHERE plan_id=$1 AND actor_id=$2 AND plan_digest=$3 AND status=$8", &[&plan_id,&actor_id,&expected_digest,&to.as_db(),&campaign_id_i64,&error_class,&readback_json,&from.as_db()]).await.map_err(|_| OzonPlanStoreError::Unavailable)?;
        if updated != 1 {
            return Err(OzonPlanStoreError::InvalidState);
        }
        if to == OzonLaunchStatus::Applied {
            let effective_campaign_id = campaign_id
                .or(plan.campaign_id)
                .ok_or(OzonPlanStoreError::InvalidPlan)?;
            let effective_campaign_id = i64::try_from(effective_campaign_id)
                .map_err(|_| OzonPlanStoreError::InvalidPlan)?;
            let spend_cap = i64::try_from(plan.manifest.spec.per_sku_spend_cap_microrubles)
                .map_err(|_| OzonPlanStoreError::InvalidPlan)?;
            tx.execute(
                "INSERT INTO control.ozon_campaign_guards(plan_id,account_id,sku,campaign_id,date_from,spend_cap_microrubles,target_drr_percent,status,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,'active',clock_timestamp()) ON CONFLICT(plan_id) DO NOTHING",
                &[&plan_id,&plan.account_id,&i64::try_from(plan.sku).map_err(|_|OzonPlanStoreError::InvalidPlan)?,&effective_campaign_id,&plan.manifest.spec.from_date,&spend_cap,&i16::from(plan.manifest.spec.target_drr_percent)],
            ).await.map_err(|_|OzonPlanStoreError::Unavailable)?;
        }
        insert_audit(
            &tx,
            plan_id,
            actor_id,
            to.as_db(),
            &serde_json::json!({"campaign_id":campaign_id,"error_class":error_class}),
        )
        .await?;
        tx.commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        self.load(plan_id).await
    }

    pub(in crate::control) async fn revalidate_write_permit(
        &self,
        plan_id: &str,
        actor_id: &str,
        expected_digest: &str,
        expected_status: OzonLaunchStatus,
    ) -> Result<(), OzonPlanStoreError> {
        let plan = self.load(plan_id).await?;
        if plan.actor_id != actor_id
            || plan.plan_digest != expected_digest
            || plan.status != expected_status
        {
            return Err(OzonPlanStoreError::InvalidState);
        }
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let tx = client
            .transaction()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        require_policy(
            &tx,
            i32::try_from(plan.schema_version).map_err(|_| OzonPlanStoreError::InvalidPlan)?,
            i64::try_from(plan.policy_revision).map_err(|_| OzonPlanStoreError::InvalidPlan)?,
            &plan.policy_digest,
        )
        .await?;
        require_gates(&tx, &plan.account_id, plan.sku).await?;
        let now = database_now(&tx).await?;
        let approval = plan.approval.ok_or(OzonPlanStoreError::InvalidState)?;
        if approval.expires_at <= now {
            return Err(OzonPlanStoreError::ApprovalExpired);
        }
        let committed = tx
            .commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable);
        drop(client);
        committed
    }

    pub async fn active_guards(&self) -> Result<Vec<OzonCampaignGuard>, OzonPlanStoreError> {
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let rows=client.query("SELECT plan_id,account_id,sku,campaign_id,date_from,spend_cap_microrubles,target_drr_percent FROM control.ozon_campaign_guards WHERE status='active' ORDER BY campaign_id",&[]).await.map_err(|_|OzonPlanStoreError::Unavailable)?;
        drop(client);
        rows.iter()
            .map(|row| {
                Ok(OzonCampaignGuard {
                    plan_id: row.get(0),
                    account_id: row.get(1),
                    sku: u64::try_from(row.get::<_, i64>(2))
                        .map_err(|_| OzonPlanStoreError::Unavailable)?,
                    campaign_id: u64::try_from(row.get::<_, i64>(3))
                        .map_err(|_| OzonPlanStoreError::Unavailable)?,
                    date_from: row.get(4),
                    spend_cap_microrubles: u64::try_from(row.get::<_, i64>(5))
                        .map_err(|_| OzonPlanStoreError::Unavailable)?,
                    target_drr_percent: u8::try_from(row.get::<_, i16>(6))
                        .map_err(|_| OzonPlanStoreError::Unavailable)?,
                })
            })
            .collect()
    }

    pub async fn claim_guard_stop(
        &self,
        plan_id: &str,
        campaign_id: u64,
        reason: &str,
        spend_minor: u64,
        revenue_minor: u64,
    ) -> Result<(), OzonPlanStoreError> {
        validate_digest(plan_id)?;
        validate_error_class(reason)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let updated=client.execute("UPDATE control.ozon_campaign_guards SET status='stopping',stop_reason=$3,last_spend_minor=$4,last_revenue_minor=$5,last_checked_at=clock_timestamp() WHERE plan_id=$1 AND campaign_id=$2 AND status='active'",&[&plan_id,&i64::try_from(campaign_id).map_err(|_|OzonPlanStoreError::InvalidPlan)?,&reason,&i64::try_from(spend_minor).map_err(|_|OzonPlanStoreError::InvalidPlan)?,&i64::try_from(revenue_minor).map_err(|_|OzonPlanStoreError::InvalidPlan)?]).await.map_err(|_|OzonPlanStoreError::Unavailable)?;
        drop(client);
        if updated == 1 {
            Ok(())
        } else {
            Err(OzonPlanStoreError::InvalidState)
        }
    }

    pub async fn revalidate_stop_permit(
        &self,
        plan_id: &str,
        campaign_id: u64,
    ) -> Result<(), OzonPlanStoreError> {
        validate_digest(plan_id)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let campaign_id =
            i64::try_from(campaign_id).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let active:bool=client.query_one("SELECT EXISTS(SELECT 1 FROM control.ozon_campaign_guards WHERE plan_id=$1 AND campaign_id=$2 AND status='stopping')",&[&plan_id,&campaign_id]).await.map_err(|_|OzonPlanStoreError::Unavailable)?.get(0);
        drop(client);
        active.then_some(()).ok_or(OzonPlanStoreError::InvalidState)
    }

    pub async fn finish_guard(
        &self,
        plan_id: &str,
        campaign_id: u64,
        reason: &str,
        spend_minor: u64,
        revenue_minor: u64,
    ) -> Result<(), OzonPlanStoreError> {
        validate_digest(plan_id)?;
        validate_error_class(reason)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let updated=client.execute("UPDATE control.ozon_campaign_guards SET status='stopped',stop_reason=$3,last_spend_minor=$4,last_revenue_minor=$5,last_checked_at=clock_timestamp(),stopped_at=clock_timestamp() WHERE plan_id=$1 AND campaign_id=$2 AND status='stopping'",&[&plan_id,&i64::try_from(campaign_id).map_err(|_|OzonPlanStoreError::InvalidPlan)?,&reason,&i64::try_from(spend_minor).map_err(|_|OzonPlanStoreError::InvalidPlan)?,&i64::try_from(revenue_minor).map_err(|_|OzonPlanStoreError::InvalidPlan)?]).await.map_err(|_|OzonPlanStoreError::Unavailable)?;
        drop(client);
        if updated == 1 {
            Ok(())
        } else {
            Err(OzonPlanStoreError::InvalidState)
        }
    }

    pub async fn record_guard_observation(
        &self,
        plan_id: &str,
        spend_minor: u64,
        revenue_minor: u64,
    ) -> Result<(), OzonPlanStoreError> {
        validate_digest(plan_id)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let updated=client.execute("UPDATE control.ozon_campaign_guards SET last_spend_minor=$2,last_revenue_minor=$3,last_checked_at=clock_timestamp() WHERE plan_id=$1 AND status='active'",&[&plan_id,&i64::try_from(spend_minor).map_err(|_|OzonPlanStoreError::InvalidPlan)?,&i64::try_from(revenue_minor).map_err(|_|OzonPlanStoreError::InvalidPlan)?]).await.map_err(|_|OzonPlanStoreError::Unavailable)?;
        drop(client);
        if updated == 1 {
            Ok(())
        } else {
            Err(OzonPlanStoreError::InvalidState)
        }
    }

    pub async fn mark_guard_incident(
        &self,
        plan_id: &str,
        error_class: &str,
    ) -> Result<(), OzonPlanStoreError> {
        validate_digest(plan_id)?;
        validate_error_class(error_class)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let updated=client.execute("UPDATE control.ozon_campaign_guards SET status='incident',stop_reason=$2,last_checked_at=clock_timestamp() WHERE plan_id=$1 AND status='stopping'",&[&plan_id,&error_class]).await.map_err(|_|OzonPlanStoreError::Unavailable)?;
        drop(client);
        if updated == 1 {
            Ok(())
        } else {
            Err(OzonPlanStoreError::InvalidState)
        }
    }
}

fn plan_from_row(row: &Row) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
    let manifest: OzonCampaignLaunchManifest =
        serde_json::from_str(row.get::<_, &str>(8)).map_err(|_| OzonPlanStoreError::Unavailable)?;
    let readback = row
        .get::<_, Option<String>>(16)
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(|_| OzonPlanStoreError::Unavailable)?;
    let approval_id: Option<String> = row.get(17);
    let approval = approval_id.map(|approval_id| OzonPlanApproval {
        approval_id,
        approver_id: row.get(18),
        reference: row.get(19),
        approved_at: row.get(20),
        expires_at: row.get(21),
    });
    Ok(OzonCampaignPlan {
        plan_id: row.get(0),
        plan_digest: row.get(1),
        actor_id: row.get(2),
        account_id: row.get(3),
        sku: u64::try_from(row.get::<_, i64>(4)).map_err(|_| OzonPlanStoreError::Unavailable)?,
        schema_version: u32::try_from(row.get::<_, i32>(5))
            .map_err(|_| OzonPlanStoreError::Unavailable)?,
        policy_revision: u64::try_from(row.get::<_, i64>(6))
            .map_err(|_| OzonPlanStoreError::Unavailable)?,
        policy_digest: row.get(7),
        manifest,
        status: OzonLaunchStatus::from_db(row.get(9))?,
        campaign_id: row
            .get::<_, Option<i64>>(10)
            .map(u64::try_from)
            .transpose()
            .map_err(|_| OzonPlanStoreError::Unavailable)?,
        created_at: row.get(11),
        expires_at: row.get(12),
        operation_started_at: row.get(13),
        finished_at: row.get(14),
        last_error_class: row.get(15),
        readback,
        approval,
    })
}

fn validate_manifest(manifest: &OzonCampaignLaunchManifest) -> Result<(), OzonPlanStoreError> {
    validate_identity(&manifest.actor_id)?;
    validate_identity(&manifest.spec.account_id)?;
    validate_digest(&manifest.manifest_digest)?;
    validate_digest(&manifest.policy_digest)?;
    if manifest.spec.skus.len() != 1
        || manifest.spec.skus[0] == 0
        || manifest.policy_schema_version == 0
        || manifest.policy_revision == 0
    {
        return Err(OzonPlanStoreError::InvalidPlan);
    }
    Ok(())
}

fn validate_digest(value: &str) -> Result<(), OzonPlanStoreError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(OzonPlanStoreError::InvalidPlan)
    }
}

fn validate_identity(value: &str) -> Result<(), OzonPlanStoreError> {
    if !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| matches!(b,b'A'..=b'Z'|b'a'..=b'z'|b'0'..=b'9'|b'_'|b'-'|b'.'))
    {
        Ok(())
    } else {
        Err(OzonPlanStoreError::InvalidPlan)
    }
}

fn validate_reference(value: &str) -> Result<(), OzonPlanStoreError> {
    if !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| matches!(b,b'A'..=b'Z'|b'a'..=b'z'|b'0'..=b'9'|b'_'|b'-'|b'.'|b':'|b'/'))
    {
        Ok(())
    } else {
        Err(OzonPlanStoreError::InvalidPlan)
    }
}

fn validate_error_class(value: &str) -> Result<(), OzonPlanStoreError> {
    if !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        Ok(())
    } else {
        Err(OzonPlanStoreError::InvalidPlan)
    }
}

fn digest_fields(fields: &[&[u8]]) -> String {
    let mut hash = Sha256::new();
    for field in fields {
        hash.update((field.len() as u64).to_be_bytes());
        hash.update(field);
    }
    hash.finalize()
        .iter()
        .fold(String::with_capacity(64), |mut out, b| {
            use std::fmt::Write as _;
            write!(out, "{b:02x}").expect("String write");
            out
        })
}

async fn database_now(tx: &Transaction<'_>) -> Result<DateTime<Utc>, OzonPlanStoreError> {
    tx.query_one("SELECT clock_timestamp()", &[])
        .await
        .map(|row| row.get(0))
        .map_err(|_| OzonPlanStoreError::Unavailable)
}

async fn lock_policy(tx: &Transaction<'_>) -> Result<(), OzonPlanStoreError> {
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended('ozon/policy-revision',0))",
        &[],
    )
    .await
    .map(|_| ())
    .map_err(|_| OzonPlanStoreError::Unavailable)
}
async fn lock_sku(tx: &Transaction<'_>, account: &str, sku: u64) -> Result<(), OzonPlanStoreError> {
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        &[&format!("ozon/{account}/{sku}")],
    )
    .await
    .map(|_| ())
    .map_err(|_| OzonPlanStoreError::Unavailable)
}

async fn require_policy(
    tx: &Transaction<'_>,
    schema: i32,
    revision: i64,
    digest: &str,
) -> Result<(), OzonPlanStoreError> {
    lock_policy(tx).await?;
    let row=tx.query_opt("SELECT schema_version,policy_revision,policy_digest FROM control.ozon_policy_revisions ORDER BY policy_revision DESC LIMIT 1",&[]).await.map_err(|_|OzonPlanStoreError::Unavailable)?.ok_or(OzonPlanStoreError::PolicyChanged)?;
    if row.get::<_, i32>(0) == schema
        && row.get::<_, i64>(1) == revision
        && row.get::<_, &str>(2) == digest
    {
        Ok(())
    } else {
        Err(OzonPlanStoreError::PolicyChanged)
    }
}

async fn require_gates(
    tx: &Transaction<'_>,
    account: &str,
    sku: u64,
) -> Result<(), OzonPlanStoreError> {
    let count:i64=tx.query_one("SELECT count(*) FROM control.ozon_runtime_gates WHERE gate_key=ANY($1) AND enabled AND lease_expires_at>clock_timestamp() AND (disabled_until IS NULL OR disabled_until<=clock_timestamp())",&[&vec!["global".to_owned(),format!("account/{account}"),format!("sku/{account}/{sku}")]]).await.map_err(|_|OzonPlanStoreError::Unavailable)?.get(0);
    if count == 3 {
        Ok(())
    } else {
        Err(OzonPlanStoreError::RuntimeDisabled)
    }
}

async fn has_incident_or_open_plan(
    tx: &Transaction<'_>,
    account: &str,
    sku: u64,
    except: Option<&str>,
) -> Result<bool, OzonPlanStoreError> {
    let sku = i64::try_from(sku).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
    tx.query_one("SELECT EXISTS(SELECT 1 FROM control.ozon_campaign_plans WHERE account_id=$1 AND sku=$2 AND ($3::text IS NULL OR plan_id<>$3) AND status IN ('prepared','approved','creating','created','adding_products','products_added','activating','ambiguous'))",&[&account,&sku,&except]).await.map(|row|row.get(0)).map_err(|_|OzonPlanStoreError::Unavailable)
}

async fn expire(
    tx: &Transaction<'_>,
    plan_id: &str,
    now: DateTime<Utc>,
) -> Result<(), OzonPlanStoreError> {
    tx.execute("UPDATE control.ozon_campaign_plans SET status='expired',finished_at=$2 WHERE plan_id=$1 AND status IN ('prepared','approved')",&[&plan_id,&now]).await.map(|_|()).map_err(|_|OzonPlanStoreError::Unavailable)
}

async fn insert_audit(
    tx: &Transaction<'_>,
    plan_id: &str,
    actor: &str,
    event: &str,
    payload: &Value,
) -> Result<(), OzonPlanStoreError> {
    let payload = serde_json::to_string(payload).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
    tx.execute("INSERT INTO control.ozon_campaign_audit_events(plan_id,actor_id,event_type,payload_json,created_at) VALUES($1,$2,$3,$4,clock_timestamp())",&[&plan_id,&actor,&event,&payload]).await.map(|_|()).map_err(|_|OzonPlanStoreError::Unavailable)
}

fn map_policy_insert(error: &tokio_postgres::Error) -> OzonPlanStoreError {
    if error
        .as_db_error()
        .is_some_and(|db| db.code() == &SqlState::UNIQUE_VIOLATION)
    {
        OzonPlanStoreError::PolicyChanged
    } else {
        OzonPlanStoreError::Unavailable
    }
}
fn map_plan_insert(error: &tokio_postgres::Error) -> OzonPlanStoreError {
    if error
        .as_db_error()
        .is_some_and(|db| db.code() == &SqlState::UNIQUE_VIOLATION)
    {
        OzonPlanStoreError::SkuLocked
    } else {
        OzonPlanStoreError::Unavailable
    }
}
