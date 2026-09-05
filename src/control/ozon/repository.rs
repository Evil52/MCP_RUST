use std::{future::Future, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio_postgres::{Config, Row, Transaction, error::SqlState};

use crate::postgres::SupervisedClient;

use super::{
    OzonCampaignLaunchManifest,
    model::{
        OzonCampaignGuard, OzonCampaignGuardStatus, OzonCampaignPlan, OzonGuardStopLease,
        OzonGuardStopReadback, OzonLaunchAction, OzonLaunchClaimMode, OzonLaunchLease,
        OzonLaunchStatus, OzonPlanApproval, OzonPlanStoreError, OzonStaticGuardMutation,
        OzonStaticGuardWriteIntent,
    },
    plan::provider_title_for_plan_id,
};

const COMPONENT: &str = "mcp-ozon-control-ozon-writer";
const PLAN_TTL: Duration = Duration::minutes(15);
const APPROVAL_TTL: Duration = Duration::minutes(3);
pub(super) const WORKFLOW_LEASE_TTL: Duration = Duration::minutes(5);
const WORKFLOW_RECOVERY_BACKOFF: Duration = Duration::minutes(2);
pub(super) const GUARD_STOP_LEASE_TTL: Duration = Duration::minutes(5);
const VERIFY_RUNTIME_CONTRACT_SQL: &str = include_str!("verify_runtime_contract.sql");
const PLAN_SELECT: &str = "SELECT p.plan_id,p.plan_digest,p.actor_id,p.account_id,p.sku,\
 p.schema_version,p.policy_revision,p.policy_digest,p.manifest_json,p.status,\
 p.campaign_id,p.created_at,p.expires_at,p.operation_started_at,p.finished_at,\
 p.last_error_class,p.readback_json,a.approval_id,a.approver_id,a.reference,\
 a.approved_at,a.expires_at,workflow.requested_at,workflow.action,workflow.generation,\
 workflow.lease_expires_at,workflow.write_started_at \
 FROM control.ozon_campaign_plans p LEFT JOIN \
 control.ozon_campaign_plan_approvals a ON a.plan_id=p.plan_id JOIN \
 control.ozon_campaign_launch_workflows workflow ON workflow.plan_id=p.plan_id";

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
        if let Err(error) = tx
            .execute(
                "INSERT INTO control.ozon_policy_revisions(schema_version,policy_revision,policy_digest,registered_at) VALUES($1,$2,$3,clock_timestamp())",
                &[&schema_version, &policy_revision, &policy_digest],
            )
            .await
        {
            return Err(map_policy_insert(&error));
        }
        let committed = tx
            .commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable);
        drop(client);
        committed
    }

    /// Holds the latest policy revision and all three account/SKU gate rows
    /// through a local static-state mutation marker. The caller must perform
    /// the provider request immediately after this permit returns.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::control) async fn authorize_static_guard_write<F, Fut>(
        &self,
        schema_version: u32,
        policy_revision: u64,
        policy_digest: &str,
        intent: &OzonStaticGuardWriteIntent,
        worker_id: &str,
        expected_prior_event_id: Option<u64>,
        persist_marker: F,
    ) -> Result<(), OzonPlanStoreError>
    where
        F: FnOnce(u64) -> Fut,
        Fut: Future<Output = Result<(), OzonPlanStoreError>>,
    {
        validate_digest(policy_digest)?;
        validate_identity(&intent.account_id)?;
        validate_identity(worker_id)?;
        validate_digest(&intent.config_digest)?;
        let target_bid = match (intent.mutation, intent.target_bid_microrubles) {
            (OzonStaticGuardMutation::SetBid, Some(bid)) if bid > 0 => {
                Some(i64::try_from(bid).map_err(|_| OzonPlanStoreError::InvalidPlan)?)
            }
            (OzonStaticGuardMutation::Activate | OzonStaticGuardMutation::Deactivate, None) => None,
            _ => return Err(OzonPlanStoreError::InvalidPlan),
        };
        if schema_version == 0 || policy_revision == 0 || intent.sku == 0 || intent.campaign_id == 0
        {
            return Err(OzonPlanStoreError::InvalidPlan);
        }
        let schema_version =
            i32::try_from(schema_version).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let policy_revision =
            i64::try_from(policy_revision).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let sku = i64::try_from(intent.sku).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let campaign_id =
            i64::try_from(intent.campaign_id).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let tx = client
            .transaction()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        if expected_prior_event_id.is_none() {
            return Err(OzonPlanStoreError::InvalidState);
        }
        lock_static_guard_audit_cursor(&tx, &intent.account_id, expected_prior_event_id).await?;
        require_policy(&tx, schema_version, policy_revision, policy_digest).await?;
        require_gates(&tx, &intent.account_id, intent.sku).await?;
        let audit_event_id = tx
            .query_one(
                "INSERT INTO control.ozon_static_guard_audit_events( \
             account_id,sku,campaign_id,mutation,target_bid_microrubles, \
             config_digest,schema_version,policy_revision,policy_digest, \
             worker_id,event_type,occurred_at) \
             VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, \
                    'write_authorized',clock_timestamp()) \
             RETURNING event_id",
                &[
                    &intent.account_id,
                    &sku,
                    &campaign_id,
                    &intent.mutation.as_db(),
                    &target_bid,
                    &intent.config_digest,
                    &schema_version,
                    &policy_revision,
                    &policy_digest,
                    &worker_id,
                ],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?
            .get::<_, i64>(0);
        let audit_event_id =
            u64::try_from(audit_event_id).map_err(|_| OzonPlanStoreError::Unavailable)?;
        // The durable database event is staged before local persistence. If
        // staging fails, the callback is never invoked. If local persistence
        // fails, the transaction rolls back. A lost COMMIT acknowledgement is
        // conservatively represented by the already-written local marker.
        persist_marker(audit_event_id).await?;
        tx.commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        Ok(())
    }

    /// Creates the first account-wide audit cursor before static state may be
    /// used. The database event and the local cursor are one logical commit:
    /// either side missing on restart is an explicit fail-closed mismatch.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::control) async fn initialize_static_guard_state<F, Fut>(
        &self,
        schema_version: u32,
        policy_revision: u64,
        policy_digest: &str,
        account_id: &str,
        config_digest: &str,
        worker_id: &str,
        expected_prior_event_id: Option<u64>,
        persist_cursor: F,
    ) -> Result<(), OzonPlanStoreError>
    where
        F: FnOnce(u64) -> Fut,
        Fut: Future<Output = Result<(), OzonPlanStoreError>>,
    {
        validate_digest(policy_digest)?;
        validate_identity(account_id)?;
        validate_digest(config_digest)?;
        validate_identity(worker_id)?;
        if schema_version == 0 || policy_revision == 0 {
            return Err(OzonPlanStoreError::InvalidPlan);
        }
        let schema_version =
            i32::try_from(schema_version).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let policy_revision =
            i64::try_from(policy_revision).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let tx = client
            .transaction()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        lock_static_guard_audit_cursor(&tx, account_id, expected_prior_event_id).await?;
        require_policy(&tx, schema_version, policy_revision, policy_digest).await?;
        let audit_event_id = tx
            .query_one(
                "INSERT INTO control.ozon_static_guard_audit_events( \
                 account_id,sku,campaign_id,mutation,target_bid_microrubles, \
                 config_digest,schema_version,policy_revision,policy_digest, \
                 worker_id,event_type,occurred_at) \
                 VALUES($1,NULL,NULL,NULL,NULL,$2,$3,$4,$5,$6, \
                        'state_initialized',clock_timestamp()) \
                 RETURNING event_id",
                &[
                    &account_id,
                    &config_digest,
                    &schema_version,
                    &policy_revision,
                    &policy_digest,
                    &worker_id,
                ],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?
            .get::<_, i64>(0);
        let audit_event_id =
            u64::try_from(audit_event_id).map_err(|_| OzonPlanStoreError::Unavailable)?;
        persist_cursor(audit_event_id).await?;
        tx.commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        Ok(())
    }

    pub(in crate::control) async fn latest_static_guard_audit_event_id(
        &self,
        account_id: &str,
    ) -> Result<Option<u64>, OzonPlanStoreError> {
        validate_identity(account_id)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let event_id = client
            .query_one(
                "SELECT max(event_id) \
                 FROM control.ozon_static_guard_audit_events \
                 WHERE account_id=$1",
                &[&account_id],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?
            .get::<_, Option<i64>>(0);
        drop(client);
        event_id
            .map(u64::try_from)
            .transpose()
            .map_err(|_| OzonPlanStoreError::Unavailable)
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
        expire_stale_open_plans_for_sku(&tx, &manifest.spec.account_id, sku, &manifest.actor_id)
            .await?;
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
        let mut persisted_manifest = manifest.clone();
        persisted_manifest.create_request.title = provider_title_for_plan_id(&plan_id);
        if !persisted_manifest.has_exact_persisted_integrity(&plan_id) {
            return Err(OzonPlanStoreError::InvalidPlan);
        }
        let manifest_json = serde_json::to_string(&persisted_manifest)
            .map_err(|_| OzonPlanStoreError::InvalidPlan)?;
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

    /// Records the explicit operator apply command without claiming a lease or
    /// touching Ozon. Approval by itself is deliberately not executable: the
    /// independent worker only sees rows with this durable request marker.
    pub(in crate::control) async fn enqueue_launch(
        &self,
        plan_id: &str,
        actor_id: &str,
        expected_digest: &str,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
        validate_digest(plan_id)?;
        validate_identity(actor_id)?;
        validate_digest(expected_digest)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let tx = client
            .transaction()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let plan = load_plan_for_update(&tx, plan_id).await?;
        if plan.actor_id != actor_id || plan.plan_digest != expected_digest {
            return Err(OzonPlanStoreError::InvalidState);
        }
        let row = tx
            .query_one(
                "SELECT requested_at,requested_by_actor_id \
                 FROM control.ozon_campaign_launch_workflows \
                 WHERE plan_id=$1 FOR UPDATE",
                &[&plan_id],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let requested_at: Option<DateTime<Utc>> = row.get(0);
        let requested_by: Option<String> = row.get(1);
        if requested_at.is_some() {
            if requested_by.as_deref() != Some(actor_id) {
                return Err(OzonPlanStoreError::InvalidState);
            }
            tx.commit()
                .await
                .map_err(|_| OzonPlanStoreError::Unavailable)?;
            drop(client);
            return self.load(plan_id).await;
        }
        if plan.status != OzonLaunchStatus::Approved {
            return Err(OzonPlanStoreError::InvalidState);
        }
        if plan.manifest.create_request.title != provider_title_for_plan_id(plan_id) {
            // Legacy approvals used an uncorrelated human title. They remain
            // visible for audit/status but cannot be turned into a create job.
            return Err(OzonPlanStoreError::InvalidPlan);
        }
        require_policy(
            &tx,
            i32::try_from(plan.schema_version).map_err(|_| OzonPlanStoreError::InvalidPlan)?,
            i64::try_from(plan.policy_revision).map_err(|_| OzonPlanStoreError::InvalidPlan)?,
            &plan.policy_digest,
        )
        .await?;
        require_gates(&tx, &plan.account_id, plan.sku).await?;
        require_active_approval(&tx, &plan).await?;
        let now = database_now(&tx).await?;
        let updated = tx
            .execute(
                "UPDATE control.ozon_campaign_launch_workflows \
                 SET requested_at=$2,requested_by_actor_id=$3,available_at=$2 \
                 WHERE plan_id=$1 AND requested_at IS NULL \
                   AND requested_by_actor_id IS NULL",
                &[&plan_id, &now, &actor_id],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        if updated != 1 {
            return Err(OzonPlanStoreError::InvalidState);
        }
        insert_audit(
            &tx,
            plan_id,
            actor_id,
            "workflow_enqueued",
            &serde_json::json!({"plan_digest":expected_digest}),
        )
        .await?;
        tx.commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        self.load(plan_id).await
    }

    /// Claims the next explicitly requested executable action for one runtime
    /// account. It never returns an uncertain action; those are exclusively
    /// handled by `claim_launch_recovery`.
    pub(in crate::control) async fn claim_next_launch_action(
        &self,
        account_id: &str,
        worker_id: &str,
    ) -> Result<Option<OzonLaunchLease>, OzonPlanStoreError> {
        validate_identity(account_id)?;
        validate_identity(worker_id)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let tx = client
            .transaction()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        sweep_stale_launch_request(&tx, account_id, worker_id).await?;
        let candidate = tx
            .query_opt(
                "SELECT p.plan_id FROM control.ozon_campaign_plans p \
                 JOIN control.ozon_campaign_launch_workflows workflow \
                   ON workflow.plan_id=p.plan_id \
                 WHERE p.account_id=$1 \
                   AND p.status IN ('approved','created','products_added') \
                   AND workflow.requested_at IS NOT NULL \
                   AND EXISTS (SELECT 1 \
                       FROM control.ozon_campaign_plan_approvals approval \
                       WHERE approval.plan_id=p.plan_id \
                         AND approval.plan_digest=p.plan_digest \
                         AND (p.status<>'approved' \
                              OR approval.expires_at>clock_timestamp())) \
                   AND (p.status<>'approved' \
                        OR p.expires_at>clock_timestamp()) \
                   AND EXISTS (SELECT 1 \
                       FROM control.ozon_policy_revisions policy \
                       WHERE policy.schema_version=p.schema_version \
                         AND policy.policy_revision=p.policy_revision \
                         AND policy.policy_digest=p.policy_digest \
                         AND policy.policy_revision=(SELECT max(policy_revision) \
                             FROM control.ozon_policy_revisions)) \
                   AND (SELECT count(*)=3 AND bool_and( \
                           gate.enabled AND gate.lease_expires_at>clock_timestamp() \
                           AND (gate.disabled_until IS NULL \
                                OR gate.disabled_until<=clock_timestamp())) \
                        FROM control.ozon_runtime_gates gate \
                        WHERE gate.gate_key IN ( \
                            'global','account/'||p.account_id, \
                            'sku/'||p.account_id||'/'||p.sku::text)) \
                   AND workflow.available_at<=clock_timestamp() \
                   AND (workflow.lease_expires_at IS NULL \
                        OR workflow.lease_expires_at<=clock_timestamp()) \
                 ORDER BY workflow.requested_at,p.plan_id \
                 LIMIT 1 FOR UPDATE OF p,workflow SKIP LOCKED",
                &[&account_id],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let Some(candidate) = candidate else {
            tx.commit()
                .await
                .map_err(|_| OzonPlanStoreError::Unavailable)?;
            return Ok(None);
        };
        let plan_id: String = candidate.get(0);
        let plan = load_plan_for_update(&tx, &plan_id).await?;
        let lease = claim_workflow_locked(&tx, plan, worker_id, true).await?;
        if lease.mode != OzonLaunchClaimMode::Execute {
            return Err(OzonPlanStoreError::Unavailable);
        }
        tx.commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        Ok(Some(lease))
    }

    /// Claims one abandoned external-write state without ever making it
    /// executable again. Competing workers use `SKIP LOCKED`, and a strictly
    /// increasing generation fences the previous owner.
    pub(in crate::control) async fn claim_launch_recovery(
        &self,
        account_id: &str,
        worker_id: &str,
    ) -> Result<Option<OzonLaunchLease>, OzonPlanStoreError> {
        validate_identity(account_id)?;
        validate_identity(worker_id)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let tx = client
            .transaction()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let candidate = tx
            .query_opt(
                "SELECT p.plan_id FROM control.ozon_campaign_plans p \
                 JOIN control.ozon_campaign_launch_workflows workflow \
                   ON workflow.plan_id=p.plan_id \
                 WHERE p.account_id=$1 \
                   AND p.status IN ('creating','adding_products','activating','ambiguous') \
                   AND workflow.requested_at IS NOT NULL \
                   AND workflow.available_at<=clock_timestamp() \
                   AND (workflow.lease_expires_at IS NULL \
                        OR workflow.lease_expires_at<=clock_timestamp()) \
                 ORDER BY COALESCE(workflow.lease_expires_at,p.operation_started_at),p.plan_id \
                 LIMIT 1 FOR UPDATE OF p,workflow SKIP LOCKED",
                &[&account_id],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let Some(candidate) = candidate else {
            tx.commit()
                .await
                .map_err(|_| OzonPlanStoreError::Unavailable)?;
            return Ok(None);
        };
        let plan_id: String = candidate.get(0);
        let plan = load_plan_for_update(&tx, &plan_id).await?;
        let lease = claim_workflow_locked(&tx, plan, worker_id, true).await?;
        if lease.mode != OzonLaunchClaimMode::Reconcile {
            return Err(OzonPlanStoreError::Unavailable);
        }
        tx.commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        Ok(Some(lease))
    }

    /// Performs the final database-time authorization check and durably marks
    /// the HTTP boundary.  Recovery leases are deliberately rejected here.
    pub(in crate::control) async fn start_launch_write<F>(
        &self,
        lease: &OzonLaunchLease,
        create_identity_preflight_digest: Option<&str>,
        on_commit_attempted: F,
    ) -> Result<(), OzonPlanStoreError>
    where
        F: FnOnce(),
    {
        validate_launch_lease(lease)?;
        if lease.mode != OzonLaunchClaimMode::Execute {
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
        let plan = load_plan_for_update(&tx, &lease.plan.plan_id).await?;
        require_lease(&tx, lease).await?;
        if plan.actor_id != lease.plan.actor_id
            || plan.plan_digest != lease.plan.plan_digest
            || plan.status != lease.action.stable_status()
        {
            return Err(OzonPlanStoreError::InvalidState);
        }
        let expected_identity_digest = create_identity_preflight_digest_for(&plan);
        match lease.action {
            OzonLaunchAction::CreateCampaign
                if create_identity_preflight_digest != Some(expected_identity_digest.as_str())
                    || plan.manifest.create_request.title
                        != provider_title_for_plan_id(&plan.plan_id) =>
            {
                return Err(OzonPlanStoreError::InvalidPlan);
            }
            OzonLaunchAction::AddProducts | OzonLaunchAction::ActivateCampaign
                if create_identity_preflight_digest.is_some() =>
            {
                return Err(OzonPlanStoreError::InvalidPlan);
            }
            _ => {}
        }
        require_policy(
            &tx,
            i32::try_from(plan.schema_version).map_err(|_| OzonPlanStoreError::InvalidPlan)?,
            i64::try_from(plan.policy_revision).map_err(|_| OzonPlanStoreError::InvalidPlan)?,
            &plan.policy_digest,
        )
        .await?;
        require_gates(&tx, &plan.account_id, plan.sku).await?;
        if lease.action == OzonLaunchAction::CreateCampaign {
            require_active_approval(&tx, &plan).await?;
        } else {
            require_matching_approval(&tx, &plan).await?;
        }
        let now = database_now(&tx).await?;
        let generation =
            i64::try_from(lease.generation).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let started = tx
            .execute(
                "UPDATE control.ozon_campaign_launch_workflows \
                 SET write_started_at=$5, \
                     create_identity_preflight_at=CASE WHEN action='create_campaign' \
                         THEN $5 ELSE create_identity_preflight_at END, \
                     create_identity_preflight_digest=COALESCE( \
                         $6,create_identity_preflight_digest) \
                 WHERE plan_id=$1 AND generation=$2 AND lease_owner_id=$3 \
                   AND lease_token=$4 AND lease_expires_at>$5 \
                   AND write_started_at IS NULL",
                &[
                    &lease.plan.plan_id,
                    &generation,
                    &lease.owner_id,
                    &lease.lease_token,
                    &now,
                    &create_identity_preflight_digest,
                ],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        if started != 1 {
            return Err(OzonPlanStoreError::InvalidState);
        }
        let updated = tx
            .execute(
                "UPDATE control.ozon_campaign_plans SET status=$2 \
                 WHERE plan_id=$1 AND status=$3",
                &[
                    &plan.plan_id,
                    &lease.action.in_progress_status().as_db(),
                    &lease.action.stable_status().as_db(),
                ],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        if updated != 1 {
            return Err(OzonPlanStoreError::InvalidState);
        }
        insert_audit(
            &tx,
            &plan.plan_id,
            &lease.owner_id,
            "workflow_write_started",
            &serde_json::json!({
                "action": lease.action.as_db(),
                "generation": lease.generation,
            }),
        )
        .await?;
        // From this point a lost/cancelled COMMIT response is indistinguishable
        // from an applied marker. Tell the caller before the await so it never
        // classifies that boundary as definitely not started.
        on_commit_attempted();
        tx.commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        Ok(())
    }

    /// Commits one stage result under the lease fence and advances the durable
    /// continuation pointer. Reconciliation requires explicit readback proof.
    pub(in crate::control) async fn complete_launch_action(
        &self,
        lease: &OzonLaunchLease,
        campaign_id: Option<u64>,
        readback: Option<&Value>,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
        self.complete_launch_action_inner(lease, campaign_id, readback, false)
            .await
    }

    /// A recovery observation may prove the final running state even when the
    /// abandoned action was create or add-products. This never executes a POST.
    pub(in crate::control) async fn confirm_launch_applied(
        &self,
        lease: &OzonLaunchLease,
        campaign_id: u64,
        readback: &Value,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
        if !exact_running_readback(readback, campaign_id, &lease.plan) {
            return Err(OzonPlanStoreError::InvalidPlan);
        }
        self.complete_launch_action_inner(lease, Some(campaign_id), Some(readback), true)
            .await
    }

    async fn complete_launch_action_inner(
        &self,
        lease: &OzonLaunchLease,
        campaign_id: Option<u64>,
        readback: Option<&Value>,
        force_applied: bool,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
        validate_launch_lease(lease)?;
        if lease.mode == OzonLaunchClaimMode::Reconcile && readback.is_none() {
            return Err(OzonPlanStoreError::InvalidPlan);
        }
        let effective_campaign_id = campaign_id.or(lease.plan.campaign_id);
        if lease.action == OzonLaunchAction::CreateCampaign && effective_campaign_id.is_none() {
            return Err(OzonPlanStoreError::InvalidPlan);
        }
        if let (Some(expected), Some(actual)) = (lease.plan.campaign_id, campaign_id)
            && expected != actual
        {
            return Err(OzonPlanStoreError::InvalidPlan);
        }
        if !force_applied {
            let campaign_id = effective_campaign_id.ok_or(OzonPlanStoreError::InvalidPlan)?;
            let exact = match lease.action {
                OzonLaunchAction::CreateCampaign | OzonLaunchAction::AddProducts => {
                    stage_readback_is_exact(
                        readback.ok_or(OzonPlanStoreError::InvalidPlan)?,
                        lease.action,
                        &lease.plan,
                        campaign_id,
                    )
                }
                OzonLaunchAction::ActivateCampaign => exact_running_readback(
                    readback.ok_or(OzonPlanStoreError::InvalidPlan)?,
                    campaign_id,
                    &lease.plan,
                ),
            };
            if !exact {
                return Err(OzonPlanStoreError::InvalidPlan);
            }
        }
        let target = if force_applied {
            OzonLaunchStatus::Applied
        } else {
            lease.action.completed_status()
        };
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let tx = client
            .transaction()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let plan = load_plan_for_update(&tx, &lease.plan.plan_id).await?;
        require_lease(&tx, lease).await?;
        if lease.action == OzonLaunchAction::CreateCampaign {
            require_create_identity_preflight(&tx, &plan).await?;
        }
        let expected_status = match lease.mode {
            OzonLaunchClaimMode::Execute => lease.action.in_progress_status(),
            OzonLaunchClaimMode::Reconcile => plan.status,
        };
        if plan.status != expected_status
            || (lease.mode == OzonLaunchClaimMode::Reconcile
                && !matches!(
                    plan.status,
                    OzonLaunchStatus::Creating
                        | OzonLaunchStatus::AddingProducts
                        | OzonLaunchStatus::Activating
                        | OzonLaunchStatus::Ambiguous
                ))
        {
            return Err(OzonPlanStoreError::InvalidState);
        }
        let readback_json = readback
            .map(serde_json::to_string)
            .transpose()
            .map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        if readback_json.is_some() {
            record_recovery_readback(&tx, lease, readback_json.as_deref()).await?;
        }
        let campaign_id_i64 = effective_campaign_id
            .map(i64::try_from)
            .transpose()
            .map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let plan_readback = (target == OzonLaunchStatus::Applied)
            .then_some(readback_json.as_deref())
            .flatten();
        let updated = tx
            .execute(
                "UPDATE control.ozon_campaign_plans \
                 SET status=$2,campaign_id=COALESCE($3,campaign_id), \
                     last_error_class=NULL,readback_json=$4 \
                 WHERE plan_id=$1 AND status=$5",
                &[
                    &plan.plan_id,
                    &target.as_db(),
                    &campaign_id_i64,
                    &plan_readback,
                    &plan.status.as_db(),
                ],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        if updated != 1 {
            return Err(OzonPlanStoreError::InvalidState);
        }
        if target == OzonLaunchStatus::Applied {
            insert_guard(&tx, &plan, effective_campaign_id).await?;
        }
        finish_workflow_lease(&tx, lease, target, readback_json.as_deref()).await?;
        insert_audit(
            &tx,
            &plan.plan_id,
            &lease.owner_id,
            target.as_db(),
            &serde_json::json!({
                "action": lease.action.as_db(),
                "generation": lease.generation,
                "campaign_id": effective_campaign_id,
                "reconciled": lease.mode == OzonLaunchClaimMode::Reconcile,
            }),
        )
        .await?;
        tx.commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        self.load(&plan.plan_id).await
    }

    pub(in crate::control) async fn mark_launch_ambiguous(
        &self,
        lease: &OzonLaunchLease,
        error_class: &str,
        campaign_id: Option<u64>,
        readback: Option<&Value>,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
        validate_launch_lease(lease)?;
        validate_error_class(error_class)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let tx = client
            .transaction()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let plan = load_plan_for_update(&tx, &lease.plan.plan_id).await?;
        require_lease(&tx, lease).await?;
        let readback_json = readback
            .map(serde_json::to_string)
            .transpose()
            .map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let campaign_id = campaign_id
            .map(i64::try_from)
            .transpose()
            .map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        if plan.status != OzonLaunchStatus::Ambiguous {
            if plan.status != lease.action.in_progress_status() {
                return Err(OzonPlanStoreError::InvalidState);
            }
            let updated = tx
                .execute(
                    "UPDATE control.ozon_campaign_plans SET status='ambiguous', \
                 campaign_id=COALESCE($2,campaign_id),last_error_class=$3, \
                 readback_json=COALESCE($4,readback_json) \
                 WHERE plan_id=$1 AND status=$5",
                    &[
                        &plan.plan_id,
                        &campaign_id,
                        &error_class,
                        &readback_json,
                        &plan.status.as_db(),
                    ],
                )
                .await
                .map_err(|_| OzonPlanStoreError::Unavailable)?;
            if updated != 1 {
                return Err(OzonPlanStoreError::InvalidState);
            }
        }
        close_workflow_as_ambiguous(&tx, lease, error_class, readback_json.as_deref()).await?;
        insert_audit(
            &tx,
            &plan.plan_id,
            &lease.owner_id,
            "workflow_ambiguous",
            &serde_json::json!({"action":lease.action.as_db(),"generation":lease.generation,"error_class":error_class}),
        ).await?;
        tx.commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        self.load(&plan.plan_id).await
    }

    /// Records an exact pre-write vendor-state conflict as terminal. Once the
    /// durable write marker exists every provider outcome is ambiguous and this
    /// path is deliberately unavailable.
    pub(in crate::control) async fn fail_launch_action(
        &self,
        lease: &OzonLaunchLease,
        error_class: &str,
        campaign_id: Option<u64>,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
        validate_launch_lease(lease)?;
        if lease.mode != OzonLaunchClaimMode::Execute {
            return Err(OzonPlanStoreError::InvalidState);
        }
        validate_error_class(error_class)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let tx = client
            .transaction()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let plan = load_plan_for_update(&tx, &lease.plan.plan_id).await?;
        require_lease(&tx, lease).await?;
        let expected_error_class = match lease.action {
            OzonLaunchAction::CreateCampaign => "ozon_create_precondition_conflict",
            OzonLaunchAction::AddProducts => "ozon_products_precondition_conflict",
            OzonLaunchAction::ActivateCampaign => "ozon_activate_precondition_conflict",
        };
        if plan.status != lease.action.stable_status()
            || error_class != expected_error_class
            || campaign_id != plan.campaign_id
        {
            return Err(OzonPlanStoreError::InvalidState);
        }
        let campaign_id = campaign_id
            .map(i64::try_from)
            .transpose()
            .map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let updated = tx
            .execute(
                "UPDATE control.ozon_campaign_plans SET status='failed', \
             campaign_id=COALESCE($2,campaign_id),last_error_class=$3 \
             WHERE plan_id=$1 AND status=$4",
                &[
                    &plan.plan_id,
                    &campaign_id,
                    &error_class,
                    &plan.status.as_db(),
                ],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        if updated != 1 {
            return Err(OzonPlanStoreError::InvalidState);
        }
        close_workflow_as_ambiguous(&tx, lease, error_class, None).await?;
        insert_audit(&tx,&plan.plan_id,&lease.owner_id,"workflow_failed",
            &serde_json::json!({"action":lease.action.as_db(),"generation":lease.generation,"error_class":error_class})).await?;
        tx.commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        self.load(&plan.plan_id).await
    }

    /// Relinquishes a lease. If the write boundary was crossed, the persisted
    /// plan status ensures the next owner receives reconciliation-only work.
    pub(in crate::control) async fn release_launch_lease(
        &self,
        lease: &OzonLaunchLease,
        error_class: &str,
    ) -> Result<(), OzonPlanStoreError> {
        validate_launch_lease(lease)?;
        validate_error_class(error_class)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let tx = client
            .transaction()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let generation =
            i64::try_from(lease.generation).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let updated = tx
            .execute(
                "UPDATE control.ozon_campaign_launch_workflows SET \
             lease_owner_id=NULL,lease_token=NULL,lease_claimed_at=NULL, \
             lease_expires_at=NULL,write_started_at=NULL, \
             last_error_class=$5, \
             available_at=clock_timestamp()+interval '2 minutes' \
             WHERE plan_id=$1 AND generation=$2 AND lease_owner_id=$3 \
               AND lease_token=$4 AND lease_expires_at>clock_timestamp()",
                &[
                    &lease.plan.plan_id,
                    &generation,
                    &lease.owner_id,
                    &lease.lease_token,
                    &error_class,
                ],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        if updated != 1 {
            return Err(OzonPlanStoreError::InvalidState);
        }
        insert_audit(
            &tx,
            &lease.plan.plan_id,
            &lease.owner_id,
            "workflow_not_started",
            &serde_json::json!({
                "action": lease.action.as_db(),
                "generation": lease.generation,
                "error_class": error_class,
            }),
        )
        .await?;
        tx.commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        Ok(())
    }

    /// Returns only active guards owned by the configured runtime account.
    pub async fn active_guards_for_account(
        &self,
        account_id: &str,
    ) -> Result<Vec<OzonCampaignGuard>, OzonPlanStoreError> {
        validate_identity(account_id)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let rows = client
            .query(
                "SELECT plan_id,account_id,sku,campaign_id,date_from, \
                 spend_cap_microrubles,target_drr_percent \
                 FROM control.ozon_campaign_guards \
                 WHERE account_id=$1 AND status='active' ORDER BY campaign_id",
                &[&account_id],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        rows.iter().map(guard_from_row).collect()
    }

    /// Atomically changes an active guard to a fenced, recoverable stop.
    pub async fn claim_guard_stop_leased(
        &self,
        expected_guard: &OzonCampaignGuard,
        reason: &str,
        spend_minor: Option<u64>,
        revenue_minor: Option<u64>,
        worker_id: &str,
    ) -> Result<OzonGuardStopLease, OzonPlanStoreError> {
        validate_digest(&expected_guard.plan_id)?;
        validate_identity(&expected_guard.account_id)?;
        if expected_guard.status != OzonCampaignGuardStatus::Active
            || expected_guard.stop_reason.is_some()
            || expected_guard.incident_error_class.is_some()
        {
            return Err(OzonPlanStoreError::InvalidPlan);
        }
        validate_error_class(reason)?;
        validate_identity(worker_id)?;
        validate_guard_metrics_pair(spend_minor, revenue_minor)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let tx = client
            .transaction()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let campaign_id_i64 = i64::try_from(expected_guard.campaign_id)
            .map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let row = tx
            .query_opt(
                "SELECT plan_id,account_id,sku,campaign_id,date_from, \
             spend_cap_microrubles,target_drr_percent \
             FROM control.ozon_campaign_guards \
             WHERE plan_id=$1 AND account_id=$2 AND campaign_id=$3 \
               AND status='active' FOR UPDATE",
                &[
                    &expected_guard.plan_id,
                    &expected_guard.account_id,
                    &campaign_id_i64,
                ],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?
            .ok_or(OzonPlanStoreError::InvalidState)?;
        let mut guard = guard_from_row(&row)?;
        if &guard != expected_guard {
            return Err(OzonPlanStoreError::InvalidState);
        }
        let now = database_now(&tx).await?;
        let lease_expires_at = now + GUARD_STOP_LEASE_TTL;
        let generation = 1_i64;
        let lease_token = digest_fields(&[
            b"mcp-ozon/guard-stop-lease/v1",
            expected_guard.plan_id.as_bytes(),
            worker_id.as_bytes(),
            &generation.to_be_bytes(),
            &now.timestamp_micros().to_be_bytes(),
        ]);
        let updated = tx
            .execute(
                "UPDATE control.ozon_campaign_guards SET status='stopping',stop_reason=$4, \
             last_spend_minor=$5,last_revenue_minor=$6,last_checked_at=$7, \
             stop_generation=$8,stop_lease_owner_id=$9,stop_lease_token=$10, \
             stop_lease_claimed_at=$7,stop_lease_expires_at=$11 \
             WHERE plan_id=$1 AND account_id=$2 AND campaign_id=$3 \
               AND status='active'",
                &[
                    &expected_guard.plan_id,
                    &expected_guard.account_id,
                    &campaign_id_i64,
                    &reason,
                    &optional_i64(spend_minor)?,
                    &optional_i64(revenue_minor)?,
                    &now,
                    &generation,
                    &worker_id,
                    &lease_token,
                    &lease_expires_at,
                ],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        if updated != 1 {
            return Err(OzonPlanStoreError::InvalidState);
        }
        guard.status = OzonCampaignGuardStatus::Stopping;
        guard.stop_reason = Some(reason.to_owned());
        insert_audit(
            &tx,
            &guard.plan_id,
            worker_id,
            "guard_stop_claimed",
            &serde_json::json!({
                "generation": generation,
                "reason": reason,
                "metrics_present": spend_minor.is_some(),
            }),
        )
        .await?;
        tx.commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        Ok(OzonGuardStopLease {
            guard,
            stop_reason: reason.to_owned(),
            spend_minor,
            revenue_minor,
            generation: 1,
            owner_id: worker_id.to_owned(),
            lease_token,
            lease_expires_at,
            write_started_at: None,
        })
    }

    /// Reclaims one expired `stopping` guard. The generation increment fences
    /// the crashed owner and the caller may perform readback/deactivation
    /// reconciliation without losing the campaign from the work set.
    pub async fn claim_guard_stop_recovery(
        &self,
        account_id: &str,
        worker_id: &str,
    ) -> Result<Option<OzonGuardStopLease>, OzonPlanStoreError> {
        validate_identity(account_id)?;
        validate_identity(worker_id)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let tx = client
            .transaction()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let row = tx
            .query_opt(
                "SELECT plan_id,account_id,sku,campaign_id,date_from, \
             spend_cap_microrubles,target_drr_percent,stop_reason,stop_generation, \
             last_spend_minor,last_revenue_minor,stop_write_started_at \
             FROM control.ozon_campaign_guards WHERE account_id=$1 AND status='stopping' \
               AND stop_lease_expires_at<=clock_timestamp() \
             ORDER BY stop_lease_expires_at,plan_id LIMIT 1 \
             FOR UPDATE SKIP LOCKED",
                &[&account_id],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let Some(row) = row else {
            tx.commit()
                .await
                .map_err(|_| OzonPlanStoreError::Unavailable)?;
            return Ok(None);
        };
        let reason: String = row.get(7);
        let spend_minor = optional_u64(row.get::<_, Option<i64>>(9))?;
        let revenue_minor = optional_u64(row.get::<_, Option<i64>>(10))?;
        let write_started_at: Option<DateTime<Utc>> = row.get(11);
        validate_guard_metrics_pair(spend_minor, revenue_minor)
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let mut guard = guard_from_row(&row)?;
        guard.status = OzonCampaignGuardStatus::Stopping;
        guard.stop_reason = Some(reason.clone());
        let old_generation: i64 = row.get(8);
        let generation = old_generation
            .checked_add(1)
            .ok_or(OzonPlanStoreError::InvalidState)?;
        let now = database_now(&tx).await?;
        let lease_expires_at = now + GUARD_STOP_LEASE_TTL;
        let lease_token = digest_fields(&[
            b"mcp-ozon/guard-stop-recovery/v1",
            guard.plan_id.as_bytes(),
            worker_id.as_bytes(),
            &generation.to_be_bytes(),
            &now.timestamp_micros().to_be_bytes(),
        ]);
        let updated = tx
            .execute(
                "UPDATE control.ozon_campaign_guards SET stop_generation=$2, \
             stop_lease_owner_id=$3,stop_lease_token=$4,stop_lease_claimed_at=$5, \
             stop_lease_expires_at=$6 WHERE plan_id=$1 AND account_id=$8 \
             AND campaign_id=$9 AND status='stopping' \
             AND stop_generation=$7 AND stop_lease_expires_at<=$5",
                &[
                    &guard.plan_id,
                    &generation,
                    &worker_id,
                    &lease_token,
                    &now,
                    &lease_expires_at,
                    &old_generation,
                    &guard.account_id,
                    &i64::try_from(guard.campaign_id)
                        .map_err(|_| OzonPlanStoreError::InvalidPlan)?,
                ],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        if updated != 1 {
            return Err(OzonPlanStoreError::InvalidState);
        }
        insert_audit(
            &tx,
            &guard.plan_id,
            worker_id,
            "guard_stop_reclaimed",
            &serde_json::json!({
                "previous_generation": old_generation,
                "generation": generation,
                "write_started": write_started_at.is_some(),
            }),
        )
        .await?;
        tx.commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        Ok(Some(OzonGuardStopLease {
            guard,
            stop_reason: reason,
            spend_minor,
            revenue_minor,
            generation: u64::try_from(generation).map_err(|_| OzonPlanStoreError::Unavailable)?,
            owner_id: worker_id.to_owned(),
            lease_token,
            lease_expires_at,
            write_started_at,
        }))
    }

    pub async fn finish_guard_leased(
        &self,
        lease: &OzonGuardStopLease,
        spend_minor: Option<u64>,
        revenue_minor: Option<u64>,
    ) -> Result<(), OzonPlanStoreError> {
        self.finish_guard_lease(lease, "stopped", None, spend_minor, revenue_minor)
            .await
    }

    /// Persists the irreversible guard mutation boundary under the full lease
    /// fence. Once this succeeds, every later owner is readback-only.
    pub async fn start_guard_stop_write(
        &self,
        lease: &OzonGuardStopLease,
    ) -> Result<(), OzonPlanStoreError> {
        validate_guard_stop_lease(lease)?;
        if lease.write_started_at.is_some() {
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
        let plan_policy = tx
            .query_opt(
                "SELECT schema_version,policy_revision,policy_digest \
                 FROM control.ozon_campaign_plans \
                 WHERE plan_id=$1 AND account_id=$2 AND sku=$3 \
                   AND campaign_id=$4 AND status='applied'",
                &[
                    &lease.guard.plan_id,
                    &lease.guard.account_id,
                    &i64::try_from(lease.guard.sku).map_err(|_| OzonPlanStoreError::InvalidPlan)?,
                    &i64::try_from(lease.guard.campaign_id)
                        .map_err(|_| OzonPlanStoreError::InvalidPlan)?,
                ],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?
            .ok_or(OzonPlanStoreError::InvalidState)?;
        require_policy(
            &tx,
            plan_policy.get(0),
            plan_policy.get(1),
            plan_policy.get(2),
        )
        .await?;
        // `require_gates` takes row locks on every current permit.  Holding
        // those locks through the marker commit closes the revoke-before-send
        // race: after this transaction succeeds the caller's next fallible
        // operation is the provider mutation.
        require_gates(&tx, &lease.guard.account_id, lease.guard.sku).await?;
        let updated = tx
            .execute(
                "UPDATE control.ozon_campaign_guards \
                 SET stop_write_started_at=clock_timestamp() \
                 WHERE plan_id=$1 AND account_id=$2 AND sku=$3 AND campaign_id=$4 \
                   AND date_from=$5 AND spend_cap_microrubles=$6 \
                   AND target_drr_percent=$7 AND stop_reason=$8 \
                   AND status='stopping' AND stop_generation=$9 \
                   AND stop_lease_owner_id=$10 AND stop_lease_token=$11 \
                   AND stop_lease_expires_at>clock_timestamp() \
                   AND stop_write_started_at IS NULL",
                &[
                    &lease.guard.plan_id,
                    &lease.guard.account_id,
                    &i64::try_from(lease.guard.sku).map_err(|_| OzonPlanStoreError::InvalidPlan)?,
                    &i64::try_from(lease.guard.campaign_id)
                        .map_err(|_| OzonPlanStoreError::InvalidPlan)?,
                    &lease.guard.date_from,
                    &i64::try_from(lease.guard.spend_cap_microrubles)
                        .map_err(|_| OzonPlanStoreError::InvalidPlan)?,
                    &i16::from(lease.guard.target_drr_percent),
                    &lease.stop_reason,
                    &i64::try_from(lease.generation)
                        .map_err(|_| OzonPlanStoreError::InvalidPlan)?,
                    &lease.owner_id,
                    &lease.lease_token,
                ],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        if updated != 1 {
            return Err(OzonPlanStoreError::LeaseLost);
        }
        insert_audit(
            &tx,
            &lease.guard.plan_id,
            &lease.owner_id,
            "guard_stop_write_started",
            &serde_json::json!({"generation": lease.generation}),
        )
        .await?;
        tx.commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        Ok(())
    }

    /// Persists one bounded readback result under the current stop lease. The
    /// audit row is append-only; unavailable evidence intentionally leaves the
    /// mutable guard in `stopping` for the next readback-only recovery owner.
    pub async fn record_guard_stop_readback(
        &self,
        lease: &OzonGuardStopLease,
        observation: OzonGuardStopReadback,
    ) -> Result<(), OzonPlanStoreError> {
        validate_guard_stop_lease(lease)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let tx = client
            .transaction()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        require_guard_stop_lease(&tx, lease).await?;
        insert_audit(
            &tx,
            &lease.guard.plan_id,
            &lease.owner_id,
            observation.event_type(),
            &serde_json::json!({"generation": lease.generation}),
        )
        .await?;
        tx.commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        Ok(())
    }

    pub async fn mark_guard_incident_leased(
        &self,
        lease: &OzonGuardStopLease,
        error_class: &str,
        spend_minor: Option<u64>,
        revenue_minor: Option<u64>,
    ) -> Result<(), OzonPlanStoreError> {
        validate_error_class(error_class)?;
        self.finish_guard_lease(
            lease,
            "incident",
            Some(error_class),
            spend_minor,
            revenue_minor,
        )
        .await
    }

    async fn finish_guard_lease(
        &self,
        lease: &OzonGuardStopLease,
        status: &str,
        incident_error_class: Option<&str>,
        spend_minor: Option<u64>,
        revenue_minor: Option<u64>,
    ) -> Result<(), OzonPlanStoreError> {
        validate_guard_stop_lease(lease)?;
        if let Some(error_class) = incident_error_class {
            validate_error_class(error_class)?;
        }
        if (status == "incident") != incident_error_class.is_some() {
            return Err(OzonPlanStoreError::InvalidPlan);
        }
        validate_guard_metrics_pair(spend_minor, revenue_minor)?;
        if spend_minor != lease.spend_minor || revenue_minor != lease.revenue_minor {
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
        let generation =
            i64::try_from(lease.generation).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let campaign_id =
            i64::try_from(lease.guard.campaign_id).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let sku = i64::try_from(lease.guard.sku).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let spend_cap = i64::try_from(lease.guard.spend_cap_microrubles)
            .map_err(|_| OzonPlanStoreError::InvalidPlan)?;
        let target_drr = i16::from(lease.guard.target_drr_percent);
        let spend = optional_i64(spend_minor)?;
        let revenue = optional_i64(revenue_minor)?;
        let updated = tx
            .execute(
                "UPDATE control.ozon_campaign_guards \
             SET status=$11,incident_error_class=$12, \
                 stopped_at=CASE WHEN $11='stopped' THEN clock_timestamp() ELSE NULL END \
             WHERE plan_id=$1 AND account_id=$2 AND sku=$3 AND campaign_id=$4 \
               AND date_from=$5 AND spend_cap_microrubles=$6 \
               AND target_drr_percent=$7 AND status='stopping' \
               AND stop_generation=$8 AND stop_lease_owner_id=$9 \
               AND stop_lease_token=$10 AND stop_lease_expires_at>clock_timestamp() \
               AND stop_reason=$13 \
               AND last_spend_minor IS NOT DISTINCT FROM $14 \
               AND last_revenue_minor IS NOT DISTINCT FROM $15",
                &[
                    &lease.guard.plan_id,
                    &lease.guard.account_id,
                    &sku,
                    &campaign_id,
                    &lease.guard.date_from,
                    &spend_cap,
                    &target_drr,
                    &generation,
                    &lease.owner_id,
                    &lease.lease_token,
                    &status,
                    &incident_error_class,
                    &lease.stop_reason,
                    &spend,
                    &revenue,
                ],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        if updated == 1 {
            insert_audit(
                &tx,
                &lease.guard.plan_id,
                &lease.owner_id,
                if status == "stopped" {
                    "guard_stop_stopped"
                } else {
                    "guard_stop_incident"
                },
                &serde_json::json!({
                    "generation": lease.generation,
                    "stop_reason": lease.stop_reason,
                    "incident_error_class": incident_error_class,
                    "metrics_present": spend_minor.is_some(),
                }),
            )
            .await?;
            tx.commit()
                .await
                .map_err(|_| OzonPlanStoreError::Unavailable)?;
            drop(client);
            return Ok(());
        }
        let exact: bool = tx
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM control.ozon_campaign_guards \
             WHERE plan_id=$1 AND account_id=$2 AND sku=$3 AND campaign_id=$4 \
               AND date_from=$5 AND spend_cap_microrubles=$6 \
               AND target_drr_percent=$7 AND status=$11 \
               AND stop_generation=$8 AND stop_lease_owner_id=$9 \
               AND stop_lease_token=$10 AND stop_reason=$13 \
               AND incident_error_class IS NOT DISTINCT FROM $12 \
               AND last_spend_minor IS NOT DISTINCT FROM $14 \
               AND last_revenue_minor IS NOT DISTINCT FROM $15 \
               AND (($11='stopped' AND stopped_at IS NOT NULL) \
                    OR ($11='incident' AND stopped_at IS NULL)))",
                &[
                    &lease.guard.plan_id,
                    &lease.guard.account_id,
                    &sku,
                    &campaign_id,
                    &lease.guard.date_from,
                    &spend_cap,
                    &target_drr,
                    &generation,
                    &lease.owner_id,
                    &lease.lease_token,
                    &status,
                    &incident_error_class,
                    &lease.stop_reason,
                    &spend,
                    &revenue,
                ],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?
            .get(0);
        tx.commit()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        if exact {
            Ok(())
        } else {
            Err(OzonPlanStoreError::InvalidState)
        }
    }

    pub async fn record_guard_observation(
        &self,
        expected_guard: &OzonCampaignGuard,
        spend_minor: u64,
        revenue_minor: u64,
    ) -> Result<(), OzonPlanStoreError> {
        validate_digest(&expected_guard.plan_id)?;
        validate_identity(&expected_guard.account_id)?;
        if expected_guard.status != OzonCampaignGuardStatus::Active
            || expected_guard.stop_reason.is_some()
            || expected_guard.incident_error_class.is_some()
        {
            return Err(OzonPlanStoreError::InvalidPlan);
        }
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        let updated = client
            .execute(
                "UPDATE control.ozon_campaign_guards \
                 SET last_spend_minor=$8,last_revenue_minor=$9, \
                     last_checked_at=clock_timestamp() \
                 WHERE plan_id=$1 AND account_id=$2 AND sku=$3 \
                   AND campaign_id=$4 AND date_from=$5 \
                   AND spend_cap_microrubles=$6 AND target_drr_percent=$7 \
                   AND status='active'",
                &[
                    &expected_guard.plan_id,
                    &expected_guard.account_id,
                    &i64::try_from(expected_guard.sku)
                        .map_err(|_| OzonPlanStoreError::InvalidPlan)?,
                    &i64::try_from(expected_guard.campaign_id)
                        .map_err(|_| OzonPlanStoreError::InvalidPlan)?,
                    &expected_guard.date_from,
                    &i64::try_from(expected_guard.spend_cap_microrubles)
                        .map_err(|_| OzonPlanStoreError::InvalidPlan)?,
                    &i16::from(expected_guard.target_drr_percent),
                    &i64::try_from(spend_minor).map_err(|_| OzonPlanStoreError::InvalidPlan)?,
                    &i64::try_from(revenue_minor).map_err(|_| OzonPlanStoreError::InvalidPlan)?,
                ],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
        drop(client);
        if updated == 1 {
            Ok(())
        } else {
            Err(OzonPlanStoreError::InvalidState)
        }
    }
}

async fn load_plan_for_update(
    tx: &Transaction<'_>,
    plan_id: &str,
) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
    let query = format!("{PLAN_SELECT} WHERE p.plan_id=$1 FOR UPDATE OF p");
    let row = tx
        .query_opt(&query, &[&plan_id])
        .await
        .map_err(|_| OzonPlanStoreError::Unavailable)?
        .ok_or(OzonPlanStoreError::NotFound)?;
    plan_from_row(&row)
}

async fn sweep_stale_launch_request(
    tx: &Transaction<'_>,
    account_id: &str,
    worker_id: &str,
) -> Result<(), OzonPlanStoreError> {
    let stale = tx
        .query_opt(
            "SELECT p.plan_id,p.status FROM control.ozon_campaign_plans p \
             JOIN control.ozon_campaign_launch_workflows workflow \
               ON workflow.plan_id=p.plan_id \
             WHERE p.account_id=$1 \
               AND p.status='approved' \
               AND workflow.requested_at IS NOT NULL \
               AND (workflow.lease_expires_at IS NULL \
                    OR workflow.lease_expires_at<=clock_timestamp()) \
               AND (p.expires_at<=clock_timestamp() \
                    OR NOT EXISTS (SELECT 1 \
                        FROM control.ozon_campaign_plan_approvals approval \
                        WHERE approval.plan_id=p.plan_id \
                          AND approval.plan_digest=p.plan_digest \
                          AND approval.expires_at>clock_timestamp()) \
                    OR NOT EXISTS (SELECT 1 \
                        FROM control.ozon_policy_revisions policy \
                        WHERE policy.schema_version=p.schema_version \
                          AND policy.policy_revision=p.policy_revision \
                          AND policy.policy_digest=p.policy_digest \
                          AND policy.policy_revision=(SELECT max(policy_revision) \
                              FROM control.ozon_policy_revisions))) \
             ORDER BY workflow.requested_at,p.plan_id \
             LIMIT 1 FOR UPDATE OF p,workflow SKIP LOCKED",
            &[&account_id],
        )
        .await
        .map_err(|_| OzonPlanStoreError::Unavailable)?;
    let Some(stale) = stale else {
        return Ok(());
    };
    let plan_id: String = stale.get(0);
    let status = OzonLaunchStatus::from_db(stale.get(1))?;
    if status != OzonLaunchStatus::Approved {
        return Err(OzonPlanStoreError::Unavailable);
    }
    let target = OzonLaunchStatus::Expired;
    let error_class: Option<&str> = None;
    let updated = tx
        .execute(
            "UPDATE control.ozon_campaign_plans SET status=$2, \
             last_error_class=$3,finished_at=clock_timestamp() \
             WHERE plan_id=$1 AND status=$4",
            &[&plan_id, &target.as_db(), &error_class, &status.as_db()],
        )
        .await
        .map_err(|_| OzonPlanStoreError::Unavailable)?;
    if updated != 1 {
        return Err(OzonPlanStoreError::InvalidState);
    }
    insert_audit(
        tx,
        &plan_id,
        worker_id,
        "workflow_initial_authorization_expired",
        &serde_json::json!({"previous_status":status.as_db()}),
    )
    .await
}

async fn claim_workflow_locked(
    tx: &Transaction<'_>,
    plan: OzonCampaignPlan,
    worker_id: &str,
    respect_retry_delay: bool,
) -> Result<OzonLaunchLease, OzonPlanStoreError> {
    if !plan.status.is_durable_workflow_pending() {
        return Err(OzonPlanStoreError::InvalidState);
    }
    let row = tx
        .query_one(
            "SELECT action,generation,lease_expires_at,available_at,requested_at \
             FROM control.ozon_campaign_launch_workflows \
             WHERE plan_id=$1 FOR UPDATE",
            &[&plan.plan_id],
        )
        .await
        .map_err(|_| OzonPlanStoreError::Unavailable)?;
    let action = OzonLaunchAction::from_db(row.get(0))?;
    let current_generation: i64 = row.get(1);
    let lease_expires_at: Option<DateTime<Utc>> = row.get(2);
    let available_at: DateTime<Utc> = row.get(3);
    let requested_at: Option<DateTime<Utc>> = row.get(4);
    let now = database_now(tx).await?;
    if requested_at.is_none()
        || lease_expires_at.is_some_and(|expires_at| expires_at > now)
        || (respect_retry_delay && available_at > now)
    {
        return Err(OzonPlanStoreError::InvalidState);
    }
    let mode = workflow_claim_mode(plan.status, action)?;
    if mode == OzonLaunchClaimMode::Execute {
        require_policy(
            tx,
            i32::try_from(plan.schema_version).map_err(|_| OzonPlanStoreError::InvalidPlan)?,
            i64::try_from(plan.policy_revision).map_err(|_| OzonPlanStoreError::InvalidPlan)?,
            &plan.policy_digest,
        )
        .await?;
        require_gates(tx, &plan.account_id, plan.sku).await?;
        if action == OzonLaunchAction::CreateCampaign {
            require_active_approval(tx, &plan).await?;
        } else {
            require_matching_approval(tx, &plan).await?;
        }
        if action == OzonLaunchAction::CreateCampaign {
            let sku = i64::try_from(plan.sku).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
            tx.execute(
                "INSERT INTO control.ozon_campaign_action_reservations( \
                 plan_id,account_id,sku,reserved_at) \
                 VALUES($1,$2,$3,clock_timestamp()) ON CONFLICT(plan_id) DO NOTHING",
                &[&plan.plan_id, &plan.account_id, &sku],
            )
            .await
            .map_err(|_| OzonPlanStoreError::Unavailable)?;
            let reservation_matches: bool = tx
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM control.ozon_campaign_action_reservations \
                     WHERE plan_id=$1 AND account_id=$2 AND sku=$3)",
                    &[&plan.plan_id, &plan.account_id, &sku],
                )
                .await
                .map_err(|_| OzonPlanStoreError::Unavailable)?
                .get(0);
            if !reservation_matches {
                return Err(OzonPlanStoreError::InvalidState);
            }
        }
    }
    let generation = current_generation
        .checked_add(1)
        .ok_or(OzonPlanStoreError::InvalidState)?;
    let lease_expires_at = now + WORKFLOW_LEASE_TTL;
    let lease_token = digest_fields(&[
        b"mcp-ozon/launch-lease/v1",
        plan.plan_id.as_bytes(),
        worker_id.as_bytes(),
        &generation.to_be_bytes(),
        &now.timestamp_micros().to_be_bytes(),
    ]);
    let updated = tx
        .execute(
            "UPDATE control.ozon_campaign_launch_workflows SET \
             generation=$2,lease_owner_id=$3,lease_token=$4, \
             lease_claimed_at=$5,lease_expires_at=$6,write_started_at=NULL \
             WHERE plan_id=$1 AND generation=$7 \
               AND (lease_expires_at IS NULL OR lease_expires_at<=$5)",
            &[
                &plan.plan_id,
                &generation,
                &worker_id,
                &lease_token,
                &now,
                &lease_expires_at,
                &current_generation,
            ],
        )
        .await
        .map_err(|_| OzonPlanStoreError::Unavailable)?;
    if updated != 1 {
        return Err(OzonPlanStoreError::InvalidState);
    }
    insert_audit(
        tx,
        &plan.plan_id,
        worker_id,
        match mode {
            OzonLaunchClaimMode::Execute => "workflow_execute_claimed",
            OzonLaunchClaimMode::Reconcile => "workflow_reconcile_claimed",
        },
        &serde_json::json!({"action":action.as_db(),"generation":generation}),
    )
    .await?;
    Ok(OzonLaunchLease {
        plan,
        action,
        mode,
        generation: u64::try_from(generation).map_err(|_| OzonPlanStoreError::Unavailable)?,
        owner_id: worker_id.to_owned(),
        lease_token,
    })
}

fn workflow_claim_mode(
    status: OzonLaunchStatus,
    action: OzonLaunchAction,
) -> Result<OzonLaunchClaimMode, OzonPlanStoreError> {
    if status == action.stable_status() {
        Ok(OzonLaunchClaimMode::Execute)
    } else if status == action.in_progress_status() || status == OzonLaunchStatus::Ambiguous {
        Ok(OzonLaunchClaimMode::Reconcile)
    } else {
        Err(OzonPlanStoreError::Unavailable)
    }
}

async fn require_active_approval(
    tx: &Transaction<'_>,
    plan: &OzonCampaignPlan,
) -> Result<(), OzonPlanStoreError> {
    let active: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM control.ozon_campaign_plan_approvals \
             WHERE plan_id=$1 AND plan_digest=$2 \
               AND expires_at>clock_timestamp())",
            &[&plan.plan_id, &plan.plan_digest],
        )
        .await
        .map_err(|_| OzonPlanStoreError::Unavailable)?
        .get(0);
    active
        .then_some(())
        .ok_or(OzonPlanStoreError::ApprovalExpired)
}

async fn require_matching_approval(
    tx: &Transaction<'_>,
    plan: &OzonCampaignPlan,
) -> Result<(), OzonPlanStoreError> {
    let matching: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM control.ozon_campaign_plan_approvals \
             WHERE plan_id=$1 AND plan_digest=$2)",
            &[&plan.plan_id, &plan.plan_digest],
        )
        .await
        .map_err(|_| OzonPlanStoreError::Unavailable)?
        .get(0);
    matching
        .then_some(())
        .ok_or(OzonPlanStoreError::InvalidState)
}

fn validate_launch_lease(lease: &OzonLaunchLease) -> Result<(), OzonPlanStoreError> {
    validate_digest(&lease.plan.plan_id)?;
    validate_identity(&lease.owner_id)?;
    validate_digest(&lease.lease_token)?;
    if lease.generation == 0 {
        return Err(OzonPlanStoreError::InvalidPlan);
    }
    Ok(())
}

fn validate_guard_stop_lease(lease: &OzonGuardStopLease) -> Result<(), OzonPlanStoreError> {
    validate_digest(&lease.guard.plan_id)?;
    validate_identity(&lease.guard.account_id)?;
    validate_identity(&lease.owner_id)?;
    validate_digest(&lease.lease_token)?;
    if lease.guard.campaign_id == 0
        || lease.generation == 0
        || lease.guard.status != OzonCampaignGuardStatus::Stopping
        || lease.guard.stop_reason.as_deref() != Some(lease.stop_reason.as_str())
        || lease.guard.incident_error_class.is_some()
    {
        return Err(OzonPlanStoreError::InvalidPlan);
    }
    Ok(())
}

async fn require_lease(
    tx: &Transaction<'_>,
    lease: &OzonLaunchLease,
) -> Result<(), OzonPlanStoreError> {
    let generation =
        i64::try_from(lease.generation).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
    let active: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM control.ozon_campaign_launch_workflows \
             WHERE plan_id=$1 AND action=$2 AND generation=$3 \
               AND lease_owner_id=$4 AND lease_token=$5 \
               AND lease_expires_at>clock_timestamp())",
            &[
                &lease.plan.plan_id,
                &lease.action.as_db(),
                &generation,
                &lease.owner_id,
                &lease.lease_token,
            ],
        )
        .await
        .map_err(|_| OzonPlanStoreError::Unavailable)?
        .get(0);
    active.then_some(()).ok_or(OzonPlanStoreError::InvalidState)
}

pub(super) fn create_identity_preflight_digest_for(plan: &OzonCampaignPlan) -> String {
    digest_fields(&[
        b"mcp-ozon/create-identity-preflight/v1",
        plan.plan_digest.as_bytes(),
        plan.account_id.as_bytes(),
        plan.manifest.create_request.title.as_bytes(),
    ])
}

async fn require_create_identity_preflight(
    tx: &Transaction<'_>,
    plan: &OzonCampaignPlan,
) -> Result<(), OzonPlanStoreError> {
    let expected = create_identity_preflight_digest_for(plan);
    let exact: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 \
             FROM control.ozon_campaign_launch_workflows \
             WHERE plan_id=$1 AND action='create_campaign' \
               AND create_identity_preflight_at IS NOT NULL \
               AND create_identity_preflight_digest=$2)",
            &[&plan.plan_id, &expected],
        )
        .await
        .map_err(|_| OzonPlanStoreError::Unavailable)?
        .get(0);
    exact.then_some(()).ok_or(OzonPlanStoreError::InvalidPlan)
}

async fn require_guard_stop_lease(
    tx: &Transaction<'_>,
    lease: &OzonGuardStopLease,
) -> Result<(), OzonPlanStoreError> {
    let generation =
        i64::try_from(lease.generation).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
    let sku = i64::try_from(lease.guard.sku).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
    let campaign_id =
        i64::try_from(lease.guard.campaign_id).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
    let spend_cap = i64::try_from(lease.guard.spend_cap_microrubles)
        .map_err(|_| OzonPlanStoreError::InvalidPlan)?;
    let active: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM control.ozon_campaign_guards \
             WHERE plan_id=$1 AND account_id=$2 AND sku=$3 AND campaign_id=$4 \
               AND date_from=$5 AND spend_cap_microrubles=$6 \
               AND target_drr_percent=$7 AND status='stopping' \
               AND stop_reason=$8 AND stop_generation=$9 \
               AND stop_lease_owner_id=$10 AND stop_lease_token=$11 \
               AND stop_lease_expires_at>clock_timestamp() \
               AND last_spend_minor IS NOT DISTINCT FROM $12 \
               AND last_revenue_minor IS NOT DISTINCT FROM $13)",
            &[
                &lease.guard.plan_id,
                &lease.guard.account_id,
                &sku,
                &campaign_id,
                &lease.guard.date_from,
                &spend_cap,
                &i16::from(lease.guard.target_drr_percent),
                &lease.stop_reason,
                &generation,
                &lease.owner_id,
                &lease.lease_token,
                &optional_i64(lease.spend_minor)?,
                &optional_i64(lease.revenue_minor)?,
            ],
        )
        .await
        .map_err(|_| OzonPlanStoreError::Unavailable)?
        .get(0);
    active.then_some(()).ok_or(OzonPlanStoreError::LeaseLost)
}

async fn record_recovery_readback(
    tx: &Transaction<'_>,
    lease: &OzonLaunchLease,
    readback_json: Option<&str>,
) -> Result<(), OzonPlanStoreError> {
    let generation =
        i64::try_from(lease.generation).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
    let updated = tx
        .execute(
            "UPDATE control.ozon_campaign_launch_workflows \
             SET last_readback_json=$6 \
             WHERE plan_id=$1 AND action=$2 AND generation=$3 \
               AND lease_owner_id=$4 AND lease_token=$5 \
               AND lease_expires_at>clock_timestamp()",
            &[
                &lease.plan.plan_id,
                &lease.action.as_db(),
                &generation,
                &lease.owner_id,
                &lease.lease_token,
                &readback_json,
            ],
        )
        .await
        .map_err(|_| OzonPlanStoreError::Unavailable)?;
    if updated == 1 {
        Ok(())
    } else {
        Err(OzonPlanStoreError::InvalidState)
    }
}

async fn finish_workflow_lease(
    tx: &Transaction<'_>,
    lease: &OzonLaunchLease,
    target: OzonLaunchStatus,
    readback_json: Option<&str>,
) -> Result<(), OzonPlanStoreError> {
    let generation =
        i64::try_from(lease.generation).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
    let next_action = if target == OzonLaunchStatus::Applied {
        OzonLaunchAction::ActivateCampaign
    } else {
        lease
            .action
            .next()
            .ok_or(OzonPlanStoreError::InvalidState)?
    };
    let updated = tx
        .execute(
            "UPDATE control.ozon_campaign_launch_workflows SET action=$6, \
         lease_owner_id=NULL,lease_token=NULL,lease_claimed_at=NULL, \
         lease_expires_at=NULL,write_started_at=NULL,available_at=clock_timestamp(), \
         last_completed_at=clock_timestamp(),last_error_class=NULL, \
         last_readback_json=COALESCE($7,last_readback_json) \
         WHERE plan_id=$1 AND action=$2 AND generation=$3 \
           AND lease_owner_id=$4 AND lease_token=$5 \
           AND lease_expires_at>clock_timestamp()",
            &[
                &lease.plan.plan_id,
                &lease.action.as_db(),
                &generation,
                &lease.owner_id,
                &lease.lease_token,
                &next_action.as_db(),
                &readback_json,
            ],
        )
        .await
        .map_err(|_| OzonPlanStoreError::Unavailable)?;
    if updated == 1 {
        Ok(())
    } else {
        Err(OzonPlanStoreError::InvalidState)
    }
}

async fn close_workflow_as_ambiguous(
    tx: &Transaction<'_>,
    lease: &OzonLaunchLease,
    error_class: &str,
    readback_json: Option<&str>,
) -> Result<(), OzonPlanStoreError> {
    let generation =
        i64::try_from(lease.generation).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
    let available_at = database_now(tx).await? + WORKFLOW_RECOVERY_BACKOFF;
    let updated = tx
        .execute(
            "UPDATE control.ozon_campaign_launch_workflows SET \
         lease_owner_id=NULL,lease_token=NULL,lease_claimed_at=NULL, \
         lease_expires_at=NULL,write_started_at=NULL,available_at=$6, \
         last_error_class=$7,last_readback_json=COALESCE($8,last_readback_json) \
         WHERE plan_id=$1 AND action=$2 AND generation=$3 \
           AND lease_owner_id=$4 AND lease_token=$5 \
           AND lease_expires_at>clock_timestamp()",
            &[
                &lease.plan.plan_id,
                &lease.action.as_db(),
                &generation,
                &lease.owner_id,
                &lease.lease_token,
                &available_at,
                &error_class,
                &readback_json,
            ],
        )
        .await
        .map_err(|_| OzonPlanStoreError::Unavailable)?;
    if updated == 1 {
        Ok(())
    } else {
        Err(OzonPlanStoreError::InvalidState)
    }
}

async fn insert_guard(
    tx: &Transaction<'_>,
    plan: &OzonCampaignPlan,
    campaign_id: Option<u64>,
) -> Result<(), OzonPlanStoreError> {
    let campaign_id = i64::try_from(campaign_id.ok_or(OzonPlanStoreError::InvalidPlan)?)
        .map_err(|_| OzonPlanStoreError::InvalidPlan)?;
    let sku = i64::try_from(plan.sku).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
    let spend = i64::try_from(plan.manifest.spec.per_sku_spend_cap_microrubles)
        .map_err(|_| OzonPlanStoreError::InvalidPlan)?;
    tx.execute(
        "INSERT INTO control.ozon_campaign_guards( \
         plan_id,account_id,sku,campaign_id,date_from,spend_cap_microrubles, \
         target_drr_percent,status,created_at) \
         VALUES($1,$2,$3,$4,$5,$6,$7,'active',clock_timestamp()) \
         ON CONFLICT(plan_id) DO NOTHING",
        &[
            &plan.plan_id,
            &plan.account_id,
            &sku,
            &campaign_id,
            &plan.manifest.spec.from_date,
            &spend,
            &i16::from(plan.manifest.spec.target_drr_percent),
        ],
    )
    .await
    .map_err(|_| OzonPlanStoreError::Unavailable)?;
    let stored = tx
        .query_one(
            "SELECT account_id,sku,campaign_id,date_from,spend_cap_microrubles, \
             target_drr_percent,status FROM control.ozon_campaign_guards \
             WHERE plan_id=$1",
            &[&plan.plan_id],
        )
        .await
        .map_err(|_| OzonPlanStoreError::Unavailable)?;
    if stored.get::<_, &str>(0) != plan.account_id
        || stored.get::<_, i64>(1) != sku
        || stored.get::<_, i64>(2) != campaign_id
        || stored.get::<_, &str>(3) != plan.manifest.spec.from_date
        || stored.get::<_, i64>(4) != spend
        || stored.get::<_, i16>(5) != i16::from(plan.manifest.spec.target_drr_percent)
        || stored.get::<_, &str>(6) != "active"
    {
        return Err(OzonPlanStoreError::InvalidState);
    }
    Ok(())
}

fn stage_readback_is_exact(
    readback: &Value,
    action: OzonLaunchAction,
    plan: &OzonCampaignPlan,
    campaign_id: u64,
) -> bool {
    if json_u64(readback.get("campaign_id")) != Some(campaign_id)
        || readback.get("action").and_then(Value::as_str) != Some(action.as_db())
        || readback.get("verified").and_then(Value::as_bool) != Some(true)
        || readback.get("title").and_then(Value::as_str)
            != Some(plan.manifest.create_request.title.as_str())
    {
        return false;
    }
    match action {
        OzonLaunchAction::CreateCampaign => readback
            .get("state")
            .and_then(Value::as_str)
            .is_some_and(is_supported_non_running_state),
        OzonLaunchAction::AddProducts => {
            json_u64(readback.get("sku")) == Some(plan.sku)
                && json_u64(readback.get("bid_microrubles"))
                    == Some(plan.manifest.spec.initial_cpc_bid_microrubles)
        }
        // Activation is committed only by the stricter running-readback path.
        OzonLaunchAction::ActivateCampaign => false,
    }
}

fn is_supported_non_running_state(state: &str) -> bool {
    matches!(
        state,
        "CAMPAIGN_STATE_STOPPED" | "CAMPAIGN_STATE_INACTIVE" | "CAMPAIGN_STATE_PLANNED"
    )
}

fn exact_running_readback(readback: &Value, campaign_id: u64, plan: &OzonCampaignPlan) -> bool {
    json_u64(readback.get("campaign_id")) == Some(campaign_id)
        && json_u64(readback.get("sku")) == Some(plan.sku)
        && json_u64(readback.get("bid_microrubles"))
            == Some(plan.manifest.spec.initial_cpc_bid_microrubles)
        && readback.get("title").and_then(Value::as_str)
            == Some(plan.manifest.create_request.title.as_str())
        && readback.get("state").and_then(Value::as_str) == Some("CAMPAIGN_STATE_RUNNING")
}

fn json_u64(value: Option<&Value>) -> Option<u64> {
    let value = value?;
    if let Some(number) = value.as_u64() {
        return Some(number);
    }
    let text = value.as_str()?;
    let parsed = text.parse::<u64>().ok()?;
    (parsed.to_string() == text).then_some(parsed)
}

fn guard_from_row(row: &Row) -> Result<OzonCampaignGuard, OzonPlanStoreError> {
    Ok(OzonCampaignGuard {
        plan_id: row.get(0),
        account_id: row.get(1),
        sku: u64::try_from(row.get::<_, i64>(2)).map_err(|_| OzonPlanStoreError::Unavailable)?,
        campaign_id: u64::try_from(row.get::<_, i64>(3))
            .map_err(|_| OzonPlanStoreError::Unavailable)?,
        date_from: row.get(4),
        spend_cap_microrubles: u64::try_from(row.get::<_, i64>(5))
            .map_err(|_| OzonPlanStoreError::Unavailable)?,
        target_drr_percent: u8::try_from(row.get::<_, i16>(6))
            .map_err(|_| OzonPlanStoreError::Unavailable)?,
        status: OzonCampaignGuardStatus::Active,
        stop_reason: None,
        incident_error_class: None,
    })
}

fn plan_from_row(row: &Row) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
    let manifest: OzonCampaignLaunchManifest =
        serde_json::from_str(row.get::<_, &str>(8)).map_err(|_| OzonPlanStoreError::Unavailable)?;
    let plan_id: &str = row.get(0);
    let plan_digest: &str = row.get(1);
    let created_at: DateTime<Utc> = row.get(11);
    let expires_at: DateTime<Utc> = row.get(12);
    let expected_plan_digest = digest_fields(&[
        b"mcp-ozon/ozon-plan/v1",
        manifest.manifest_digest.as_bytes(),
        &created_at.timestamp_micros().to_be_bytes(),
        &expires_at.timestamp_micros().to_be_bytes(),
    ]);
    let expected_plan_id = digest_fields(&[b"mcp-ozon/ozon-plan-id/v1", plan_digest.as_bytes()]);
    let uses_provider_identity =
        manifest.create_request.title == provider_title_for_plan_id(plan_id);
    if !manifest.has_exact_persisted_integrity(plan_id)
        || (uses_provider_identity && plan_digest != expected_plan_digest)
        || plan_id != expected_plan_id
    {
        return Err(OzonPlanStoreError::Unavailable);
    }
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
        execution_requested_at: row.get(22),
        current_action: OzonLaunchAction::from_db(row.get(23))?,
        workflow_generation: u64::try_from(row.get::<_, i64>(24))
            .map_err(|_| OzonPlanStoreError::Unavailable)?,
        workflow_lease_expires_at: row.get(25),
        workflow_write_started_at: row.get(26),
    })
}

pub(super) fn validate_manifest(
    manifest: &OzonCampaignLaunchManifest,
) -> Result<(), OzonPlanStoreError> {
    validate_identity(&manifest.actor_id)?;
    validate_identity(&manifest.spec.account_id)?;
    validate_digest(&manifest.manifest_digest)?;
    validate_digest(&manifest.policy_digest)?;
    if manifest.spec.skus.len() != 1
        || manifest.spec.skus[0] == 0
        || manifest.policy_schema_version == 0
        || manifest.policy_revision == 0
        || !manifest.has_exact_integrity()
    {
        return Err(OzonPlanStoreError::InvalidPlan);
    }
    Ok(())
}

pub(super) fn validate_digest(value: &str) -> Result<(), OzonPlanStoreError> {
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

pub(super) fn validate_identity(value: &str) -> Result<(), OzonPlanStoreError> {
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

pub(super) fn validate_reference(value: &str) -> Result<(), OzonPlanStoreError> {
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

pub(super) fn validate_error_class(value: &str) -> Result<(), OzonPlanStoreError> {
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

pub(super) fn digest_fields(fields: &[&[u8]]) -> String {
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

fn optional_i64(value: Option<u64>) -> Result<Option<i64>, OzonPlanStoreError> {
    value
        .map(i64::try_from)
        .transpose()
        .map_err(|_| OzonPlanStoreError::InvalidPlan)
}

fn optional_u64(value: Option<i64>) -> Result<Option<u64>, OzonPlanStoreError> {
    value
        .map(u64::try_from)
        .transpose()
        .map_err(|_| OzonPlanStoreError::Unavailable)
}

const fn validate_guard_metrics_pair(
    spend_minor: Option<u64>,
    revenue_minor: Option<u64>,
) -> Result<(), OzonPlanStoreError> {
    if spend_minor.is_some() == revenue_minor.is_some() {
        Ok(())
    } else {
        Err(OzonPlanStoreError::InvalidPlan)
    }
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

async fn lock_static_guard_audit_cursor(
    tx: &Transaction<'_>,
    account_id: &str,
    expected_prior_event_id: Option<u64>,
) -> Result<(), OzonPlanStoreError> {
    let expected_prior_event_id = expected_prior_event_id
        .map(i64::try_from)
        .transpose()
        .map_err(|_| OzonPlanStoreError::InvalidPlan)?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock( \
         hashtextextended('ozon/static-guard-audit/'||$1,0))",
        &[&account_id],
    )
    .await
    .map_err(|_| OzonPlanStoreError::Unavailable)?;
    let actual_prior_event_id = tx
        .query_one(
            "SELECT max(event_id) FROM control.ozon_static_guard_audit_events \
             WHERE account_id=$1",
            &[&account_id],
        )
        .await
        .map_err(|_| OzonPlanStoreError::Unavailable)?
        .get::<_, Option<i64>>(0);
    if actual_prior_event_id == expected_prior_event_id {
        Ok(())
    } else {
        Err(OzonPlanStoreError::LeaseLost)
    }
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
    let sku = i64::try_from(sku).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
    let active = tx
        .query_one(
            "SELECT control.ozon_runtime_gates_active_locked($1,$2)",
            &[&account, &sku],
        )
        .await
        .map_err(|_| OzonPlanStoreError::Unavailable)?;
    if active.get::<_, bool>(0) {
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
    tx.query_one("SELECT EXISTS(SELECT 1 FROM control.ozon_campaign_plans WHERE account_id=$1 AND sku=$2 AND ($3::text IS NULL OR plan_id<>$3) AND (status IN ('prepared','approved','creating','created','adding_products','products_added','activating','ambiguous') OR (status='failed' AND campaign_id IS NOT NULL)))",&[&account,&sku,&except]).await.map(|row|row.get(0)).map_err(|_|OzonPlanStoreError::Unavailable)
}

async fn expire_stale_open_plans_for_sku(
    tx: &Transaction<'_>,
    account: &str,
    sku: u64,
    audit_actor: &str,
) -> Result<(), OzonPlanStoreError> {
    let sku = i64::try_from(sku).map_err(|_| OzonPlanStoreError::InvalidPlan)?;
    let rows = tx
        .query(
            "UPDATE control.ozon_campaign_plans plan SET status='expired', \
             finished_at=clock_timestamp() \
             WHERE plan.account_id=$1 AND plan.sku=$2 \
               AND plan.status IN ('prepared','approved') \
               AND (plan.expires_at<=clock_timestamp() OR ( \
                   plan.status='approved' AND NOT EXISTS (SELECT 1 \
                       FROM control.ozon_campaign_plan_approvals approval \
                       WHERE approval.plan_id=plan.plan_id \
                         AND approval.plan_digest=plan.plan_digest \
                         AND approval.expires_at>clock_timestamp()))) \
             RETURNING plan.plan_id",
            &[&account, &sku],
        )
        .await
        .map_err(|_| OzonPlanStoreError::Unavailable)?;
    for row in rows {
        let plan_id: String = row.get(0);
        insert_audit(
            tx,
            &plan_id,
            audit_actor,
            "stale_plan_expired",
            &serde_json::json!({"reason":"new_plan_preflight"}),
        )
        .await?;
    }
    Ok(())
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

pub(super) fn map_policy_insert(error: &tokio_postgres::Error) -> OzonPlanStoreError {
    if error
        .as_db_error()
        .is_some_and(|db| db.code() == &SqlState::UNIQUE_VIOLATION)
    {
        OzonPlanStoreError::PolicyChanged
    } else {
        OzonPlanStoreError::Unavailable
    }
}
pub(super) fn map_plan_insert(error: &tokio_postgres::Error) -> OzonPlanStoreError {
    if error
        .as_db_error()
        .is_some_and(|db| db.code() == &SqlState::UNIQUE_VIOLATION)
    {
        OzonPlanStoreError::SkuLocked
    } else {
        OzonPlanStoreError::Unavailable
    }
}

#[cfg(test)]
mod lease_budget_tests {
    use super::{GUARD_STOP_LEASE_TTL, WORKFLOW_LEASE_TTL};
    use chrono::Duration;

    #[test]
    fn five_minute_leases_cover_composed_vendor_io_with_margin() {
        // Launch: OAuth + bounded final preflight + both pacing boundaries +
        // one mutation + an overall-bounded readback.
        let launch_worst_case = Duration::seconds(30 + 60 + 2 + 30 + 2 + 60);
        // Guard: metrics/campaign pre-read, OAuth, mutation and final readback,
        // plus both cross-client pacing boundaries.
        let guard_worst_case = Duration::seconds(4 * 30 + 2 * 2);
        let safety_margin = Duration::seconds(60);

        assert!(launch_worst_case + safety_margin < WORKFLOW_LEASE_TTL);
        assert!(guard_worst_case + safety_margin < GUARD_STOP_LEASE_TTL);
    }
}
