mod model;
mod repository;
mod validation;

pub use model::{PlanStoreError, WbActionQuota, WbPlanStatus};
pub use repository::WbPlanRepository;

pub(in crate::control) use model::{WbApplyContext, WbControlPlan, WbPlanFinish};
pub(in crate::control) use validation::validate_control_database_url;

#[cfg(test)]
pub(super) use model::WbPlanApproval;

#[cfg(test)]
use repository::{PLAN_TTL, STALE_APPLY_AFTER, expire_plan, plan_from_row, reserve_action_quota};
#[cfg(test)]
use validation::{
    cumulative_abs_delta, make_plan_digest, make_plan_id, map_prepare_insert_error,
    validate_actor_or_account, validate_approval_reason, validate_digest, validate_plan_id,
};

#[cfg(test)]
#[expect(
    clippy::redundant_pub_crate,
    reason = "the shared test lock is deliberately restricted to this crate"
)]
pub(crate) static CONTROL_DB_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
mod tests;
