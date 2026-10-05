use crate::control::{plan::WbControlPlan, wb::WbBidWriteClient};

/// Preserve the reviewed approval window after the final asynchronous quota
/// lookup. Cloning retains the token-wide pacer shared by concurrent applies.
pub(super) fn plan_write_client(
    writer: &WbBidWriteClient,
    plan: &WbControlPlan,
) -> WbBidWriteClient {
    let (starts_at, expires_at) =
        plan.approval
            .as_ref()
            .map_or((plan.created_at, plan.expires_at), |approval| {
                (
                    plan.created_at.max(approval.approved_at),
                    plan.expires_at.min(approval.expires_at),
                )
            });
    writer
        .clone()
        .with_authorization_window(starts_at, expires_at)
}
