use rmcp::Json;

use crate::control::{plan::WbControlPlan, wb::campaign_snapshot};

use super::{
    WbControlServices,
    contract::WbPlanResult,
    presentation::{plan_result, plan_store_error},
};

pub(super) async fn read_plan_snapshot(
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

pub(super) async fn load_plan_result(
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
