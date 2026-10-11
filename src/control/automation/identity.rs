use super::WbAutomationPolicy;

pub(super) fn valid_policy_identity(policy: &WbAutomationPolicy) -> bool {
    let account_id_valid = !policy.account_id.is_empty()
        && policy.account_id.len() <= 128
        && policy
            .account_id
            .bytes()
            .all(|byte| matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-'));
    let campaign_name_valid = !policy.campaign_name.is_empty()
        && policy.campaign_name.len() <= 128
        && policy.campaign_name.trim() == policy.campaign_name
        && !policy.campaign_name.chars().any(char::is_control);
    policy.policy_version == "wb_ads_robot.v1"
        && policy.payment_type == "cpc"
        && policy.placement == "search"
        && policy
            .expected_recommendation_bid_kopecks
            .is_none_or(|bid| bid > 0 && i64::try_from(bid).is_ok())
        && policy.timezone == "Europe/Moscow"
        && account_id_valid
        && campaign_name_valid
        && policy.campaign_id != 0
}
