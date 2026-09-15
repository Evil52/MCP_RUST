use super::{Command, WbAutomationPolicy, parse_command, wb_automation::v4_corridor};
use mcp_ozon::control::validate_wb_automation_policy;
use std::path::PathBuf;

fn corridor_target(source: &WbAutomationPolicy) -> WbAutomationPolicy {
    let mut target = source.clone();
    target.authorization_reference =
        "chat/2026-09-15/oduvanchik-nexus-traffic-frontier-v4-7-12".to_owned();
    target.authorized_at = "2026-09-15T05:00:00Z".parse().unwrap();
    target.authorization_expires_at = "2026-09-16T05:00:00Z".parse().unwrap();
    target.observe_until = "2026-09-15T05:00:01Z".parse().unwrap();
    target.min_bid_kopecks = 700;
    target.max_bid_kopecks = 1_200;
    target
}

fn oduvanchik() -> WbAutomationPolicy {
    serde_json::from_str(include_str!(
        "../../../config/wb-automation-oduvanchik.v4.json"
    ))
    .expect("repository Oduvanchik v4 policy parses")
}

#[test]
fn accepts_only_both_reviewed_campaigns() {
    let oduvanchik = oduvanchik();
    let target = corridor_target(&oduvanchik);
    v4_corridor::validate(&oduvanchik, &target).expect("reviewed Oduvanchik 7-12 is accepted");
    validate_wb_automation_policy(&target)
        .expect("reviewed Oduvanchik 7-12 remains a valid runnable policy");

    let mut nexus = oduvanchik;
    nexus.campaign_id = 40_141_836;
    nexus.campaign_name = "Nexus".to_owned();
    nexus.authorization_reference =
        "chat/2026-09-14/nexus-funded-bids-and-automation-like-oduvanchik".to_owned();
    nexus.min_bid_kopecks = 102;
    let nexus_target = corridor_target(&nexus);
    v4_corridor::validate(&nexus, &nexus_target).expect("reviewed Nexus 7-12 is accepted");
    validate_wb_automation_policy(&nexus_target)
        .expect("reviewed Nexus 7-12 remains a valid runnable policy");

    let mut changed_budget = nexus_target.clone();
    changed_budget.daily_spend_cap_minor += 1;
    assert!(v4_corridor::validate(&nexus, &changed_budget).is_err());

    let mut wrong_campaign = nexus;
    wrong_campaign.campaign_id += 1;
    assert!(v4_corridor::validate(&wrong_campaign, &nexus_target).is_err());
}

#[test]
fn command_parses_only_activation_inputs() {
    let arguments = [
        "adjust-traffic-frontier-v4-corridor-pg",
        "source.json",
        "target.json",
        "access.json",
        "reader.token",
        "false",
    ]
    .map(str::to_owned);
    let Command::AdjustTrafficFrontierV4CorridorPostgres(options) =
        parse_command(&arguments).expect("v4 corridor command parses")
    else {
        panic!("unexpected command variant");
    };
    assert_eq!(options.source.policy, PathBuf::from("source.json"));
    assert_eq!(options.target_policy, PathBuf::from("target.json"));
    assert!(!options.source.allow_broad_reader);
    assert!(options.source.reader_proxy_url.is_none());
}
