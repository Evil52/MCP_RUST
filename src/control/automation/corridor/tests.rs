use super::*;
use crate::control::automation::{
    WbAutomationCampaignMetrics, WbAutomationDecisionError, WbAutomationHoldReason,
    WbAutomationObservation, evaluate_wb_automation,
};
use chrono::TimeZone;

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 29, 12, 0, 0).unwrap()
}
fn source() -> WbAutomationPolicy {
    serde_json::from_str(include_str!(
        "../../../../config/wb-automation-oduvanchik.v4.json"
    ))
    .unwrap()
}
fn target() -> WbAutomationPolicy {
    let mut p = source();
    p.authorization_reference = "chat/2026-09-29/authorized-corridor".into();
    p.authorized_at = now() - Duration::minutes(2);
    p.observe_until = now() - Duration::minutes(1);
    p.authorization_expires_at = now() + Duration::days(30);
    p.min_bid_kopecks = 700;
    p.max_bid_kopecks = 1200;
    p
}
fn observation(p: &WbAutomationPolicy) -> WbAutomationObservation {
    WbAutomationObservation {
        observed_at: now(),
        campaign_status: 9,
        paused_by_automation: false,
        budget_remaining_minor: 100_000,
        daily_spend_minor: 1000,
        daily_spend_complete: true,
        actions_today: 0,
        last_action_at: None,
        attribution_complete: true,
        campaign_level_metrics: None,
        current_campaign_metrics: None,
        skus: p
            .nm_ids
            .iter()
            .map(|id| WbAutomationSkuObservation {
                nm_id: *id,
                minimum_bid_kopecks: 102,
                current_bid_kopecks: 700,
                sellable_stock: 10,
                impressions: 1,
                clicks: 0,
                spend_minor: 0,
                attributed_orders: 0,
                attributed_revenue_minor: 0,
            })
            .collect(),
    }
}

#[test]
fn renewal_accepts_any_existing_campaign_but_only_authorized_fields() {
    let a = source();
    let b = target();
    validate_wb_automation_corridor_update(&a, &b, now()).unwrap();
    let mut a2 = a.clone();
    let mut b2 = b.clone();
    a2.campaign_id = 42;
    b2.campaign_id = 42;
    a2.account_id = "another_wb".into();
    b2.account_id = "another_wb".into();
    validate_wb_automation_corridor_update(&a2, &b2, now()).unwrap();
    // Exercise every top-level field, not just the fields used by production.
    let base = serde_json::to_value(&b).unwrap();
    for (key, value) in base.as_object().unwrap() {
        if [
            "authorization_reference",
            "authorized_at",
            "observe_until",
            "authorization_expires_at",
            "min_bid_kopecks",
            "max_bid_kopecks",
        ]
        .contains(&key.as_str())
        {
            continue;
        }
        let mut changed = base.clone();
        changed[key] = match value {
            serde_json::Value::Bool(v) => (!v).into(),
            serde_json::Value::Number(v) => (v.as_u64().unwrap() + 1).into(),
            serde_json::Value::String(v) => format!("{v}_changed").into(),
            serde_json::Value::Array(v) => {
                let mut v = v.clone();
                v.push(999.into());
                v.into()
            }
            _ => panic!("unexpected policy field {key}"),
        };
        if let Ok(changed) = serde_json::from_value::<WbAutomationPolicy>(changed) {
            assert!(
                validate_wb_automation_corridor_update(&a, &changed, now()).is_err(),
                "{key}"
            );
        }
    }
    let mut expired = b.clone();
    expired.authorization_expires_at = now();
    assert!(validate_wb_automation_corridor_update(&a, &expired, now()).is_err());
    let mut excessive = b;
    excessive.authorization_expires_at = now() + Duration::days(32);
    assert!(validate_wb_automation_corridor_update(&a, &excessive, now()).is_err());
}

#[test]
fn recovery_reaches_tail_skus_before_ordinary_exploration() {
    let p = target();
    let mut o = observation(&p);
    o.skus[3].current_bid_kopecks = 102;
    o.skus[4].current_bid_kopecks = 102;
    for _ in 0..2 {
        let d = evaluate_wb_automation(&p, &o).unwrap();
        let WbAutomationAction::ChangeBids { changes } = d.action else {
            panic!("repair expected")
        };
        assert_eq!(changes.len(), 1);
        let c = &changes[0];
        assert_eq!(c.reason, WbAutomationBidReason::PolicyMinimumNotMet);
        assert_eq!((c.from_bid_kopecks, c.to_bid_kopecks), (102, 700));
        o.skus
            .iter_mut()
            .find(|s| s.nm_id == c.nm_id)
            .unwrap()
            .current_bid_kopecks = c.to_bid_kopecks;
        o.observed_at += Duration::seconds(i64::from(p.cooldown_seconds));
    }
    assert!(o.skus.iter().all(|s| s.current_bid_kopecks >= 700));
}

#[test]
fn below_floor_keeps_all_campaign_guards_and_invalid_data_rejection() {
    let p = target();
    let mut o = observation(&p);
    o.skus[0].current_bid_kopecks = 102;
    let cases = [
        (WbAutomationHoldReason::AuthorizationExpired, 0),
        (WbAutomationHoldReason::SpendDataIncomplete, 1),
        (WbAutomationHoldReason::CooldownActive, 2),
        (WbAutomationHoldReason::ActionQuotaExhausted, 3),
        (WbAutomationHoldReason::CampaignNotActive, 4),
    ];
    for (reason, case) in cases {
        let mut c = o.clone();
        match case {
            0 => c.observed_at = p.authorization_expires_at,
            1 => c.daily_spend_complete = false,
            2 => c.last_action_at = Some(c.observed_at),
            3 => c.actions_today = p.max_actions_per_day,
            _ => c.campaign_status = 11,
        }
        assert_eq!(
            evaluate_wb_automation(&p, &c).unwrap().action,
            WbAutomationAction::Hold { reason }
        );
    }
    let mut cap = o.clone();
    cap.daily_spend_minor = p.daily_pause_threshold_minor;
    assert_eq!(
        evaluate_wb_automation(&p, &cap).unwrap().action,
        WbAutomationAction::PauseCampaignForDailyCap
    );
    o.skus[0].current_bid_kopecks = 0;
    assert_eq!(
        evaluate_wb_automation(&p, &o),
        Err(WbAutomationDecisionError::InvalidObservation)
    );
}

#[test]
fn stopped_below_floor_skus_are_never_raised_or_allowed_to_freeze_healthy_peers() {
    let p = target();
    let mut o = observation(&p);
    for s in &mut o.skus {
        s.current_bid_kopecks = 102;
    }
    o.skus[0].sellable_stock = p.min_sellable_stock;
    o.skus[1].impressions = 100;
    o.skus[1].clicks = p.no_order_disable_clicks;
    o.skus[1].spend_minor = p.no_order_disable_spend_minor;
    o.skus[2].impressions = 100;
    o.skus[2].clicks = 5;
    o.skus[2].attributed_orders = 1;
    o.skus[2].spend_minor = 5000;
    o.skus[2].attributed_revenue_minor = 10000;
    let d = evaluate_wb_automation(&p, &o).unwrap();
    assert_eq!(d.unresolved_stops.len(), 3);
    let WbAutomationAction::ChangeBids { changes } = d.action else {
        panic!("repair expected")
    };
    assert!([o.skus[3].nm_id, o.skus[4].nm_id].contains(&changes[0].nm_id));
}

#[test]
fn aggregate_observations_keep_stock_guard_and_require_complete_evidence() {
    let p = target();
    let mut o = observation(&p);
    o.attribution_complete = false;
    o.skus[0].current_bid_kopecks = 102;
    assert_eq!(
        evaluate_wb_automation(&p, &o).unwrap().action,
        WbAutomationAction::Hold {
            reason: WbAutomationHoldReason::AttributionIncomplete
        }
    );
    o.campaign_level_metrics = Some(WbAutomationCampaignMetrics {
        impressions: 100,
        clicks: 0,
        spend_minor: 0,
        attributed_orders: 0,
        attributed_revenue_minor: 0,
    });
    assert!(matches!(
        evaluate_wb_automation(&p, &o).unwrap().action,
        WbAutomationAction::ChangeBids { .. }
    ));
    o.skus[0].sellable_stock = 0;
    assert!(matches!(
        evaluate_wb_automation(&p, &o).unwrap().action,
        WbAutomationAction::Hold { .. }
    ));
}

#[test]
fn a_raised_vendor_floor_is_repaired_under_its_own_reason_not_by_a_decrease() {
    let p = target();
    let mut o = observation(&p);
    // WB raised the floor above a bid that still satisfies the policy minimum,
    // while the SKU's own evidence asks for a reduction.
    let sku = &mut o.skus[0];
    sku.minimum_bid_kopecks = 800;
    sku.current_bid_kopecks = 750;
    sku.impressions = 300;
    sku.clicks = p.no_order_reduce_clicks;
    sku.spend_minor = 1000;
    let d = evaluate_wb_automation(&p, &o).unwrap();
    let WbAutomationAction::ChangeBids { changes } = d.action else {
        panic!("repair expected")
    };
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].nm_id, o.skus[0].nm_id);
    assert_eq!(
        changes[0].reason,
        WbAutomationBidReason::PolicyMinimumNotMet
    );
    assert_eq!(
        (changes[0].from_bid_kopecks, changes[0].to_bid_kopecks),
        (750, 800)
    );
    // Even reached directly, a reduction that cannot go below the floor holds.
    assert_eq!(super::super::bid_change(&p, &o.skus[0]), Ok(None));
}
