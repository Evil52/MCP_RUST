//! V3/V4 marginal feedback: only incremental orders bought by incremental
//! clicks at or below the target DRR may raise a traffic-frontier bid.

use chrono::Duration as ChronoDuration;

use super::super::feedback::{
    traffic_feedback_delta, traffic_frontier_dynamic_cap, traffic_frontier_pacing_decision,
};
use super::{traffic_frontier_v3_baseline, traffic_frontier_v3_snapshot};
use crate::control::automation::{
    WbAutomationAction, WbAutomationBidReason, WbAutomationCampaignMetrics, WbAutomationHoldReason,
    WbAutomationPacingMode,
};

#[test]
fn traffic_frontier_v3_increases_only_after_efficient_incremental_orders() {
    let (policy, snapshot) = traffic_frontier_v3_snapshot();
    let baseline = traffic_frontier_v3_baseline(&snapshot);
    let delta = traffic_feedback_delta(
        &snapshot,
        snapshot
            .observation
            .current_campaign_metrics
            .as_ref()
            .unwrap(),
        Some(&baseline),
    )
    .unwrap();
    assert_eq!(
        delta,
        WbAutomationCampaignMetrics {
            impressions: 220,
            clicks: 10,
            spend_minor: 1_000,
            attributed_orders: 1,
            attributed_revenue_minor: 100_000,
        }
    );

    assert!(matches!(
        traffic_frontier_pacing_decision(&policy, &snapshot, Some(&baseline))
            .unwrap()
            .expect("efficient marginal order permits one bounded increase")
            .action,
        WbAutomationAction::ChangeBids { ref changes }
            if changes.len() == 1
                && changes[0].reason == WbAutomationBidReason::TrafficFrontierBootstrap
    ));

    let mut target_reached = snapshot;
    target_reached.observation.current_campaign_metrics = Some(WbAutomationCampaignMetrics {
        impressions: 320,
        clicks: 12,
        spend_minor: 3_000,
        attributed_orders: 3,
        attributed_revenue_minor: 300_000,
    });
    let mut target_baseline = baseline;
    target_baseline.observation.current_campaign_metrics = Some(WbAutomationCampaignMetrics {
        impressions: 100,
        clicks: 2,
        spend_minor: 2_000,
        attributed_orders: 2,
        attributed_revenue_minor: 200_000,
    });
    assert_eq!(
        traffic_frontier_pacing_decision(&policy, &target_reached, Some(&target_baseline),)
            .unwrap(),
        None
    );

    assert_eq!(
        traffic_feedback_delta(
            &target_reached,
            target_reached
                .observation
                .current_campaign_metrics
                .as_ref()
                .unwrap(),
            None,
        )
        .unwrap(),
        target_reached
            .observation
            .current_campaign_metrics
            .clone()
            .unwrap()
    );

    let mut previous_day = target_baseline;
    previous_day.observation.observed_at -= ChronoDuration::days(1);
    assert_eq!(
        traffic_feedback_delta(
            &target_reached,
            target_reached
                .observation
                .current_campaign_metrics
                .as_ref()
                .unwrap(),
            Some(&previous_day),
        )
        .unwrap(),
        target_reached
            .observation
            .current_campaign_metrics
            .clone()
            .unwrap()
    );
}

/// WB attributes orders to clicks from earlier windows, so an order can
/// arrive while the campaign shows no new click. Such a delta carries no
/// price per incremental click: it must hold, never divide by zero clicks.
#[test]
fn late_order_without_incremental_clicks_holds_instead_of_raising() {
    let (mut policy, snapshot) = traffic_frontier_v3_snapshot();
    let mut baseline = traffic_frontier_v3_baseline(&snapshot);
    baseline.observation.current_campaign_metrics = Some(WbAutomationCampaignMetrics {
        impressions: 100,
        clicks: 12,
        spend_minor: 1_000,
        attributed_orders: 0,
        attributed_revenue_minor: 0,
    });
    let current = snapshot
        .observation
        .current_campaign_metrics
        .as_ref()
        .unwrap();
    let delta = traffic_feedback_delta(&snapshot, current, Some(&baseline)).unwrap();
    assert_eq!(
        (delta.clicks, delta.spend_minor, delta.attributed_orders),
        (0, 0, 1)
    );
    assert!(delta.impressions >= policy.traffic_frontier_min_feedback_impressions.unwrap());

    for mode in [
        WbAutomationPacingMode::TrafficFrontierV3,
        WbAutomationPacingMode::TrafficFrontierV4,
    ] {
        policy.autonomous_pacing = mode;
        let decision = traffic_frontier_pacing_decision(&policy, &snapshot, Some(&baseline))
            .unwrap()
            .expect("late attribution waits for incremental click feedback");
        assert_eq!(
            decision.action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::TrafficFeedbackPending,
            },
            "{mode:?}"
        );
    }
}

#[test]
fn dynamic_cap_refuses_orders_without_clicks() {
    let (policy, snapshot) = traffic_frontier_v3_snapshot();
    let error = traffic_frontier_dynamic_cap(
        &policy,
        &snapshot,
        &WbAutomationCampaignMetrics {
            impressions: 220,
            clicks: 0,
            spend_minor: 0,
            attributed_orders: 1,
            attributed_revenue_minor: 100_000,
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("without clicks"), "{error:#}");
}

/// A below-floor observation must traverse the ordinary write/readback path.
#[tokio::test]
async fn authorized_floor_recovery_uses_one_write_and_exact_readback() {
    let fixture = super::Fixture::new();
    let mut policy: crate::control::WbAutomationPolicy =
        serde_json::from_slice(&std::fs::read(&fixture.policy).unwrap()).unwrap();
    policy.min_bid_kopecks = 700;
    policy.max_bid_kopecks = 1200;
    std::fs::write(&fixture.policy, serde_json::to_vec(&policy).unwrap()).unwrap();
    let (reader, _) = super::reader_server(9, 102, "2026-08-25", None, 10);
    let (writer, requests) = super::mock_http(vec![(200, "{}".into())]);
    let executor = fixture.executor(&reader, &writer);
    let receipt = executor.run_once(super::now()).await.unwrap();
    assert_eq!(
        receipt.outcome,
        super::WbAutomationExecutionOutcome::WriteSentReconciliationRequired
    );
    assert!(
        matches!(receipt.decision.action,WbAutomationAction::ChangeBids {ref changes}
        if changes.len()==1 && changes[0].from_bid_kopecks==102
            && changes[0].to_bid_kopecks==700 && changes[0].reason==WbAutomationBidReason::PolicyMinimumNotMet)
    );
    let request = requests
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap();
    assert!(request.starts_with("PATCH /api/advert/v1/bids"));
    assert!(request.contains("\"bid_kopecks\":700"));
    let (reader, _) = super::reader_server(9, 700, "2026-08-25", None, 10);
    let readback = fixture.executor(&reader, "http://127.0.0.1:1");
    assert_eq!(
        readback
            .run_once(super::now() + ChronoDuration::minutes(1))
            .await
            .unwrap()
            .outcome,
        super::WbAutomationExecutionOutcome::Reconciled
    );
}
