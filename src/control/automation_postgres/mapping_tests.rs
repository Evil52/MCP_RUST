use super::*;

#[test]
fn durable_action_kinds_preserve_the_database_contract() {
    for (kind, database) in [
        (WbAutomationDurableActionKind::ChangeBids, "change_bids"),
        (
            WbAutomationDurableActionKind::PauseCampaignForDailyCap,
            "pause_campaign_for_daily_cap",
        ),
        (
            WbAutomationDurableActionKind::ResumeCampaignAfterDailyCap,
            "resume_campaign_after_daily_cap",
        ),
    ] {
        assert_eq!(kind.as_database(), database);
        assert_eq!(parse_action_kind(database), Ok(kind));
    }
    assert_eq!(
        parse_action_kind("unknown"),
        Err(WbAutomationPostgresError::Unavailable)
    );
}

#[test]
fn durable_action_statuses_preserve_the_database_contract() {
    for (status, database) in [
        (WbAutomationDurableActionStatus::Reserved, "reserved"),
        (
            WbAutomationDurableActionStatus::WriteStarted,
            "write_started",
        ),
        (
            WbAutomationDurableActionStatus::AwaitingReadback,
            "awaiting_readback",
        ),
        (WbAutomationDurableActionStatus::Applied, "applied"),
        (
            WbAutomationDurableActionStatus::ReconciliationRequired,
            "reconciliation_required",
        ),
        (WbAutomationDurableActionStatus::Cancelled, "cancelled"),
    ] {
        assert_eq!(status_database(status), database);
        assert_eq!(parse_action_status(database), Ok(status));
    }
    assert_eq!(
        parse_action_status("unknown"),
        Err(WbAutomationPostgresError::Unavailable)
    );
}
