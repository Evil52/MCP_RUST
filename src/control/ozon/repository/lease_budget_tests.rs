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
