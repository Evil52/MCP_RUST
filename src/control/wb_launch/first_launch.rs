use super::{Journal, Result, ensure};

/// A ready-state observation contains no spend/attribution evidence. Require
/// the independently confirmed, once-only creation and funding provenance
/// instead; never reinterpret missing statistics as complete zero statistics.
pub(super) fn validate_receipts(
    journal: &Journal,
    id: u64,
    expected_bids: &std::collections::BTreeMap<u64, u64>,
    budget_rubles: u64,
) -> Result<()> {
    let created = journal.require_receipt("create")?;
    let funded = journal.require_receipt("fund")?;
    let response = journal.require_receipt("fund-response")?;
    let bids = journal.require_receipt("bids")?;
    ensure!(
        created["campaign_id"] == id
            && created["wb_http"] == 200
            && funded["campaign_id"] == id
            && funded["type"] == 1
            && funded["transferred_rubles"] == budget_rubles
            && funded["budget_after"] == budget_rubles
            && response["wb_http"] == 200
            && response["total"] == budget_rubles
            && bids["campaign_id"] == id
            && bids["bids_kopecks"] == serde_json::to_value(expected_bids)?,
        "first launch requires confirmed create, exact bids and one-time funding receipts"
    );
    Ok(())
}
