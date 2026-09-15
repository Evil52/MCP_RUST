use super::{CollectedAdvertisingFact, NaiveDate, Result, ensure};

/// Campaign details independently prove the current SKU set. Fullstats can
/// retain rows for products that delivered earlier in the requested period but
/// are no longer active, so only campaign and date define the response scope.
pub(super) fn validate_advertising_scope(
    advertising: &[CollectedAdvertisingFact],
    campaign_id: u64,
    current_date: NaiveDate,
    previous_date: NaiveDate,
) -> Result<()> {
    ensure!(
        advertising.iter().all(|fact| {
            fact.campaign_id == campaign_id
                && matches!(fact.business_date, date if date == current_date || date == previous_date)
        }),
        "WB automation stats вышли за campaign/date/SKU scope"
    );
    Ok(())
}
