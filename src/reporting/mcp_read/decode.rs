//! Scalar decoding and reporting provenance validation.

use super::{
    AccountScope, CollectionState, DateTime, FinanceCategory, FromSql, Marketplace,
    ReadyReportKind, ReadyReportState, ReportingReadError, Row, SnapshotSource, SnapshotStatus,
    Utc,
};

pub(super) fn column<'row, T>(row: &'row Row, index: usize) -> Result<T, ReportingReadError>
where
    T: FromSql<'row>,
{
    row.try_get(index)
        .map_err(|_| ReportingReadError::InvalidPublishedData)
}

pub(super) fn validate_scope_columns(
    row: &Row,
    account: &AccountScope,
    account_index: usize,
    marketplace_index: usize,
) -> Result<(), ReportingReadError> {
    let account_id: String = column(row, account_index)?;
    let marketplace = parse_marketplace(&column::<String>(row, marketplace_index)?)?;
    validate_scope(&account_id, marketplace, account)
}

pub(super) fn validate_scope(
    account_id: &str,
    marketplace: Marketplace,
    account: &AccountScope,
) -> Result<(), ReportingReadError> {
    if account_id == account.account_id() && marketplace == account.marketplace() {
        Ok(())
    } else {
        Err(ReportingReadError::InvalidPublishedData)
    }
}

pub(super) fn parse_marketplace(value: &str) -> Result<Marketplace, ReportingReadError> {
    match value {
        "ozon" => Ok(Marketplace::Ozon),
        "wildberries" => Ok(Marketplace::Wildberries),
        _ => Err(ReportingReadError::InvalidPublishedData),
    }
}

pub(super) const fn marketplace_str(value: Marketplace) -> &'static str {
    match value {
        Marketplace::Ozon => "ozon",
        Marketplace::Wildberries => "wildberries",
    }
}

pub(super) fn parse_source(value: &str) -> Result<SnapshotSource, ReportingReadError> {
    match value {
        "sales" => Ok(SnapshotSource::Sales),
        "advertising" => Ok(SnapshotSource::Advertising),
        "finance" => Ok(SnapshotSource::Finance),
        "stocks" => Ok(SnapshotSource::Stocks),
        "prices" => Ok(SnapshotSource::Prices),
        _ => Err(ReportingReadError::InvalidPublishedData),
    }
}

pub(super) fn parse_collection_status(value: &str) -> Result<CollectionState, ReportingReadError> {
    match value {
        "running" => Ok(CollectionState::Running),
        "succeeded" => Ok(CollectionState::Succeeded),
        "partial" => Ok(CollectionState::Partial),
        "failed" => Ok(CollectionState::Failed),
        _ => Err(ReportingReadError::InvalidPublishedData),
    }
}

pub(super) fn parse_published_collection_status(
    value: &str,
) -> Result<CollectionState, ReportingReadError> {
    match parse_collection_status(value)? {
        status @ (CollectionState::Succeeded | CollectionState::Partial) => Ok(status),
        _ => Err(ReportingReadError::InvalidPublishedData),
    }
}

pub(super) fn parse_snapshot_status(value: &str) -> Result<SnapshotStatus, ReportingReadError> {
    match value {
        "succeeded" => Ok(SnapshotStatus::Succeeded),
        "partial" => Ok(SnapshotStatus::Partial),
        _ => Err(ReportingReadError::InvalidPublishedData),
    }
}

pub(super) fn parse_finance_category(value: &str) -> Result<FinanceCategory, ReportingReadError> {
    match value {
        "sale" => Ok(FinanceCategory::Sale),
        "commission" => Ok(FinanceCategory::Commission),
        "acquiring" => Ok(FinanceCategory::Acquiring),
        "logistics" => Ok(FinanceCategory::Logistics),
        "storage" => Ok(FinanceCategory::Storage),
        "paid_acceptance" => Ok(FinanceCategory::PaidAcceptance),
        "compensation" => Ok(FinanceCategory::Compensation),
        "marketplace_discount" => Ok(FinanceCategory::MarketplaceDiscount),
        "advertising" => Ok(FinanceCategory::Advertising),
        "other" => Ok(FinanceCategory::Other),
        _ => Err(ReportingReadError::InvalidPublishedData),
    }
}

pub(super) fn parse_report_kind(value: &str) -> Result<ReadyReportKind, ReportingReadError> {
    match value {
        "morning" => Ok(ReadyReportKind::Morning),
        "evening" => Ok(ReadyReportKind::Evening),
        _ => Err(ReportingReadError::InvalidPublishedData),
    }
}

pub(super) fn parse_report_state(value: &str) -> Result<ReadyReportState, ReportingReadError> {
    match value {
        "ready" => Ok(ReadyReportState::Ready),
        "sent" => Ok(ReadyReportState::Sent),
        _ => Err(ReportingReadError::InvalidPublishedData),
    }
}

pub(super) fn nonnegative_i64(value: i64) -> Result<u64, ReportingReadError> {
    u64::try_from(value).map_err(|_| ReportingReadError::InvalidPublishedData)
}

pub(super) fn positive_i64(value: i64) -> Result<u64, ReportingReadError> {
    let value = nonnegative_i64(value)?;
    (value > 0)
        .then_some(value)
        .ok_or(ReportingReadError::InvalidPublishedData)
}

pub(super) fn nonnegative_optional_i64(
    value: Option<i64>,
) -> Result<Option<u64>, ReportingReadError> {
    value.map(nonnegative_i64).transpose()
}

pub(super) fn nonnegative_i32(value: i32) -> Result<u64, ReportingReadError> {
    u64::try_from(value).map_err(|_| ReportingReadError::InvalidPublishedData)
}

pub(super) fn positive_i32(value: i32) -> Result<u64, ReportingReadError> {
    let value = nonnegative_i32(value)?;
    (value > 0)
        .then_some(value)
        .ok_or(ReportingReadError::InvalidPublishedData)
}

pub(super) fn nonnegative_optional_i32(
    value: Option<i32>,
) -> Result<Option<u64>, ReportingReadError> {
    value.map(nonnegative_i32).transpose()
}

pub(super) fn valid_http_status(value: i16) -> Result<u16, ReportingReadError> {
    if (400..=599).contains(&value) {
        u16::try_from(value).map_err(|_| ReportingReadError::InvalidPublishedData)
    } else {
        Err(ReportingReadError::InvalidPublishedData)
    }
}

pub(super) fn validate_currency(value: &str) -> Result<(), ReportingReadError> {
    (value == "RUB")
        .then_some(())
        .ok_or(ReportingReadError::InvalidPublishedData)
}

pub(super) fn validate_collector_version(value: &str) -> Result<(), ReportingReadError> {
    if !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        Ok(())
    } else {
        Err(ReportingReadError::InvalidPublishedData)
    }
}

pub(super) fn valid_error_class(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

pub(super) fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

pub(super) fn valid_warehouse_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

pub(super) fn timestamp_string(value: DateTime<Utc>) -> String {
    super::super::business_timestamp(value)
}
