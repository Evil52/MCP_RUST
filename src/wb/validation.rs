//! Bounded validation of read-only Wildberries request arguments.

use super::{
    BTreeSet, HeaderValue, MAX_PROMOTION_CLUSTER_BID_ITEMS, MAX_SEARCH_REPORT_TEXTS,
    MAX_SEARCH_TEXT_BYTES, MAX_WB_SIGNED_ID, NaiveDate, WbError,
};

pub(super) fn comma_separated<T: ToString>(values: impl IntoIterator<Item = T>) -> String {
    values
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn bearer_authorization(token: &str) -> Result<HeaderValue, WbError> {
    let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|_| WbError::Unauthorized { request_id: None })?;
    // Prevent accidental disclosure if reqwest headers are ever formatted by
    // future middleware or debug instrumentation.
    authorization.set_sensitive(true);
    Ok(authorization)
}

pub(super) fn validate_promotion_ids(ids: &[u64], maximum: usize) -> Result<(), WbError> {
    let unique = ids.iter().collect::<BTreeSet<_>>().len();
    if ids.is_empty() || ids.len() > maximum || ids.contains(&0) || unique != ids.len() {
        return Err(WbError::InvalidArguments { field: "ids" });
    }
    Ok(())
}

pub(super) fn validate_promotion_statuses(statuses: &[i32]) -> Result<(), WbError> {
    let unique = statuses.iter().collect::<BTreeSet<_>>().len();
    if statuses.len() > 6
        || unique != statuses.len()
        || statuses
            .iter()
            .any(|status| !matches!(status, -1 | 4 | 7 | 8 | 9 | 11))
    {
        return Err(WbError::InvalidArguments { field: "statuses" });
    }
    Ok(())
}

pub(super) fn validate_payment_type(payment_type: Option<&str>) -> Result<(), WbError> {
    if payment_type.is_some_and(|value| !matches!(value, "cpm" | "cpc")) {
        return Err(WbError::InvalidArguments {
            field: "payment_type",
        });
    }
    Ok(())
}

pub(super) fn validate_unsigned_id(
    value: u64,
    field: &'static str,
    maximum: Option<u64>,
) -> Result<(), WbError> {
    if value == 0 || maximum.is_some_and(|maximum| value > maximum) {
        return Err(WbError::InvalidArguments { field });
    }
    Ok(())
}

pub(super) fn validate_positive_unique_ids(
    values: &[u64],
    maximum_count: usize,
    field: &'static str,
    maximum_value: Option<u64>,
) -> Result<(), WbError> {
    let unique = values.iter().collect::<BTreeSet<_>>().len();
    if values.is_empty()
        || values.len() > maximum_count
        || unique != values.len()
        || values
            .iter()
            .any(|value| validate_unsigned_id(*value, field, maximum_value).is_err())
    {
        return Err(WbError::InvalidArguments { field });
    }
    Ok(())
}

pub(super) fn validate_top_order_by(value: &str) -> Result<(), WbError> {
    if !matches!(
        value,
        "openCard" | "addToCart" | "openToCart" | "orders" | "cartToOrder"
    ) {
        return Err(WbError::InvalidArguments {
            field: "top_order_by",
        });
    }
    Ok(())
}

pub(super) fn validate_search_texts(values: &[String]) -> Result<(), WbError> {
    let unique = values.iter().collect::<BTreeSet<_>>().len();
    if values.is_empty()
        || values.len() > MAX_SEARCH_REPORT_TEXTS
        || unique != values.len()
        || values.iter().any(|value| {
            value.is_empty()
                || value.len() > MAX_SEARCH_TEXT_BYTES
                || value.trim() != value
                || value.chars().any(char::is_control)
        })
    {
        return Err(WbError::InvalidArguments {
            field: "search_texts",
        });
    }
    Ok(())
}

pub(super) fn validate_placement_types(values: &[String]) -> Result<(), WbError> {
    let unique = values.iter().collect::<BTreeSet<_>>().len();
    if values.is_empty()
        || values.len() > 3
        || unique != values.len()
        || values
            .iter()
            .any(|value| !matches!(value.as_str(), "combined" | "search" | "recommendation"))
    {
        return Err(WbError::InvalidArguments {
            field: "placement_types",
        });
    }
    Ok(())
}

pub(super) fn validate_bid_items(items: &[(u64, u64)]) -> Result<(), WbError> {
    let unique = items.iter().collect::<BTreeSet<_>>().len();
    if items.is_empty()
        || items.len() > MAX_PROMOTION_CLUSTER_BID_ITEMS
        || unique != items.len()
        || items.iter().any(|(advert_id, nm_id)| {
            validate_unsigned_id(*advert_id, "items", Some(MAX_WB_SIGNED_ID)).is_err()
                || validate_unsigned_id(*nm_id, "items", Some(MAX_WB_SIGNED_ID)).is_err()
        })
    {
        return Err(WbError::InvalidArguments { field: "items" });
    }
    Ok(())
}

pub(super) fn parse_strict_date(value: &str, field: &'static str) -> Result<NaiveDate, WbError> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| WbError::InvalidArguments { field })?;
    if date.format("%Y-%m-%d").to_string() != value {
        return Err(WbError::InvalidArguments { field });
    }
    Ok(date)
}

pub(super) fn validate_search_period(
    start: &str,
    end: &str,
    start_field: &'static str,
    end_field: &'static str,
    maximum_days: i64,
) -> Result<i64, WbError> {
    let start = parse_strict_date(start, start_field)?;
    let end = parse_strict_date(end, end_field)?;
    let span = end.signed_duration_since(start).num_days();
    if !(0..maximum_days).contains(&span) {
        return Err(WbError::InvalidArguments {
            field: "date_range",
        });
    }
    Ok(span + 1)
}

pub(super) fn validate_promotion_period(begin_date: &str, end_date: &str) -> Result<(), WbError> {
    let begin = parse_strict_date(begin_date, "begin_date")?;
    let end = parse_strict_date(end_date, "end_date")?;
    let days = end.signed_duration_since(begin).num_days();
    if !(0..=30).contains(&days) {
        return Err(WbError::InvalidArguments {
            field: "date_range",
        });
    }
    Ok(())
}
