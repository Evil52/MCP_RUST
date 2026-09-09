//! Bounded marketplace and reporting input validation.

use chrono::Datelike;

use super::{
    BTreeSet, DateTime, MAX_ENUM_VALUE_CHARS, MAX_IDENTIFIER_CHARS, MAX_OPAQUE_TOKEN_CHARS,
    MAX_OZON_SIGNED_API_ID, MAX_PERFORMANCE_CAMPAIGNS, MAX_PRODUCT_FILTER_ITEMS,
    MAX_REPORTING_HISTORY_DAYS, MAX_SUPPLY_ORDER_DROPOFF_WAREHOUSES, MAX_SUPPLY_ORDER_STATES,
    MAX_WB_MINIMUM_BID_NM_IDS, MAX_WB_PROMOTION_PERIOD_DAYS, MAX_WB_SEARCH_CLUSTER_PAIRS,
    MAX_WB_SEARCH_NM_IDS, MAX_WB_SEARCH_ORDERS_PERIOD_DAYS, MAX_WB_SEARCH_REPORT_PERIOD_DAYS,
    MAX_WB_SEARCH_TEXT_BYTES, MAX_WB_SEARCH_TEXTS, MAX_WB_SIGNED_API_ID, NaiveDate, NaiveDateTime,
    REPORTING_INVALID_REQUEST, SupplyOrderListInput, SupplyOrderTimeslotRangeInput, Utc, Value,
    WbProductCardsInput, WbPromotionMinimumBidsInput, WbPromotionSearchClusterPair,
    WbSearchOrdersPositionsInput, WbSearchProductQueriesInput, json,
};

pub(super) fn parse_date(value: &str, field: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| format!("{field} должен иметь формат YYYY-MM-DD"))
}

pub(super) fn parse_reporting_cutoff(value: Option<&str>) -> Result<Option<DateTime<Utc>>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    validate_non_blank("cutoff_at", value)?;
    validate_max_chars("cutoff_at", value, 64)?;
    DateTime::parse_from_rfc3339(value)
        .map(|cutoff| Some(cutoff.with_timezone(&Utc)))
        .map_err(|_| format!("{REPORTING_INVALID_REQUEST}: cutoff_at должен иметь формат RFC 3339"))
}

pub(super) fn parse_reporting_date_range(
    date_from: Option<&str>,
    date_to: Option<&str>,
) -> Result<(Option<NaiveDate>, Option<NaiveDate>), String> {
    match (date_from, date_to) {
        (None, None) => Ok((None, None)),
        (Some(date_from), Some(date_to)) => {
            let from = parse_date(date_from, "date_from")?;
            let to = parse_date(date_to, "date_to")?;
            if to < from {
                return Err(format!(
                    "{REPORTING_INVALID_REQUEST}: date_to не может быть раньше date_from"
                ));
            }
            if (to - from).num_days() + 1 > MAX_REPORTING_HISTORY_DAYS {
                return Err(format!(
                    "{REPORTING_INVALID_REQUEST}: период истории не может превышать {MAX_REPORTING_HISTORY_DAYS} дней"
                ));
            }
            Ok((Some(from), Some(to)))
        }
        _ => Err(format!(
            "{REPORTING_INVALID_REQUEST}: date_from и date_to нужно передавать вместе"
        )),
    }
}

pub(super) fn weekly_ranking_period(
    date_from: Option<&str>,
    date_to: Option<&str>,
    current_business_date: NaiveDate,
) -> Result<(NaiveDate, NaiveDate), String> {
    let (from, to) = match (date_from, date_to) {
        (None, None) => {
            let current_week_start = current_business_date
                - chrono::Duration::days(i64::from(
                    current_business_date.weekday().num_days_from_monday(),
                ));
            (
                current_week_start - chrono::Duration::days(7),
                current_week_start - chrono::Duration::days(1),
            )
        }
        (Some(date_from), Some(date_to)) => (
            parse_date(date_from, "date_from")?,
            parse_date(date_to, "date_to")?,
        ),
        _ => {
            return Err(format!(
                "{REPORTING_INVALID_REQUEST}: date_from и date_to нужно передавать вместе"
            ));
        }
    };
    if (to - from).num_days() != 6
        || from.weekday() != chrono::Weekday::Mon
        || to.weekday() != chrono::Weekday::Sun
        || to >= current_business_date
    {
        return Err(format!(
            "{REPORTING_INVALID_REQUEST}: рейтинг требует одну завершённую календарную неделю с понедельника по воскресенье"
        ));
    }
    Ok((from, to))
}

pub(super) fn validate_reporting_limit(limit: u16, maximum: u16) -> Result<(), String> {
    if !(1..=maximum).contains(&limit) {
        return Err(format!(
            "{REPORTING_INVALID_REQUEST}: limit должен быть от 1 до {maximum}"
        ));
    }
    Ok(())
}

pub(super) fn validate_date_range(
    date_from: &str,
    date_to: &str,
    max_days: i64,
) -> Result<(), String> {
    let from = parse_date(date_from, "date_from")?;
    let to = parse_date(date_to, "date_to")?;
    if to < from {
        return Err("date_to не может быть раньше date_from".to_owned());
    }
    if (to - from).num_days() + 1 > max_days {
        return Err(format!("период не может превышать {max_days} дней"));
    }
    Ok(())
}

pub(super) fn validate_and_expand_dates(
    date_from: &str,
    date_to: &str,
    max_days: i64,
) -> Result<(String, String), String> {
    validate_date_range(date_from, date_to, max_days)?;
    Ok((
        format!("{date_from}T00:00:00.000Z"),
        format!("{date_to}T23:59:59.999Z"),
    ))
}

pub(super) fn validate_cash_flow_period(
    date_from: &str,
    date_to: &str,
) -> Result<(String, String), String> {
    let from = parse_date(date_from, "date_from")?;
    let to = parse_date(date_to, "date_to")?;
    let first_half = from.day() == 1 && to.day() == 15;
    let second_half = from.day() == 16 && to.succ_opt().is_some_and(|next_day| next_day.day() == 1);
    if from.year() != to.year() || from.month() != to.month() || (!first_half && !second_half) {
        return Err(
            "период cash-flow должен быть одним расчётным интервалом Ozon: 01–15 или 16–последний день одного месяца"
                .to_owned(),
        );
    }
    Ok((
        format!("{date_from}T00:00:00.000Z"),
        format!("{date_to}T23:59:59.999Z"),
    ))
}

pub(super) fn validate_year_month(value: &str) -> Result<(), String> {
    NaiveDate::parse_from_str(&format!("{value}-01"), "%Y-%m-%d")
        .map(|_| ())
        .map_err(|_| "date должен иметь формат YYYY-MM".to_owned())
}

pub(super) fn validate_optional_date_range(
    field: &str,
    from: Option<&str>,
    to: Option<&str>,
) -> Result<Option<(String, String)>, String> {
    match (from, to) {
        (None, None) => Ok(None),
        (Some(from), Some(to)) => {
            let from_date = parse_date(from, &format!("{field}_from"))?;
            let to_date = parse_date(to, &format!("{field}_to"))?;
            if to_date < from_date {
                return Err(format!("{field}_to не может быть раньше {field}_from"));
            }
            Ok(Some((
                format!("{from}T00:00:00.000Z"),
                format!("{to}T23:59:59.999Z"),
            )))
        }
        _ => Err(format!("{field}_from и {field}_to нужно передавать вместе")),
    }
}

pub(super) fn validate_limit(limit: u32, maximum: u32) -> Result<(), String> {
    if !(1..=maximum).contains(&limit) {
        return Err(format!("limit должен быть от 1 до {maximum}"));
    }
    Ok(())
}

pub(super) fn validate_count(
    field: &str,
    count: usize,
    minimum: usize,
    maximum: usize,
) -> Result<(), String> {
    if !(minimum..=maximum).contains(&count) {
        return Err(format!(
            "{field} должен содержать от {minimum} до {maximum} значений"
        ));
    }
    Ok(())
}

pub(super) fn validate_campaign_ids(values: &[u64]) -> Result<(), String> {
    validate_count("campaign_ids", values.len(), 0, MAX_PERFORMANCE_CAMPAIGNS)?;
    let mut unique = BTreeSet::new();
    for value in values {
        if *value == 0 {
            return Err("campaign_ids не должен содержать 0".to_owned());
        }
        if !unique.insert(*value) {
            return Err("campaign_ids не должен содержать дубликаты".to_owned());
        }
    }
    Ok(())
}

pub(super) fn validate_max_chars(field: &str, value: &str, maximum: usize) -> Result<(), String> {
    if value.chars().count() > maximum {
        return Err(format!("{field} не может быть длиннее {maximum} символов"));
    }
    Ok(())
}

pub(super) fn validate_non_blank(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{field} не может быть пустым"));
    }
    Ok(())
}

pub(super) fn validate_string_list(
    field: &str,
    values: &[String],
    maximum_items: usize,
    maximum_chars: usize,
) -> Result<(), String> {
    if values.len() > maximum_items {
        return Err(format!(
            "{field} должен содержать не более {maximum_items} значений"
        ));
    }
    for value in values {
        validate_non_blank(field, value)?;
        validate_max_chars(field, value, maximum_chars)?;
    }
    Ok(())
}

pub(super) fn validate_product_identifiers(
    offer_ids: &[String],
    product_ids: &[String],
    skus: &[u64],
) -> Result<(), String> {
    validate_string_list(
        "offer_ids",
        offer_ids,
        MAX_PRODUCT_FILTER_ITEMS,
        MAX_IDENTIFIER_CHARS,
    )?;
    validate_string_list(
        "product_ids",
        product_ids,
        MAX_PRODUCT_FILTER_ITEMS,
        MAX_IDENTIFIER_CHARS,
    )?;
    validate_count("skus", skus.len(), 0, MAX_PRODUCT_FILTER_ITEMS)?;
    validate_unique_ozon_ids("skus", skus)?;
    if offer_ids.len() + product_ids.len() + skus.len() > MAX_PRODUCT_FILTER_ITEMS {
        return Err(format!(
            "offer_ids, product_ids и skus вместе должны содержать не более {MAX_PRODUCT_FILTER_ITEMS} значений"
        ));
    }
    Ok(())
}

pub(super) fn validate_positive_ids(field: &str, values: &[u64]) -> Result<(), String> {
    if values.contains(&0) {
        return Err(format!("{field} должен содержать только положительные ID"));
    }
    Ok(())
}

pub(super) fn validate_unique_positive_ids(field: &str, values: &[u64]) -> Result<(), String> {
    validate_positive_ids(field, values)?;
    let unique = values
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    if unique.len() != values.len() {
        return Err(format!("{field} не должен содержать повторяющиеся ID"));
    }
    Ok(())
}

pub(super) fn validate_ozon_id(field: &str, value: u64) -> Result<(), String> {
    if !(1..=MAX_OZON_SIGNED_API_ID).contains(&value) {
        return Err(format!(
            "{field} должен быть от 1 до {MAX_OZON_SIGNED_API_ID}"
        ));
    }
    Ok(())
}

pub(super) fn validate_unique_ozon_ids(field: &str, values: &[u64]) -> Result<(), String> {
    validate_unique_positive_ids(field, values)?;
    if values.iter().any(|value| *value > MAX_OZON_SIGNED_API_ID) {
        return Err(format!(
            "{field} не должен содержать ID больше {MAX_OZON_SIGNED_API_ID}"
        ));
    }
    Ok(())
}

pub(super) fn validate_rfc3339(
    field: &str,
    value: &str,
) -> Result<chrono::DateTime<chrono::FixedOffset>, String> {
    validate_max_chars(field, value, 64)?;
    chrono::DateTime::parse_from_rfc3339(value)
        .map_err(|_| format!("{field} должен иметь формат RFC3339"))
}

pub(super) fn validate_supply_order_list_input(input: &SupplyOrderListInput) -> Result<(), String> {
    validate_count("states", input.states.len(), 0, MAX_SUPPLY_ORDER_STATES)?;
    if input.states.iter().collect::<BTreeSet<_>>().len() != input.states.len() {
        return Err("states должен содержать уникальные значения".to_owned());
    }
    validate_count(
        "dropoff_warehouse_ids",
        input.dropoff_warehouse_ids.len(),
        0,
        MAX_SUPPLY_ORDER_DROPOFF_WAREHOUSES,
    )?;
    validate_unique_ozon_ids("dropoff_warehouse_ids", &input.dropoff_warehouse_ids)?;
    validate_supply_order_search(input.order_number_search.as_deref())?;
    if let Some(last_id) = input.last_id.as_deref() {
        validate_max_chars("last_id", last_id, MAX_OPAQUE_TOKEN_CHARS)?;
    }
    validate_supply_order_timeslot(input.timeslot_from_range.as_ref())?;
    validate_limit(input.limit, 100)
}

pub(super) fn validate_supply_order_search(search: Option<&str>) -> Result<(), String> {
    let Some(search) = search else {
        return Ok(());
    };
    validate_non_blank("order_number_search", search)?;
    if !(3..=MAX_IDENTIFIER_CHARS).contains(&search.chars().count()) {
        return Err(format!(
            "order_number_search должен содержать от 3 до {MAX_IDENTIFIER_CHARS} символов"
        ));
    }
    Ok(())
}

pub(super) fn validate_supply_order_timeslot(
    range: Option<&SupplyOrderTimeslotRangeInput>,
) -> Result<(), String> {
    let Some(range) = range else {
        return Ok(());
    };
    let from = range
        .from
        .as_deref()
        .map(|value| validate_rfc3339("timeslot_from_range.from", value))
        .transpose()?;
    let to = range
        .to
        .as_deref()
        .map(|value| validate_rfc3339("timeslot_from_range.to", value))
        .transpose()?;
    if from.zip(to).is_some_and(|(from, to)| from > to) {
        return Err(
            "timeslot_from_range.to не может быть раньше timeslot_from_range.from".to_owned(),
        );
    }
    Ok(())
}

pub(super) fn build_supply_order_filter(
    input: &SupplyOrderListInput,
) -> serde_json::Map<String, Value> {
    let mut filter = serde_json::Map::from_iter([("states".to_owned(), json!(&input.states))]);
    if !input.dropoff_warehouse_ids.is_empty() {
        filter.insert(
            "dropoff_warehouse_ids".to_owned(),
            json!(&input.dropoff_warehouse_ids),
        );
    }
    if let Some(search) = input.order_number_search.as_deref() {
        filter.insert("order_number_search".to_owned(), json!(search));
    }
    if let Some(range) = input.timeslot_from_range.as_ref() {
        filter.insert(
            "timeslot_from_range".to_owned(),
            Value::Object(build_supply_order_timeslot(range)),
        );
    }
    filter
}

pub(super) fn build_supply_order_timeslot(
    range: &SupplyOrderTimeslotRangeInput,
) -> serde_json::Map<String, Value> {
    let mut payload = serde_json::Map::new();
    if let Some(from) = range.from.as_deref() {
        payload.insert("from".to_owned(), json!(from));
    }
    if let Some(to) = range.to.as_deref() {
        payload.insert("to".to_owned(), json!(to));
    }
    if let Some(filter_type) = range.timeslot_filter_type {
        payload.insert("timeslot_filter_type".to_owned(), json!(filter_type));
    }
    payload
}

pub(super) fn validate_unique_wb_signed_ids(field: &str, values: &[u64]) -> Result<(), String> {
    validate_unique_positive_ids(field, values)?;
    if values.iter().any(|value| *value > MAX_WB_SIGNED_API_ID) {
        return Err(format!(
            "{field} не должен содержать ID больше {MAX_WB_SIGNED_API_ID}"
        ));
    }
    Ok(())
}

/// Preserve actual zeroes while refusing malformed, duplicate or unrelated
/// rows. A valid but partial response explicitly identifies the missing IDs.
pub(super) fn wb_missing_stock_ids(data: &Value, requested: &[u64]) -> Result<Vec<u64>, String> {
    const INVALID: &str = "WB_STOCKS_INVALID_RESPONSE: ответ остатков WB некорректен; остановите выгрузку, не заменяйте отсутствующие данные нулями";
    let rows = data
        .get("stocks")
        .and_then(Value::as_array)
        .ok_or(INVALID)?;
    let requested_set = requested.iter().copied().collect::<BTreeSet<_>>();
    let mut returned = BTreeSet::new();
    for row in rows {
        let id = row.get("chrtId").and_then(Value::as_u64).ok_or(INVALID)?;
        if !requested_set.contains(&id)
            || !returned.insert(id)
            || row.get("amount").and_then(Value::as_u64).is_none()
        {
            return Err(INVALID.to_owned());
        }
    }
    Ok(requested
        .iter()
        .copied()
        .filter(|id| !returned.contains(id))
        .collect())
}

pub(super) fn validate_wb_promotion_statuses(statuses: &[i32]) -> Result<(), String> {
    const ALLOWED_STATUSES: &[i32] = &[-1, 4, 7, 8, 9, 11];
    validate_count("statuses", statuses.len(), 1, ALLOWED_STATUSES.len())?;
    let unique = statuses.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != statuses.len() {
        return Err("statuses не должен содержать повторяющиеся значения".to_owned());
    }
    if statuses
        .iter()
        .any(|status| !ALLOWED_STATUSES.contains(status))
    {
        return Err(
            "statuses допускает только официальные значения WB: -1, 4, 7, 8, 9, 11".to_owned(),
        );
    }
    Ok(())
}

pub(super) fn validate_wb_promotion_date_range(
    begin_date: &str,
    end_date: &str,
) -> Result<(), String> {
    let begin = parse_date(begin_date, "begin_date")?;
    let end = parse_date(end_date, "end_date")?;
    if end < begin {
        return Err("end_date не может быть раньше begin_date".to_owned());
    }
    if (end - begin).num_days() + 1 > MAX_WB_PROMOTION_PERIOD_DAYS {
        return Err(format!(
            "период WB Promotion не может превышать {MAX_WB_PROMOTION_PERIOD_DAYS} день"
        ));
    }
    Ok(())
}

pub(super) fn validate_wb_search_product_queries_input(
    input: &WbSearchProductQueriesInput,
) -> Result<(), String> {
    validate_date_range(
        &input.date_from,
        &input.date_to,
        MAX_WB_SEARCH_REPORT_PERIOD_DAYS,
    )?;
    validate_count("nm_ids", input.nm_ids.len(), 1, MAX_WB_SEARCH_NM_IDS)?;
    validate_unique_positive_ids("nm_ids", &input.nm_ids)?;
    validate_limit(
        input.limit,
        u32::try_from(MAX_WB_SEARCH_TEXTS).expect("WB search text limit fits u32"),
    )?;
    Ok(())
}

pub(super) fn validate_wb_search_texts(search_texts: &[String]) -> Result<(), String> {
    validate_count("search_texts", search_texts.len(), 1, MAX_WB_SEARCH_TEXTS)?;
    let mut unique = BTreeSet::new();
    for text in search_texts {
        validate_non_blank("search_texts", text)?;
        validate_max_chars("search_texts", text, MAX_WB_SEARCH_TEXT_BYTES)?;
        if text.len() > MAX_WB_SEARCH_TEXT_BYTES {
            return Err(format!(
                "search_texts не может быть длиннее {MAX_WB_SEARCH_TEXT_BYTES} байт"
            ));
        }
        if text.trim() != text || text.chars().any(char::is_control) {
            return Err(
                "search_texts не должен содержать управляющие символы или пробелы по краям"
                    .to_owned(),
            );
        }
        if !unique.insert(text) {
            return Err("search_texts не должен содержать повторяющиеся фразы".to_owned());
        }
    }
    Ok(())
}

pub(super) fn validate_wb_search_orders_positions_input(
    input: &WbSearchOrdersPositionsInput,
) -> Result<(), String> {
    validate_date_range(
        &input.date_from,
        &input.date_to,
        MAX_WB_SEARCH_ORDERS_PERIOD_DAYS,
    )?;
    if input.nm_id == 0 {
        return Err("nm_id должен быть положительным".to_owned());
    }
    validate_wb_search_texts(&input.search_texts)
}

pub(super) fn validate_wb_promotion_minimum_bids_input(
    input: &WbPromotionMinimumBidsInput,
) -> Result<(), String> {
    if !(1..=MAX_WB_SIGNED_API_ID).contains(&input.campaign_id) {
        return Err(format!(
            "campaign_id должен быть от 1 до {MAX_WB_SIGNED_API_ID}"
        ));
    }
    validate_count("nm_ids", input.nm_ids.len(), 1, MAX_WB_MINIMUM_BID_NM_IDS)?;
    validate_unique_wb_signed_ids("nm_ids", &input.nm_ids)?;
    validate_count("placement_types", input.placement_types.len(), 1, 3)?;
    let unique = input
        .placement_types
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if unique.len() != input.placement_types.len() {
        return Err("placement_types не должен содержать повторяющиеся значения".to_owned());
    }
    Ok(())
}

pub(super) fn validate_wb_promotion_search_cluster_pairs(
    items: &[WbPromotionSearchClusterPair],
) -> Result<(), String> {
    validate_count("items", items.len(), 1, MAX_WB_SEARCH_CLUSTER_PAIRS)?;
    let mut unique = BTreeSet::new();
    for item in items {
        if !(1..=MAX_WB_SIGNED_API_ID).contains(&item.campaign_id) {
            return Err(format!(
                "items.campaign_id должен быть от 1 до {MAX_WB_SIGNED_API_ID}"
            ));
        }
        if !(1..=MAX_WB_SIGNED_API_ID).contains(&item.nm_id) {
            return Err(format!(
                "items.nm_id должен быть от 1 до {MAX_WB_SIGNED_API_ID}"
            ));
        }
        if !unique.insert((item.campaign_id, item.nm_id)) {
            return Err(
                "items не должен содержать повторяющиеся пары campaign_id + nm_id".to_owned(),
            );
        }
    }
    Ok(())
}

pub(super) fn validate_wb_product_cards_input(input: &WbProductCardsInput) -> Result<(), String> {
    validate_limit(input.limit, 100)?;
    if input
        .with_photo
        .is_some_and(|with_photo| !(-1..=1).contains(&with_photo))
    {
        return Err("with_photo должен быть равен -1, 0 или 1".to_owned());
    }
    if let Some(text_search) = input.text_search.as_deref() {
        validate_non_blank("text_search", text_search)?;
        validate_max_chars("text_search", text_search, MAX_IDENTIFIER_CHARS)?;
        if text_search.trim() != text_search || text_search.chars().any(char::is_control) {
            return Err(
                "text_search не должен содержать управляющие символы или пробелы по краям"
                    .to_owned(),
            );
        }
    }
    validate_count("tag_ids", input.tag_ids.len(), 0, 100)?;
    validate_count("object_ids", input.object_ids.len(), 0, 100)?;
    validate_positive_ids("tag_ids", &input.tag_ids)?;
    validate_positive_ids("object_ids", &input.object_ids)?;
    validate_string_list("brands", &input.brands, 100, MAX_ENUM_VALUE_CHARS)?;
    if input
        .brands
        .iter()
        .any(|brand| brand.trim() != brand || brand.chars().any(char::is_control))
    {
        return Err(
            "brands не должен содержать управляющие символы или пробелы по краям".to_owned(),
        );
    }
    if input.imt_id == Some(0) {
        return Err("imt_id должен быть положительным ID".to_owned());
    }
    if input.cursor_nm_id == Some(0) {
        return Err("cursor_nm_id должен быть положительным ID".to_owned());
    }
    match (&input.cursor_updated_at, input.cursor_nm_id) {
        (Some(updated_at), Some(_)) => {
            validate_max_chars("cursor_updated_at", updated_at, 64)?;
            chrono::DateTime::parse_from_rfc3339(updated_at).map_err(|_| {
                "cursor_updated_at должен иметь формат RFC3339 с часовым поясом".to_owned()
            })?;
        }
        (None, None) => {}
        _ => {
            return Err(
                "cursor_updated_at и cursor_nm_id должны передаваться только вместе".to_owned(),
            );
        }
    }

    Ok(())
}

pub(super) fn wb_product_cards_filter(
    input: &WbProductCardsInput,
) -> serde_json::Map<String, Value> {
    let mut filter = serde_json::Map::new();
    if let Some(with_photo) = input.with_photo {
        filter.insert("withPhoto".to_owned(), json!(with_photo));
    }
    if let Some(text_search) = &input.text_search {
        filter.insert("textSearch".to_owned(), json!(text_search));
    }
    if let Some(allowed_categories_only) = input.allowed_categories_only {
        filter.insert(
            "allowedCategoriesOnly".to_owned(),
            json!(allowed_categories_only),
        );
    }
    if !input.tag_ids.is_empty() {
        filter.insert("tagIDs".to_owned(), json!(input.tag_ids));
    }
    if !input.object_ids.is_empty() {
        filter.insert("objectIDs".to_owned(), json!(input.object_ids));
    }
    if !input.brands.is_empty() {
        filter.insert("brands".to_owned(), json!(input.brands));
    }
    if let Some(imt_id) = input.imt_id {
        filter.insert("imtID".to_owned(), json!(imt_id));
    }
    filter
}

pub(super) fn wb_product_cards_cursor(
    input: &WbProductCardsInput,
) -> serde_json::Map<String, Value> {
    let mut cursor = serde_json::Map::new();
    cursor.insert("limit".to_owned(), json!(input.limit));
    if let (Some(updated_at), Some(nm_id)) = (&input.cursor_updated_at, input.cursor_nm_id) {
        cursor.insert("updatedAt".to_owned(), json!(updated_at));
        cursor.insert("nmID".to_owned(), json!(nm_id));
    }
    cursor
}

pub(super) fn wb_product_cards_payload(input: &WbProductCardsInput) -> Result<Value, String> {
    validate_wb_product_cards_input(input)?;
    let filter = wb_product_cards_filter(input);
    let cursor = wb_product_cards_cursor(input);
    let mut settings = serde_json::Map::new();
    settings.insert("sort".to_owned(), json!({ "ascending": input.ascending }));
    if !filter.is_empty() {
        settings.insert("filter".to_owned(), Value::Object(filter));
    }
    settings.insert("cursor".to_owned(), Value::Object(cursor));
    Ok(json!({ "settings": settings }))
}

pub(super) fn validate_wb_change_date(value: &str) -> Result<(), String> {
    validate_non_blank("date_from", value)?;
    validate_max_chars("date_from", value, 64)?;
    if NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()
        || chrono::DateTime::parse_from_rfc3339(value).is_ok()
        || NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f").is_ok()
    {
        return Ok(());
    }
    Err("date_from должен иметь формат YYYY-MM-DD или RFC3339".to_owned())
}

pub(super) fn validate_flag(flag: u8) -> Result<(), String> {
    if flag > 1 {
        return Err("flag должен быть равен 0 или 1".to_owned());
    }
    Ok(())
}

pub(super) fn validate_max_u32(field: &str, value: u32, maximum: u32) -> Result<(), String> {
    if value > maximum {
        return Err(format!("{field} не может превышать {maximum}"));
    }
    Ok(())
}
