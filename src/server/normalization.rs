//! Normalization of untrusted marketplace price and product content.

use super::{
    BTreeMap, MAX_PRODUCT_FILTER_ITEMS, OZON_PRICE_NORMALIZATION_FAILED,
    OZON_PRODUCT_CONTENT_NORMALIZATION_FAILED, OzonLiveMarketingAction, OzonLivePriceItem,
    OzonLivePricesResult, OzonProductContentDiagnosticItem, OzonProductContentError, OzonResult,
    OzonSppPriceAvailability, Value,
};

pub(super) fn price_normalization_error(message: &str) -> String {
    format!("{OZON_PRICE_NORMALIZATION_FAILED}: {message}")
}

pub(super) fn parse_price_minor(value: &Value) -> Result<u64, String> {
    let source = match value {
        Value::Number(number) => number.to_string(),
        Value::String(value) if !value.is_empty() => value.clone(),
        _ => {
            return Err(price_normalization_error(
                "денежное поле Ozon имеет неподдерживаемый тип",
            ));
        }
    };
    let (whole, fraction) = source
        .split_once('.')
        .map_or((source.as_str(), ""), |(whole, fraction)| (whole, fraction));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > 2
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(price_normalization_error(
            "денежное поле Ozon не является неотрицательной суммой с точностью до копеек",
        ));
    }
    let whole = whole
        .parse::<u64>()
        .map_err(|_| price_normalization_error("денежное поле Ozon слишком велико"))?;
    let fraction = fraction.as_bytes();
    let fraction = fraction
        .first()
        .map_or(0, |digit| u64::from(*digit - b'0') * 10)
        + fraction.get(1).map_or(0, |digit| u64::from(*digit - b'0'));
    whole
        .checked_mul(100)
        .and_then(|minor| minor.checked_add(fraction))
        .ok_or_else(|| price_normalization_error("денежное поле Ozon слишком велико"))
}

pub(super) fn format_price_minor(minor: u64) -> String {
    format!("{}.{:02}", minor / 100, minor % 100)
}

pub(super) fn optional_price_minor(
    price: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Option<u64>, String> {
    match price.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(None),
        Some(value) => parse_price_minor(value).map(Some),
    }
}

/// Reads one money field under the Ozon convention that a zero amount means
/// "not set" rather than a real price of nothing.
///
/// `old_price`, `marketing_seller_price` and `marketing_price` are all returned
/// as `0` when the corresponding price does not exist, and a listed product
/// never has a genuine seller price of zero. Reporting `"0.00"` would put a
/// fabricated list price into column O and break the documented O − U formula.
pub(super) fn optional_positive_price_minor(
    price: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Option<u64>, String> {
    Ok(optional_price_minor(price, field)?.filter(|amount| *amount > 0))
}

pub(super) fn optional_string_field(
    object: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Option<String>, String> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(price_normalization_error(
            "текстовое поле Ozon имеет неподдерживаемый тип",
        )),
    }
}

pub(super) fn optional_identifier(value: Option<&Value>) -> Result<Option<String>, String> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.is_empty() => Ok(Some(value.clone())),
        Some(Value::Number(value)) => Ok(Some(value.to_string())),
        Some(_) => Err(price_normalization_error(
            "идентификатор товара Ozon имеет неподдерживаемый тип",
        )),
    }
}

pub(super) fn diagnostic_text(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(value)) if !value.is_empty() => Some(value.clone()),
        Some(Value::Number(value)) => Some(value.to_string()),
        _ => None,
    }
}

pub(super) fn redact_urls(value: &str) -> String {
    value
        .split_whitespace()
        .map(|token| {
            if token.contains("://") {
                "[URL_REDACTED]"
            } else {
                token
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

pub(super) fn response_array<'a>(value: &'a Value, pointers: &[&str]) -> &'a [Value] {
    pointers
        .iter()
        .find_map(|pointer| value.pointer(pointer).and_then(Value::as_array))
        .map_or(&[], Vec::as_slice)
}

pub(super) fn response_objects_by_id<'a>(
    items: &'a [Value],
    field: &str,
) -> BTreeMap<String, &'a serde_json::Map<String, Value>> {
    items
        .iter()
        .filter_map(Value::as_object)
        .filter_map(|item| diagnostic_text(item.get(field)).map(|id| (id, item)))
        .collect()
}

pub(super) fn diagnostic_error(
    source: &'static str,
    value: &Value,
) -> Option<OzonProductContentError> {
    let error = value.as_object()?;
    let description = error
        .get("texts")
        .and_then(Value::as_object)
        .and_then(|texts| diagnostic_text(texts.get("description")))
        .or_else(|| diagnostic_text(error.get("message")))
        .map(|value| redact_urls(&value));
    Some(OzonProductContentError {
        source,
        code: diagnostic_text(error.get("code")),
        field: diagnostic_text(error.get("field")),
        level: diagnostic_text(error.get("level")),
        state: diagnostic_text(error.get("state")),
        description,
    })
}

pub(super) fn is_photo_error(error: &OzonProductContentError) -> bool {
    if error.source == "pictures_info" {
        return true;
    }
    error.code.as_deref().is_some_and(|code| {
        let code = code.to_ascii_lowercase();
        code.contains("image") || code.contains("pic") || code.contains("photo")
    }) || error
        .field
        .as_deref()
        .is_some_and(|field| field.eq_ignore_ascii_case("pictures"))
}

pub(super) fn array_len(value: Option<&Value>) -> usize {
    value.and_then(Value::as_array).map_or(0, Vec::len)
}

pub(super) fn normalize_product_content_diagnostics(
    catalog: &Value,
    product_info: &Value,
    pictures_info: &Value,
) -> Result<Vec<OzonProductContentDiagnosticItem>, String> {
    let catalog_items = response_array(catalog, &["/result/items", "/items"]);
    let product_info_by_id = response_objects_by_id(
        response_array(product_info, &["/items", "/result/items"]),
        "id",
    );
    let pictures_info_by_id =
        response_objects_by_id(response_array(pictures_info, &["/items"]), "product_id");

    catalog_items
        .iter()
        .map(|catalog_item| {
            let catalog_item = catalog_item.as_object().ok_or_else(|| {
                format!(
                    "{OZON_PRODUCT_CONTENT_NORMALIZATION_FAILED}: элемент каталога не является объектом"
                )
            })?;
            let product_id = diagnostic_text(catalog_item.get("product_id")).ok_or_else(|| {
                format!(
                    "{OZON_PRODUCT_CONTENT_NORMALIZATION_FAILED}: товар не содержит product_id"
                )
            })?;
            let product_info = product_info_by_id.get(&product_id).copied();
            let pictures_info = pictures_info_by_id.get(&product_id).copied();
            let statuses = product_info
                .and_then(|item| item.get("statuses"))
                .and_then(Value::as_object);

            let mut errors = product_info
                .and_then(|item| item.get("errors"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|error| diagnostic_error("product_info", error))
                .collect::<Vec<_>>();
            errors.extend(
                pictures_info
                    .and_then(|item| item.get("errors"))
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|error| diagnostic_error("pictures_info", error)),
            );

            let primary_image_available = product_info
                .and_then(|item| diagnostic_text(item.get("primary_image")))
                .is_some()
                || pictures_info
                    .is_some_and(|item| array_len(item.get("primary_photo")) > 0);
            let has_photo_error = errors.iter().any(is_photo_error);

            Ok(OzonProductContentDiagnosticItem {
                product_id,
                sku: product_info
                    .and_then(|item| diagnostic_text(item.get("sku")))
                    .or_else(|| diagnostic_text(catalog_item.get("sku"))),
                offer_id: product_info
                    .and_then(|item| diagnostic_text(item.get("offer_id")))
                    .or_else(|| diagnostic_text(catalog_item.get("offer_id"))),
                name: product_info.and_then(|item| diagnostic_text(item.get("name"))),
                primary_image_available,
                image_count: product_info.map_or(0, |item| array_len(item.get("images"))),
                primary_photo_count: pictures_info
                    .map_or(0, |item| array_len(item.get("primary_photo"))),
                photo_count: pictures_info.map_or(0, |item| array_len(item.get("photo"))),
                has_photo_error,
                status: statuses.and_then(|value| diagnostic_text(value.get("status"))),
                status_name: statuses
                    .and_then(|value| diagnostic_text(value.get("status_name"))),
                status_description: statuses
                    .and_then(|value| diagnostic_text(value.get("status_description"))),
                status_failed: statuses
                    .and_then(|value| diagnostic_text(value.get("status_failed"))),
                status_tooltip: statuses
                    .and_then(|value| diagnostic_text(value.get("status_tooltip")))
                    .map(|value| redact_urls(&value)),
                moderate_status: statuses
                    .and_then(|value| diagnostic_text(value.get("moderate_status"))),
                validation_status: statuses
                    .and_then(|value| diagnostic_text(value.get("validation_status"))),
                errors,
            })
        })
        .collect()
}

pub(super) fn normalize_marketing_actions(
    item: &serde_json::Map<String, Value>,
) -> Result<Vec<OzonLiveMarketingAction>, String> {
    let Some(marketing_actions) = item.get("marketing_actions") else {
        return Ok(Vec::new());
    };
    if marketing_actions.is_null() {
        return Ok(Vec::new());
    }
    let marketing_actions = marketing_actions.as_object().ok_or_else(|| {
        price_normalization_error("marketing_actions Ozon имеет неподдерживаемую форму")
    })?;
    let Some(actions) = marketing_actions.get("actions") else {
        return Ok(Vec::new());
    };
    if actions.is_null() {
        return Ok(Vec::new());
    }
    let actions = actions.as_array().ok_or_else(|| {
        price_normalization_error("marketing_actions.actions Ozon не является массивом")
    })?;
    actions
        .iter()
        .map(|action| {
            let action = action.as_object().ok_or_else(|| {
                price_normalization_error("элемент marketing_actions.actions не является объектом")
            })?;
            Ok(OzonLiveMarketingAction {
                title: optional_string_field(action, "title")?,
                value: action
                    .get("value")
                    .cloned()
                    .filter(|value| !value.is_null()),
                date_from: optional_string_field(action, "date_from")?,
                date_to: optional_string_field(action, "date_to")?,
            })
        })
        .collect()
}

pub(super) fn normalize_live_prices(result: OzonResult) -> Result<OzonLivePricesResult, String> {
    let OzonResult {
        store,
        endpoint,
        fetched_at,
        data_classification,
        data,
    } = result;
    let data = data.as_object().ok_or_else(|| {
        price_normalization_error("ответ /v5/product/info/prices не является объектом")
    })?;
    let items = data
        .get("items")
        .and_then(Value::as_array)
        .ok_or_else(|| price_normalization_error("ответ Ozon не содержит массив items"))?;
    if items.len() > MAX_PRODUCT_FILTER_ITEMS {
        return Err(price_normalization_error(
            "ответ Ozon содержит больше 1000 товаров",
        ));
    }

    let items = items
        .iter()
        .map(|item| {
            let item = item.as_object().ok_or_else(|| {
                price_normalization_error("элемент items Ozon не является объектом")
            })?;
            let offer_id = optional_string_field(item, "offer_id")?
                .filter(|value| !value.is_empty())
                .ok_or_else(|| price_normalization_error("товар Ozon не содержит offer_id"))?;
            let price = item
                .get("price")
                .and_then(Value::as_object)
                .ok_or_else(|| price_normalization_error("товар Ozon не содержит объект price"))?;
            let list_price = optional_positive_price_minor(price, "old_price")?;
            let seller_price = optional_positive_price_minor(price, "price")?;
            let action_price = optional_positive_price_minor(price, "marketing_seller_price")?;
            let buyer_price = optional_positive_price_minor(price, "marketing_price")?;
            let discount = list_price
                .zip(buyer_price)
                .and_then(|(list_price, buyer_price)| list_price.checked_sub(buyer_price));
            let spp_price_availability = if buyer_price.is_some() {
                OzonSppPriceAvailability::LegacyMarketingPrice
            } else {
                OzonSppPriceAvailability::Unavailable
            };

            Ok(OzonLivePriceItem {
                offer_id,
                product_id: optional_identifier(item.get("product_id"))?,
                currency_code: optional_string_field(price, "currency_code")?,
                list_price_before_discount_rub: list_price.map(format_price_minor),
                seller_current_price_rub: seller_price.map(format_price_minor),
                action_or_strategy_price_rub: action_price.map(format_price_minor),
                buyer_price_with_spp_rub: buyer_price.map(format_price_minor),
                discount_with_promotion_rub: discount.map(format_price_minor),
                spp_price_availability,
                marketing_actions: normalize_marketing_actions(item)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    let cursor = optional_string_field(data, "cursor")?;
    let total = match data.get("total") {
        None | Some(Value::Null) => None,
        Some(value) => Some(value.as_u64().ok_or_else(|| {
            price_normalization_error("поле total Ozon не является неотрицательным целым")
        })?),
    };

    Ok(OzonLivePricesResult {
        store,
        endpoint,
        fetched_at,
        data_classification,
        buyer_price_formula: "buyer_price_with_spp_rub = list_price_before_discount_rub - discount_with_promotion_rub",
        exact_spp_price_note: "Точная цена с СПП доступна только если Ozon явно вернул legacy-поле price.marketing_price; price.marketing_seller_price является ценой акции или стратегии и не подставляется вместо неё.",
        cursor,
        total,
        items,
    })
}
