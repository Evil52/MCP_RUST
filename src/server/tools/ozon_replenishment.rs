//! Read-only replenishment evidence. These tools never create supplies.
use super::super::inputs_replenishment::{
    ClusterListInput, ClusterStockAnalyticsInput, SupplyBundleInput,
};
use super::super::{
    Json, OzonMcp, OzonResult, Parameters, RequestIdentity, json, tool, validate_count,
    validate_limit, validate_max_chars,
};
use rmcp::tool_router;
use std::collections::BTreeSet;

fn numeric_ids(name: &str, values: &[String], required: bool) -> Result<(), String> {
    validate_count(name, values.len(), usize::from(required), 100)?;
    let mut seen = BTreeSet::new();
    for value in values {
        if !value.bytes().all(|c| c.is_ascii_digit())
            || value.starts_with('0')
            || !value.parse::<i64>().is_ok_and(|id| id > 0)
            || !seen.insert(value)
        {
            return Err(format!(
                "INVALID_ARGUMENT: {name} must contain unique positive int64 strings"
            ));
        }
    }
    Ok(())
}

fn warehouse_ids(values: &[String]) -> Result<(), String> {
    validate_count("warehouse_ids", values.len(), 0, 100)?;
    let mut seen = BTreeSet::new();
    for value in values {
        if !value
            .parse::<i64>()
            .is_ok_and(|id| id != 0 && id.to_string() == *value)
            || !seen.insert(value)
        {
            return Err(
                "INVALID_ARGUMENT: warehouse_ids must contain unique nonzero int64 strings"
                    .to_owned(),
            );
        }
    }
    Ok(())
}

#[tool_router(router = ozon_replenishment_router, vis = "pub(in crate::server)")]
impl OzonMcp {
    /// Возвращает справочник кластеров Ozon и их складов. Идентификаторы кластеров
    /// не выводятся из названий городов или складов эвристически.
    #[tool(
        name = "ozon_clusters",
        annotations(title = "Кластеры и склады Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn clusters(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ClusterListInput>,
    ) -> Result<Json<OzonResult>, String> {
        numeric_ids("cluster_ids", &input.cluster_ids, false)?;
        self.request(
            &identity,
            input.store,
            "/v1/cluster/list",
            json!({"cluster_ids":input.cluster_ids,"cluster_type":"CLUSTER_TYPE_OZON"}),
        )
        .await
    }

    /// Читает аналитику запасов по SKU, складам и кластерам Ozon.
    /// фильтр 1–100 SKU обязателен. Сохраняйте поля ответа и его период наблюдения.
    /// `warehouse_ids` — знаковые int64 Ozon; отрицательные ID сохраняются без изменения.
    /// Общий остаток FBO из `product_stocks` не заменяет складские/кластерные данные.
    #[tool(
        name = "ozon_cluster_stock_analytics",
        annotations(title = "Аналитика запасов Ozon по складам", read_only_hint = true)
    )]
    pub(in crate::server) async fn cluster_stock_analytics(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ClusterStockAnalyticsInput>,
    ) -> Result<Json<OzonResult>, String> {
        numeric_ids("skus", &input.skus, true)?;
        warehouse_ids(&input.warehouse_ids)?;
        numeric_ids("cluster_ids", &input.cluster_ids, false)?;
        self.request(&identity,input.store,"/v1/analytics/stocks",
            json!({"skus":input.skus,"warehouse_ids":input.warehouse_ids,"cluster_ids":input.cluster_ids})).await
    }

    /// Читает товарный состав существующих поставок по `bundle_id` из `supply_order_get`.
    /// Одна страница; при `has_next=true` передайте `last_id` с теми же фильтрами.
    /// Статус заявки, кластер и таймслот берите из `supply_order_get`.
    /// Запланированные и уже принятые количества не равнозначны. Новая поставка не создаётся.
    #[tool(
        name = "ozon_supply_bundle",
        annotations(title = "Товары существующей поставки Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn supply_bundle(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<SupplyBundleInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_count("bundle_ids", input.bundle_ids.len(), 1, 100)?;
        let mut seen = BTreeSet::new();
        for id in &input.bundle_ids {
            if id.is_empty()
                || id.len() > 128
                || !id.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-')
                || !seen.insert(id)
            {
                return Err("INVALID_ARGUMENT: invalid or duplicate bundle_id".to_owned());
            }
        }
        validate_limit(input.limit, 1000)?;
        validate_max_chars("query", &input.query, 256)?;
        if let Some(cursor) = input.last_id.as_deref() {
            validate_max_chars("last_id", cursor, 128)?;
            if cursor.is_empty() || cursor.chars().any(char::is_control) {
                return Err("INVALID_ARGUMENT: invalid last_id".to_owned());
            }
        }
        self.request(&identity,input.store,"/v1/supply-order/bundle",
            json!({"bundle_ids":input.bundle_ids,"limit":input.limit,"last_id":input.last_id.unwrap_or_default(),"is_asc":true,"query":input.query,"sort_field":"UNSPECIFIED"})).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn id_filters_are_bounded_and_exact() {
        for value in [
            "",
            "0",
            "01",
            "-1",
            "1.0",
            "9223372036854775808",
            "1/../../write",
        ] {
            assert!(numeric_ids("skus", &[value.to_owned()], true).is_err());
        }
        assert!(numeric_ids("skus", &[], true).is_err());
        assert!(numeric_ids("skus", &["1".into(), "1".into()], true).is_err());
        assert!(numeric_ids("skus", &["9223372036854775807".into()], true).is_ok());
    }
}
