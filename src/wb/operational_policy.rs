//! Exact read-only operational endpoints; no order mutations or fund transfers.
use super::policy::{ApiHost, EndpointPolicy, RequestClass};
use reqwest::Method;

pub(super) const ENDPOINTS: &[EndpointPolicy] = &[
    EndpointPolicy {
        method: Method::GET,
        path: super::fbs_orders::NEW_PATH,
        label: "marketplace:/api/v3/orders/new",
        host: ApiHost::Marketplace,
        request_class: RequestClass::FbsOrders,
    },
    EndpointPolicy {
        method: Method::GET,
        path: super::fbs_orders::LIST_PATH,
        label: "marketplace:/api/v3/orders",
        host: ApiHost::Marketplace,
        request_class: RequestClass::FbsOrders,
    },
    EndpointPolicy {
        method: Method::POST,
        path: super::fbs_orders::STATUS_PATH,
        label: "marketplace:/api/v3/orders/status",
        host: ApiHost::Marketplace,
        request_class: RequestClass::FbsOrders,
    },
    EndpointPolicy {
        method: Method::GET,
        path: super::promotion_money::COSTS_PATH,
        label: "promotion:/adv/v1/upd",
        host: ApiHost::Promotion,
        request_class: RequestClass::PromotionCosts,
    },
    EndpointPolicy {
        method: Method::GET,
        path: super::promotion_money::PAYMENTS_PATH,
        label: super::promotion_money::PAYMENTS_LABEL,
        host: ApiHost::Promotion,
        request_class: RequestClass::PromotionPayments,
    },
];
