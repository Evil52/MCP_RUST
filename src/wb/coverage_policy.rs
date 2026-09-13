//! Reviewed read operations; no arbitrary host, method or path can be supplied.
use super::policy::{ApiHost, EndpointPolicy, RequestClass};
use reqwest::Method;

pub(super) const REVIEWS: &str = "/api/v1/feedbacks";
pub(super) const REVIEW: &str = "/api/v1/feedback";
pub(super) const QUESTIONS: &str = "/api/v1/questions";
pub(super) const QUESTION: &str = "/api/v1/question";
pub(super) const REVIEWS_ARCHIVE: &str = "/api/v1/feedbacks/archive";
pub(super) const CLAIMS: &str = "/api/v1/claims";
pub(super) const CARD_ERRORS: &str = "/content/v2/cards/error/list";
pub(super) const CARD_LIMITS: &str = "/content/v2/cards/limits";
pub(super) const CARDS_TRASH: &str = "/content/v2/get/cards/trash";
pub(super) const SUBJECT_CHARACTERISTICS: &str = "/content/v2/object/charcs/{subjectId}";
pub(super) const SUPPLIES: &str = "/api/v1/supplies";
pub(super) const SUPPLY: &str = "/api/v1/supplies/{ID}";
pub(super) const SUPPLY_GOODS: &str = "/api/v1/supplies/{ID}/goods";
pub(super) const SUPPLY_PACKAGES: &str = "/api/v1/supplies/{ID}/package";

pub(super) const ENDPOINTS: &[EndpointPolicy] = &[
    EndpointPolicy {
        method: Method::GET,
        path: REVIEWS,
        label: "feedbacks:/api/v1/feedbacks",
        host: ApiHost::Feedbacks,
        request_class: RequestClass::FeedbackReport,
    },
    EndpointPolicy {
        method: Method::GET,
        path: REVIEW,
        label: "feedbacks:/api/v1/feedback",
        host: ApiHost::Feedbacks,
        request_class: RequestClass::FeedbackReport,
    },
    EndpointPolicy {
        method: Method::GET,
        path: QUESTIONS,
        label: "feedbacks:/api/v1/questions",
        host: ApiHost::Feedbacks,
        request_class: RequestClass::FeedbackReport,
    },
    EndpointPolicy {
        method: Method::GET,
        path: QUESTION,
        label: "feedbacks:/api/v1/question",
        host: ApiHost::Feedbacks,
        request_class: RequestClass::FeedbackReport,
    },
    EndpointPolicy {
        method: Method::GET,
        path: REVIEWS_ARCHIVE,
        label: "feedbacks:/api/v1/feedbacks/archive",
        host: ApiHost::Feedbacks,
        request_class: RequestClass::FeedbackReport,
    },
    EndpointPolicy {
        method: Method::GET,
        path: CLAIMS,
        label: "returns:/api/v1/claims",
        host: ApiHost::Returns,
        request_class: RequestClass::ReturnClaims,
    },
    EndpointPolicy {
        method: Method::POST,
        path: CARD_ERRORS,
        label: "content:/content/v2/cards/error/list",
        host: ApiHost::Content,
        request_class: RequestClass::CardErrors,
    },
    EndpointPolicy {
        method: Method::GET,
        path: CARD_LIMITS,
        label: "content:/content/v2/cards/limits",
        host: ApiHost::Content,
        request_class: RequestClass::ContentReport,
    },
    EndpointPolicy {
        method: Method::POST,
        path: CARDS_TRASH,
        label: "content:/content/v2/get/cards/trash",
        host: ApiHost::Content,
        request_class: RequestClass::ContentReport,
    },
    EndpointPolicy {
        method: Method::GET,
        path: SUBJECT_CHARACTERISTICS,
        label: "content:/content/v2/object/charcs/{subjectId}",
        host: ApiHost::Content,
        request_class: RequestClass::ContentReport,
    },
    EndpointPolicy {
        method: Method::POST,
        path: SUPPLIES,
        label: "supplies:/api/v1/supplies",
        host: ApiHost::Supplies,
        request_class: RequestClass::SupplyReport,
    },
    EndpointPolicy {
        method: Method::GET,
        path: SUPPLY,
        label: "supplies:/api/v1/supplies/{ID}",
        host: ApiHost::Supplies,
        request_class: RequestClass::SupplyReport,
    },
    EndpointPolicy {
        method: Method::GET,
        path: SUPPLY_GOODS,
        label: "supplies:/api/v1/supplies/{ID}/goods",
        host: ApiHost::Supplies,
        request_class: RequestClass::SupplyReport,
    },
    EndpointPolicy {
        method: Method::GET,
        path: SUPPLY_PACKAGES,
        label: "supplies:/api/v1/supplies/{ID}/package",
        host: ApiHost::Supplies,
        request_class: RequestClass::SupplyReport,
    },
];

pub(super) fn matches_dynamic(template: &str, path: &str) -> Option<bool> {
    let (prefix, suffix) = template
        .split_once("{ID}")
        .or_else(|| template.split_once("{subjectId}"))?;
    Some(
        path.strip_prefix(prefix)
            .and_then(|rest| rest.strip_suffix(suffix))
            .is_some_and(|id| {
                !id.starts_with('0')
                    && id.bytes().all(|b| b.is_ascii_digit())
                    && id.parse::<i64>().is_ok_and(|v| v > 0)
            }),
    )
}
