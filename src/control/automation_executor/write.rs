use anyhow::Result;
use serde_json::{Value, json};

use crate::control::{
    automation_postgres::WbAutomationPostgresError,
    wb::{WbGuardedWriteError, WbWriteError, WbWriteOutcomeKind},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PostgresWriteResult {
    Sent,
    Ambiguous,
}

pub(super) fn classify_postgres_write(
    result: &Result<(), WbGuardedWriteError<WbAutomationPostgresError>>,
) -> Result<PostgresWriteResult> {
    match result {
        Ok(()) => Ok(PostgresWriteResult::Sent),
        Err(WbGuardedWriteError::Permit(error)) => Err(anyhow::Error::new(*error)
            .context("WB automation final PostgreSQL permit is unavailable")),
        Err(WbGuardedWriteError::Write(error)) => {
            // A bounded allowlist keeps credentials, URLs and vendor bodies
            // out of logs. stderr works even in CLI workers without a tracing
            // subscriber. Preserve the existing durable reconciliation lock.
            eprintln!("{}", diagnostic(error));
            Ok(PostgresWriteResult::Ambiguous)
        }
    }
}

fn diagnostic(error: &WbWriteError) -> Value {
    let (class, status, request_id) = match error {
        WbWriteError::SharedQuota(_) => ("shared_quota", None, None),
        WbWriteError::InvalidRequest(_) => ("invalid_request", None, None),
        WbWriteError::HttpStatus { status, request_id } => {
            ("http_error", Some(status.as_u16()), request_id.as_deref())
        }
        WbWriteError::Ambiguous { reason, request_id } => (*reason, None, request_id.as_deref()),
    };
    let request_id = request_id.filter(|value| {
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:/".contains(&byte))
    });
    json!({
        "event": "wb_write_failed",
        "error_class": class,
        "http_status": status,
        "request_id": request_id,
        "departure_uncertain": error.outcome_kind() == WbWriteOutcomeKind::Ambiguous,
        "outcome": "reconciliation_required",
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::StatusCode;

    #[test]
    fn safe_diagnostics_keep_http_and_timeout_evidence() {
        let http = diagnostic(&WbWriteError::HttpStatus {
            status: StatusCode::TOO_MANY_REQUESTS,
            request_id: Some("request-42".to_owned()),
        });
        assert_eq!(http["http_status"], 429);
        assert_eq!(http["request_id"], "request-42");
        assert_eq!(http["departure_uncertain"], true);
        let timeout = diagnostic(&WbWriteError::Ambiguous {
            reason: "timeout",
            request_id: None,
        });
        assert_eq!(timeout["error_class"], "timeout");
        assert!(timeout["http_status"].is_null());
        assert_eq!(timeout["outcome"], "reconciliation_required");
    }

    #[test]
    fn unsafe_header_and_validation_details_are_omitted() {
        let error = diagnostic(&WbWriteError::HttpStatus {
            status: StatusCode::BAD_REQUEST,
            request_id: Some("Bearer sensitive header\n".to_owned()),
        });
        assert!(error["request_id"].is_null());
        assert!(!error.to_string().contains("sensitive"));
        let invalid = diagnostic(&WbWriteError::InvalidRequest("private request content"));
        assert_eq!(invalid["error_class"], "invalid_request");
        assert_eq!(invalid["departure_uncertain"], false);
        assert!(!invalid.to_string().contains("private"));
        let quota = diagnostic(&WbWriteError::SharedQuota(
            crate::marketplace_quota::QuotaError::Unavailable,
        ));
        assert_eq!(quota["error_class"], "shared_quota");
        assert_eq!(quota["departure_uncertain"], false);
    }
}
