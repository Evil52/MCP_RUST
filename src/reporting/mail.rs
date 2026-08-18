//! Bounded, provider-independent daily-report email envelope.
//!
//! Address resolution remains server-side: callers pass values loaded from
//! the validated report policy's environment-variable names. This module
//! neither reads process environment nor performs network I/O.

use std::fmt;

use super::{ReportKind, artifact_store::StoredReportBundle, postgres_outbox::ClaimedDelivery};

const MAX_ADDRESS_BYTES: usize = 254;
const MAX_HTML_BYTES: usize = 2 * 1024 * 1024;
const MAX_XLSX_BYTES: usize = 16 * 1024 * 1024;

/// One fully scoped email ready for a future provider transport.
#[derive(Clone, PartialEq, Eq)]
pub struct ReportEmail {
    sender: String,
    recipient: String,
    subject: String,
    html: String,
    attachment_name: String,
    xlsx: Vec<u8>,
}

impl fmt::Debug for ReportEmail {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReportEmail")
            .field("sender", &"<redacted>")
            .field("recipient", &"<redacted>")
            .field("subject", &self.subject)
            .field("html_bytes", &self.html.len())
            .field("attachment_name", &self.attachment_name)
            .field("xlsx_bytes", &self.xlsx.len())
            .finish()
    }
}

impl ReportEmail {
    pub fn sender(&self) -> &str {
        &self.sender
    }

    pub fn recipient(&self) -> &str {
        &self.recipient
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }

    pub fn html(&self) -> &str {
        &self.html
    }

    pub fn attachment_name(&self) -> &str {
        &self.attachment_name
    }

    pub fn xlsx(&self) -> &[u8] {
        &self.xlsx
    }
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum MailBuildError {
    #[error("daily report email address is invalid")]
    InvalidAddress,
    #[error("daily report delivery scope is invalid")]
    InvalidScope,
    #[error("daily report email artifact is invalid")]
    InvalidArtifact,
}

/// Builds a single-recipient message from one already-claimed artifact.
///
/// Consolidated morning+evening delivery remains rejected until the renderer
/// produces two explicit sections. Accepting it here would silently attach one
/// interval while claiming that two reports were covered.
pub fn build_report_email(
    sender: &str,
    recipient: &str,
    claim: &ClaimedDelivery,
    bundle: StoredReportBundle,
) -> Result<ReportEmail, MailBuildError> {
    validate_address(sender)?;
    validate_address(recipient)?;
    let [key] = claim.covered_keys.as_slice() else {
        return Err(MailBuildError::InvalidScope);
    };
    if claim.batch_id <= 0
        || claim.attempt_no == 0
        || key.recipient_id != claim.recipient_id
        || key.report_version != claim.report_version
    {
        return Err(MailBuildError::InvalidScope);
    }
    if bundle.html.trim().is_empty()
        || bundle.html.len() > MAX_HTML_BYTES
        || bundle.xlsx.is_empty()
        || bundle.xlsx.len() > MAX_XLSX_BYTES
    {
        return Err(MailBuildError::InvalidArtifact);
    }
    let kind = match key.kind {
        ReportKind::Morning => "утренний",
        ReportKind::Evening => "вечерний",
    };
    let file_kind = match key.kind {
        ReportKind::Morning => "morning",
        ReportKind::Evening => "evening",
    };
    Ok(ReportEmail {
        sender: sender.to_owned(),
        recipient: recipient.to_owned(),
        subject: format!("Ежедневный отчёт Ozon/WB — {kind} — {}", key.local_date),
        html: bundle.html,
        attachment_name: format!("ozonofk-daily-{}-{file_kind}.xlsx", key.local_date),
        xlsx: bundle.xlsx,
    })
}

fn validate_address(value: &str) -> Result<(), MailBuildError> {
    if value.is_empty()
        || value.len() > MAX_ADDRESS_BYTES
        || !value.is_ascii()
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(MailBuildError::InvalidAddress);
    }
    let mut parts = value.split('@');
    let (Some(local), Some(domain), None) = (parts.next(), parts.next(), parts.next()) else {
        return Err(MailBuildError::InvalidAddress);
    };
    let local_valid = !local.is_empty()
        && local.len() <= 64
        && !local.starts_with('.')
        && !local.ends_with('.')
        && !local.contains("..")
        && local.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'.' | b'!'
                        | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'/'
                        | b'='
                        | b'?'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'{'
                        | b'|'
                        | b'}'
                        | b'~'
                )
        });
    let domain_valid = domain.len() <= 253
        && domain.contains('.')
        && domain.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        });
    if local_valid && domain_valid {
        Ok(())
    } else {
        Err(MailBuildError::InvalidAddress)
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;
    use crate::reporting::{ReportKey, outbox::ArtifactIdentity};

    fn claim(kind: ReportKind) -> ClaimedDelivery {
        ClaimedDelivery {
            batch_id: 7,
            recipient_id: "pilot_owner".to_owned(),
            report_version: 1,
            attempt_no: 1,
            artifact: ArtifactIdentity {
                object_key: "daily-reports/2026/08/18/pilot_owner/v1/morning.xlsx".to_owned(),
                sha256: "a".repeat(64),
                html_sha256: "b".repeat(64),
            },
            covered_keys: vec![ReportKey {
                local_date: NaiveDate::from_ymd_opt(2026, 8, 18).unwrap(),
                kind,
                recipient_id: "pilot_owner".to_owned(),
                report_version: 1,
            }],
        }
    }

    fn bundle() -> StoredReportBundle {
        StoredReportBundle {
            html: "<html><body>Без фотографий</body></html>".to_owned(),
            xlsx: vec![1, 2, 3],
        }
    }

    #[test]
    fn one_scoped_artifact_becomes_a_redacted_bounded_email() {
        for (kind, label, file_kind) in [
            (ReportKind::Morning, "утренний", "morning"),
            (ReportKind::Evening, "вечерний", "evening"),
        ] {
            let email = build_report_email(
                "reports@example.test",
                "owner@example.test",
                &claim(kind),
                bundle(),
            )
            .unwrap();
            assert_eq!(email.sender(), "reports@example.test");
            assert_eq!(email.recipient(), "owner@example.test");
            assert!(email.subject().contains(label));
            assert_eq!(email.html(), "<html><body>Без фотографий</body></html>");
            assert_eq!(email.xlsx(), &[1, 2, 3]);
            assert_eq!(
                email.attachment_name(),
                format!("ozonofk-daily-2026-08-18-{file_kind}.xlsx")
            );
            let debug = format!("{email:?}");
            assert!(!debug.contains("reports@example.test"));
            assert!(!debug.contains("owner@example.test"));
        }
    }

    #[test]
    fn malformed_addresses_are_rejected_without_header_injection() {
        for address in [
            "",
            "a@b",
            "a@@example.test",
            ".a@example.test",
            "a.@example.test",
            "a..b@example.test",
            "a@-example.test",
            "a@example-.test",
            "a@example..test",
            "a b@example.test",
            "a@example.test\r\nBcc:x@example.test",
            "а@example.test",
        ] {
            assert_eq!(
                build_report_email(
                    address,
                    "ok@example.test",
                    &claim(ReportKind::Morning),
                    bundle()
                ),
                Err(MailBuildError::InvalidAddress)
            );
            assert_eq!(
                build_report_email(
                    "ok@example.test",
                    address,
                    &claim(ReportKind::Morning),
                    bundle()
                ),
                Err(MailBuildError::InvalidAddress)
            );
        }
        assert!(
            build_report_email(
                "a!#$%&'*+-/=?^_`{|}~@example.test",
                "ok@example.test",
                &claim(ReportKind::Morning),
                bundle(),
            )
            .is_ok()
        );
    }

    #[test]
    fn cross_scope_and_consolidated_claims_fail_closed() {
        let mut invalid = claim(ReportKind::Morning);
        invalid.batch_id = 0;
        assert_eq!(
            build_report_email("a@example.test", "b@example.test", &invalid, bundle()),
            Err(MailBuildError::InvalidScope)
        );
        let mut invalid = claim(ReportKind::Morning);
        invalid.attempt_no = 0;
        assert_eq!(
            build_report_email("a@example.test", "b@example.test", &invalid, bundle()),
            Err(MailBuildError::InvalidScope)
        );
        let mut invalid = claim(ReportKind::Morning);
        invalid.covered_keys[0].recipient_id = "foreign".to_owned();
        assert_eq!(
            build_report_email("a@example.test", "b@example.test", &invalid, bundle()),
            Err(MailBuildError::InvalidScope)
        );
        let mut invalid = claim(ReportKind::Morning);
        invalid.covered_keys[0].report_version = 2;
        assert_eq!(
            build_report_email("a@example.test", "b@example.test", &invalid, bundle()),
            Err(MailBuildError::InvalidScope)
        );
        let mut invalid = claim(ReportKind::Morning);
        invalid.covered_keys.push(invalid.covered_keys[0].clone());
        assert_eq!(
            build_report_email("a@example.test", "b@example.test", &invalid, bundle()),
            Err(MailBuildError::InvalidScope)
        );
    }

    #[test]
    fn empty_and_oversized_artifacts_are_rejected() {
        for invalid in [
            StoredReportBundle {
                html: " ".to_owned(),
                xlsx: vec![1],
            },
            StoredReportBundle {
                html: "ok".to_owned(),
                xlsx: Vec::new(),
            },
            StoredReportBundle {
                html: "x".repeat(MAX_HTML_BYTES + 1),
                xlsx: vec![1],
            },
            StoredReportBundle {
                html: "ok".to_owned(),
                xlsx: vec![0; MAX_XLSX_BYTES + 1],
            },
        ] {
            assert_eq!(
                build_report_email(
                    "a@example.test",
                    "b@example.test",
                    &claim(ReportKind::Morning),
                    invalid,
                ),
                Err(MailBuildError::InvalidArtifact)
            );
        }
    }
}
