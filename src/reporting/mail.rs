//! Bounded, provider-independent daily-report email envelope.
//!
//! Address resolution remains server-side: callers pass values loaded from
//! the validated report policy's environment-variable names. This module
//! neither reads process environment nor performs network I/O.

use std::fmt;

use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use sha2::{Digest, Sha256};

use super::{ReportKind, artifact_store::StoredReportBundle, postgres_outbox::ClaimedDelivery};

const MAX_ADDRESS_BYTES: usize = 254;
const MAX_HTML_BYTES: usize = 1024 * 1024;
const MAX_XLSX_BYTES: usize = 8 * 1024 * 1024;

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
    #[must_use]
    pub fn sender(&self) -> &str {
        &self.sender
    }

    #[must_use]
    pub fn recipient(&self) -> &str {
        &self.recipient
    }

    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    #[must_use]
    pub fn html(&self) -> &str {
        &self.html
    }

    #[must_use]
    pub fn attachment_name(&self) -> &str {
        &self.attachment_name
    }

    #[must_use]
    pub fn xlsx(&self) -> &[u8] {
        &self.xlsx
    }

    /// Produces the URL-safe raw RFC 5322 message required by Gmail's API.
    ///
    /// Every interpolated header value was validated or generated locally.
    /// Body parts are base64 encoded and wrapped, so neither HTML nor workbook
    /// bytes can escape their MIME section.
    #[must_use]
    pub fn gmail_raw(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(self.sender.as_bytes());
        digest.update([0]);
        digest.update(self.recipient.as_bytes());
        digest.update([0]);
        digest.update(self.attachment_name.as_bytes());
        let boundary = format!("mcp-ozon-{}", URL_SAFE_NO_PAD.encode(digest.finalize()));
        let encoded_subject = STANDARD.encode(self.subject.as_bytes());
        let mut message = Vec::with_capacity(
            self.html.len().saturating_mul(2) + self.xlsx.len().saturating_mul(2) + 1_024,
        );
        message.extend_from_slice(
            format!(
                "From: {}\r\nTo: {}\r\nSubject: =?UTF-8?B?{}?=\r\n\
             MIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=\"{}\"\r\n\r\n\
             --{}\r\nContent-Type: text/html; charset=UTF-8\r\n\
             Content-Transfer-Encoding: base64\r\n\r\n",
                self.sender, self.recipient, encoded_subject, boundary, boundary
            )
            .as_bytes(),
        );
        append_wrapped_base64(&mut message, self.html.as_bytes());
        message.extend_from_slice(format!(
            "\r\n--{}\r\nContent-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet; name=\"{}\"\r\n\
             Content-Disposition: attachment; filename=\"{}\"\r\n\
             Content-Transfer-Encoding: base64\r\n\r\n",
            boundary, self.attachment_name, self.attachment_name
        ).as_bytes());
        append_wrapped_base64(&mut message, &self.xlsx);
        message.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        URL_SAFE_NO_PAD.encode(message)
    }
}

fn append_wrapped_base64(output: &mut Vec<u8>, bytes: &[u8]) {
    let encoded = STANDARD.encode(bytes);
    for (index, chunk) in encoded.as_bytes().chunks(76).enumerate() {
        if index > 0 {
            output.extend_from_slice(b"\r\n");
        }
        output.extend_from_slice(chunk);
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

pub(super) fn validate_address(value: &str) -> Result<(), MailBuildError> {
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
    use chrono::{NaiveDate, TimeZone, Utc};

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
            deadline_at: Utc.with_ymd_and_hms(2026, 8, 18, 9, 0, 0).unwrap(),
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
            let raw = URL_SAFE_NO_PAD.decode(email.gmail_raw()).unwrap();
            let raw = String::from_utf8(raw).unwrap();
            assert!(raw.contains("Content-Type: multipart/mixed"));
            assert!(raw.contains("Content-Disposition: attachment"));
            assert!(raw.contains("Subject: =?UTF-8?B?"));
            assert!(!raw.contains("Ежедневный отчёт"));
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
            "a:b@example.test",
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

    #[test]
    fn gmail_mime_wraps_long_base64_parts_at_seventy_six_columns() {
        let email = build_report_email(
            "reports@example.test",
            "owner@example.test",
            &claim(ReportKind::Morning),
            StoredReportBundle {
                html: "x".repeat(100),
                xlsx: vec![7; 100],
            },
        )
        .unwrap();
        let raw = URL_SAFE_NO_PAD.decode(email.gmail_raw()).unwrap();
        let raw = String::from_utf8(raw).unwrap();
        assert!(raw.lines().any(|line| line.len() == 76));
    }
}
