use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use super::{
    ReportKey, ReportKind,
    dataset::ReportDataset,
    html::{HtmlReport, HtmlReportError, render_html},
    outbox::ArtifactIdentity,
    reporting_interval,
    rules::PriorityProblem,
    xlsx::{XlsxReport, XlsxReportError, render_xlsx},
};

const MAX_ARTIFACT_BYTES: usize = 16 * 1024 * 1024;

/// Immutable input for one morning or evening report artifact.
///
/// Account scope and data quality are deliberately derived from the frozen
/// dataset rather than accepted from a caller-controlled email request.
#[derive(Debug, Clone, Copy)]
pub struct ReportBundleRequest<'a> {
    pub key: &'a ReportKey,
    pub manager_name: &'a str,
    pub generated_at: DateTime<Utc>,
    pub dataset: &'a ReportDataset,
    pub problems: &'a [PriorityProblem],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportBundle {
    pub html: String,
    pub xlsx: Vec<u8>,
    pub attachment_name: String,
    pub artifact: ArtifactIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DryRunReceipt {
    pub artifact: ArtifactIdentity,
    pub size_bytes: usize,
    pub persisted: bool,
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum BundleError {
    #[error("daily report bundle input is invalid")]
    InvalidInput,
    #[error("daily report bundle rendering failed")]
    Rendering,
    #[error("daily report bundle integrity check failed")]
    Integrity,
}

pub fn render_bundle(request: ReportBundleRequest<'_>) -> Result<ReportBundle, BundleError> {
    let (interval_start, interval_end) =
        reporting_interval(request.key).map_err(|_| BundleError::InvalidInput)?;
    let account_ids = account_ids(request.dataset)?;
    validate_dataset_scope(request.dataset, &account_ids)?;
    let quality = request
        .dataset
        .source_quality
        .iter()
        .map(|row| row.quality)
        .max()
        .ok_or(BundleError::InvalidInput)?;
    let summary = HtmlReport {
        manager_name: request.manager_name,
        account_ids: &account_ids,
        generated_at: request.generated_at,
        interval_start,
        interval_end,
        preliminary: request.key.kind == ReportKind::Evening,
        quality,
        kpis: &request.dataset.kpis,
        problems: request.problems,
    };
    let html = render_html(summary).map_err(map_html)?;
    let sales = request.dataset.sales_details();
    let advertising = request.dataset.advertising_details();
    let inventory = request.dataset.inventory_details();
    let source_quality = request.dataset.quality_details();
    let xlsx = render_xlsx(XlsxReport {
        summary,
        sales: &sales,
        advertising: &advertising,
        inventory: &inventory,
        source_quality: &source_quality,
    })
    .map_err(map_xlsx)?;
    let artifact = ArtifactIdentity {
        object_key: object_key(request.key),
        sha256: sha256(&xlsx),
    };
    Ok(ReportBundle {
        html,
        xlsx,
        attachment_name: attachment_name(request.key),
        artifact,
    })
}

/// Validates a fully rendered bundle without performing filesystem, S3, or
/// network I/O. A dry-run receipt must never be treated as a persisted outbox
/// artifact.
pub fn inspect_dry_run(bundle: &ReportBundle) -> Result<DryRunReceipt, BundleError> {
    if bundle.xlsx.is_empty()
        || bundle.xlsx.len() > MAX_ARTIFACT_BYTES
        || bundle.html.trim().is_empty()
        || bundle.attachment_name.trim().is_empty()
        || bundle.artifact.object_key.trim().is_empty()
        || bundle.artifact.object_key.len() > 512
        || bundle.artifact.sha256 != sha256(&bundle.xlsx)
    {
        return Err(BundleError::Integrity);
    }
    Ok(DryRunReceipt {
        artifact: bundle.artifact.clone(),
        size_bytes: bundle.xlsx.len(),
        persisted: false,
    })
}

fn account_ids(dataset: &ReportDataset) -> Result<Vec<&str>, BundleError> {
    let accounts = dataset
        .source_quality
        .iter()
        .map(|row| row.account_id.as_str())
        .collect::<BTreeSet<_>>();
    if accounts.is_empty() || accounts.len() > 64 {
        return Err(BundleError::InvalidInput);
    }
    Ok(accounts.into_iter().collect())
}

fn validate_dataset_scope(dataset: &ReportDataset, accounts: &[&str]) -> Result<(), BundleError> {
    let allowed = accounts.iter().copied().collect::<BTreeSet<_>>();
    if dataset
        .sales
        .iter()
        .map(|row| row.account_id.as_str())
        .chain(
            dataset
                .advertising
                .iter()
                .map(|row| row.account_id.as_str()),
        )
        .chain(dataset.inventory.iter().map(|row| row.account_id.as_str()))
        .any(|account| !allowed.contains(account))
    {
        Err(BundleError::InvalidInput)
    } else {
        Ok(())
    }
}

fn object_key(key: &ReportKey) -> String {
    format!(
        "daily-reports/{}/{:02}/{:02}/{}/v{}/{}.xlsx",
        key.local_date.format("%Y"),
        key.local_date.format("%m"),
        key.local_date.format("%d"),
        key.recipient_id,
        key.report_version,
        kind_name(key.kind)
    )
}

fn attachment_name(key: &ReportKey) -> String {
    format!(
        "daily-report-{}-{}.xlsx",
        key.local_date,
        kind_name(key.kind)
    )
}

fn kind_name(kind: ReportKind) -> &'static str {
    match kind {
        ReportKind::Morning => "morning",
        ReportKind::Evening => "evening",
    }
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn map_html(_: HtmlReportError) -> BundleError {
    BundleError::InvalidInput
}

fn map_xlsx(error: XlsxReportError) -> BundleError {
    match error {
        XlsxReportError::InvalidInput => BundleError::InvalidInput,
        XlsxReportError::Generation | XlsxReportError::OutputTooLarge => BundleError::Rendering,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeZone};

    use super::*;
    use crate::reporting::{
        dataset::{AdvertisingReportRow, InventoryReportRow, SalesReportRow, SourceQualityRow},
        kpi::calculate_kpis,
        snapshot::{SnapshotQuality, SnapshotSource},
    };

    fn utc(hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 16, hour, 0, 0).unwrap()
    }

    fn key(kind: ReportKind) -> ReportKey {
        ReportKey {
            local_date: NaiveDate::from_ymd_opt(2026, 8, 16).unwrap(),
            kind,
            recipient_id: "pilot_owner".to_owned(),
            report_version: 2,
        }
    }

    fn dataset() -> ReportDataset {
        ReportDataset {
            kpis: calculate_kpis(&[], &[]).unwrap(),
            sales: vec![SalesReportRow {
                account_id: "store".to_owned(),
                sku: "10".to_owned(),
                ordered_units: 2,
                operational_gmv_minor: 20_000,
                cancelled_units: 0,
                returned_units: 0,
            }],
            advertising: vec![AdvertisingReportRow {
                account_id: "store".to_owned(),
                campaign_id: "20".to_owned(),
                sku: "10".to_owned(),
                impressions: 100,
                clicks: 10,
                spend_minor: 1_000,
                attributed_orders: 1,
                attributed_revenue_minor: 10_000,
            }],
            inventory: vec![InventoryReportRow {
                account_id: "store".to_owned(),
                sku: "10".to_owned(),
                sellable_stock: 3,
                price_minor: Some(12_345),
                observed_at: utc(11),
            }],
            source_quality: [
                SnapshotSource::Sales,
                SnapshotSource::Advertising,
                SnapshotSource::Stocks,
                SnapshotSource::Prices,
            ]
            .into_iter()
            .map(|source| SourceQualityRow {
                account_id: "store".to_owned(),
                source,
                quality: SnapshotQuality::Complete,
                source_as_of: utc(11),
                row_count: 1,
            })
            .collect(),
        }
    }

    fn request<'a>(key: &'a ReportKey, dataset: &'a ReportDataset) -> ReportBundleRequest<'a> {
        ReportBundleRequest {
            key,
            manager_name: "Диана",
            generated_at: utc(13),
            dataset,
            problems: &[],
        }
    }

    #[test]
    fn one_frozen_input_produces_one_deterministic_bundle() {
        let key = key(ReportKind::Evening);
        let dataset = dataset();
        let first = render_bundle(request(&key, &dataset)).unwrap();
        let second = render_bundle(request(&key, &dataset)).unwrap();
        assert_eq!(first, second);
        assert!(first.html.contains("показатели предварительные"));
        assert_eq!(
            first.artifact.object_key,
            "daily-reports/2026/08/16/pilot_owner/v2/evening.xlsx"
        );
        assert_eq!(
            first.attachment_name,
            "daily-report-2026-08-16-evening.xlsx"
        );
        let receipt = inspect_dry_run(&first).unwrap();
        assert_eq!(receipt.artifact, first.artifact);
        assert_eq!(receipt.size_bytes, first.xlsx.len());
        assert!(!receipt.persisted);
    }

    #[test]
    fn morning_bundle_is_final_and_uses_its_own_identity() {
        let key = key(ReportKind::Morning);
        let dataset = dataset();
        let bundle = render_bundle(ReportBundleRequest {
            generated_at: utc(12),
            ..request(&key, &dataset)
        })
        .unwrap();
        assert!(!bundle.html.contains("показатели предварительные"));
        assert!(bundle.artifact.object_key.ends_with("/morning.xlsx"));
    }

    #[test]
    fn invalid_scope_time_and_renderer_input_fail_closed() {
        let key = key(ReportKind::Evening);
        {
            let mut input = dataset();
            input.sales[0].account_id = "foreign".to_owned();
            assert_eq!(
                render_bundle(request(&key, &input)),
                Err(BundleError::InvalidInput)
            );
        }
        {
            let mut input = dataset();
            input.source_quality.clear();
            assert_eq!(
                render_bundle(request(&key, &input)),
                Err(BundleError::InvalidInput)
            );
        }
        let dataset = dataset();
        assert_eq!(
            render_bundle(ReportBundleRequest {
                manager_name: "",
                ..request(&key, &dataset)
            }),
            Err(BundleError::InvalidInput)
        );
        assert_eq!(
            render_bundle(ReportBundleRequest {
                generated_at: utc(11),
                ..request(&key, &dataset)
            }),
            Err(BundleError::InvalidInput)
        );
    }

    #[test]
    fn dry_run_detects_tampering_and_invalid_metadata() {
        let key = key(ReportKind::Evening);
        let dataset = dataset();
        let mut bundle = render_bundle(request(&key, &dataset)).unwrap();
        bundle.xlsx.push(0);
        assert_eq!(inspect_dry_run(&bundle), Err(BundleError::Integrity));
        bundle.artifact.sha256 = sha256(&bundle.xlsx);
        bundle.html.clear();
        assert_eq!(inspect_dry_run(&bundle), Err(BundleError::Integrity));
    }

    #[test]
    fn xlsx_input_errors_are_mapped_without_leaking_details() {
        assert_eq!(
            map_xlsx(XlsxReportError::InvalidInput),
            BundleError::InvalidInput
        );
        assert_eq!(
            map_xlsx(XlsxReportError::Generation),
            BundleError::Rendering
        );
        assert_eq!(
            map_xlsx(XlsxReportError::OutputTooLarge),
            BundleError::Rendering
        );
    }
}
