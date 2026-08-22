use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use rust_xlsxwriter::{
    Color, DocProperties, ExcelDateTime, Format, Workbook, Worksheet, XlsxError,
};

use super::{
    html::{HtmlReport, validate_report},
    kpi::{BasisPoints, KpiSummary},
    rules::{PriorityProblem, ProblemKind, Severity},
    snapshot::{SnapshotQuality, SnapshotSource},
};

const MAX_ROWS: usize = 25_000;
const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
const MAX_EXACT_EXCEL_INTEGER: u64 = 9_007_199_254_740_991;
const MAX_TEXT_BYTES: usize = 256;

#[derive(Debug, Clone, Copy)]
pub struct SalesDetail<'a> {
    pub account_id: &'a str,
    pub sku: &'a str,
    pub ordered_units: u64,
    pub operational_gmv_minor: u64,
    pub cancelled_units: Option<u64>,
    pub returned_units: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub struct AdvertisingDetail<'a> {
    pub account_id: &'a str,
    pub campaign_id: &'a str,
    pub sku: &'a str,
    pub impressions: u64,
    pub clicks: u64,
    pub spend_minor: u64,
    pub attributed_orders: u64,
    pub attributed_revenue_minor: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct InventoryDetail<'a> {
    pub account_id: &'a str,
    pub sku: &'a str,
    pub sellable_stock: u64,
    pub price_minor: Option<u64>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
pub struct SourceQualityDetail<'a> {
    pub account_id: &'a str,
    pub source: SnapshotSource,
    pub quality: SnapshotQuality,
    pub source_as_of: DateTime<Utc>,
    pub row_count: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct XlsxReport<'a> {
    pub summary: HtmlReport<'a>,
    pub sales: &'a [SalesDetail<'a>],
    pub advertising: &'a [AdvertisingDetail<'a>],
    pub inventory: &'a [InventoryDetail<'a>],
    pub source_quality: &'a [SourceQualityDetail<'a>],
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum XlsxReportError {
    #[error("daily XLSX report input is invalid")]
    InvalidInput,
    #[error("daily XLSX report generation failed")]
    Generation,
    #[error("daily XLSX report exceeds the output limit")]
    OutputTooLarge,
}

pub fn render_xlsx(report: XlsxReport<'_>) -> Result<Vec<u8>, XlsxReportError> {
    validate(&report)?;
    let mut workbook = Workbook::new();
    // XLSX embeds document creation time by default. Keep technical metadata
    // fixed so the same frozen report input has one stable artifact hash; the
    // factual generation time remains visible in the Summary sheet and email.
    let created = ExcelDateTime::from_ymd(2000, 1, 1).map_err(|_| XlsxReportError::Generation)?;
    let properties = DocProperties::new()
        .set_title("Daily marketplace report")
        .set_creation_datetime(&created);
    workbook.set_properties(&properties);
    let formats = Formats::new();
    write_summary(&mut workbook, &formats, &report)?;
    write_sales(&mut workbook, &formats, report.sales, 0)?;
    write_advertising(&mut workbook, &formats, report.advertising, 0)?;
    write_inventory(&mut workbook, &formats, report.inventory, 0)?;
    write_recommendations(&mut workbook, &formats, report.summary.problems, 0)?;
    write_quality(&mut workbook, &formats, report.source_quality, 0)?;
    let output = workbook
        .save_to_buffer()
        .map_err(|_| XlsxReportError::Generation)?;
    validate_output_size(output.len())?;
    Ok(output)
}

struct Formats {
    title: Format,
    header: Format,
    money: Format,
    percent: Format,
    red: Format,
    yellow: Format,
}

impl Formats {
    fn new() -> Self {
        Self {
            title: Format::new()
                .set_bold()
                .set_font_size(16)
                .set_font_color(Color::RGB(0x17_3B70)),
            header: Format::new()
                .set_bold()
                .set_font_color(Color::White)
                .set_background_color(Color::RGB(0x25_63EB)),
            money: Format::new().set_num_format("#,##0.00 [$₽-419]"),
            percent: Format::new().set_num_format("0.00%"),
            red: Format::new()
                .set_bold()
                .set_font_color(Color::RGB(0xB4_2318)),
            yellow: Format::new()
                .set_bold()
                .set_font_color(Color::RGB(0xB5_4708)),
        }
    }
}

fn validate(report: &XlsxReport<'_>) -> Result<(), XlsxReportError> {
    validate_report(&report.summary).map_err(|_| XlsxReportError::InvalidInput)?;
    let total_rows = report
        .sales
        .len()
        .checked_add(report.advertising.len())
        .and_then(|value| value.checked_add(report.inventory.len()))
        .and_then(|value| value.checked_add(report.source_quality.len()))
        .ok_or(XlsxReportError::InvalidInput)?;
    if total_rows > MAX_ROWS || report.summary.problems.len() > 5 {
        return Err(XlsxReportError::InvalidInput);
    }
    let mut seen_quality = BTreeSet::new();
    for row in report.sales {
        validate_id(row.account_id)?;
        validate_text(row.sku)?;
        validate_numbers(&[row.ordered_units, row.operational_gmv_minor])?;
        validate_optional_numbers(&[row.cancelled_units, row.returned_units])?;
    }
    for row in report.advertising {
        validate_id(row.account_id)?;
        validate_text(row.campaign_id)?;
        validate_text(row.sku)?;
        // Attributed orders are ordered units; one click may produce more than
        // one unit. Only clicks above impressions is an impossible relation.
        if row.clicks > row.impressions {
            return Err(XlsxReportError::InvalidInput);
        }
        validate_numbers(&[
            row.impressions,
            row.clicks,
            row.spend_minor,
            row.attributed_orders,
            row.attributed_revenue_minor,
        ])?;
    }
    for row in report.inventory {
        validate_id(row.account_id)?;
        validate_text(row.sku)?;
        if row.observed_at > report.summary.generated_at {
            return Err(XlsxReportError::InvalidInput);
        }
        validate_numbers(&[row.sellable_stock, row.price_minor.unwrap_or(0)])?;
    }
    for row in report.source_quality {
        validate_id(row.account_id)?;
        if row.source_as_of > report.summary.generated_at
            || !seen_quality.insert((row.account_id, row.source))
        {
            return Err(XlsxReportError::InvalidInput);
        }
    }
    Ok(())
}

fn validate_output_size(size: usize) -> Result<(), XlsxReportError> {
    if size > MAX_OUTPUT_BYTES {
        Err(XlsxReportError::OutputTooLarge)
    } else {
        Ok(())
    }
}

fn validate_id(value: &str) -> Result<(), XlsxReportError> {
    if value.is_empty()
        || value.len() > MAX_TEXT_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        Err(XlsxReportError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_text(value: &str) -> Result<(), XlsxReportError> {
    if value.trim().is_empty()
        || value.len() > MAX_TEXT_BYTES
        || value.chars().any(char::is_control)
    {
        Err(XlsxReportError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_numbers(values: &[u64]) -> Result<(), XlsxReportError> {
    if values.iter().any(|value| *value > MAX_EXACT_EXCEL_INTEGER) {
        Err(XlsxReportError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_optional_numbers(values: &[Option<u64>]) -> Result<(), XlsxReportError> {
    if values
        .iter()
        .flatten()
        .any(|value| *value > MAX_EXACT_EXCEL_INTEGER)
    {
        Err(XlsxReportError::InvalidInput)
    } else {
        Ok(())
    }
}

fn worksheet<'a>(
    workbook: &'a mut Workbook,
    name: &str,
) -> Result<&'a mut Worksheet, XlsxReportError> {
    workbook.add_worksheet().set_name(name).map_err(map_xlsx)
}

fn write_summary(
    workbook: &mut Workbook,
    formats: &Formats,
    report: &XlsxReport<'_>,
) -> Result<(), XlsxReportError> {
    let sheet = worksheet(workbook, "Сводка")?;
    sheet.set_column_width(0, 34).map_err(map_xlsx)?;
    sheet.set_column_width(1, 24).map_err(map_xlsx)?;
    sheet
        .write_string_with_format(0, 0, "Ежедневный отчёт менеджера", &formats.title)
        .map_err(map_xlsx)?;
    let metadata = [
        ("Менеджер", report.summary.manager_name.to_owned()),
        (
            "Период Asia/Yekaterinburg (UTC+5)",
            format!(
                "{} — {}",
                super::business_timestamp(report.summary.interval_start),
                super::business_timestamp(report.summary.interval_end)
            ),
        ),
        (
            "Сформирован Asia/Yekaterinburg (UTC+5)",
            super::business_timestamp(report.summary.generated_at),
        ),
        (
            "Качество данных",
            quality_text(report.summary.quality).to_owned(),
        ),
        (
            "Статус периода",
            if report.summary.preliminary {
                "предварительный"
            } else {
                "закрытый"
            }
            .to_owned(),
        ),
    ];
    for (index, (label, value)) in metadata.into_iter().enumerate() {
        let row = u32::try_from(index + 2).map_err(|_| XlsxReportError::InvalidInput)?;
        sheet
            .write_string_with_format(row, 0, label, &formats.header)
            .map_err(map_xlsx)?;
        sheet
            .write_string(row, 1, safe_text(&value))
            .map_err(map_xlsx)?;
    }
    write_kpi_table(sheet, formats, report.summary.kpis, 8)
}

fn write_kpi_table(
    sheet: &mut Worksheet,
    formats: &Formats,
    kpis: &KpiSummary,
    start_row: u32,
) -> Result<(), XlsxReportError> {
    write_headers(sheet, formats, start_row, &["Показатель", "Значение"])?;
    let integer_rows = [
        ("Заказано единиц", Some(kpis.ordered_units)),
        ("Отменено единиц", kpis.cancelled_units),
        ("Возвращено единиц", kpis.returned_units),
        ("Показы рекламы", Some(kpis.ad_impressions)),
        ("Клики рекламы", Some(kpis.ad_clicks)),
    ];
    for (index, (label, value)) in integer_rows.into_iter().enumerate() {
        let row =
            start_row + u32::try_from(index + 1).map_err(|_| XlsxReportError::InvalidInput)?;
        sheet.write_string(row, 0, label).map_err(map_xlsx)?;
        write_optional_u64(sheet, row, 1, value)?;
    }
    let mut row = start_row + 6;
    for (label, value) in [
        ("Операционный GMV", Some(kpis.operational_gmv_minor)),
        ("Расходы на рекламу", Some(kpis.ad_spend_minor)),
        ("CPC", kpis.cpc_minor),
        ("CPO", kpis.cpo_minor),
    ] {
        sheet.write_string(row, 0, label).map_err(map_xlsx)?;
        write_optional_minor(sheet, row, 1, value, formats)?;
        row += 1;
    }
    for (label, value) in [
        ("CTR", kpis.ctr),
        ("Рекламная конверсия", kpis.ad_conversion),
        ("ДРР", kpis.drr),
    ] {
        sheet.write_string(row, 0, label).map_err(map_xlsx)?;
        write_optional_rate(sheet, row, 1, value, formats)?;
        row += 1;
    }
    Ok(())
}

fn write_sales(
    workbook: &mut Workbook,
    formats: &Formats,
    rows: &[SalesDetail<'_>],
    header_row: u32,
) -> Result<(), XlsxReportError> {
    let sheet = worksheet(workbook, "Продажи по SKU")?;
    write_headers(
        sheet,
        formats,
        header_row,
        &["Кабинет", "SKU", "Единицы", "GMV", "Отмены", "Возвраты"],
    )?;
    for (index, item) in rows.iter().enumerate() {
        let row = header_row
            .checked_add(u32::try_from(index + 1).map_err(|_| XlsxReportError::InvalidInput)?)
            .ok_or(XlsxReportError::InvalidInput)?;
        write_text(sheet, row, 0, item.account_id)?;
        write_text(sheet, row, 1, item.sku)?;
        write_u64(sheet, row, 2, item.ordered_units)?;
        write_minor(sheet, row, 3, item.operational_gmv_minor, formats)?;
        write_optional_u64(sheet, row, 4, item.cancelled_units)?;
        write_optional_u64(sheet, row, 5, item.returned_units)?;
    }
    finish_table(sheet, &[20, 20, 14, 16, 14, 14])
}

fn write_advertising(
    workbook: &mut Workbook,
    formats: &Formats,
    rows: &[AdvertisingDetail<'_>],
    header_row: u32,
) -> Result<(), XlsxReportError> {
    let sheet = worksheet(workbook, "Реклама")?;
    write_headers(
        sheet,
        formats,
        header_row,
        &[
            "Кабинет",
            "Кампания",
            "SKU",
            "Показы",
            "Клики",
            "Расход",
            "Заказы",
            "Атрибутированная выручка",
        ],
    )?;
    for (index, item) in rows.iter().enumerate() {
        let row = header_row
            .checked_add(u32::try_from(index + 1).map_err(|_| XlsxReportError::InvalidInput)?)
            .ok_or(XlsxReportError::InvalidInput)?;
        write_text(sheet, row, 0, item.account_id)?;
        write_text(sheet, row, 1, item.campaign_id)?;
        write_text(sheet, row, 2, item.sku)?;
        write_u64(sheet, row, 3, item.impressions)?;
        write_u64(sheet, row, 4, item.clicks)?;
        write_minor(sheet, row, 5, item.spend_minor, formats)?;
        write_u64(sheet, row, 6, item.attributed_orders)?;
        write_minor(sheet, row, 7, item.attributed_revenue_minor, formats)?;
    }
    finish_table(sheet, &[20, 20, 20, 14, 14, 16, 14, 24])
}

fn write_inventory(
    workbook: &mut Workbook,
    formats: &Formats,
    rows: &[InventoryDetail<'_>],
    header_row: u32,
) -> Result<(), XlsxReportError> {
    let sheet = worksheet(workbook, "Остатки и цены")?;
    write_headers(
        sheet,
        formats,
        header_row,
        &[
            "Кабинет",
            "SKU",
            "Доступный остаток",
            "Цена",
            "Срез Asia/Yekaterinburg (UTC+5)",
        ],
    )?;
    for (index, item) in rows.iter().enumerate() {
        let row = header_row
            .checked_add(u32::try_from(index + 1).map_err(|_| XlsxReportError::InvalidInput)?)
            .ok_or(XlsxReportError::InvalidInput)?;
        write_text(sheet, row, 0, item.account_id)?;
        write_text(sheet, row, 1, item.sku)?;
        write_u64(sheet, row, 2, item.sellable_stock)?;
        write_optional_minor(sheet, row, 3, item.price_minor, formats)?;
        sheet
            .write_string(row, 4, super::business_timestamp(item.observed_at))
            .map_err(map_xlsx)?;
    }
    finish_table(sheet, &[20, 20, 20, 16, 20])
}

fn write_recommendations(
    workbook: &mut Workbook,
    formats: &Formats,
    rows: &[PriorityProblem],
    header_row: u32,
) -> Result<(), XlsxReportError> {
    let sheet = worksheet(workbook, "Рекомендации")?;
    write_headers(
        sheet,
        formats,
        header_row,
        &[
            "Приоритет",
            "Кабинет",
            "SKU",
            "Проблема",
            "Действие",
            "Влияние",
        ],
    )?;
    for (index, item) in rows.iter().enumerate() {
        let row = header_row
            .checked_add(u32::try_from(index + 1).map_err(|_| XlsxReportError::InvalidInput)?)
            .ok_or(XlsxReportError::InvalidInput)?;
        let (severity, format) = match item.severity {
            Severity::Red => ("Критично", &formats.red),
            Severity::Yellow => ("Внимание", &formats.yellow),
        };
        let (problem, action) = problem_text(item.kind);
        sheet
            .write_string_with_format(row, 0, severity, format)
            .map_err(map_xlsx)?;
        write_text(sheet, row, 1, &item.account_id)?;
        write_u64(sheet, row, 2, item.sku)?;
        sheet.write_string(row, 3, problem).map_err(map_xlsx)?;
        sheet.write_string(row, 4, action).map_err(map_xlsx)?;
        write_minor(sheet, row, 5, item.impact_minor, formats)?;
    }
    finish_table(sheet, &[14, 20, 20, 42, 44, 16])
}

fn write_quality(
    workbook: &mut Workbook,
    formats: &Formats,
    rows: &[SourceQualityDetail<'_>],
    header_row: u32,
) -> Result<(), XlsxReportError> {
    let sheet = worksheet(workbook, "Качество данных")?;
    write_headers(
        sheet,
        formats,
        header_row,
        &[
            "Кабинет",
            "Источник",
            "Качество",
            "Срез Asia/Yekaterinburg (UTC+5)",
            "Строк",
        ],
    )?;
    for (index, item) in rows.iter().enumerate() {
        let row = header_row
            .checked_add(u32::try_from(index + 1).map_err(|_| XlsxReportError::InvalidInput)?)
            .ok_or(XlsxReportError::InvalidInput)?;
        write_text(sheet, row, 0, item.account_id)?;
        sheet
            .write_string(row, 1, source_text(item.source))
            .map_err(map_xlsx)?;
        sheet
            .write_string(row, 2, quality_text(item.quality))
            .map_err(map_xlsx)?;
        sheet
            .write_string(row, 3, super::business_timestamp(item.source_as_of))
            .map_err(map_xlsx)?;
        write_u64(sheet, row, 4, u64::from(item.row_count))?;
    }
    finish_table(sheet, &[20, 18, 24, 20, 14])
}

#[inline(never)]
fn write_headers(
    sheet: &mut Worksheet,
    formats: &Formats,
    row: u32,
    headers: &[&str],
) -> Result<(), XlsxReportError> {
    for (column, header) in headers.iter().enumerate() {
        sheet
            .write_string_with_format(
                row,
                u16::try_from(column).map_err(|_| XlsxReportError::InvalidInput)?,
                *header,
                &formats.header,
            )
            .map_err(map_xlsx)?;
    }
    Ok(())
}

fn finish_table(sheet: &mut Worksheet, widths: &[u16]) -> Result<(), XlsxReportError> {
    sheet.set_freeze_panes(1, 0).map_err(map_xlsx)?;
    for (column, width) in widths.iter().enumerate() {
        sheet
            .set_column_width(
                u16::try_from(column).map_err(|_| XlsxReportError::InvalidInput)?,
                *width,
            )
            .map_err(map_xlsx)?;
    }
    Ok(())
}

fn write_text(
    sheet: &mut Worksheet,
    row: u32,
    column: u16,
    value: &str,
) -> Result<(), XlsxReportError> {
    sheet
        .write_string(row, column, safe_text(value))
        .map_err(map_xlsx)?;
    Ok(())
}

fn safe_text(value: &str) -> String {
    if value.starts_with(['=', '+', '-', '@']) {
        format!("'{value}")
    } else {
        value.to_owned()
    }
}

fn write_u64(
    sheet: &mut Worksheet,
    row: u32,
    column: u16,
    value: u64,
) -> Result<(), XlsxReportError> {
    sheet
        .write_number(row, column, excel_number(value))
        .map_err(map_xlsx)?;
    Ok(())
}

fn write_optional_u64(
    sheet: &mut Worksheet,
    row: u32,
    column: u16,
    value: Option<u64>,
) -> Result<(), XlsxReportError> {
    if let Some(value) = value {
        write_u64(sheet, row, column, value)
    } else {
        sheet.write_string(row, column, "N/D").map_err(map_xlsx)?;
        Ok(())
    }
}

fn write_minor(
    sheet: &mut Worksheet,
    row: u32,
    column: u16,
    value: u64,
    formats: &Formats,
) -> Result<(), XlsxReportError> {
    sheet
        .write_number_with_format(row, column, excel_number(value) / 100.0, &formats.money)
        .map_err(map_xlsx)?;
    Ok(())
}

fn write_optional_minor(
    sheet: &mut Worksheet,
    row: u32,
    column: u16,
    value: Option<u64>,
    formats: &Formats,
) -> Result<(), XlsxReportError> {
    if let Some(value) = value {
        write_minor(sheet, row, column, value, formats)
    } else {
        sheet.write_string(row, column, "N/D").map_err(map_xlsx)?;
        Ok(())
    }
}

fn write_optional_rate(
    sheet: &mut Worksheet,
    row: u32,
    column: u16,
    value: Option<BasisPoints>,
    formats: &Formats,
) -> Result<(), XlsxReportError> {
    if let Some(BasisPoints(points)) = value {
        sheet
            .write_number_with_format(
                row,
                column,
                excel_number(points) / 10_000.0,
                &formats.percent,
            )
            .map_err(map_xlsx)?;
        Ok(())
    } else {
        sheet.write_string(row, column, "N/D").map_err(map_xlsx)?;
        Ok(())
    }
}

// Every caller is downstream of validate(), which rejects integers above the
// exact IEEE-754 range accepted by Excel. Keep the unavoidable workbook API
// conversion in one audited location.
#[allow(clippy::cast_precision_loss)]
fn excel_number(value: u64) -> f64 {
    debug_assert!(value <= MAX_EXACT_EXCEL_INTEGER);
    value as f64
}

fn source_text(source: SnapshotSource) -> &'static str {
    match source {
        SnapshotSource::Sales => "Продажи",
        SnapshotSource::Advertising => "Реклама",
        SnapshotSource::Finance => "Финансы",
        SnapshotSource::Stocks => "Остатки",
        SnapshotSource::Prices => "Цены",
    }
}

fn quality_text(quality: SnapshotQuality) -> &'static str {
    match quality {
        SnapshotQuality::Complete => "Полные и свежие",
        SnapshotQuality::Partial => "Частичные",
        SnapshotQuality::Stale => "Устаревшие",
        SnapshotQuality::Critical => "Критические",
    }
}

fn problem_text(kind: ProblemKind) -> (&'static str, &'static str) {
    match kind {
        ProblemKind::AdvertisedWithoutStock => (
            "Реклама при нулевом остатке",
            "Проверить остаток и кампанию",
        ),
        ProblemKind::Stockout => ("Товар закончился", "Запланировать пополнение"),
        ProblemKind::LowStockCover => ("Низкий запас", "Уточнить поставку"),
        ProblemKind::SpendWithoutOrders => (
            "Расход без атрибутированных заказов",
            "Проверить запросы, карточку и ставку",
        ),
        ProblemKind::HighDrr => ("Высокий ДРР", "Проверить кампанию до изменения ставки"),
    }
}

fn map_xlsx(_: XlsxError) -> XlsxReportError {
    XlsxReportError::Generation
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone};

    use super::*;

    fn generated_at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 16, 12, 1, 0).unwrap()
    }

    fn summary<'a>(kpis: &'a KpiSummary, problems: &'a [PriorityProblem]) -> HtmlReport<'a> {
        HtmlReport {
            manager_name: "Диана",
            account_ids: &["ozon_store"],
            generated_at: generated_at(),
            interval_start: generated_at() - Duration::days(1),
            interval_end: generated_at() - Duration::minutes(1),
            preliminary: false,
            quality: SnapshotQuality::Complete,
            kpis,
            problems,
        }
    }

    fn kpis() -> KpiSummary {
        KpiSummary {
            ordered_units: 10,
            realized_units: Some(7),
            operational_gmv_minor: 100_000,
            cancelled_units: Some(1),
            returned_units: Some(2),
            ad_impressions: 1_000,
            ad_clicks: 20,
            ad_spend_minor: 10_000,
            attributed_orders: 2,
            attributed_revenue_minor: 50_000,
            ctr: Some(BasisPoints(200)),
            cpc_minor: Some(500),
            ad_conversion: Some(BasisPoints(1_000)),
            cpo_minor: Some(5_000),
            drr: Some(BasisPoints(2_000)),
            buyout_rate: Some(BasisPoints(7_000)),
        }
    }

    fn problem(sku: u64, kind: ProblemKind, severity: Severity) -> PriorityProblem {
        PriorityProblem {
            account_id: "ozon_store".to_owned(),
            sku,
            kind,
            severity,
            observed: 1,
            threshold: 2,
            impact_minor: 10_000,
        }
    }

    #[test]
    fn workbook_contains_six_bounded_sheets_and_all_supported_rows() {
        let kpis = kpis();
        let problems = [
            problem(1, ProblemKind::AdvertisedWithoutStock, Severity::Red),
            problem(2, ProblemKind::Stockout, Severity::Yellow),
            problem(3, ProblemKind::LowStockCover, Severity::Red),
            problem(4, ProblemKind::SpendWithoutOrders, Severity::Yellow),
            problem(5, ProblemKind::HighDrr, Severity::Red),
        ];
        let sales = [SalesDetail {
            account_id: "ozon_store",
            sku: "=external",
            ordered_units: 10,
            operational_gmv_minor: 100_000,
            cancelled_units: Some(1),
            returned_units: Some(2),
        }];
        let advertising = [AdvertisingDetail {
            account_id: "ozon_store",
            campaign_id: "+campaign",
            sku: "@sku",
            impressions: 1_000,
            clicks: 20,
            spend_minor: 10_000,
            attributed_orders: 2,
            attributed_revenue_minor: 50_000,
        }];
        let inventory = [InventoryDetail {
            account_id: "ozon_store",
            sku: "-sku",
            sellable_stock: 7,
            price_minor: Some(12_345),
            observed_at: generated_at() - Duration::minutes(5),
        }];
        let quality = [
            SourceQualityDetail {
                account_id: "ozon_store",
                source: SnapshotSource::Sales,
                quality: SnapshotQuality::Complete,
                source_as_of: generated_at() - Duration::minutes(10),
                row_count: 1,
            },
            SourceQualityDetail {
                account_id: "ozon_store",
                source: SnapshotSource::Advertising,
                quality: SnapshotQuality::Partial,
                source_as_of: generated_at() - Duration::minutes(20),
                row_count: 2,
            },
            SourceQualityDetail {
                account_id: "ozon_store",
                source: SnapshotSource::Stocks,
                quality: SnapshotQuality::Stale,
                source_as_of: generated_at() - Duration::minutes(30),
                row_count: 3,
            },
            SourceQualityDetail {
                account_id: "ozon_store",
                source: SnapshotSource::Prices,
                quality: SnapshotQuality::Critical,
                source_as_of: generated_at() - Duration::minutes(40),
                row_count: 4,
            },
        ];
        let mut report_summary = summary(&kpis, &problems);
        report_summary.preliminary = true;
        let bytes = render_xlsx(XlsxReport {
            summary: report_summary,
            sales: &sales,
            advertising: &advertising,
            inventory: &inventory,
            source_quality: &quality,
        })
        .unwrap();
        assert!(bytes.starts_with(b"PK"));
        assert!(bytes.len() < MAX_OUTPUT_BYTES);
        assert_eq!(safe_text("=SUM(A1:A2)"), "'=SUM(A1:A2)");
        assert_eq!(safe_text("normal"), "normal");
    }

    #[test]
    fn missing_optional_metrics_are_written_as_nd() {
        let mut no_rates = kpis();
        no_rates.ctr = None;
        no_rates.cpc_minor = None;
        no_rates.ad_conversion = None;
        no_rates.cpo_minor = None;
        no_rates.drr = None;
        let inventory = [InventoryDetail {
            account_id: "ozon_store",
            sku: "sku",
            sellable_stock: 0,
            price_minor: None,
            observed_at: generated_at(),
        }];
        let sales = [SalesDetail {
            account_id: "ozon_store",
            sku: "sku",
            ordered_units: 1,
            operational_gmv_minor: 100,
            cancelled_units: None,
            returned_units: None,
        }];
        let bytes = render_xlsx(XlsxReport {
            summary: summary(&no_rates, &[]),
            sales: &sales,
            advertising: &[],
            inventory: &inventory,
            source_quality: &[],
        })
        .unwrap();
        assert!(bytes.starts_with(b"PK"));
    }

    #[test]
    fn malformed_rows_limits_and_internal_errors_fail_closed() {
        let kpis = kpis();
        let base_sales = SalesDetail {
            account_id: "ozon_store",
            sku: "sku",
            ordered_units: 1,
            operational_gmv_minor: 1,
            cancelled_units: Some(0),
            returned_units: Some(0),
        };
        let excessive = vec![base_sales; MAX_ROWS + 1];
        assert_eq!(
            render_xlsx(XlsxReport {
                summary: summary(&kpis, &[]),
                sales: &excessive,
                advertising: &[],
                inventory: &[],
                source_quality: &[],
            }),
            Err(XlsxReportError::InvalidInput)
        );
        let invalid_optional = [SalesDetail {
            cancelled_units: Some(MAX_EXACT_EXCEL_INTEGER + 1),
            ..base_sales
        }];
        assert_eq!(
            render_xlsx(XlsxReport {
                summary: summary(&kpis, &[]),
                sales: &invalid_optional,
                advertising: &[],
                inventory: &[],
                source_quality: &[],
            }),
            Err(XlsxReportError::InvalidInput)
        );

        for invalid in [
            SalesDetail {
                account_id: "bad/account",
                ..base_sales
            },
            SalesDetail {
                sku: " ",
                ..base_sales
            },
            SalesDetail {
                ordered_units: MAX_EXACT_EXCEL_INTEGER + 1,
                ..base_sales
            },
        ] {
            assert_eq!(
                render_xlsx(XlsxReport {
                    summary: summary(&kpis, &[]),
                    sales: &[invalid],
                    advertising: &[],
                    inventory: &[],
                    source_quality: &[],
                }),
                Err(XlsxReportError::InvalidInput)
            );
        }

        for invalid in [
            AdvertisingDetail {
                account_id: "ozon_store",
                campaign_id: " ",
                sku: "sku",
                impressions: 1,
                clicks: 0,
                spend_minor: 0,
                attributed_orders: 0,
                attributed_revenue_minor: 0,
            },
            AdvertisingDetail {
                account_id: "ozon_store",
                campaign_id: "campaign",
                sku: "sku",
                impressions: 1,
                clicks: 2,
                spend_minor: 0,
                attributed_orders: 0,
                attributed_revenue_minor: 0,
            },
        ] {
            assert_eq!(
                render_xlsx(XlsxReport {
                    summary: summary(&kpis, &[]),
                    sales: &[],
                    advertising: &[invalid],
                    inventory: &[],
                    source_quality: &[],
                }),
                Err(XlsxReportError::InvalidInput)
            );
        }

        let multiple_units = [AdvertisingDetail {
            account_id: "ozon_store",
            campaign_id: "campaign",
            sku: "sku",
            impressions: 1,
            clicks: 1,
            spend_minor: 100,
            attributed_orders: 2,
            attributed_revenue_minor: 1_000,
        }];
        assert!(
            render_xlsx(XlsxReport {
                summary: summary(&kpis, &[]),
                sales: &[],
                advertising: &multiple_units,
                inventory: &[],
                source_quality: &[],
            })
            .is_ok()
        );

        let future_inventory = InventoryDetail {
            account_id: "ozon_store",
            sku: "sku",
            sellable_stock: 0,
            price_minor: None,
            observed_at: generated_at() + Duration::seconds(1),
        };
        assert_eq!(
            render_xlsx(XlsxReport {
                summary: summary(&kpis, &[]),
                sales: &[],
                advertising: &[],
                inventory: &[future_inventory],
                source_quality: &[],
            }),
            Err(XlsxReportError::InvalidInput)
        );

        let quality = SourceQualityDetail {
            account_id: "ozon_store",
            source: SnapshotSource::Sales,
            quality: SnapshotQuality::Complete,
            source_as_of: generated_at(),
            row_count: 0,
        };
        for invalid_quality in [
            vec![quality, quality],
            vec![SourceQualityDetail {
                source_as_of: generated_at() + Duration::seconds(1),
                ..quality
            }],
        ] {
            assert_eq!(
                render_xlsx(XlsxReport {
                    summary: summary(&kpis, &[]),
                    sales: &[],
                    advertising: &[],
                    inventory: &[],
                    source_quality: &invalid_quality,
                }),
                Err(XlsxReportError::InvalidInput)
            );
        }

        assert_eq!(
            validate_output_size(MAX_OUTPUT_BYTES + 1),
            Err(XlsxReportError::OutputTooLarge)
        );
        assert_eq!(validate_output_size(MAX_OUTPUT_BYTES), Ok(()));
        assert_eq!(
            map_xlsx(XlsxError::RowColumnLimitError),
            XlsxReportError::Generation
        );

        let formats = Formats::new();
        let mut workbook = Workbook::new();
        assert_eq!(
            write_sales(&mut workbook, &formats, &[], u32::MAX),
            Err(XlsxReportError::Generation)
        );
        let mut workbook = Workbook::new();
        assert_eq!(
            write_advertising(&mut workbook, &formats, &[], u32::MAX),
            Err(XlsxReportError::Generation)
        );
        let mut workbook = Workbook::new();
        assert_eq!(
            write_inventory(&mut workbook, &formats, &[], u32::MAX),
            Err(XlsxReportError::Generation)
        );
        let mut workbook = Workbook::new();
        assert_eq!(
            write_recommendations(&mut workbook, &formats, &[], u32::MAX),
            Err(XlsxReportError::Generation)
        );
        let mut workbook = Workbook::new();
        assert_eq!(
            write_quality(&mut workbook, &formats, &[], u32::MAX),
            Err(XlsxReportError::Generation)
        );
    }
}
