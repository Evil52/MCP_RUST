use std::{collections::BTreeSet, fmt::Write};

use chrono::{DateTime, Utc};

use super::{
    BUSINESS_TIMEZONE, business_timestamp,
    kpi::{BasisPoints, KpiSummary},
    rules::{PriorityProblem, ProblemKind, Severity},
    snapshot::SnapshotQuality,
};

const MAX_MANAGER_NAME_BYTES: usize = 256;
const MAX_ACCOUNTS: usize = 64;
const MAX_ACCOUNT_BYTES: usize = 128;
const MAX_PROBLEMS: usize = 5;

#[derive(Debug, Clone, Copy)]
pub struct HtmlReport<'a> {
    pub manager_name: &'a str,
    pub account_ids: &'a [&'a str],
    pub generated_at: DateTime<Utc>,
    pub interval_start: DateTime<Utc>,
    pub interval_end: DateTime<Utc>,
    pub preliminary: bool,
    pub quality: SnapshotQuality,
    pub kpis: &'a KpiSummary,
    pub problems: &'a [PriorityProblem],
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum HtmlReportError {
    #[error("daily HTML report input is invalid")]
    InvalidInput,
}

pub fn render_html(report: HtmlReport<'_>) -> Result<String, HtmlReportError> {
    validate_report(&report)?;
    let mut html = String::with_capacity(8_192);
    write!(
        html,
        "<!doctype html><html lang=\"ru\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width\"><title>Ежедневный отчёт</title><style>body{{font-family:Arial,sans-serif;color:#172033;max-width:920px;margin:auto;padding:24px}}h1{{font-size:24px}}.meta{{color:#667085}}.banner{{padding:12px;border-radius:8px;background:#eef4ff}}.warning{{background:#fff4e5}}table{{border-collapse:collapse;width:100%;margin:18px 0}}th,td{{border-bottom:1px solid #e4e7ec;text-align:left;padding:9px}}th{{background:#f8fafc}}.red{{color:#b42318;font-weight:700}}.yellow{{color:#b54708;font-weight:700}}small{{color:#667085}}</style></head><body><h1>Ежедневный отчёт — {}</h1>",
        escape(report.manager_name)
    )
    .expect("writing to String cannot fail");
    write!(
        html,
        "<p class=\"meta\">Период: {} — {}<br>Сформирован: {}<br>Часовой пояс: {} (UTC+5)<br>Кабинеты: {}</p>",
        business_timestamp(report.interval_start),
        business_timestamp(report.interval_end),
        business_timestamp(report.generated_at),
        BUSINESS_TIMEZONE,
        report
            .account_ids
            .iter()
            .map(|value| escape(value))
            .collect::<Vec<_>>()
            .join(", ")
    )
    .expect("writing to String cannot fail");
    let (quality_text, warning) = quality_label(report.quality);
    write!(
        html,
        "<div class=\"banner{}\"><strong>Качество данных:</strong> {}{}</div>",
        if warning { " warning" } else { "" },
        quality_text,
        if report.preliminary {
            " · показатели предварительные"
        } else {
            ""
        }
    )
    .expect("writing to String cannot fail");
    write_kpis(&mut html, report.kpis);
    write_problems(&mut html, report.problems, warning);
    html.push_str("<p><small>Расчёты выполнены сервером по зафиксированному срезу. Операционная выручка не является итоговой выплатой маркетплейса.</small></p></body></html>");
    Ok(html)
}

pub(super) fn validate_report(report: &HtmlReport<'_>) -> Result<(), HtmlReportError> {
    if !valid_text(report.manager_name, MAX_MANAGER_NAME_BYTES)
        || report.account_ids.is_empty()
        || report.account_ids.len() > MAX_ACCOUNTS
        || report.interval_start >= report.interval_end
        || report.generated_at < report.interval_end
        || report.problems.len() > MAX_PROBLEMS
    {
        return Err(HtmlReportError::InvalidInput);
    }
    let mut accounts = BTreeSet::new();
    for account in report.account_ids {
        if !valid_identifier(account) || !accounts.insert(*account) {
            return Err(HtmlReportError::InvalidInput);
        }
    }
    if report
        .problems
        .iter()
        .any(|problem| !accounts.contains(problem.account_id.as_str()))
    {
        return Err(HtmlReportError::InvalidInput);
    }
    Ok(())
}

fn valid_text(value: &str, max_bytes: usize) -> bool {
    !value.trim().is_empty()
        && value.len() <= max_bytes
        && value.chars().all(|character| !character.is_control())
}

fn valid_identifier(value: &str) -> bool {
    valid_text(value, MAX_ACCOUNT_BYTES)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            other => escaped.push(other),
        }
    }
    escaped
}

const fn quality_label(quality: SnapshotQuality) -> (&'static str, bool) {
    match quality {
        SnapshotQuality::Complete => ("полные и свежие", false),
        SnapshotQuality::Partial => ("частичные", true),
        SnapshotQuality::Stale => ("устаревшие", true),
        SnapshotQuality::Critical => ("критически неполные или устаревшие", true),
    }
}

fn write_kpis(html: &mut String, kpis: &KpiSummary) {
    write!(
        html,
        "<h2>Ключевые показатели</h2><table><tr><th>Показатель</th><th>Значение</th></tr><tr><td>Заказано единиц</td><td>{}</td></tr><tr><td>Операционный GMV</td><td>{}</td></tr><tr><td>Расходы на рекламу</td><td>{}</td></tr><tr><td>ДРР по атрибутированной выручке</td><td>{}</td></tr><tr><td>CTR</td><td>{}</td></tr><tr><td>CPC</td><td>{}</td></tr><tr><td>Рекламная конверсия</td><td>{}</td></tr><tr><td>CPO</td><td>{}</td></tr><tr><td>Отменено / возвращено единиц</td><td>{} / {}</td></tr></table>",
        kpis.ordered_units,
        money(kpis.operational_gmv_minor),
        money(kpis.ad_spend_minor),
        rate(kpis.drr),
        rate(kpis.ctr),
        optional_money(kpis.cpc_minor),
        rate(kpis.ad_conversion),
        optional_money(kpis.cpo_minor),
        optional_quantity(kpis.cancelled_units),
        optional_quantity(kpis.returned_units),
    )
    .expect("writing to String cannot fail");
}

fn optional_quantity(value: Option<u64>) -> String {
    value.map_or_else(|| "N/D".to_owned(), |value| value.to_string())
}

fn write_problems(html: &mut String, problems: &[PriorityProblem], suppressed: bool) {
    html.push_str("<h2>Что сделать сегодня</h2>");
    if suppressed {
        html.push_str(
            "<p>Рекомендации отключены: сначала восстановите полноту и свежесть данных.</p>",
        );
    } else if problems.is_empty() {
        html.push_str("<p>Критических действий по утверждённым правилам не найдено.</p>");
    } else {
        html.push_str(
            "<table><tr><th>Приоритет</th><th>Кабинет</th><th>SKU</th><th>Проблема</th><th>Действие</th></tr>",
        );
        for problem in problems {
            let (severity, class) = match problem.severity {
                Severity::Red => ("критично", "red"),
                Severity::Yellow => ("внимание", "yellow"),
            };
            let (label, action) = problem_label(problem.kind);
            write!(
                html,
                "<tr><td class=\"{}\">{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                class,
                severity,
                escape(&problem.account_id),
                problem.sku,
                label,
                action
            )
            .expect("writing to String cannot fail");
        }
        html.push_str("</table>");
    }
}

const fn problem_label(kind: ProblemKind) -> (&'static str, &'static str) {
    match kind {
        ProblemKind::AdvertisedWithoutStock => (
            "реклама расходуется при нулевом остатке",
            "проверьте остаток и рекламную кампанию",
        ),
        ProblemKind::Stockout => ("товар закончился", "запланируйте пополнение"),
        ProblemKind::LowStockCover => (
            "запас ниже расчётного срока поставки",
            "уточните поставку и скорость продаж",
        ),
        ProblemKind::SpendWithoutOrders => (
            "расходы на рекламу без атрибутированных заказов",
            "проверьте запросы, карточку и ставку",
        ),
        ProblemKind::HighDrr => (
            "ДРР выше утверждённого порога",
            "проверьте кампанию до изменения ставки",
        ),
    }
}

fn money(minor: u64) -> String {
    format!("{}.{:02} ₽", minor / 100, minor % 100)
}

fn optional_money(value: Option<u64>) -> String {
    value.map_or_else(|| "N/D".to_owned(), money)
}

fn rate(value: Option<BasisPoints>) -> String {
    value.map_or_else(
        || "N/D".to_owned(),
        |BasisPoints(points)| format!("{}.{:02}%", points / 100, points % 100),
    )
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone};

    use super::*;

    fn kpis() -> KpiSummary {
        KpiSummary {
            ordered_units: 12,
            realized_units: Some(9),
            operational_gmv_minor: 123_456,
            cancelled_units: Some(1),
            returned_units: Some(2),
            ad_impressions: 100,
            ad_clicks: 10,
            ad_spend_minor: 12_345,
            attributed_orders: 2,
            attributed_revenue_minor: 50_000,
            ctr: Some(BasisPoints(1_000)),
            cpc_minor: Some(1_235),
            ad_conversion: Some(BasisPoints(2_000)),
            cpo_minor: Some(6_173),
            drr: Some(BasisPoints(2_469)),
            buyout_rate: Some(BasisPoints(7_500)),
        }
    }

    fn times() -> (DateTime<Utc>, DateTime<Utc>, DateTime<Utc>) {
        let end = Utc.with_ymd_and_hms(2026, 8, 16, 12, 0, 0).unwrap();
        (end - Duration::days(1), end, end + Duration::minutes(1))
    }

    #[test]
    fn complete_report_is_escaped_bounded_and_contains_every_problem_kind() {
        let (start, end, generated) = times();
        let kinds = [
            ProblemKind::AdvertisedWithoutStock,
            ProblemKind::Stockout,
            ProblemKind::LowStockCover,
            ProblemKind::SpendWithoutOrders,
            ProblemKind::HighDrr,
        ];
        let problems = kinds
            .into_iter()
            .enumerate()
            .map(|(index, kind)| PriorityProblem {
                account_id: "ozon_store".to_owned(),
                sku: index as u64 + 1,
                kind,
                severity: if index == 0 {
                    Severity::Yellow
                } else {
                    Severity::Red
                },
                observed: 1,
                threshold: 2,
                impact_minor: 3,
            })
            .collect::<Vec<_>>();
        let html = render_html(HtmlReport {
            manager_name: "Диана <OFK> & 'команда' \"A\"",
            account_ids: &["ozon_store", "wb-store"],
            generated_at: generated,
            interval_start: start,
            interval_end: end,
            preliminary: true,
            quality: SnapshotQuality::Complete,
            kpis: &kpis(),
            problems: &problems,
        })
        .unwrap();
        assert!(html.starts_with("<!doctype html>"));
        assert!(html.contains("Диана &lt;OFK&gt; &amp; &#39;команда&#39; &quot;A&quot;"));
        assert!(html.contains("1234.56 ₽"));
        assert!(html.contains("24.69%"));
        assert!(html.contains("показатели предварительные"));
        for text in [
            "реклама расходуется при нулевом остатке",
            "товар закончился",
            "запас ниже расчётного срока поставки",
            "расходы на рекламу без атрибутированных заказов",
            "ДРР выше утверждённого порога",
        ] {
            assert!(html.contains(text));
        }
    }

    #[test]
    fn incomplete_data_suppresses_actions_and_missing_rates_remain_nd() {
        let (start, end, generated) = times();
        let mut empty_kpis = kpis();
        empty_kpis.ctr = None;
        empty_kpis.cpc_minor = None;
        empty_kpis.ad_conversion = None;
        empty_kpis.cpo_minor = None;
        empty_kpis.drr = None;
        empty_kpis.cancelled_units = None;
        empty_kpis.returned_units = None;
        for quality in [
            SnapshotQuality::Partial,
            SnapshotQuality::Stale,
            SnapshotQuality::Critical,
        ] {
            let html = render_html(HtmlReport {
                manager_name: "Анна Агзамова",
                account_ids: &["wb_store"],
                generated_at: generated,
                interval_start: start,
                interval_end: end,
                preliminary: false,
                quality,
                kpis: &empty_kpis,
                problems: &[],
            })
            .unwrap();
            assert!(html.contains("Рекомендации отключены"));
            assert!(html.matches("N/D").count() >= 7);
        }
    }

    #[test]
    fn clean_report_and_every_invalid_shape_are_handled() {
        let (start, end, generated) = times();
        let base_kpis = kpis();
        let base = HtmlReport {
            manager_name: "Диана",
            account_ids: &["ozon_store"],
            generated_at: generated,
            interval_start: start,
            interval_end: end,
            preliminary: false,
            quality: SnapshotQuality::Complete,
            kpis: &base_kpis,
            problems: &[],
        };
        assert!(render_html(base).unwrap().contains("Критических действий"));

        let excessive_accounts = vec!["a"; MAX_ACCOUNTS + 1];
        let excessive_manager_name = "x".repeat(MAX_MANAGER_NAME_BYTES + 1);
        let excessive_problems = vec![
            PriorityProblem {
                account_id: "ozon_store".to_owned(),
                sku: 1,
                kind: ProblemKind::Stockout,
                severity: Severity::Red,
                observed: 0,
                threshold: 1,
                impact_minor: 1,
            };
            MAX_PROBLEMS + 1
        ];
        let foreign_problem = [PriorityProblem {
            account_id: "foreign_store".to_owned(),
            sku: 1,
            kind: ProblemKind::Stockout,
            severity: Severity::Red,
            observed: 0,
            threshold: 1,
            impact_minor: 1,
        }];
        for invalid in [
            HtmlReport {
                manager_name: " ",
                ..base
            },
            HtmlReport {
                manager_name: "x\n",
                ..base
            },
            HtmlReport {
                manager_name: &excessive_manager_name,
                ..base
            },
            HtmlReport {
                account_ids: &[],
                ..base
            },
            HtmlReport {
                account_ids: &excessive_accounts,
                ..base
            },
            HtmlReport {
                account_ids: &["bad/account"],
                ..base
            },
            HtmlReport {
                account_ids: &["same", "same"],
                ..base
            },
            HtmlReport {
                interval_start: end,
                ..base
            },
            HtmlReport {
                generated_at: start,
                ..base
            },
            HtmlReport {
                problems: &excessive_problems,
                ..base
            },
            HtmlReport {
                problems: &foreign_problem,
                ..base
            },
        ] {
            assert_eq!(render_html(invalid), Err(HtmlReportError::InvalidInput));
        }
    }
}
