//! Sales snapshot selection and aggregation.

use super::{
    AccountScope, BTreeMap, BTreeSet, Client, DataState, MAX_SALES_ANALYTICS_FACT_ROWS, NaiveDate,
    NaiveTime, ReportingMarketplace, ReportingReadError, Row, SalesAnalyticsDirection,
    SalesAnalyticsGroup, SalesAnalyticsQuery, SalesAnalyticsResult, SalesAnalyticsRow,
    SalesAnalyticsSort, SalesDateCoverage, SalesDateCoverageState, SalesSnapshotSelection,
    SnapshotDescriptor, SnapshotSource, SnapshotStatus, Utc, WEEKLY_RANKING_DAYS, column,
    marketplace_str, nonnegative_i64, positive_i64, timestamp_string, validate_snapshot_fact_count,
};
use chrono::TimeZone;

pub(super) fn select_sales_snapshots(
    query: SalesAnalyticsQuery,
    descriptors: Vec<SnapshotDescriptor>,
) -> Result<SalesSnapshotSelection, ReportingReadError> {
    let mut latest = latest_sales_snapshots(query, descriptors)?;

    let mut coverage = Vec::new();
    let mut expected = BTreeMap::new();
    let mut date = query.date_from;
    loop {
        coverage.push(sales_date_coverage(
            date,
            latest.remove(&date),
            &mut expected,
        )?);
        if date == query.date_to {
            break;
        }
        date = date.succ_opt().ok_or(ReportingReadError::InvalidRequest)?;
    }
    let served = coverage.iter().filter(|item| item.served).count();
    let state = if served == 0 {
        DataState::Unavailable
    } else if coverage
        .iter()
        .all(|item| item.state == SalesDateCoverageState::Complete)
    {
        DataState::Complete
    } else {
        DataState::Partial
    };
    Ok(SalesSnapshotSelection {
        state,
        coverage,
        expected,
    })
}

fn sales_snapshot_date(
    query: SalesAnalyticsQuery,
    descriptor: &SnapshotDescriptor,
) -> Result<NaiveDate, ReportingReadError> {
    let offset = super::super::yekaterinburg_offset();
    if descriptor.source() != SnapshotSource::Sales {
        return Err(ReportingReadError::InvalidPublishedData);
    }
    let (period_start, period_end) = descriptor.period();
    let local_start = period_start.with_timezone(&offset);
    let business_date = local_start.date_naive();
    let next_date = business_date
        .succ_opt()
        .ok_or(ReportingReadError::InvalidPublishedData)?;
    let complete_end = offset
        .from_local_datetime(
            &next_date
                .and_hms_opt(0, 0, 0)
                .ok_or(ReportingReadError::InvalidPublishedData)?,
        )
        .single()
        .ok_or(ReportingReadError::InvalidPublishedData)?
        .with_timezone(&Utc);
    if local_start.time() != NaiveTime::MIN
        || business_date < query.date_from
        || business_date > query.date_to
        || period_end <= period_start
        || period_end > complete_end
    {
        return Err(ReportingReadError::InvalidPublishedData);
    }
    Ok(business_date)
}

fn latest_sales_snapshots(
    query: SalesAnalyticsQuery,
    descriptors: Vec<SnapshotDescriptor>,
) -> Result<BTreeMap<NaiveDate, SnapshotDescriptor>, ReportingReadError> {
    let mut latest = BTreeMap::<NaiveDate, SnapshotDescriptor>::new();
    for descriptor in descriptors {
        let business_date = sales_snapshot_date(query, &descriptor)?;
        match latest.entry(business_date) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(descriptor);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                if entry.get().cutoff_at() == descriptor.cutoff_at() {
                    return Err(ReportingReadError::InvalidPublishedData);
                }
                if entry.get().cutoff_at() < descriptor.cutoff_at() {
                    entry.insert(descriptor);
                }
            }
        }
    }

    Ok(latest)
}

fn sales_date_coverage(
    date: NaiveDate,
    descriptor: Option<SnapshotDescriptor>,
    expected: &mut BTreeMap<i64, (NaiveDate, SnapshotDescriptor)>,
) -> Result<SalesDateCoverage, ReportingReadError> {
    let offset = super::super::yekaterinburg_offset();
    let item = match descriptor {
        None => SalesDateCoverage {
            business_date: date.to_string(),
            state: SalesDateCoverageState::Unavailable,
            served: false,
            cutoff_at: None,
            source_as_of: None,
            period_end: None,
        },
        Some(descriptor) => {
            let (_, period_end) = descriptor.period();
            let next_date = date
                .succ_opt()
                .ok_or(ReportingReadError::InvalidPublishedData)?;
            let complete = period_end.with_timezone(&offset).naive_local()
                == next_date
                    .and_hms_opt(0, 0, 0)
                    .ok_or(ReportingReadError::InvalidPublishedData)?;
            let served = descriptor.status() == SnapshotStatus::Succeeded
                && descriptor.pagination_complete();
            let state = if !served {
                SalesDateCoverageState::Partial
            } else if complete {
                SalesDateCoverageState::Complete
            } else {
                SalesDateCoverageState::Preliminary
            };
            let item = SalesDateCoverage {
                business_date: date.to_string(),
                state,
                served,
                cutoff_at: Some(timestamp_string(descriptor.cutoff_at())),
                source_as_of: Some(timestamp_string(descriptor.source_as_of())),
                period_end: Some(timestamp_string(period_end)),
            };
            if served
                && expected
                    .insert(descriptor.snapshot_id(), (date, descriptor))
                    .is_some()
            {
                return Err(ReportingReadError::InvalidPublishedData);
            }
            item
        }
    };
    Ok(item)
}

pub(super) fn validate_selected_sales_fact_count(
    expected: &BTreeMap<i64, (NaiveDate, SnapshotDescriptor)>,
) -> Result<(), ReportingReadError> {
    let mut remaining = MAX_SALES_ANALYTICS_FACT_ROWS;
    for (_, descriptor) in expected.values() {
        let count = u64::from(descriptor.row_count());
        if count > remaining {
            return Err(ReportingReadError::InvalidRequest);
        }
        remaining -= count;
    }
    Ok(())
}

pub(super) async fn validate_sales_fact_rows(
    client: &Client,
    account: &AccountScope,
    expected: &BTreeMap<i64, (NaiveDate, SnapshotDescriptor)>,
) -> Result<(), ReportingReadError> {
    let snapshot_ids = expected.keys().copied().collect::<Vec<_>>();
    let marketplace = marketplace_str(account.marketplace());
    let rows = client
        .query(
            "SELECT snapshot_id, count(*)::bigint, min(business_date), max(business_date), \
                    bool_and(account_id = $2 AND marketplace = $3 AND source = 'sales' \
                        AND snapshot_status = 'succeeded' AND pagination_complete) \
             FROM daily_reporting.mcp_sales_facts \
             WHERE snapshot_id = ANY($1::bigint[]) \
             GROUP BY snapshot_id",
            &[&snapshot_ids, &account.account_id(), &marketplace],
        )
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    let mut actual = BTreeMap::<i64, u64>::new();
    for row in rows {
        let snapshot_id: i64 = column(&row, 0)?;
        let count = nonnegative_i64(column(&row, 1)?)?;
        let minimum: Option<NaiveDate> = column(&row, 2)?;
        let maximum: Option<NaiveDate> = column(&row, 3)?;
        let valid: bool = column(&row, 4)?;
        // WHERE limits IDs to this map; GROUP BY returns each ID exactly once.
        let (business_date, _) = &expected[&snapshot_id];
        if !valid || minimum != Some(*business_date) || maximum != Some(*business_date) {
            return Err(ReportingReadError::InvalidPublishedData);
        }
        actual.insert(snapshot_id, count);
    }
    for (snapshot_id, (_, descriptor)) in expected {
        let actual = actual.get(snapshot_id).copied().unwrap_or(0);
        let actual =
            usize::try_from(actual).map_err(|_| ReportingReadError::InvalidPublishedData)?;
        validate_snapshot_fact_count(actual, descriptor.row_count())?;
    }
    Ok(())
}

pub(super) async fn aggregate_sales_rows(
    client: &Client,
    snapshot_ids: &[i64],
    query: SalesAnalyticsQuery,
) -> Result<(u64, Vec<SalesAnalyticsRow>), ReportingReadError> {
    let (_, group_by, _) = sales_group_sql(query.group_by, SalesAnalyticsDirection::Asc);
    let count_query = format!(
        "SELECT count(*)::bigint FROM (\
             SELECT 1 FROM daily_reporting.mcp_sales_facts \
             WHERE snapshot_id = ANY($1::bigint[]) GROUP BY {group_by}\
         ) AS grouped_sales"
    );
    let total_rows = client
        .query_one(&count_query, &[&snapshot_ids])
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    let total_rows = nonnegative_i64(column(&total_rows, 0)?)?;
    if total_rows == 0 || u64::from(query.offset) >= total_rows {
        return Ok((total_rows, Vec::new()));
    }
    let (dimensions, group_by, dimension_order) = sales_group_sql(query.group_by, query.direction);
    let direction = sales_direction_sql(query.direction);
    let order_by = match query.sort_by {
        SalesAnalyticsSort::Dimension => dimension_order.to_owned(),
        SalesAnalyticsSort::OrderedUnits => {
            format!("sum(ordered_units) {direction}, {dimension_order}")
        }
        SalesAnalyticsSort::OperationalGmv => {
            format!("sum(operational_gmv_minor) {direction}, {dimension_order}")
        }
    };
    let page_query = format!(
        "SELECT {dimensions}, sum(ordered_units)::text, \
                sum(operational_gmv_minor)::text \
         FROM daily_reporting.mcp_sales_facts \
         WHERE snapshot_id = ANY($1::bigint[]) \
         GROUP BY {group_by} \
         ORDER BY {order_by} \
         LIMIT $2 OFFSET $3"
    );
    let rows = client
        .query(
            &page_query,
            &[
                &snapshot_ids,
                &i64::from(query.limit),
                &i64::from(query.offset),
            ],
        )
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    let rows = rows
        .iter()
        .map(|row| sales_analytics_row(row, query.group_by))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((total_rows, rows))
}

pub(super) const fn sales_group_sql(
    group: SalesAnalyticsGroup,
    direction: SalesAnalyticsDirection,
) -> (&'static str, &'static str, &'static str) {
    match (group, direction) {
        (SalesAnalyticsGroup::Day, SalesAnalyticsDirection::Asc) => (
            "business_date, NULL::bigint AS sku",
            "business_date",
            "business_date ASC",
        ),
        (SalesAnalyticsGroup::Day, SalesAnalyticsDirection::Desc) => (
            "business_date, NULL::bigint AS sku",
            "business_date",
            "business_date DESC",
        ),
        (SalesAnalyticsGroup::Sku, SalesAnalyticsDirection::Asc) => {
            ("NULL::date AS business_date, sku", "sku", "sku ASC")
        }
        (SalesAnalyticsGroup::Sku, SalesAnalyticsDirection::Desc) => {
            ("NULL::date AS business_date, sku", "sku", "sku DESC")
        }
        (SalesAnalyticsGroup::DaySku, SalesAnalyticsDirection::Asc) => (
            "business_date, sku",
            "business_date, sku",
            "business_date ASC, sku ASC",
        ),
        (SalesAnalyticsGroup::DaySku, SalesAnalyticsDirection::Desc) => (
            "business_date, sku",
            "business_date, sku",
            "business_date DESC, sku DESC",
        ),
    }
}

pub(super) const fn sales_direction_sql(direction: SalesAnalyticsDirection) -> &'static str {
    match direction {
        SalesAnalyticsDirection::Asc => "ASC",
        SalesAnalyticsDirection::Desc => "DESC",
    }
}

pub(super) fn sales_analytics_row(
    row: &Row,
    group: SalesAnalyticsGroup,
) -> Result<SalesAnalyticsRow, ReportingReadError> {
    let business_date: Option<NaiveDate> = column(row, 0)?;
    let sku: Option<i64> = column(row, 1)?;
    let dimensions_valid = match group {
        SalesAnalyticsGroup::Day => business_date.is_some() && sku.is_none(),
        SalesAnalyticsGroup::Sku => business_date.is_none() && sku.is_some(),
        SalesAnalyticsGroup::DaySku => business_date.is_some() && sku.is_some(),
    };
    if !dimensions_valid {
        return Err(ReportingReadError::InvalidPublishedData);
    }
    let sku = sku
        .map(positive_i64)
        .transpose()?
        .map(|value| value.to_string());
    let ordered_units: String = column(row, 2)?;
    let operational_gmv_minor: String = column(row, 3)?;
    Ok(SalesAnalyticsRow {
        business_date: business_date.map(|value| value.to_string()),
        sku,
        ordered_units: decimal_u64(&ordered_units)?,
        operational_gmv_minor: decimal_u64(&operational_gmv_minor)?,
        currency: "RUB".to_owned(),
    })
}

pub(super) fn decimal_u64(value: &str) -> Result<u64, ReportingReadError> {
    value
        .parse()
        .map_err(|_| ReportingReadError::InvalidPublishedData)
}

pub(super) fn validate_weekly_ranking_result(
    account: &AccountScope,
    date_from: NaiveDate,
    date_to: NaiveDate,
    result: &SalesAnalyticsResult,
) -> Result<(), ReportingReadError> {
    let expected_marketplace: ReportingMarketplace = account.marketplace().into();
    if result.account_id != account.account_id()
        || result.marketplace != expected_marketplace
        || result.date_from != date_from.to_string()
        || result.date_to != date_to.to_string()
        || result.source != "published_postgresql_snapshots"
        || result.group_by != SalesAnalyticsGroup::Day
        || result.sort_by != SalesAnalyticsSort::Dimension
        || result.direction != SalesAnalyticsDirection::Asc
        || i64::from(result.limit) != WEEKLY_RANKING_DAYS
        || result.offset != 0
        || usize::try_from(result.total_rows).ok() != Some(result.rows.len())
    {
        return Err(ReportingReadError::InvalidPublishedData);
    }

    let mut expected_dates = BTreeSet::new();
    for offset in 0..WEEKLY_RANKING_DAYS {
        let date = date_from
            .checked_add_days(chrono::Days::new(
                u64::try_from(offset).map_err(|_| ReportingReadError::InvalidPublishedData)?,
            ))
            .ok_or(ReportingReadError::InvalidPublishedData)?;
        expected_dates.insert(date.to_string());
    }
    if expected_dates.last().map(String::as_str) != Some(date_to.to_string().as_str()) {
        return Err(ReportingReadError::InvalidPublishedData);
    }

    let coverage_dates = result
        .coverage
        .iter()
        .map(|coverage| coverage.business_date.clone())
        .collect::<BTreeSet<_>>();
    if coverage_dates != expected_dates || coverage_dates.len() != result.coverage.len() {
        return Err(ReportingReadError::InvalidPublishedData);
    }

    let mut row_dates = BTreeSet::new();
    for row in &result.rows {
        let Some(business_date) = &row.business_date else {
            return Err(ReportingReadError::InvalidPublishedData);
        };
        if row.sku.is_some()
            || row.currency != "RUB"
            || !expected_dates.contains(business_date)
            || !row_dates.insert(business_date.clone())
            || !result
                .coverage
                .iter()
                .any(|date| date.business_date == *business_date && date.served)
        {
            return Err(ReportingReadError::InvalidPublishedData);
        }
    }
    Ok(())
}
