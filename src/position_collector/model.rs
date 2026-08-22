use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, Utc};
use thiserror::Error;

use super::schedule::is_aligned_slot;

const MAX_TARGETS_PER_BATCH: usize = 4_096;
const MAX_QUERIES_PER_BATCH: usize = 64;
const MAX_QUERIES_PER_REGION_PER_BATCH: usize = 16;
const MAX_PUBLIC_RESULTS: u16 = 100;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ValidationError {
    #[error("{0} must be positive")]
    NonPositiveId(&'static str),
    #[error("{0} must contain between 1 and {1} UTF-8 bytes and no control characters")]
    InvalidText(&'static str, usize),
    #[error("product_id must contain only ASCII digits")]
    InvalidProductId,
    #[error("max_position must be between 1 and 100")]
    InvalidMaxPosition,
    #[error("batch slot must be an exact UTC :00 or :30 boundary")]
    InvalidSlot,
    #[error("a batch must contain between 1 and 4096 targets")]
    InvalidTargetCount,
    #[error("a batch must contain at most 64 unique region-and-phrase queries")]
    TooManyQueries,
    #[error("a batch must contain at most 16 unique queries for one region")]
    TooManyRegionQueries,
    #[error("monitor_id {0} occurs more than once")]
    DuplicateMonitorId(i64),
    #[error("one region_code maps to conflicting region names")]
    InconsistentRegionName,
    #[error("scan region does not match the requested region")]
    RegionMismatch,
    #[error("scan did not confirm the requested region")]
    RegionUnconfirmed,
    #[error("scan did not cover the requested top-N range")]
    IncompleteScan,
    #[error("search hit position is outside the requested range")]
    InvalidHitPosition,
    #[error("scan returned more hits than the requested top-N range can contain")]
    TooManyHits,
    #[error("scan observation time is outside its logical half-hour slot")]
    ObservationOutsideSlot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MonitorTarget {
    monitor_id: i64,
    store_id: String,
    product_id: String,
    search_phrase: String,
    region_code: String,
    region_name: String,
    max_position: u16,
}

impl MonitorTarget {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        monitor_id: i64,
        store_id: impl Into<String>,
        product_id: impl Into<String>,
        search_phrase: impl Into<String>,
        region_code: impl Into<String>,
        region_name: impl Into<String>,
        max_position: u16,
    ) -> Result<Self, ValidationError> {
        if monitor_id <= 0 {
            return Err(ValidationError::NonPositiveId("monitor_id"));
        }

        let store_id = validate_text("store_id", store_id.into(), 128)?;
        let product_id = validate_text("product_id", product_id.into(), 128)?;
        if !product_id.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(ValidationError::InvalidProductId);
        }
        let search_phrase = validate_text("search_phrase", search_phrase.into(), 256)?;
        let region_code = validate_text("region_code", region_code.into(), 64)?;
        let region_name = validate_text("region_name", region_name.into(), 128)?;
        if !(1..=MAX_PUBLIC_RESULTS).contains(&max_position) {
            return Err(ValidationError::InvalidMaxPosition);
        }

        Ok(Self {
            monitor_id,
            store_id,
            product_id,
            search_phrase,
            region_code,
            region_name,
            max_position,
        })
    }

    #[must_use]
    pub const fn monitor_id(&self) -> i64 {
        self.monitor_id
    }

    #[must_use]
    pub fn store_id(&self) -> &str {
        &self.store_id
    }

    #[must_use]
    pub fn product_id(&self) -> &str {
        &self.product_id
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct QueryKey {
    region_code: String,
    normalized_phrase: String,
    max_position: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryPlan {
    request: QueryRequest,
    targets: Vec<MonitorTarget>,
}

impl QueryPlan {
    #[must_use]
    pub const fn request(&self) -> &QueryRequest {
        &self.request
    }

    #[must_use]
    pub fn targets(&self) -> &[MonitorTarget] {
        &self.targets
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryRequest {
    slot: DateTime<Utc>,
    region_code: String,
    region_name: String,
    search_phrase: String,
    max_position: u16,
    product_ids: Vec<String>,
}

impl QueryRequest {
    #[must_use]
    pub const fn slot(&self) -> DateTime<Utc> {
        self.slot
    }

    #[must_use]
    pub fn region_code(&self) -> &str {
        &self.region_code
    }

    #[must_use]
    pub fn region_name(&self) -> &str {
        &self.region_name
    }

    #[must_use]
    pub fn search_phrase(&self) -> &str {
        &self.search_phrase
    }

    #[must_use]
    pub const fn max_position(&self) -> u16 {
        self.max_position
    }

    #[must_use]
    pub fn product_ids(&self) -> &[String] {
        &self.product_ids
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchPlan {
    slot: DateTime<Utc>,
    queries: Vec<QueryPlan>,
    target_count: usize,
}

impl BatchPlan {
    pub fn new(slot: DateTime<Utc>, targets: Vec<MonitorTarget>) -> Result<Self, ValidationError> {
        if !is_aligned_slot(slot) {
            return Err(ValidationError::InvalidSlot);
        }
        if targets.is_empty() || targets.len() > MAX_TARGETS_PER_BATCH {
            return Err(ValidationError::InvalidTargetCount);
        }

        let mut monitor_ids = BTreeSet::new();
        let mut region_names = BTreeMap::new();
        let mut grouped: BTreeMap<QueryKey, Vec<MonitorTarget>> = BTreeMap::new();
        for target in targets {
            if !monitor_ids.insert(target.monitor_id) {
                return Err(ValidationError::DuplicateMonitorId(target.monitor_id));
            }
            if region_names
                .insert(target.region_code.clone(), target.region_name.clone())
                .is_some_and(|existing| existing != target.region_name)
            {
                return Err(ValidationError::InconsistentRegionName);
            }
            let key = QueryKey {
                region_code: target.region_code.clone(),
                normalized_phrase: normalize_phrase(&target.search_phrase),
                max_position: target.max_position,
            };
            grouped.entry(key).or_default().push(target);
        }
        if grouped.len() > MAX_QUERIES_PER_BATCH {
            return Err(ValidationError::TooManyQueries);
        }
        let mut queries_per_region = BTreeMap::new();
        for key in grouped.keys() {
            let count = queries_per_region
                .entry(key.region_code.as_str())
                .or_insert(0_usize);
            *count += 1;
            if *count > MAX_QUERIES_PER_REGION_PER_BATCH {
                return Err(ValidationError::TooManyRegionQueries);
            }
        }

        let target_count = monitor_ids.len();
        let queries = grouped
            .into_iter()
            .map(|(key, mut targets)| {
                targets.sort_by_key(|target| target.monitor_id);
                let first = &targets[0];
                let product_ids = targets
                    .iter()
                    .map(|target| target.product_id.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                QueryPlan {
                    request: QueryRequest {
                        slot,
                        region_code: key.region_code,
                        region_name: first.region_name.clone(),
                        search_phrase: first.search_phrase.clone(),
                        max_position: key.max_position,
                        product_ids,
                    },
                    targets,
                }
            })
            .collect();

        Ok(Self {
            slot,
            queries,
            target_count,
        })
    }

    #[must_use]
    pub const fn slot(&self) -> DateTime<Utc> {
        self.slot
    }

    #[must_use]
    pub fn queries(&self) -> &[QueryPlan] {
        &self.queries
    }

    #[must_use]
    pub const fn target_count(&self) -> usize {
        self.target_count
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PlacementKind {
    Organic,
    Sponsored,
    #[default]
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchHit {
    product_id: String,
    overall_position: u16,
    placement: PlacementKind,
}

impl SearchHit {
    pub fn new(
        product_id: impl Into<String>,
        overall_position: u16,
        placement: PlacementKind,
    ) -> Result<Self, ValidationError> {
        let product_id = validate_text("product_id", product_id.into(), 128)?;
        if !product_id.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(ValidationError::InvalidProductId);
        }
        if overall_position == 0 {
            return Err(ValidationError::InvalidHitPosition);
        }
        Ok(Self {
            product_id,
            overall_position,
            placement,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryScan {
    observed_at: DateTime<Utc>,
    region_code: String,
    region_confirmed: bool,
    complete_top_n: bool,
    hits: Vec<SearchHit>,
}

impl QueryScan {
    pub fn new(
        observed_at: DateTime<Utc>,
        region_code: impl Into<String>,
        region_confirmed: bool,
        complete_top_n: bool,
        hits: Vec<SearchHit>,
    ) -> Self {
        Self {
            observed_at,
            region_code: region_code.into(),
            region_confirmed,
            complete_top_n,
            hits,
        }
    }

    pub(crate) fn into_observations(
        self,
        plan: &QueryPlan,
    ) -> Result<Vec<Observation>, ValidationError> {
        if self.region_code != plan.request.region_code {
            return Err(ValidationError::RegionMismatch);
        }
        if !self.region_confirmed {
            return Err(ValidationError::RegionUnconfirmed);
        }
        if !self.complete_top_n {
            return Err(ValidationError::IncompleteScan);
        }

        let slot_end = plan
            .request
            .slot
            .checked_add_signed(Duration::minutes(30))
            .ok_or(ValidationError::ObservationOutsideSlot)?;
        if self.observed_at < plan.request.slot || self.observed_at >= slot_end {
            return Err(ValidationError::ObservationOutsideSlot);
        }
        if self.hits.len() > usize::from(plan.request.max_position) {
            return Err(ValidationError::TooManyHits);
        }

        let mut hits: BTreeMap<String, HitPositions> = BTreeMap::new();
        for hit in self.hits {
            if hit.overall_position > plan.request.max_position {
                return Err(ValidationError::InvalidHitPosition);
            }
            hits.entry(hit.product_id)
                .or_default()
                .record(hit.overall_position, hit.placement);
        }

        Ok(plan
            .targets
            .iter()
            .map(|target| {
                let outcome =
                    hits.get(&target.product_id)
                        .map_or(ObservationOutcome::NotFound, |hit| {
                            ObservationOutcome::Found {
                                overall_position: hit.overall_position,
                                organic_position: hit.organic_position,
                                sponsored_position: hit.sponsored_position,
                                placement: hit.overall_placement,
                            }
                        });
                Observation {
                    monitor_id: target.monitor_id,
                    observed_at: self.observed_at,
                    outcome,
                }
            })
            .collect())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct HitPositions {
    overall_position: u16,
    overall_placement: PlacementKind,
    organic_position: Option<u16>,
    sponsored_position: Option<u16>,
}

impl HitPositions {
    fn record(&mut self, position: u16, placement: PlacementKind) {
        if self.overall_position == 0 || position < self.overall_position {
            self.overall_position = position;
            self.overall_placement = placement;
        }
        match placement {
            PlacementKind::Organic => update_best(&mut self.organic_position, position),
            PlacementKind::Sponsored => update_best(&mut self.sponsored_position, position),
            PlacementKind::Unknown => {}
        }
    }
}

fn update_best(current: &mut Option<u16>, position: u16) {
    if current.is_none_or(|existing| position < existing) {
        *current = Some(position);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObservationOutcome {
    Found {
        overall_position: u16,
        organic_position: Option<u16>,
        sponsored_position: Option<u16>,
        placement: PlacementKind,
    },
    NotFound,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Observation {
    monitor_id: i64,
    observed_at: DateTime<Utc>,
    outcome: ObservationOutcome,
}

impl Observation {
    #[must_use]
    pub const fn monitor_id(&self) -> i64 {
        self.monitor_id
    }

    #[must_use]
    pub const fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }

    #[must_use]
    pub const fn outcome(&self) -> ObservationOutcome {
        self.outcome
    }
}

fn validate_text(
    field: &'static str,
    value: String,
    max_bytes: usize,
) -> Result<String, ValidationError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.len() > max_bytes || trimmed.chars().any(char::is_control) {
        return Err(ValidationError::InvalidText(field, max_bytes));
    }
    if trimmed.len() == value.len() {
        Ok(value)
    } else {
        Ok(trimmed.to_owned())
    }
}

fn normalize_phrase(value: &str) -> String {
    value
        .split_whitespace()
        .map(str::to_lowercase)
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::{
        BatchPlan, MonitorTarget, ObservationOutcome, PlacementKind, QueryScan, SearchHit,
        ValidationError,
    };

    fn slot() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 16, 7, 30, 0).unwrap()
    }

    fn target(id: i64, product: &str, phrase: &str, region: &str) -> MonitorTarget {
        MonitorTarget::new(id, "store", product, phrase, region, "Москва", 100).unwrap()
    }

    #[test]
    fn plan_coalesces_same_public_query_and_deduplicates_products() {
        let plan = BatchPlan::new(
            slot(),
            vec![
                target(2, "3411079879", " Ручка   кнопка ", "moscow"),
                target(1, "3411079879", "ручка кнопка", "moscow"),
                target(3, "3388722638", "РУЧКА КНОПКА", "moscow"),
            ],
        )
        .unwrap();

        assert_eq!(plan.slot(), slot());
        assert_eq!(plan.target_count(), 3);
        assert_eq!(plan.queries().len(), 1);
        let query = &plan.queries()[0];
        assert_eq!(query.targets()[0].monitor_id(), 1);
        assert_eq!(query.targets()[0].store_id(), "store");
        assert_eq!(query.targets()[0].product_id(), "3411079879");
        assert_eq!(
            query.request().product_ids(),
            &["3388722638".to_owned(), "3411079879".to_owned()]
        );
        assert_eq!(query.request().slot(), slot());
        assert_eq!(query.request().region_code(), "moscow");
        assert_eq!(query.request().region_name(), "Москва");
        assert_eq!(query.request().search_phrase(), "ручка кнопка");
        assert_eq!(query.request().max_position(), 100);
    }

    #[test]
    fn region_and_phrase_are_part_of_query_identity() {
        let plan = BatchPlan::new(
            slot(),
            vec![
                target(1, "1", "ручка", "moscow"),
                target(2, "2", "ручка", "ekb"),
                target(3, "3", "кнопка", "moscow"),
            ],
        )
        .unwrap();
        assert_eq!(plan.queries().len(), 3);

        let inconsistent = vec![
            MonitorTarget::new(1, "s", "1", "p", "r", "Москва", 100).unwrap(),
            MonitorTarget::new(2, "s", "2", "other", "r", "Екатеринбург", 100).unwrap(),
        ];
        assert_eq!(
            BatchPlan::new(slot(), inconsistent),
            Err(ValidationError::InconsistentRegionName)
        );
    }

    #[test]
    fn target_and_batch_validation_is_fail_closed() {
        assert_eq!(
            MonitorTarget::new(0, "store", "1", "phrase", "r", "R", 100),
            Err(ValidationError::NonPositiveId("monitor_id"))
        );
        assert!(matches!(
            MonitorTarget::new(1, "\n", "1", "phrase", "r", "R", 100),
            Err(ValidationError::InvalidText("store_id", 128))
        ));
        assert_eq!(
            MonitorTarget::new(1, "store", "sku-x", "phrase", "r", "R", 100),
            Err(ValidationError::InvalidProductId)
        );
        assert_eq!(
            MonitorTarget::new(1, "store", "1", "phrase", "r", "R", 101),
            Err(ValidationError::InvalidMaxPosition)
        );
        assert_eq!(
            BatchPlan::new(
                Utc.with_ymd_and_hms(2026, 8, 16, 7, 5, 0).unwrap(),
                vec![target(1, "1", "p", "r")]
            ),
            Err(ValidationError::InvalidSlot)
        );
        assert_eq!(
            BatchPlan::new(slot(), Vec::new()),
            Err(ValidationError::InvalidTargetCount)
        );
        let repeated = target(1, "1", "p", "r");
        assert_eq!(
            BatchPlan::new(slot(), vec![repeated.clone(), repeated]),
            Err(ValidationError::DuplicateMonitorId(1))
        );
        let too_many_queries = (1..=65)
            .map(|id| target(id, &id.to_string(), &format!("phrase-{id}"), "r"))
            .collect();
        assert_eq!(
            BatchPlan::new(slot(), too_many_queries),
            Err(ValidationError::TooManyQueries)
        );
        let too_many_region_queries = (1..=17)
            .map(|id| target(id, &id.to_string(), &format!("region-phrase-{id}"), "r"))
            .collect();
        assert_eq!(
            BatchPlan::new(slot(), too_many_region_queries),
            Err(ValidationError::TooManyRegionQueries)
        );
    }

    #[test]
    fn scan_preserves_unknown_placement_and_uses_none_for_misses() {
        let plan = BatchPlan::new(
            slot(),
            vec![
                target(1, "3411079879", "ручка", "moscow"),
                target(2, "3388722638", "ручка", "moscow"),
            ],
        )
        .unwrap();
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 16, 7, 36, 0).unwrap();
        let scan = QueryScan::new(
            observed_at,
            "moscow",
            true,
            true,
            vec![SearchHit::new("3411079879", 21, PlacementKind::Unknown).unwrap()],
        );
        let observations = scan.into_observations(&plan.queries()[0]).unwrap();

        assert_eq!(observations[0].monitor_id(), 1);
        assert_eq!(observations[0].observed_at(), observed_at);
        assert_eq!(
            observations[0].outcome(),
            ObservationOutcome::Found {
                overall_position: 21,
                organic_position: None,
                sponsored_position: None,
                placement: PlacementKind::Unknown
            }
        );
        assert_eq!(observations[1].outcome(), ObservationOutcome::NotFound);
    }

    #[test]
    fn malformed_scan_is_rejected_instead_of_published() {
        let plan = BatchPlan::new(slot(), vec![target(1, "1", "p", "r")]).unwrap();
        let query = &plan.queries()[0];
        let now = slot();

        let cases = [
            (
                QueryScan::new(now, "other", true, true, vec![]),
                ValidationError::RegionMismatch,
            ),
            (
                QueryScan::new(now, "r", false, true, vec![]),
                ValidationError::RegionUnconfirmed,
            ),
            (
                QueryScan::new(now, "r", true, false, vec![]),
                ValidationError::IncompleteScan,
            ),
            (
                QueryScan::new(
                    now,
                    "r",
                    true,
                    true,
                    vec![SearchHit::new("1", 101, PlacementKind::Organic).unwrap()],
                ),
                ValidationError::InvalidHitPosition,
            ),
            (
                QueryScan::new(now - chrono::Duration::seconds(1), "r", true, true, vec![]),
                ValidationError::ObservationOutsideSlot,
            ),
            (
                QueryScan::new(now + chrono::Duration::minutes(30), "r", true, true, vec![]),
                ValidationError::ObservationOutsideSlot,
            ),
        ];

        for (scan, expected) in cases {
            assert_eq!(scan.into_observations(query), Err(expected));
        }
        let excessive_hits = (0..=100)
            .map(|_| SearchHit::new("1", 1, PlacementKind::Unknown).unwrap())
            .collect();
        assert_eq!(
            QueryScan::new(now, "r", true, true, excessive_hits).into_observations(query),
            Err(ValidationError::TooManyHits)
        );
        assert_eq!(
            SearchHit::new("sku", 1, PlacementKind::Unknown),
            Err(ValidationError::InvalidProductId)
        );
        assert_eq!(
            SearchHit::new("1", 0, PlacementKind::Unknown),
            Err(ValidationError::InvalidHitPosition)
        );
    }

    #[test]
    fn unsupported_end_of_time_slot_fails_closed() {
        let last_day = chrono::DateTime::<Utc>::MAX_UTC.date_naive();
        let last_slot = last_day.and_hms_opt(23, 30, 0).unwrap().and_utc();
        let plan = BatchPlan::new(last_slot, vec![target(1, "1", "p", "r")]).unwrap();
        let scan = QueryScan::new(last_slot, "r", true, true, vec![]);
        assert_eq!(
            scan.into_observations(&plan.queries()[0]),
            Err(ValidationError::ObservationOutsideSlot)
        );
    }

    #[test]
    fn repeated_product_preserves_best_organic_and_sponsored_positions() {
        let plan = BatchPlan::new(slot(), vec![target(1, "1", "p", "r")]).unwrap();
        let scan = QueryScan::new(
            slot() + chrono::Duration::minutes(5),
            "r",
            true,
            true,
            vec![
                SearchHit::new("1", 8, PlacementKind::Sponsored).unwrap(),
                SearchHit::new("1", 20, PlacementKind::Organic).unwrap(),
                SearchHit::new("1", 12, PlacementKind::Organic).unwrap(),
            ],
        );

        let observation = scan.into_observations(&plan.queries()[0]).unwrap();
        assert_eq!(
            observation[0].outcome(),
            ObservationOutcome::Found {
                overall_position: 8,
                organic_position: Some(12),
                sponsored_position: Some(8),
                placement: PlacementKind::Sponsored,
            }
        );
    }
}
