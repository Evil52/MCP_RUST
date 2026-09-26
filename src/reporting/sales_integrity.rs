//! Narrow recovery for repeated zero facts, gated by independent daily totals.
use std::collections::{BTreeMap, BTreeSet};

use chrono::NaiveDate;

use super::postgres_collector::CollectedSalesFact;

pub(super) type SalesTotals = BTreeMap<NaiveDate, (u64, u64)>;

#[derive(Default)]
pub(super) struct SalesPages {
    facts: Vec<CollectedSalesFact>,
    identities: BTreeMap<(NaiveDate, u64), usize>,
    pub zero_overlap: bool,
}

impl SalesPages {
    pub fn add(&mut self, rows: Vec<CollectedSalesFact>) -> bool {
        let mut page_ids = BTreeSet::new();
        for row in rows {
            let key = (row.business_date, row.sku);
            if !page_ids.insert(key) {
                return false;
            }
            if let Some(index) = self.identities.get(&key) {
                if self.facts[*index] != row
                    || row.ordered_units != 0
                    || row.operational_gmv_minor != 0
                    || row.cancelled_units.is_some_and(|n| n != 0)
                    || row.returned_units.is_some_and(|n| n != 0)
                {
                    return false;
                }
                self.zero_overlap = true;
            } else {
                self.identities.insert(key, self.facts.len());
                self.facts.push(row);
            }
        }
        true
    }

    pub fn matches(&self, expected: &SalesTotals) -> bool {
        let mut actual = SalesTotals::new();
        for row in &self.facts {
            let total = actual.entry(row.business_date).or_default();
            let Some(units) = total.0.checked_add(row.ordered_units) else {
                return false;
            };
            let Some(gmv) = total.1.checked_add(row.operational_gmv_minor) else {
                return false;
            };
            *total = (units, gmv);
        }
        actual
            .iter()
            .all(|(day, total)| *total == expected.get(day).copied().unwrap_or_default())
            && expected
                .iter()
                .all(|(day, total)| *total == actual.get(day).copied().unwrap_or_default())
    }

    pub fn into_facts(self) -> Vec<CollectedSalesFact> {
        self.facts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(sku: u64, units: u64) -> CollectedSalesFact {
        CollectedSalesFact {
            business_date: NaiveDate::from_ymd_opt(2026, 9, 26).unwrap(),
            sku,
            ordered_units: units,
            operational_gmv_minor: units * 100,
            cancelled_units: Some(0),
            returned_units: None,
        }
    }

    #[test]
    fn only_exact_zero_cross_page_repeats_can_reach_control_validation() {
        let mut pages = SalesPages::default();
        assert!(pages.add(vec![row(1, 2), row(2, 0)]));
        assert!(pages.add(vec![row(2, 0), row(3, 1)]));
        assert!(pages.zero_overlap);
        let day = row(1, 0).business_date;
        assert!(pages.matches(&SalesTotals::from([(day, (3, 300))])));
        assert!(!pages.matches(&SalesTotals::from([(day, (4, 300))])));
        assert!(!pages.matches(&SalesTotals::from([(day, (3, 301))])));
        assert!(!pages.matches(&SalesTotals::new()));
        assert_eq!(pages.into_facts().len(), 3);
        for duplicate in [row(1, 2), row(1, 0), row(2, 1)] {
            let mut pages = SalesPages::default();
            assert!(pages.add(vec![row(1, 2), row(2, 0)]));
            assert!(!pages.add(vec![duplicate]));
        }
        let mut pages = SalesPages::default();
        assert!(!pages.add(vec![row(1, 0), row(1, 0)]));
    }

    #[test]
    fn unknown_events_are_not_equated_with_zero_and_totals_cannot_overflow() {
        let mut pages = SalesPages::default();
        assert!(pages.add(vec![row(1, 0)]));
        let mut unknown = row(1, 0);
        unknown.cancelled_units = None;
        assert!(!pages.add(vec![unknown]));
        let mut pages = SalesPages::default();
        let mut huge = row(1, 0);
        huge.ordered_units = u64::MAX;
        assert!(pages.add(vec![huge, row(2, 1)]));
        assert!(!pages.matches(&SalesTotals::new()));
    }
}
