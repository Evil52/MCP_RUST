//! Explicit, audited reconciliation for the observed one-ruble WB aggregate gap.
//! This is not a claimed WB rounding contract and never changes source amounts.
use std::collections::BTreeMap;

use super::{
    CollectedSalesFact, NaiveDate, WbReportSource, WbReportSourceError, checkpointed, json,
};
use crate::reporting::{
    sales_integrity::SalesTotals,
    wb_adapter::{parse_sales_control_totals, parse_sales_history},
};

impl WbReportSource {
    pub(super) async fn verify_whole_ruble_difference(
        &self,
        date: NaiveDate,
        facts: &[CollectedSalesFact],
        control: &SalesTotals,
    ) -> Result<(), WbReportSourceError> {
        let (units, amount) =
            candidate(date, facts, control).ok_or(WbReportSourceError::SalesPageOverlap)?;
        let positive: Vec<_> = facts.iter().filter(|row| row.ordered_units > 0).collect();
        for chunk in positive.chunks(20) {
            let ids: Vec<_> = chunk.iter().map(|row| row.sku).collect();
            let history: Vec<CollectedSalesFact> = checkpointed(
                &self.checkpoints,
                json!(["wb-sales-reconcile-history-v1", date, ids]),
                || async {
                    parse_sales_history(&self.transport.sales_history(date, ids.clone()).await?)
                        .map_err(|_| WbReportSourceError::SalesPageOverlap)
                },
            )
            .await?;
            let expected: BTreeMap<_, _> = chunk
                .iter()
                .map(|row| (row.sku, (row.ordered_units, row.operational_gmv_minor)))
                .collect();
            if history.len() != expected.len()
                || history.iter().any(|row| {
                    row.business_date != date
                        || expected.get(&row.sku)
                            != Some(&(row.ordered_units, row.operational_gmv_minor))
                })
            {
                return Err(WbReportSourceError::SalesPageOverlap);
            }
        }
        // A distinct HTTP checkpoint certifies that the independent account
        // totals stayed unchanged throughout all SKU history requests. This
        // small certificate is copied to the snapshot audit before page cleanup.
        let evidence: serde_json::Value = checkpointed(
            &self.checkpoints,
            json!(["wb-sales-reconcile-stable-control-v1", date]),
            || async {
                let response = self.transport.sales_control_totals(date).await?;
                let final_control = parse_sales_control_totals(&response, date)
                    .map_err(|_| WbReportSourceError::SalesPageOverlap)?;
                if &final_control != control {
                    return Err(WbReportSourceError::SalesPageOverlap);
                }
                Ok(
                    json!({"kind":"wb_sales_whole_ruble_v1", "business_date":date,
                    "ordered_units":units, "sku_gmv_minor":amount,
                    "group_gmv_minor":control[&date].1, "verified_skus":positive.len(),
                    "history_matches":true, "group_stable":true}),
                )
            },
        )
        .await?;
        tracing::warn!(%date, evidence = %evidence,
            "WB whole-ruble aggregate difference retained with independent SKU and stable group verification");
        Ok(())
    }
}

fn candidate(
    date: NaiveDate,
    facts: &[CollectedSalesFact],
    control: &SalesTotals,
) -> Option<(u64, u64)> {
    if control.len() != 1 {
        return None;
    }
    let expected = control.get(&date)?;
    let mut total = (0_u64, 0_u64);
    for row in facts {
        if row.business_date != date
            || row.operational_gmv_minor % 100 != 0
            || (row.ordered_units == 0 && row.operational_gmv_minor != 0)
        {
            return None;
        }
        total.0 = total.0.checked_add(row.ordered_units)?;
        total.1 = total.1.checked_add(row.operational_gmv_minor)?;
    }
    (total.0 > 0
        && total.0 == expected.0
        && expected.1 % 100 == 0
        && total.1.abs_diff(expected.1) == 100)
        .then_some(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_one_whole_ruble_with_exact_positive_orders_is_a_candidate() {
        let date = NaiveDate::from_ymd_opt(2026, 9, 27).unwrap();
        let row = CollectedSalesFact {
            business_date: date,
            sku: 1,
            ordered_units: 2,
            operational_gmv_minor: 10000,
            cancelled_units: None,
            returned_units: None,
        };
        let control = SalesTotals::from([(date, (2, 10100))]);
        assert_eq!(
            candidate(date, std::slice::from_ref(&row), &control),
            Some((2, 10000))
        );
        for (units, gmv) in [
            (0, 10000),
            (1, 10000),
            (2, 9999),
            (2, 9900),
            (u64::MAX, 10000),
        ] {
            let bad = CollectedSalesFact {
                ordered_units: units,
                operational_gmv_minor: gmv,
                ..row
            };
            assert!(candidate(date, &[bad], &control).is_none());
        }
        assert!(candidate(date, std::slice::from_ref(&row), &SalesTotals::new()).is_none());
        let other = date.succ_opt().unwrap();
        assert!(candidate(other, std::slice::from_ref(&row), &control).is_none());
        assert!(
            candidate(
                other,
                std::slice::from_ref(&row),
                &SalesTotals::from([(other, (2, 10100))])
            )
            .is_none()
        );
        let huge = CollectedSalesFact {
            ordered_units: u64::MAX,
            ..row
        };
        assert!(candidate(date, &[huge, row.clone()], &control).is_none());
        let huge = CollectedSalesFact {
            operational_gmv_minor: u64::MAX - 15,
            ..row
        };
        assert!(candidate(date, &[huge, row], &control).is_none());
    }
}
