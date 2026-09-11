use super::{WbAutomationCampaignLease, WbAutomationPostgresError};
use chrono::{DateTime, Duration, Utc};

impl WbAutomationCampaignLease<'_> {
    /// Startup is permitted only after two recent observations by the already
    /// registered robot, under the exact target policy and account lease.
    /// A healthy container or an operator-supplied boolean is not evidence.
    pub(in crate::control) async fn verify_launch_cycles(
        &self,
        digest: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, WbAutomationPostgresError> {
        let client = self
            .client
            .as_ref()
            .ok_or(WbAutomationPostgresError::Unavailable)?;
        let rows = client
            .query(
                "SELECT policy_digest, observed_at FROM wb_automation.cycles \
             WHERE account_id=$1 AND advert_id=$2 ORDER BY observed_at DESC LIMIT 2",
                &[&self.account_id, &self.campaign_id],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        if rows.len() != 2 || rows.iter().any(|row| row.get::<_, &str>(0) != digest) {
            return Ok(false);
        }
        let latest: DateTime<Utc> = rows[0].get(1);
        let preceding: DateTime<Utc> = rows[1].get(1);
        Ok(cycle_times_are_fresh(latest, preceding, now))
    }
}

fn cycle_times_are_fresh(
    latest: DateTime<Utc>,
    preceding: DateTime<Utc>,
    now: DateTime<Utc>,
) -> bool {
    // Preserve subsecond precision: num_seconds truncates negative fractions
    // to zero and would accept future timestamps (including PostgreSQL's
    // microsecond round-trip of a nanosecond-resolution observation).
    (Duration::zero()..=Duration::seconds(90)).contains(&(now - latest))
        && (Duration::seconds(240)..=Duration::seconds(420)).contains(&(latest - preceding))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cycle_freshness_preserves_fractional_boundaries() {
        let now = DateTime::from_timestamp(1_800_000_000, 123_456_789).unwrap();
        let ns = Duration::nanoseconds(1);
        for age in [Duration::zero(), Duration::seconds(90)] {
            for gap in [Duration::seconds(240), Duration::seconds(420)] {
                let latest = now - age;
                assert!(cycle_times_are_fresh(latest, latest - gap, now));
            }
        }
        for age in [
            -ns,
            -Duration::milliseconds(999),
            Duration::seconds(90) + ns,
        ] {
            let latest = now - age;
            assert!(!cycle_times_are_fresh(
                latest,
                latest - Duration::seconds(300),
                now
            ));
        }
        for gap in [
            Duration::seconds(240) - ns,
            Duration::seconds(420) + ns,
            -ns,
        ] {
            assert!(!cycle_times_are_fresh(now, now - gap, now));
        }
        let stored = DateTime::from_timestamp_micros(now.timestamp_micros()).unwrap();
        assert!(!cycle_times_are_fresh(
            stored,
            stored - Duration::seconds(300),
            now - Duration::seconds(1)
        ));
    }
}
