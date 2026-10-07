//! The business clocks of the product, defined once.
//!
//! Three different "days" exist, and each module used to restate its own
//! offset arithmetic:
//!
//! * daily reports, their schedules and every manager-facing timestamp use
//!   the company calendar, Asia/Yekaterinburg (UTC+5);
//! * WB promotion budgets, the WB ads robot, closed WB finance periods and
//!   both marketplace cabinets use Europe/Moscow (UTC+3);
//! * live marketplace tools that expand a `YYYY-MM-DD` period into Seller API
//!   timestamps cover whole UTC days; their input schemas say so explicitly.
//!
//! Neither zone has observed daylight saving time since 2014, so fixed offsets
//! are exact.

use chrono::{DateTime, FixedOffset, NaiveDate, Utc};

const YEKATERINBURG_UTC_OFFSET_SECONDS: i32 = 5 * 60 * 60;
const MOSCOW_UTC_OFFSET_SECONDS: i32 = 3 * 60 * 60;

/// Asia/Yekaterinburg: daily reports and manager-facing timestamps.
///
/// # Panics
///
/// Never: the offset is a valid compile-time constant.
#[must_use]
pub const fn yekaterinburg() -> FixedOffset {
    FixedOffset::east_opt(YEKATERINBURG_UTC_OFFSET_SECONDS)
        .expect("the fixed Yekaterinburg UTC offset is valid")
}

/// Europe/Moscow: WB promotion budgets, the WB ads robot and cabinets.
///
/// # Panics
///
/// Never: the offset is a valid compile-time constant.
#[must_use]
pub const fn moscow() -> FixedOffset {
    FixedOffset::east_opt(MOSCOW_UTC_OFFSET_SECONDS).expect("the fixed Moscow UTC offset is valid")
}

/// The calendar date of `instant` on the clock of `zone`.
#[must_use]
pub fn date_in(zone: FixedOffset, instant: DateTime<Utc>) -> NaiveDate {
    instant.with_timezone(&zone).date_naive()
}

/// Seconds elapsed since local midnight of `instant` on the clock of `zone`.
#[must_use]
pub fn seconds_since_midnight(zone: FixedOffset, instant: DateTime<Utc>) -> u32 {
    use chrono::Timelike as _;

    instant.with_timezone(&zone).num_seconds_from_midnight()
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn each_business_day_changes_at_its_own_utc_hour() {
        let before = Utc.with_ymd_and_hms(2026, 8, 16, 18, 59, 59).unwrap();
        let after = Utc.with_ymd_and_hms(2026, 8, 16, 19, 0, 0).unwrap();
        let date = |day| NaiveDate::from_ymd_opt(2026, 8, day).unwrap();
        // Yekaterinburg midnight is 19:00 UTC.
        assert_eq!(date_in(yekaterinburg(), before), date(16));
        assert_eq!(date_in(yekaterinburg(), after), date(17));
        // Moscow midnight is 21:00 UTC.
        let moscow_midnight = Utc.with_ymd_and_hms(2026, 8, 16, 21, 0, 0).unwrap();
        assert_eq!(date_in(moscow(), after), date(16));
        assert_eq!(date_in(moscow(), moscow_midnight), date(17));
        assert_eq!(seconds_since_midnight(moscow(), moscow_midnight), 0);
        assert_eq!(seconds_since_midnight(yekaterinburg(), after), 0);
        assert_eq!(seconds_since_midnight(moscow(), before), 21 * 3600 + 3599);
    }
}
