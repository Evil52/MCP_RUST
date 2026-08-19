BEGIN;

-- A marketplace response is observed when collection completes, not at the
-- logical 08:00/17:00 report cutoff. Preserve that truth while keeping the
-- already documented thirty-minute publication window bounded.
ALTER TABLE daily_reporting.source_snapshots
    DROP CONSTRAINT source_snapshots_check,
    DROP CONSTRAINT source_snapshots_check2;

ALTER TABLE daily_reporting.source_snapshots
    ADD CONSTRAINT source_snapshots_observation_window_check
        CHECK (source_as_of <= cutoff_at + interval '30 minutes'),
    ADD CONSTRAINT source_snapshots_period_window_check
        CHECK (
            (source IN ('sales', 'advertising')
                AND period_start < period_end
                AND period_end <= cutoff_at)
            OR
            (source IN ('stocks', 'prices')
                AND period_start = period_end
                AND period_end = source_as_of)
        );

COMMIT;
