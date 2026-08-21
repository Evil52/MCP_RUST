BEGIN;

-- Automatic collection remains limited to the thirty-minute scheduler
-- window.  A bounded operator recovery may, however, start and observe
-- current state for up to 24 hours after the logical cutoff.  Preserve the
-- real observation time instead of backdating recovered facts.
ALTER TABLE daily_reporting.source_snapshots
    DROP CONSTRAINT source_snapshots_observation_window_check,
    DROP CONSTRAINT source_snapshots_check1;

ALTER TABLE daily_reporting.source_snapshots
    ADD CONSTRAINT source_snapshots_observation_window_check
        CHECK (source_as_of <= cutoff_at + interval '24 hours'),
    ADD CONSTRAINT source_snapshots_recovery_start_window_check
        CHECK (started_at <= cutoff_at + interval '24 hours');

COMMIT;
