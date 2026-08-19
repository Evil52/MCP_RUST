BEGIN;

-- A morning occurrence normally closes at 14:00 EKB. If the service was
-- unavailable through that window, the scheduler may recover it only after
-- the 17:00 EKB boundary and gives it the same 23:00 EKB terminal deadline as
-- the evening occurrence. The two periods remain separate delivery rows and
-- artifacts; no report is allowed to claim coverage for the other interval.
DO $$
DECLARE
    schedule_constraint name;
BEGIN
    SELECT constraint_row.conname
    INTO schedule_constraint
    FROM pg_constraint AS constraint_row
    WHERE constraint_row.conrelid =
            'daily_reporting.delivery_coverage'::regclass
      AND constraint_row.contype = 'c'
      AND pg_get_constraintdef(constraint_row.oid) LIKE '%14:00%'
      AND pg_get_constraintdef(constraint_row.oid) LIKE '%23:00%';

    IF schedule_constraint IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'undefined_object',
            MESSAGE = 'delivery coverage schedule constraint is unavailable';
    END IF;

    EXECUTE format(
        'ALTER TABLE daily_reporting.delivery_coverage DROP CONSTRAINT %I',
        schedule_constraint
    );
END;
$$;

ALTER TABLE daily_reporting.delivery_coverage
    ADD CONSTRAINT delivery_coverage_schedule_check CHECK (
        (
            report_kind = 'morning'
            AND (
                (
                    scheduled_for =
                        ((local_date + time '08:00') AT TIME ZONE 'Asia/Yekaterinburg')
                    AND deadline_at =
                        ((local_date + time '14:00') AT TIME ZONE 'Asia/Yekaterinburg')
                )
                OR (
                    scheduled_for =
                        ((local_date + time '17:00') AT TIME ZONE 'Asia/Yekaterinburg')
                    AND deadline_at =
                        ((local_date + time '23:00') AT TIME ZONE 'Asia/Yekaterinburg')
                )
            )
        )
        OR (
            report_kind = 'evening'
            AND scheduled_for =
                ((local_date + time '17:00') AT TIME ZONE 'Asia/Yekaterinburg')
            AND deadline_at =
                ((local_date + time '23:00') AT TIME ZONE 'Asia/Yekaterinburg')
        )
    );

COMMIT;
