-- Pin a render timestamp only after the worker has loaded complete inputs.
-- Keep NULL for pre-migration batches: their artifacts used created_at.
BEGIN;
ALTER TABLE daily_reporting.delivery_batches
    ADD COLUMN generation_started_at timestamptz;

CREATE FUNCTION daily_reporting.enforce_generation_clock()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.generation_started_at IS NOT NULL THEN
            RAISE EXCEPTION 'generation clock is assigned on generation start';
        END IF;
    ELSIF OLD.status = 'planned' AND NEW.status = 'generating' THEN
        IF NEW.generation_started_at IS DISTINCT FROM OLD.generation_started_at THEN
            RAISE EXCEPTION 'generation clock cannot be supplied by the caller';
        END IF;
        NEW.generation_started_at := clock_timestamp();
    ELSIF NEW.generation_started_at IS DISTINCT FROM OLD.generation_started_at THEN
        RAISE EXCEPTION 'generation clock is immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER delivery_batch_generation_clock
BEFORE INSERT OR UPDATE ON daily_reporting.delivery_batches
FOR EACH ROW EXECUTE FUNCTION daily_reporting.enforce_generation_clock();
REVOKE ALL ON FUNCTION daily_reporting.enforce_generation_clock() FROM PUBLIC;
COMMIT;
