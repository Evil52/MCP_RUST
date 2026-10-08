-- One process may open several sessions per role: one supervised session per
-- repository plus a process-wide marketplace quota session. A second runtime,
-- or a reconnect that races a backend PostgreSQL has not reaped yet, needs
-- more. At exactly 4, one analytics server used every report_refresh_requester
-- slot, so the standalone reporting reader could not start and a reconnect
-- failed with "too many connections for role" (and, through fail-closed tool
-- telemetry, refused every tool call). Keep at least twice the steady demand.
-- 003_roles.sh converges the same limits on every migrator run.
BEGIN;
ALTER ROLE report_refresh_requester CONNECTION LIMIT 12;
ALTER ROLE report_collector CONNECTION LIMIT 8;
COMMIT;
