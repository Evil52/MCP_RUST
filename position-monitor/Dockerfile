FROM postgres:17-alpine@sha256:d4bb0a8c1b7bb2e29f976d099e7bfb9a5d8858cffe9e46b35cd302cd1f1f8168

COPY --chmod=0644 initdb/001_schema.sql /docker-entrypoint-initdb.d/001_schema.sql
COPY --chmod=0644 initdb/002_ozon_collector_contract.sql /docker-entrypoint-initdb.d/002_ozon_collector_contract.sql
COPY --chmod=0644 initdb/002_wb_official_history.sql /docker-entrypoint-initdb.d/002_wb_official_history.sql
COPY --chmod=0755 initdb/003_roles.sh /docker-entrypoint-initdb.d/003_roles.sh
COPY --chmod=0644 initdb/004_ozon_postgres_adapter.sql /docker-entrypoint-initdb.d/004_ozon_postgres_adapter.sql
COPY --chmod=0644 initdb/005_daily_reporting_outbox.sql /docker-entrypoint-initdb.d/005_daily_reporting_outbox.sql
COPY --chmod=0644 initdb/006_daily_report_snapshots.sql /docker-entrypoint-initdb.d/006_daily_report_snapshots.sql
COPY --chmod=0644 initdb/007_daily_reporting_optional_metrics.sql /docker-entrypoint-initdb.d/007_daily_reporting_optional_metrics.sql
COPY --chmod=0644 initdb/008_daily_reporting_artifact_identity.sql /docker-entrypoint-initdb.d/008_daily_reporting_artifact_identity.sql
COPY --chmod=0644 initdb/009_daily_reporting_generation_backoff.sql /docker-entrypoint-initdb.d/009_daily_reporting_generation_backoff.sql
COPY --chmod=0644 initdb/010_daily_reporting_observation_window.sql /docker-entrypoint-initdb.d/010_daily_reporting_observation_window.sql
COPY --chmod=0644 initdb/011_daily_reporting_collection_claims.sql /docker-entrypoint-initdb.d/011_daily_reporting_collection_claims.sql
COPY --chmod=0644 initdb/012_daily_reporting_delivery_error_classes.sql /docker-entrypoint-initdb.d/012_daily_reporting_delivery_error_classes.sql
COPY --chmod=0644 initdb/013_daily_reporting_delivery_reconciliation.sql /docker-entrypoint-initdb.d/013_daily_reporting_delivery_reconciliation.sql
COPY --chmod=0644 initdb/014_daily_reporting_period_preserving_catchup.sql /docker-entrypoint-initdb.d/014_daily_reporting_period_preserving_catchup.sql
COPY --chmod=0755 healthcheck.sh /usr/local/bin/position-db-healthcheck

# This image always runs as postgres, so the root-only privilege-drop helper is
# unreachable. Removing it also avoids shipping its independently compiled Go
# runtime and keeps the production database image's executable surface minimal.
RUN rm -f -- /usr/local/bin/gosu

# The entrypoint drops to this user anyway once it has prepared the data
# directory; declaring it keeps the container off root and satisfies DS-0002.
# Verified against a fresh named volume: initdb still completes.
USER postgres
