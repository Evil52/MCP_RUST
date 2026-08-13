FROM postgres:17-alpine@sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193

COPY --chmod=0644 initdb/001_schema.sql /docker-entrypoint-initdb.d/001_schema.sql
COPY --chmod=0755 initdb/002_roles.sh /docker-entrypoint-initdb.d/002_roles.sh
COPY --chmod=0755 healthcheck.sh /usr/local/bin/position-db-healthcheck

# The entrypoint drops to this user anyway once it has prepared the data
# directory; declaring it keeps the container off root and satisfies DS-0002.
# Verified against a fresh named volume: initdb still completes.
USER postgres
