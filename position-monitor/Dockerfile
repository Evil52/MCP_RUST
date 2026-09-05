FROM postgres:17-alpine3.23@sha256:9ae4e8f8d0284836a505f0b2e825144e32e20499856e7dc5f7b99e19d10eedd6

ARG MCP_RELEASE_GIT_SHA=unverified
LABEL org.opencontainers.image.revision="${MCP_RELEASE_GIT_SHA}"

RUN install -d -m 0755 /opt/mcp-ozon/migrations
COPY --chmod=0644 initdb/*.sql /opt/mcp-ozon/migrations/
COPY --chmod=0755 initdb/003_roles.sh /opt/mcp-ozon/migrations/003_roles.sh
COPY --chmod=0755 migrate.sh /usr/local/bin/migrate-position-db
COPY --chmod=0755 migrate.sh /docker-entrypoint-initdb.d/001_migrate.sh
COPY --chmod=0755 healthcheck.sh /usr/local/bin/position-db-healthcheck

# This image always runs as postgres, so the root-only privilege-drop helper is
# unreachable. Removing it also avoids shipping its independently compiled Go
# runtime and keeps the production database image's executable surface minimal.
RUN apk upgrade --no-cache \
    && rm -f -- /usr/local/bin/gosu

# The entrypoint drops to this user anyway once it has prepared the data
# directory; declaring it keeps the container off root and satisfies DS-0002.
# Verified against a fresh named volume: initdb still completes.
USER postgres
