FROM postgres:17-alpine

COPY --chmod=0644 initdb/001_schema.sql /docker-entrypoint-initdb.d/001_schema.sql
COPY --chmod=0755 initdb/002_roles.sh /docker-entrypoint-initdb.d/002_roles.sh
