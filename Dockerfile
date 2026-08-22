FROM rust:1.98.0-alpine3.23@sha256:4743b6231029d726d7a0f81d730a7c9f4eff23225a4499c01e275efb5e260235 AS builder

RUN apk add --no-cache clang cmake

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY vendor ./vendor
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    cargo build --locked --release \
    && cp /build/target/release/mcp-ozon /build/mcp-ozon

FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b AS runtime

RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 mcp \
    && adduser -S -D -H -u 10001 -G mcp mcp

COPY --from=builder /build/mcp-ozon /usr/local/bin/mcp-ozon
COPY config/access.example.json /etc/mcp-ozon/access.json

ENV MCP_TRANSPORT=http \
    MCP_BIND=0.0.0.0:8787 \
    OZON_API_BASE_URL=https://api-seller.ozon.ru \
    OZON_REQUEST_TIMEOUT_SECONDS=30 \
    RUST_LOG=mcp_ozon=info,rmcp=info
ENV MCP_ACCESS_CONFIG=/etc/mcp-ozon/access.json

EXPOSE 8787
USER mcp

ENTRYPOINT ["/usr/local/bin/mcp-ozon"]
