FROM rust:1.97.1-alpine3.23@sha256:c4a364ddbf684fe038e6fa6a4f25b30c8dc85247423e0e660676ece0d17be4a2 AS builder

RUN apk add --no-cache clang cmake

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release

FROM alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40 AS runtime

RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 mcp \
    && adduser -S -D -H -u 10001 -G mcp mcp

COPY --from=builder /build/target/release/mcp-ozon /usr/local/bin/mcp-ozon
ARG ACCESS_CONFIG=config/access.example.json
COPY ${ACCESS_CONFIG} /etc/mcp-ozon/access.json

ENV MCP_TRANSPORT=http \
    MCP_BIND=0.0.0.0:8787 \
    OZON_API_BASE_URL=https://api-seller.ozon.ru \
    OZON_REQUEST_TIMEOUT_SECONDS=30 \
    RUST_LOG=mcp_ozon=info,rmcp=info
ENV MCP_ACCESS_CONFIG=/etc/mcp-ozon/access.json

EXPOSE 8787
USER mcp

ENTRYPOINT ["/usr/local/bin/mcp-ozon"]
