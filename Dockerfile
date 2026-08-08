FROM rust:1.97.1-bookworm@sha256:14bc9c5966e7b3a385794b3d5389a8765668342025fbcc7b2e3d2866ac4bd8c3 AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake clang \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release

FROM debian:bookworm-slim@sha256:abd67ffcfa541b485a3dff59865ab629aa048a6c613e639d36e7456b0b229241 AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home mcp

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
