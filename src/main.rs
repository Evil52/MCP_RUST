use std::sync::Arc;

use anyhow::Result;
use axum::{Json, Router, middleware, routing::get};
use mcp_ozon::{
    auth::{JwtAuthenticator, require_jwt},
    config::{AppConfig, AuthConfig, TransportMode},
    ozon::OzonClient,
    server::OzonMcp,
};
use rmcp::{
    ServiceExt,
    transport::{
        StreamableHttpServerConfig, StreamableHttpService,
        streamable_http_server::session::local::LocalSessionManager,
    },
};
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mcp_ozon=info,rmcp=info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();

    let config = AppConfig::from_env()?;
    let client = OzonClient::new(
        config.ozon_api_base_url.clone(),
        config.request_timeout,
        config.stores,
    )?;
    let registry = config.registry.clone();
    let server = match &config.auth {
        AuthConfig::Dev { actor_id } => OzonMcp::new(client, actor_id.clone(), config.registry),
        AuthConfig::Jwt(_) => OzonMcp::new_authenticated(client, config.registry),
    };

    match config.transport {
        TransportMode::Http => serve_http(config.bind, server, config.auth, registry).await,
        TransportMode::Stdio => {
            if matches!(config.auth, AuthConfig::Jwt(_)) {
                anyhow::bail!("MCP_AUTH_MODE=jwt поддерживается только с MCP_TRANSPORT=http");
            }
            serve_stdio(server).await
        }
    }
}

async fn serve_http(
    bind: std::net::SocketAddr,
    server: OzonMcp,
    auth_config: AuthConfig,
    registry: mcp_ozon::config::RegistrySource,
) -> Result<()> {
    let server = Arc::new(server);
    let service: StreamableHttpService<OzonMcp, LocalSessionManager> = StreamableHttpService::new(
        move || Ok((*server).clone()),
        Default::default(),
        StreamableHttpServerConfig::default(),
    );
    let router = match auth_config {
        AuthConfig::Dev { .. } => Router::new()
            .route("/health", get(|| async { "ok" }))
            .nest_service("/mcp", service),
        AuthConfig::Jwt(config) => {
            let authenticator = JwtAuthenticator::new(config, registry)?;
            let metadata = authenticator.protected_resource_metadata();
            let protected = Router::new()
                .nest_service("/mcp", service)
                .route_layer(middleware::from_fn_with_state(authenticator, require_jwt));
            Router::new()
                .route("/health", get(|| async { "ok" }))
                .route(
                    "/.well-known/oauth-protected-resource",
                    get(move || {
                        let metadata = metadata.clone();
                        async move { Json(metadata) }
                    }),
                )
                .merge(protected)
        }
    };
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, endpoint = %format!("http://{bind}/mcp"), "MCP Ozon запущен");

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn serve_stdio(server: OzonMcp) -> Result<()> {
    tracing::info!("MCP Ozon запущен через stdio");
    let service = server.serve(rmcp::transport::stdio()).await?;
    service.waiting().await?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = signal::ctrl_c().await;
}
