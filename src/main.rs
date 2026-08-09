use std::sync::Arc;

use anyhow::Result;
use axum::{Json, Router, routing::get};
use mcp_ozon::{
    auth::JwtAuthenticator,
    config::{AppConfig, AuthConfig, TransportMode},
    ozon::OzonClient,
    server::OzonMcp,
    wb::WbClient,
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
    let wb_client = WbClient::new(config.request_timeout, config.wildberries_accounts);
    let registry = config.registry.clone();
    let server = match &config.auth {
        AuthConfig::Dev { actor_id } => OzonMcp::new(client, actor_id.clone(), registry),
        AuthConfig::Jwt(jwt_config) => {
            let authenticator = JwtAuthenticator::new(jwt_config.clone(), registry.clone())?;
            OzonMcp::new_authenticated(client, registry, authenticator)
        }
    }
    .with_wildberries_client(wb_client)
    .with_preview_features(
        config.ozon_postings_vnext,
        config.ozon_finance_accruals_preview,
    );

    match config.transport {
        TransportMode::Http => serve_http(config.bind, server).await,
        TransportMode::Stdio => {
            if matches!(config.auth, AuthConfig::Jwt(_)) {
                anyhow::bail!("MCP_AUTH_MODE=jwt поддерживается только с MCP_TRANSPORT=http");
            }
            serve_stdio(server).await
        }
    }
}

async fn serve_http(bind: std::net::SocketAddr, server: OzonMcp) -> Result<()> {
    let protected_resource_metadata = server.protected_resource_metadata();
    let server = Arc::new(server);
    let service: StreamableHttpService<OzonMcp, LocalSessionManager> = StreamableHttpService::new(
        move || Ok((*server).clone()),
        Default::default(),
        StreamableHttpServerConfig::default(),
    );
    let mut router = Router::new()
        .route("/health", get(|| async { "ok" }))
        .nest_service("/mcp", service);
    if let Some(metadata) = protected_resource_metadata {
        router = router.route(
            "/.well-known/oauth-protected-resource",
            get(move || {
                let metadata = metadata.clone();
                async move { Json(metadata) }
            }),
        );
    }
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
