#![forbid(unsafe_code)]

use std::{collections::BTreeMap, sync::Arc};

use anyhow::Result;
use mcp_ozon::{
    auth::JwtAuthenticator,
    config::TransportMode,
    control::{
        ControlAppConfig, ControlAuthConfig, ControlMcp, WbBidWriteClient, WbControlServices,
        WbPlanRepository,
    },
    http::build_router_for_server_with_cancellation_and_session_idle_timeout,
    runtime::{
        HTTP_CANCELLED_DRAIN_TIMEOUT, HTTP_HEADER_READ_TIMEOUT, HTTP_MAX_CONNECTIONS,
        HTTP_NATURAL_DRAIN_TIMEOUT, run_http_until_bounded_shutdown, serve_hardened_http,
    },
    wb::{WbClient, WbCredentials},
};
use rmcp::ServiceExt;
use tokio::{signal, sync::Semaphore};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mcp_ozon::control=info,rmcp=info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();

    // ControlAppConfig never loads `.env`. A write token can only be read from
    // the explicitly mounted Control-only credential file when every gate is on.
    let config = ControlAppConfig::from_env()?;
    let wb_services = match &config.wb_runtime {
        Some(runtime) => {
            let accounts = BTreeMap::from([(
                runtime.account_id.clone(),
                WbCredentials {
                    token: runtime.reader_token.clone(),
                },
            )]);
            let reader = WbClient::new_with_https_proxy(
                runtime.request_timeout,
                accounts,
                &runtime.proxy_url,
            )?;
            let writer = runtime
                .writer_token
                .as_deref()
                .map(|token| {
                    WbBidWriteClient::new(runtime.request_timeout, token, &runtime.proxy_url)
                        .map(Arc::new)
                })
                .transpose()?;
            let plans = WbPlanRepository::connect(&runtime.database)
                .await
                .map_err(|_| anyhow::anyhow!("Control plan store недоступен"))?;
            plans.verify_runtime_contract().await.map_err(|_| {
                anyhow::anyhow!("роль control_writer не соответствует runtime contract")
            })?;
            plans
                .register_policy(
                    config.policy.version,
                    config.policy.revision,
                    config.policy.digest(),
                    chrono::Utc::now(),
                )
                .await
                .map_err(|_| {
                    anyhow::anyhow!("Control policy revision rollback/reuse отклонён plan store")
                })?;
            Some(WbControlServices {
                account_id: runtime.account_id.clone(),
                seller_sid: runtime.seller_sid.clone(),
                reader: Arc::new(reader),
                writer,
                plans: Arc::new(plans),
            })
        }
        None => None,
    };
    if config.wb_runtime.is_none()
        && let Some(policy_database) = &config.policy_database
    {
        let plans = WbPlanRepository::connect(&policy_database.database)
            .await
            .map_err(|_| anyhow::anyhow!("Control policy store недоступен"))?;
        plans.verify_runtime_contract().await.map_err(|_| {
            anyhow::anyhow!("роль control_writer не соответствует policy store contract")
        })?;
        plans
            .register_policy(
                config.policy.version,
                config.policy.revision,
                config.policy.digest(),
                chrono::Utc::now(),
            )
            .await
            .map_err(|_| {
                anyhow::anyhow!("Control disabled policy tombstone rollback/reuse отклонён")
            })?;
    }
    let registry = config.registry.clone();
    let mut server = match &config.auth {
        ControlAuthConfig::Dev { actor_id } => {
            ControlMcp::new_disabled(actor_id.clone(), registry, config.policy)
        }
        ControlAuthConfig::Jwt(jwt_config) => {
            let authenticator = JwtAuthenticator::new(jwt_config.clone(), registry.clone())?;
            ControlMcp::new_authenticated_disabled(registry, config.policy, authenticator)
        }
    };
    if let Some(services) = wb_services {
        server = server.with_wb_control_services(services);
    }

    if server.transport_authenticator().is_none() {
        tracing::warn!("Control MCP работает в dev/no-auth режиме; marketplace writes fail-closed");
    }
    match config.transport {
        TransportMode::Http => {
            let cancellation_token = CancellationToken::new();
            let router = build_router_for_server_with_cancellation_and_session_idle_timeout(
                server,
                config.max_sessions,
                config.session_idle_timeout,
                cancellation_token.clone(),
            );
            let listener = tokio::net::TcpListener::bind(config.bind).await?;
            let (graceful_tx, graceful_rx) = tokio::sync::oneshot::channel();
            let serve = serve_hardened_http(
                listener,
                router,
                graceful_rx,
                Arc::new(Semaphore::new(HTTP_MAX_CONNECTIONS)),
                HTTP_HEADER_READ_TIMEOUT,
            );
            let result = run_http_until_bounded_shutdown(
                Box::pin(serve),
                Box::pin(shutdown_signal()),
                graceful_tx,
                cancellation_token,
                HTTP_NATURAL_DRAIN_TIMEOUT,
                HTTP_CANCELLED_DRAIN_TIMEOUT,
            )
            .await;
            if let Some(result) = result {
                result?;
            }
            Ok(())
        }
        TransportMode::Stdio => {
            let service = server.serve(rmcp::transport::stdio()).await?;
            service.waiting().await?;
            Ok(())
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let ctrl_c = async {
            if signal::ctrl_c().await.is_err() {
                std::future::pending::<()>().await;
            }
        };
        let terminate = async {
            match signal::unix::signal(signal::unix::SignalKind::terminate()) {
                Ok(mut stream) => {
                    let _ = stream.recv().await;
                }
                Err(_) => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            _ = ctrl_c => {}
            _ = terminate => {}
        }
    }

    #[cfg(not(unix))]
    if signal::ctrl_c().await.is_err() {
        std::future::pending::<()>().await;
    }
}
