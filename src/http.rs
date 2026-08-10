//! HTTP surface of the MCP server.
//!
//! The router lives here rather than in `main.rs` so tests exercise the exact
//! wiring production runs. Assembling an equivalent router inside a test is not
//! the same guarantee: the copy drifts, and the routes that only exist in the
//! binary — `/health` and the OAuth resource metadata — go unverified.

use std::{num::NonZeroUsize, sync::Arc};

use axum::{Json, Router, routing::get};
use rmcp::transport::{
    StreamableHttpServerConfig, StreamableHttpService,
    streamable_http_server::session::local::LocalSessionManager,
};

use crate::server::OzonMcp;

/// Builds the complete HTTP router: the MCP endpoint, a liveness probe, and —
/// only when the server authenticates requests — the OAuth protected-resource
/// metadata document.
///
/// `max_sessions` bounds concurrently retained MCP sessions, so an
/// unauthenticated client cannot exhaust memory by opening sessions.
pub fn build_router(server: OzonMcp, max_sessions: NonZeroUsize) -> Router {
    let protected_resource_metadata = server.protected_resource_metadata();
    let server = Arc::new(server);
    let session_manager = Arc::new(LocalSessionManager::default().with_max_sessions(max_sessions));
    let service: StreamableHttpService<OzonMcp, LocalSessionManager> = StreamableHttpService::new(
        move || Ok((*server).clone()),
        session_manager,
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
    router
}
