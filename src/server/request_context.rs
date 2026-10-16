//! Verified identity and registry attached to an MCP request.
use super::{AccessRegistry, Arc, AuthenticatedActor, RequestContext, RequestRegistry, RoleServer};

pub(super) fn authenticated_actor(
    context: &RequestContext<RoleServer>,
) -> Option<&AuthenticatedActor> {
    context.extensions.get::<AuthenticatedActor>().or_else(|| {
        context
            .extensions
            .get::<axum::http::request::Parts>()
            .and_then(|parts| parts.extensions.get::<AuthenticatedActor>())
    })
}

pub(super) fn request_registry(context: &RequestContext<RoleServer>) -> Option<RequestRegistry> {
    context
        .extensions
        .get::<RequestRegistry>()
        .cloned()
        .or_else(|| {
            context
                .extensions
                .get::<axum::http::request::Parts>()
                .and_then(|parts| parts.extensions.get::<Arc<AccessRegistry>>())
                .map(|registry| RequestRegistry::Loaded(Arc::clone(registry)))
        })
}

pub(super) fn request_headers(context: &RequestContext<RoleServer>) -> axum::http::HeaderMap {
    context
        .extensions
        .get::<axum::http::request::Parts>()
        .map(|parts| parts.headers.clone())
        .unwrap_or_default()
}
