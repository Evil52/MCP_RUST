//! Shared schema annotations and authentication metadata for fixed MCP routers.

use super::{
    JsonObject, JwtAuthenticator, MetaObject, OzonMcp, REPORT_REFRESH_WRITE_TOOLS, ToolRouter,
    Value,
};

fn tool_security_schemes(authenticator: Option<&JwtAuthenticator>) -> Vec<JsonObject> {
    let mut scheme = JsonObject::new();
    match authenticator {
        Some(authenticator) => {
            scheme.insert("type".to_owned(), Value::String("oauth2".to_owned()));
            scheme.insert(
                "scopes".to_owned(),
                Value::Array(
                    authenticator
                        .required_scopes()
                        .iter()
                        .cloned()
                        .map(Value::String)
                        .collect(),
                ),
            );
        }
        None => {
            scheme.insert("type".to_owned(), Value::String("noauth".to_owned()));
        }
    }
    vec![scheme]
}

impl OzonMcp {
    pub(super) fn default_tool_router(
        authenticator: Option<&JwtAuthenticator>,
    ) -> ToolRouter<Self> {
        Self::configure_tool_router(Self::build_tool_router(), authenticator)
    }

    pub(super) fn configure_tool_router(
        mut tool_router: ToolRouter<Self>,
        authenticator: Option<&JwtAuthenticator>,
    ) -> ToolRouter<Self> {
        let security_schemes = tool_security_schemes(authenticator);
        let security_schemes_value = Value::Array(
            security_schemes
                .iter()
                .cloned()
                .map(Value::Object)
                .collect(),
        );
        for route in tool_router.map.values_mut() {
            let read_only = !REPORT_REFRESH_WRITE_TOOLS.contains(&route.attr.name.as_ref());
            let annotations = route.attr.annotations.get_or_insert_default();
            annotations.read_only_hint = Some(read_only);
            annotations.destructive_hint = Some(false);
            annotations.idempotent_hint = Some(true);
            annotations.open_world_hint.get_or_insert(true);
            route.attr.security_schemes = Some(security_schemes.clone());
            route
                .attr
                .meta
                .get_or_insert_with(MetaObject::new)
                .0
                .insert("securitySchemes".to_owned(), security_schemes_value.clone());
        }
        tool_router
    }
}
