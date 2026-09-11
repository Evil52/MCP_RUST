//! Bounded request bodies and admission for responses that retain a stream.

use super::{
    McpHttpLimits, Method, OwnedSemaphorePermit, Request, Response, buffer_mcp_post_body,
    capacity_exhausted_response,
};

pub(super) async fn prepare_body(
    limits: &McpHttpLimits,
    request: Request,
) -> Result<(Request, Option<OwnedSemaphorePermit>), Box<Response>> {
    if request.method() != Method::POST {
        return Ok((request, None));
    }
    let (request, needs_response) = buffer_mcp_post_body(request, limits.body_read_timeout).await?;
    let permit = if needs_response {
        Some(limits.try_enter_post_response().ok_or_else(|| {
            Box::new(capacity_exhausted_response(
                "MCP response capacity exhausted",
            ))
        })?)
    } else {
        None
    };
    Ok((request, permit))
}
