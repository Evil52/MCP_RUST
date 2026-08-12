//! HTTP surface of the MCP server.
//!
//! The router lives here rather than in `main.rs` so tests exercise the exact
//! wiring production runs. Assembling an equivalent router inside a test is not
//! the same guarantee: the copy drifts, and the routes that only exist in the
//! binary — `/health` and the OAuth resource metadata — go unverified.

use std::{
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::{
    Json, Router,
    body::{Body, HttpBody},
    extract::{Request, State},
    http::{
        HeaderValue, Method, StatusCode,
        header::{CONTENT_TYPE, RETRY_AFTER},
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use http_body::{Frame, SizeHint};
use rmcp::transport::{
    StreamableHttpServerConfig, StreamableHttpService,
    streamable_http_server::session::local::LocalSessionManager,
};
use serde::de::{self, Deserialize, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::server::OzonMcp;

/// Maximum JSON body accepted by the MCP endpoint.
///
/// Tool schemas already bound pages, arrays, strings and opaque cursors.
/// 256 KiB leaves ample room for the intended bounded tool schemas while
/// limiting aggregate parser memory across the 32-slot ingress gate. Larger
/// bulk work must be split across bounded pages.
pub const MCP_REQUEST_BODY_LIMIT_BYTES: usize = 262_144;

/// Total time allowed to receive one MCP POST body.
///
/// The deadline covers the entire streamed body rather than resetting for
/// every chunk, so a client cannot retain one execution permit indefinitely
/// by sending bytes just before an idle timeout.
const MCP_REQUEST_BODY_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum number of MCP requests executing inside the HTTP service at once.
///
/// This covers request parsing and execution through construction of the HTTP
/// response. Result-bearing POST bodies and long-lived session GET streams use
/// separate budgets, so slow readers cannot retain ingress capacity needed for
/// cancellation notifications. `/health` remains outside the gate so
/// orchestration can observe overload.
pub const MCP_MAX_IN_FLIGHT_REQUESTS: usize = 32;

/// Maximum number of result-bearing POST response bodies retained at once.
///
/// This matches the global tool-call budget. Well-formed JSON-RPC notifications
/// and client responses do not consume this budget, leaving ingress headroom
/// for a client to cancel work whose response body it no longer needs.
pub const MCP_MAX_IN_FLIGHT_POST_RESPONSES: usize = 16;

/// Maximum number of long-lived MCP GET/SSE streams.
///
/// Session GET streams use their own budget so they cannot consume the POST
/// execution permits needed for tool calls and session control messages.
pub const MCP_MAX_IN_FLIGHT_STREAMS: usize = 64;

#[derive(Clone)]
struct McpHttpLimits {
    request_permits: Arc<Semaphore>,
    post_response_permits: Arc<Semaphore>,
    stream_permits: Arc<Semaphore>,
    body_read_timeout: Duration,
}

struct PermitBody {
    inner: Body,
    permit: Option<OwnedSemaphorePermit>,
}

impl HttpBody for PermitBody {
    type Data = <Body as HttpBody>::Data;
    type Error = <Body as HttpBody>::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let frame = Pin::new(&mut this.inner).poll_frame(cx);
        if matches!(&frame, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
            this.permit.take();
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl McpHttpLimits {
    fn production() -> Self {
        Self {
            request_permits: Arc::new(Semaphore::new(MCP_MAX_IN_FLIGHT_REQUESTS)),
            post_response_permits: Arc::new(Semaphore::new(MCP_MAX_IN_FLIGHT_POST_RESPONSES)),
            stream_permits: Arc::new(Semaphore::new(MCP_MAX_IN_FLIGHT_STREAMS)),
            body_read_timeout: MCP_REQUEST_BODY_READ_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn for_test(
        requests: usize,
        post_responses: usize,
        streams: usize,
        body_read_timeout: Duration,
    ) -> Self {
        Self {
            request_permits: Arc::new(Semaphore::new(requests)),
            post_response_permits: Arc::new(Semaphore::new(post_responses)),
            stream_permits: Arc::new(Semaphore::new(streams)),
            body_read_timeout,
        }
    }

    fn try_enter_request(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.request_permits).try_acquire_owned().ok()
    }

    fn try_enter_stream(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.stream_permits).try_acquire_owned().ok()
    }

    fn try_enter_post_response(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.post_response_permits)
            .try_acquire_owned()
            .ok()
    }
}

#[derive(Clone, Copy)]
enum McpBodyReadFailure {
    TooLarge,
    Transport,
}

async fn read_bounded_mcp_body(mut body: Body) -> Result<Vec<u8>, McpBodyReadFailure> {
    let mut bytes = Vec::new();
    loop {
        let frame = std::future::poll_fn(|context| Pin::new(&mut body).poll_frame(context)).await;
        let Some(frame) = frame else {
            break;
        };
        let frame = frame.map_err(|_| McpBodyReadFailure::Transport)?;
        let data = frame.data_ref().map_or(&[][..], |data| data.as_ref());
        if data.len() > MCP_REQUEST_BODY_LIMIT_BYTES.saturating_sub(bytes.len()) {
            return Err(McpBodyReadFailure::TooLarge);
        }
        bytes.extend_from_slice(data);
    }
    Ok(bytes)
}

fn body_failure_response(status: StatusCode, message: &'static str) -> Response {
    (status, message).into_response()
}

#[derive(serde::Deserialize)]
#[serde(field_identifier)]
enum JsonRpcEnvelopeField {
    #[serde(rename = "jsonrpc")]
    JsonRpc,
    #[serde(rename = "method")]
    Method,
    #[serde(rename = "id")]
    Id,
    #[serde(rename = "params")]
    Params,
    #[serde(rename = "result")]
    Result,
    #[serde(rename = "error")]
    Error,
    #[serde(other)]
    Unknown,
}

struct JsonRpcVersion;

impl<'de> Deserialize<'de> for JsonRpcVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VersionVisitor;

        impl Visitor<'_> for VersionVisitor {
            type Value = JsonRpcVersion;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("the JSON-RPC version 2.0")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value == "2.0" {
                    Ok(JsonRpcVersion)
                } else {
                    Err(E::custom("unsupported JSON-RPC version"))
                }
            }
        }

        deserializer.deserialize_str(VersionVisitor)
    }
}

struct JsonRpcMethod;

impl<'de> Deserialize<'de> for JsonRpcMethod {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MethodVisitor;

        impl Visitor<'_> for MethodVisitor {
            type Value = JsonRpcMethod;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON-RPC method string")
            }

            fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(JsonRpcMethod)
            }
        }

        deserializer.deserialize_str(MethodVisitor)
    }
}

struct JsonRpcParams;

impl<'de> Deserialize<'de> for JsonRpcParams {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ParamsVisitor;

        impl<'de> Visitor<'de> for ParamsVisitor {
            type Value = JsonRpcParams;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON-RPC params object or array")
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
                Ok(JsonRpcParams)
            }

            fn visit_seq<S>(self, mut sequence: S) -> Result<Self::Value, S::Error>
            where
                S: SeqAccess<'de>,
            {
                while sequence.next_element::<IgnoredAny>()?.is_some() {}
                Ok(JsonRpcParams)
            }
        }

        deserializer.deserialize_any(ParamsVisitor)
    }
}

struct JsonRpcErrorObject;

#[derive(serde::Deserialize)]
#[serde(field_identifier)]
enum JsonRpcErrorField {
    #[serde(rename = "code")]
    Code,
    #[serde(rename = "message")]
    Message,
    #[serde(rename = "data")]
    Data,
    #[serde(other)]
    Unknown,
}

struct JsonRpcErrorCode;

impl<'de> Deserialize<'de> for JsonRpcErrorCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ErrorCodeVisitor;

        impl Visitor<'_> for ErrorCodeVisitor {
            type Value = JsonRpcErrorCode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a signed 64-bit JSON-RPC error code")
            }

            fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(JsonRpcErrorCode)
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                i64::try_from(value)
                    .map(|_| JsonRpcErrorCode)
                    .map_err(|_| E::custom("JSON-RPC error code exceeds signed 64-bit range"))
            }
        }

        deserializer.deserialize_any(ErrorCodeVisitor)
    }
}

struct JsonRpcErrorMessage;

impl<'de> Deserialize<'de> for JsonRpcErrorMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ErrorMessageVisitor;

        impl Visitor<'_> for ErrorMessageVisitor {
            type Value = JsonRpcErrorMessage;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON-RPC error message string")
            }

            fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(JsonRpcErrorMessage)
            }
        }

        deserializer.deserialize_str(ErrorMessageVisitor)
    }
}

fn reject_duplicate<E>(seen: bool, field: &'static str) -> Result<(), E>
where
    E: de::Error,
{
    if seen {
        Err(E::duplicate_field(field))
    } else {
        Ok(())
    }
}

#[derive(Default)]
struct JsonRpcErrorShape {
    code: bool,
    message: bool,
    data: bool,
}

impl JsonRpcErrorShape {
    fn read_field<'de, M>(&mut self, field: JsonRpcErrorField, map: &mut M) -> Result<(), M::Error>
    where
        M: MapAccess<'de>,
    {
        match field {
            JsonRpcErrorField::Code => {
                reject_duplicate::<M::Error>(self.code, "code")?;
                map.next_value::<JsonRpcErrorCode>()?;
                self.code = true;
            }
            JsonRpcErrorField::Message => {
                reject_duplicate::<M::Error>(self.message, "message")?;
                map.next_value::<JsonRpcErrorMessage>()?;
                self.message = true;
            }
            JsonRpcErrorField::Data => {
                reject_duplicate::<M::Error>(self.data, "data")?;
                map.next_value::<IgnoredAny>()?;
                self.data = true;
            }
            JsonRpcErrorField::Unknown => {
                return Err(de::Error::custom("unknown JSON-RPC error field"));
            }
        }
        Ok(())
    }

    fn finish<E>(self) -> Result<JsonRpcErrorObject, E>
    where
        E: de::Error,
    {
        if !self.code || !self.message {
            return Err(E::custom("JSON-RPC error must contain code and message"));
        }
        Ok(JsonRpcErrorObject)
    }
}

impl<'de> Deserialize<'de> for JsonRpcErrorObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ErrorVisitor;

        impl<'de> Visitor<'de> for ErrorVisitor {
            type Value = JsonRpcErrorObject;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON-RPC error object")
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut shape = JsonRpcErrorShape::default();
                while let Some(field) = map.next_key::<JsonRpcErrorField>()? {
                    shape.read_field(field, &mut map)?;
                }
                shape.finish()
            }
        }

        deserializer.deserialize_map(ErrorVisitor)
    }
}

#[derive(Clone, Copy)]
enum JsonRpcIdShape {
    Null,
    NonNull,
}

impl<'de> Deserialize<'de> for JsonRpcIdShape {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct IdVisitor;

        impl Visitor<'_> for IdVisitor {
            type Value = JsonRpcIdShape;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a string, signed 64-bit integer, or null JSON-RPC id")
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(JsonRpcIdShape::Null)
            }

            fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(JsonRpcIdShape::NonNull)
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                i64::try_from(value)
                    .map(|_| JsonRpcIdShape::NonNull)
                    .map_err(|_| E::custom("JSON-RPC id exceeds signed 64-bit range"))
            }

            fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(JsonRpcIdShape::NonNull)
            }
        }

        deserializer.deserialize_any(IdVisitor)
    }
}

#[derive(Default)]
struct JsonRpcEnvelopeShape {
    jsonrpc: bool,
    method: bool,
    id: Option<JsonRpcIdShape>,
    params: bool,
    result: bool,
    error: bool,
    unknown: bool,
}

impl JsonRpcEnvelopeShape {
    fn read_field<'de, M>(
        &mut self,
        field: JsonRpcEnvelopeField,
        map: &mut M,
    ) -> Result<(), M::Error>
    where
        M: MapAccess<'de>,
    {
        match field {
            JsonRpcEnvelopeField::JsonRpc => {
                reject_duplicate::<M::Error>(self.jsonrpc, "jsonrpc")?;
                map.next_value::<JsonRpcVersion>()?;
                self.jsonrpc = true;
            }
            JsonRpcEnvelopeField::Method => {
                reject_duplicate::<M::Error>(self.method, "method")?;
                map.next_value::<JsonRpcMethod>()?;
                self.method = true;
            }
            JsonRpcEnvelopeField::Id => {
                reject_duplicate::<M::Error>(self.id.is_some(), "id")?;
                self.id = Some(map.next_value::<JsonRpcIdShape>()?);
            }
            JsonRpcEnvelopeField::Params => {
                reject_duplicate::<M::Error>(self.params, "params")?;
                map.next_value::<JsonRpcParams>()?;
                self.params = true;
            }
            JsonRpcEnvelopeField::Result => {
                reject_duplicate::<M::Error>(self.result, "result")?;
                map.next_value::<IgnoredAny>()?;
                self.result = true;
            }
            JsonRpcEnvelopeField::Error => {
                reject_duplicate::<M::Error>(self.error, "error")?;
                map.next_value::<JsonRpcErrorObject>()?;
                self.error = true;
            }
            JsonRpcEnvelopeField::Unknown => {
                map.next_value::<IgnoredAny>()?;
                self.unknown = true;
            }
        }
        Ok(())
    }

    fn requires_response_permit(&self) -> bool {
        if !self.jsonrpc || self.unknown {
            return true;
        }
        if self.method {
            if self.result || self.error {
                return true;
            }
            return match &self.id {
                None => false,
                Some(JsonRpcIdShape::NonNull | JsonRpcIdShape::Null) => true,
            };
        }
        if self.params
            || !matches!(self.id, Some(JsonRpcIdShape::NonNull))
            || self.result == self.error
        {
            return true;
        }
        false
    }
}

impl<'de> Deserialize<'de> for JsonRpcEnvelopeShape {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EnvelopeVisitor;

        impl<'de> Visitor<'de> for EnvelopeVisitor {
            type Value = JsonRpcEnvelopeShape;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("one JSON-RPC object")
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut shape = JsonRpcEnvelopeShape::default();
                while let Some(field) = map.next_key::<JsonRpcEnvelopeField>()? {
                    shape.read_field(field, &mut map)?;
                }
                Ok(shape)
            }
        }

        deserializer.deserialize_map(EnvelopeVisitor)
    }
}

fn post_requires_response_permit(body: &[u8]) -> bool {
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    let Ok(shape) = JsonRpcEnvelopeShape::deserialize(&mut deserializer) else {
        return true;
    };
    if deserializer.end().is_err() {
        return true;
    }
    shape.requires_response_permit()
}

async fn buffer_mcp_post_body(
    request: Request,
    timeout: Duration,
) -> Result<(Request, bool), Response> {
    let (parts, body) = request.into_parts();
    match tokio::time::timeout(timeout, read_bounded_mcp_body(body)).await {
        Err(_) => Err(body_failure_response(
            StatusCode::REQUEST_TIMEOUT,
            "MCP request body deadline exceeded",
        )),
        Ok(Err(McpBodyReadFailure::TooLarge)) => Err(body_failure_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "MCP request body too large",
        )),
        Ok(Err(McpBodyReadFailure::Transport)) => Err(body_failure_response(
            StatusCode::BAD_REQUEST,
            "MCP request body could not be read",
        )),
        Ok(Ok(body)) => {
            let requires_response_permit = post_requires_response_permit(&body);
            Ok((
                Request::from_parts(parts, Body::from(body)),
                requires_response_permit,
            ))
        }
    }
}

fn capacity_exhausted_response(message: &'static str) -> Response {
    let mut response = (StatusCode::SERVICE_UNAVAILABLE, message).into_response();
    response
        .headers_mut()
        .insert(RETRY_AFTER, HeaderValue::from_static("1"));
    response
}

fn is_sse_response(response: &Response) -> bool {
    response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("text/event-stream"))
}

fn hold_permit_through_body(response: Response, permit: OwnedSemaphorePermit) -> Response {
    let (parts, body) = response.into_parts();
    Response::from_parts(
        parts,
        Body::new(PermitBody {
            inner: body,
            permit: Some(permit),
        }),
    )
}

async fn limit_mcp_request_concurrency(
    State(limits): State<McpHttpLimits>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    if method == Method::GET {
        let Some(permit) = limits.try_enter_stream() else {
            return capacity_exhausted_response("MCP stream capacity exhausted");
        };
        let response = next.run(request).await;
        if is_sse_response(&response) {
            return hold_permit_through_body(response, permit);
        }
        return response;
    }

    let Some(permit) = limits.try_enter_request() else {
        return capacity_exhausted_response("MCP request capacity exhausted");
    };
    let (request, post_response_permit) = if method == Method::POST {
        match buffer_mcp_post_body(request, limits.body_read_timeout).await {
            Ok((request, true)) => {
                let Some(response_permit) = limits.try_enter_post_response() else {
                    return capacity_exhausted_response("MCP response capacity exhausted");
                };
                (request, Some(response_permit))
            }
            Ok((request, false)) => (request, None),
            Err(response) => return response,
        }
    } else {
        (request, None)
    };
    let response = next.run(request).await;
    drop(permit);
    match post_response_permit {
        Some(response_permit) => hold_permit_through_body(response, response_permit),
        None => response,
    }
}

/// Builds the complete HTTP router: the MCP endpoint, a liveness probe, and —
/// only when the server authenticates requests — the OAuth protected-resource
/// metadata document.
///
/// `max_sessions` bounds concurrently retained MCP sessions, so an
/// unauthenticated client cannot exhaust memory by opening sessions.
pub fn build_router(server: OzonMcp, max_sessions: NonZeroUsize) -> Router {
    build_router_with_cancellation(server, max_sessions, CancellationToken::new())
}

/// Builds the complete HTTP router with a root cancellation token shared by
/// every MCP session and response stream.
///
/// Cancelling `cancellation_token` terminates active MCP session workers. The
/// caller remains responsible for stopping the listener and bounding the
/// outer HTTP connection drain.
pub fn build_router_with_cancellation(
    server: OzonMcp,
    max_sessions: NonZeroUsize,
    cancellation_token: CancellationToken,
) -> Router {
    let http_limits = McpHttpLimits::production();
    let protected_resource_metadata = server.protected_resource_metadata();
    let server = Arc::new(server);
    let session_manager = Arc::new(LocalSessionManager::default().with_max_sessions(max_sessions));
    let service: StreamableHttpService<OzonMcp, LocalSessionManager> = StreamableHttpService::new(
        move || Ok((*server).clone()),
        session_manager,
        StreamableHttpServerConfig::default()
            .with_max_request_body_bytes(MCP_REQUEST_BODY_LIMIT_BYTES)
            .with_cancellation_token(cancellation_token),
    );
    let mcp_router = Router::new()
        .fallback_service(service)
        .layer(middleware::from_fn_with_state(
            http_limits,
            limit_mcp_request_concurrency,
        ));
    let mut router = Router::new()
        .route("/health", get(|| async { "ok" }))
        .nest("/mcp", mcp_router);
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

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use axum::{
        body::{Body, Bytes, to_bytes},
        http::{Request as HttpRequest, header::CONTENT_TYPE},
    };
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::*;

    struct PendingBody;

    impl HttpBody for PendingBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Pending
        }
    }

    struct FailingBody;

    impl HttpBody for FailingBody {
        type Data = Bytes;
        type Error = std::io::Error;

        fn poll_frame(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(Some(Err(std::io::Error::other("test body failure"))))
        }
    }

    struct TrailersBody {
        emitted: bool,
    }

    impl HttpBody for TrailersBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            if self.emitted {
                return Poll::Ready(None);
            }
            self.emitted = true;
            Poll::Ready(Some(Ok(Frame::trailers(axum::http::HeaderMap::new()))))
        }
    }

    fn middleware_test_router(limits: McpHttpLimits, reached: Arc<AtomicUsize>) -> Router {
        let endpoint = move |request: Request| {
            let reached = Arc::clone(&reached);
            async move {
                if request.method() == Method::GET {
                    if request.uri().query() == Some("short=1") {
                        return "short".into_response();
                    }
                    return Response::builder()
                        .header(CONTENT_TYPE, "text/event-stream")
                        .body(Body::new(PendingBody))
                        .expect("test response builds");
                }
                reached.fetch_add(1, Ordering::SeqCst);
                let pending_response = request.uri().query() == Some("pending-response=1");
                let pending_handler = request.uri().query() == Some("pending-handler=1");
                let bytes = to_bytes(request.into_body(), MCP_REQUEST_BODY_LIMIT_BYTES)
                    .await
                    .expect("bounded test body is readable");
                if pending_handler {
                    return std::future::pending::<Response>().await;
                }
                if pending_response {
                    return Response::builder()
                        .header(CONTENT_TYPE, "application/json")
                        .body(Body::new(PendingBody))
                        .expect("test response builds");
                }
                (StatusCode::OK, bytes.len().to_string()).into_response()
            }
        };
        let mcp_router = Router::new()
            .fallback(endpoint)
            .layer(middleware::from_fn_with_state(
                limits,
                limit_mcp_request_concurrency,
            ));
        Router::new()
            .route("/health", get(|| async { "ok" }))
            .nest("/mcp", mcp_router)
    }

    fn middleware_request_at(uri: &str, method: Method, body: Body) -> HttpRequest<Body> {
        HttpRequest::builder()
            .method(method)
            .uri(uri)
            .body(body)
            .expect("test request builds")
    }

    fn middleware_request(method: Method, body: Body) -> HttpRequest<Body> {
        middleware_request_at("/mcp", method, body)
    }

    fn control_notification_request(method: &str, params: Value) -> HttpRequest<Body> {
        control_notification_request_at("/mcp", method, params)
    }

    fn control_notification_request_at(
        uri: &str,
        method: &str,
        params: Value,
    ) -> HttpRequest<Body> {
        middleware_request_at(
            uri,
            Method::POST,
            Body::from(
                json!({
                    "jsonrpc": "2.0",
                    "method": method,
                    "params": params,
                })
                .to_string(),
            ),
        )
    }

    #[tokio::test]
    async fn thirty_third_mcp_ingress_request_fails_fast_while_health_stays_available() {
        let reached = Arc::new(AtomicUsize::new(0));
        let router = middleware_test_router(
            McpHttpLimits::for_test(
                MCP_MAX_IN_FLIGHT_REQUESTS,
                MCP_MAX_IN_FLIGHT_POST_RESPONSES,
                MCP_MAX_IN_FLIGHT_STREAMS,
                Duration::from_millis(50),
            ),
            Arc::clone(&reached),
        );
        let mut active_requests = Vec::with_capacity(MCP_MAX_IN_FLIGHT_REQUESTS);
        for _ in 0..MCP_MAX_IN_FLIGHT_REQUESTS {
            let request = control_notification_request_at(
                "/mcp?pending-handler=1",
                "notifications/initialized",
                json!({}),
            );
            active_requests.push(tokio::spawn(router.clone().oneshot(request)));
        }
        tokio::time::timeout(Duration::from_millis(250), async {
            while reached.load(Ordering::SeqCst) != MCP_MAX_IN_FLIGHT_REQUESTS {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all ingress slots reach the pending handler");

        let overloaded = router
            .clone()
            .oneshot(control_notification_request(
                "notifications/initialized",
                json!({}),
            ))
            .await
            .expect("router responds");
        assert_eq!(overloaded.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            overloaded.headers().get(RETRY_AFTER),
            Some(&HeaderValue::from_static("1"))
        );

        let health = router
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("router responds");
        assert_eq!(health.status(), StatusCode::OK);

        let released = active_requests.pop().expect("one active request exists");
        released.abort();
        let _ = released.await;
        let recovered = router
            .oneshot(control_notification_request(
                "notifications/initialized",
                json!({}),
            ))
            .await
            .expect("router responds");
        assert_ne!(recovered.status(), StatusCode::SERVICE_UNAVAILABLE);
        for request in active_requests {
            request.abort();
            let _ = request.await;
        }
    }

    #[tokio::test]
    async fn sixty_fifth_get_stream_fails_without_consuming_post_capacity_and_recovers() {
        let reached = Arc::new(AtomicUsize::new(0));
        let limits = McpHttpLimits::for_test(
            MCP_MAX_IN_FLIGHT_REQUESTS,
            MCP_MAX_IN_FLIGHT_POST_RESPONSES,
            MCP_MAX_IN_FLIGHT_STREAMS,
            Duration::from_millis(50),
        );
        let router = middleware_test_router(limits, Arc::clone(&reached));

        let short = router
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method(Method::GET)
                    .uri("/mcp?short=1")
                    .body(Body::empty())
                    .expect("short GET request builds"),
            )
            .await
            .expect("router responds");
        assert_eq!(short.status(), StatusCode::OK);
        drop(short);

        let mut streams = Vec::with_capacity(MCP_MAX_IN_FLIGHT_STREAMS);
        for _ in 0..MCP_MAX_IN_FLIGHT_STREAMS {
            let response = router
                .clone()
                .oneshot(middleware_request(Method::GET, Body::empty()))
                .await
                .expect("router responds");
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers().get(CONTENT_TYPE),
                Some(&HeaderValue::from_static("text/event-stream"))
            );
            streams.push(response);
        }

        let overloaded = router
            .clone()
            .oneshot(middleware_request(Method::GET, Body::empty()))
            .await
            .expect("router responds");
        assert_eq!(overloaded.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            overloaded.headers().get(RETRY_AFTER),
            Some(&HeaderValue::from_static("1"))
        );

        let post = router
            .clone()
            .oneshot(middleware_request(Method::POST, Body::empty()))
            .await
            .expect("router responds");
        assert_eq!(post.status(), StatusCode::OK);
        assert_eq!(reached.load(Ordering::SeqCst), 1);

        let health = router
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .expect("health request builds"),
            )
            .await
            .expect("router responds");
        assert_eq!(health.status(), StatusCode::OK);

        drop(streams.pop());
        let recovered = router
            .oneshot(middleware_request(Method::GET, Body::empty()))
            .await
            .expect("router responds");
        assert_eq!(recovered.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn post_body_deadline_size_and_transport_failures_stop_before_handler() {
        let reached = Arc::new(AtomicUsize::new(0));
        let router = middleware_test_router(
            McpHttpLimits::for_test(1, 1, 1, Duration::from_millis(10)),
            Arc::clone(&reached),
        );

        let timed_out = tokio::time::timeout(
            Duration::from_millis(250),
            router
                .clone()
                .oneshot(middleware_request(Method::POST, Body::new(PendingBody))),
        )
        .await
        .expect("test deadline is bounded")
        .expect("router responds");
        assert_eq!(timed_out.status(), StatusCode::REQUEST_TIMEOUT);
        assert_eq!(reached.load(Ordering::SeqCst), 0);

        let oversized = router
            .clone()
            .oneshot(middleware_request(
                Method::POST,
                Body::from(vec![0; MCP_REQUEST_BODY_LIMIT_BYTES + 1]),
            ))
            .await
            .expect("router responds");
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(reached.load(Ordering::SeqCst), 0);

        let failed = router
            .clone()
            .oneshot(middleware_request(Method::POST, Body::new(FailingBody)))
            .await
            .expect("router responds");
        assert_eq!(failed.status(), StatusCode::BAD_REQUEST);
        assert_eq!(reached.load(Ordering::SeqCst), 0);

        let accepted = router
            .clone()
            .oneshot(middleware_request(Method::POST, Body::from("abc")))
            .await
            .expect("router responds");
        assert_eq!(accepted.status(), StatusCode::OK);
        let accepted = to_bytes(accepted.into_body(), 16)
            .await
            .expect("response is readable");
        assert_eq!(accepted.as_ref(), b"3");
        assert_eq!(reached.load(Ordering::SeqCst), 1);

        let deleted = router
            .oneshot(middleware_request(Method::DELETE, Body::empty()))
            .await
            .expect("router responds");
        assert_eq!(deleted.status(), StatusCode::OK);
        assert_eq!(reached.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn bounded_body_reader_ignores_non_data_frames() {
        let body = Body::new(TrailersBody { emitted: false });
        let bytes = read_bounded_mcp_body(body)
            .await
            .ok()
            .expect("trailers are not a transport failure");
        assert!(bytes.is_empty());
    }

    #[tokio::test]
    async fn result_body_cap_preserves_control_notifications_and_recovers_on_drop() {
        let reached = Arc::new(AtomicUsize::new(0));
        let router = middleware_test_router(
            McpHttpLimits::for_test(
                MCP_MAX_IN_FLIGHT_REQUESTS,
                MCP_MAX_IN_FLIGHT_POST_RESPONSES,
                1,
                Duration::from_millis(50),
            ),
            Arc::clone(&reached),
        );

        let pending_request = || {
            middleware_request_at(
                "/mcp?pending-response=1",
                Method::POST,
                Body::from(
                    json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/call",
                        "params": {},
                    })
                    .to_string(),
                ),
            )
        };
        let mut pending = Vec::with_capacity(MCP_MAX_IN_FLIGHT_POST_RESPONSES);
        for _ in 0..MCP_MAX_IN_FLIGHT_POST_RESPONSES {
            let response = router
                .clone()
                .oneshot(pending_request())
                .await
                .expect("router responds");
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers().get(CONTENT_TYPE),
                Some(&HeaderValue::from_static("application/json"))
            );
            pending.push(response);
        }

        let overloaded = router
            .clone()
            .oneshot(pending_request())
            .await
            .expect("router responds");
        assert_eq!(overloaded.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            reached.load(Ordering::SeqCst),
            MCP_MAX_IN_FLIGHT_POST_RESPONSES
        );

        let malformed = router
            .clone()
            .oneshot(middleware_request(Method::POST, Body::from("not json")))
            .await
            .expect("router responds");
        assert_eq!(malformed.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            reached.load(Ordering::SeqCst),
            MCP_MAX_IN_FLIGHT_POST_RESPONSES
        );

        let mut unread_controls = Vec::with_capacity(MCP_MAX_IN_FLIGHT_REQUESTS);
        for index in 0..MCP_MAX_IN_FLIGHT_REQUESTS {
            let request = if index % 2 == 0 {
                control_notification_request("notifications/initialized", json!({}))
            } else {
                control_notification_request(
                    "notifications/cancelled",
                    json!({"requestId": index, "reason": "test"}),
                )
            };
            let response = router
                .clone()
                .oneshot(request)
                .await
                .expect("router responds");
            assert_eq!(response.status(), StatusCode::OK);
            unread_controls.push(response);
        }
        assert_eq!(
            reached.load(Ordering::SeqCst),
            MCP_MAX_IN_FLIGHT_POST_RESPONSES + MCP_MAX_IN_FLIGHT_REQUESTS
        );

        drop(pending.pop());
        let recovered = router
            .oneshot(pending_request())
            .await
            .expect("router responds");
        assert_eq!(recovered.status(), StatusCode::OK);
        assert_eq!(
            reached.load(Ordering::SeqCst),
            MCP_MAX_IN_FLIGHT_POST_RESPONSES + MCP_MAX_IN_FLIGHT_REQUESTS + 1
        );
        drop(unread_controls);
    }

    #[tokio::test]
    async fn permit_body_releases_at_eof_while_wrapper_remains_alive() {
        let limits = McpHttpLimits::for_test(1, 1, 1, Duration::from_secs(1));
        let response_permit = limits
            .try_enter_post_response()
            .expect("fresh response limiter has capacity");
        let mut body = PermitBody {
            inner: Body::from("ok"),
            permit: Some(response_permit),
        };

        assert!(!body.is_end_stream());
        assert_eq!(body.size_hint().exact(), Some(2));
        let frame = std::future::poll_fn(|context| Pin::new(&mut body).poll_frame(context))
            .await
            .expect("data frame exists")
            .expect("data frame is readable");
        assert_eq!(frame.data_ref().map(Bytes::as_ref), Some(b"ok".as_slice()));
        let end = std::future::poll_fn(|context| Pin::new(&mut body).poll_frame(context)).await;
        assert!(end.is_none());
        assert_eq!(limits.post_response_permits.available_permits(), 1);
        assert!(body.permit.is_none());
    }

    #[tokio::test]
    async fn permit_body_releases_on_terminal_inner_error() {
        let limits = McpHttpLimits::for_test(1, 1, 1, Duration::from_secs(1));
        let response_permit = limits
            .try_enter_post_response()
            .expect("fresh response limiter has capacity");
        let mut body = PermitBody {
            inner: Body::new(FailingBody),
            permit: Some(response_permit),
        };

        let frame = std::future::poll_fn(|context| Pin::new(&mut body).poll_frame(context)).await;
        assert!(matches!(frame, Some(Err(_))));
        assert_eq!(limits.post_response_permits.available_permits(), 1);
        assert!(body.permit.is_none());
    }

    #[test]
    fn response_permit_classifier_is_shallow_strict_and_conservative() {
        for bypassed in [
            json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            json!({"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":"one","progress":1}}),
            json!({"jsonrpc":"2.0","id":1,"result":{"items":[1,2,3]}}),
            json!({"jsonrpc":"2.0","id":"one","error":{"code":-32600,"message":"invalid","data":{"nested":[1,2]}}}),
        ] {
            assert!(!post_requires_response_permit(
                bypassed.to_string().as_bytes()
            ));
        }
        for guarded in [
            Value::Null,
            json!([]),
            json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{}}),
            json!({"jsonrpc":"2.0","id":null,"method":"notifications/initialized"}),
            json!({"jsonrpc":"1.0","method":"notifications/initialized"}),
            json!({"jsonrpc":"2.0","method":"notifications/progress","params":1}),
            json!({"jsonrpc":"2.0","method":1,"params":{}}),
            json!({"jsonrpc":"2.0","method":"notifications/initialized","extra":true}),
            json!({"jsonrpc":"2.0","id":null,"result":{}}),
            json!({"jsonrpc":"2.0","id":1}),
            json!({"jsonrpc":"2.0","id":1,"result":{},"error":{"code":-1,"message":"x"}}),
            json!({"jsonrpc":"2.0","id":1,"error":{}}),
            json!({"jsonrpc":"2.0","id":1,"error":{"code":1.5,"message":"x"}}),
            json!({"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"x","extra":true}}),
        ] {
            assert!(post_requires_response_permit(
                guarded.to_string().as_bytes()
            ));
        }
        assert!(post_requires_response_permit(b"not json"));
        assert!(post_requires_response_permit(
            br#"{"jsonrpc":"2.0","method":"x"} trailing"#
        ));

        let large_params = format!(
            r#"{{"jsonrpc":"2.0","method":"notifications/progress","params":[{}]}}"#,
            vec!["0"; 32_000].join(",")
        );
        assert!(large_params.len() < MCP_REQUEST_BODY_LIMIT_BYTES);
        assert!(!post_requires_response_permit(large_params.as_bytes()));

        let large_top_level_array = format!("[{}]", vec!["0"; 32_000].join(","));
        assert!(post_requires_response_permit(
            large_top_level_array.as_bytes()
        ));
    }

    #[test]
    fn response_permit_classifier_covers_numeric_id_and_error_code_shapes() {
        for bypassed in [
            br#"{"jsonrpc":"2.0","id":-1,"result":null}"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":1,"error":{"code":1,"message":"x"}}"#.as_slice(),
        ] {
            assert!(!post_requires_response_permit(bypassed));
        }

        for guarded in [
            br#"{"jsonrpc":2,"method":"notifications/initialized"}"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":true,"result":null}"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":9223372036854775808,"result":null}"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":1,"error":[]}"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":1,"error":{"code":9223372036854775808,"message":"x"}}"#
                .as_slice(),
            br#"{"jsonrpc":"2.0","id":1,"error":{"code":1,"message":2}}"#.as_slice(),
            br#"{"jsonrpc":"2.0","method":"x","result":null}"#.as_slice(),
        ] {
            assert!(post_requires_response_permit(guarded));
        }
    }

    /// The classifier only lets a body bypass the response budget when it has
    /// fully recognised it as a permit-free notification or client response.
    /// JSON that is malformed *inside* an otherwise recognised position must
    /// therefore be guarded: swallowing a nested parse error would let a flood of
    /// garbage bodies produce result bodies without consuming the 16-slot
    /// response budget that bounds retained response memory.
    #[test]
    fn malformed_json_inside_any_recognised_position_still_requires_a_permit() {
        for malformed in [
            // Malformed value inside a params object and a params array.
            br#"{"jsonrpc":"2.0","method":"x","params":{"a":}}"#.as_slice(),
            br#"{"jsonrpc":"2.0","method":"x","params":{"a":1,}}"#.as_slice(),
            br#"{"jsonrpc":"2.0","method":"x","params":[1,]}"#.as_slice(),
            br#"{"jsonrpc":"2.0","method":"x","params":[{"a":}]}"#.as_slice(),
            // Malformed value inside error.data and inside result.
            br#"{"jsonrpc":"2.0","id":1,"error":{"code":1,"message":"x","data":{"a":}}}"#
                .as_slice(),
            br#"{"jsonrpc":"2.0","id":1,"result":{"a":}}"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":1,"result":[1,]}"#.as_slice(),
            // Malformed value under an unknown envelope field.
            br#"{"jsonrpc":"2.0","method":"x","extra":{"a":}}"#.as_slice(),
            // Non-string member names, which JSON does not permit at all.
            br#"{"jsonrpc":"2.0",1:2}"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":1,"error":{1:2}}"#.as_slice(),
            // Truncated at every depth, including mid-string and mid-number.
            br#"{"jsonrpc":"2.0","method":"x","params":{"a":1"#.as_slice(),
            br#"{"jsonrpc":"2.0","method":"x","params":["#.as_slice(),
            br#"{"jsonrpc":"2.0","id":1,"error":{"code":1,"message":"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":1,"error":{"code":1,"message":"unterminated"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":1,"result":"#.as_slice(),
            br#"{"jsonrpc":"2.0""#.as_slice(),
            b"{".as_slice(),
            b"".as_slice(),
        ] {
            let rendered = String::from_utf8_lossy(malformed);
            assert!(
                post_requires_response_permit(malformed),
                "malformed body must not bypass the response budget: {rendered}"
            );
        }

        // The well-formed counterparts of the two shapes above still bypass the
        // budget, so the guard above is driven by the malformedness and not by
        // the surrounding envelope having become unrecognisable.
        for bypassed in [
            br#"{"jsonrpc":"2.0","method":"x","params":{"a":1}}"#.as_slice(),
            br#"{"jsonrpc":"2.0","method":"x","params":[1]}"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":1,"result":{"a":1}}"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":1,"error":{"code":1,"message":"x","data":{"a":1}}}"#
                .as_slice(),
        ] {
            let rendered = String::from_utf8_lossy(bypassed);
            assert!(
                !post_requires_response_permit(bypassed),
                "well-formed body must stay permit-free: {rendered}"
            );
        }
    }

    #[test]
    fn response_permit_classifier_rejects_duplicate_envelope_and_error_fields() {
        for duplicate in [
            br#"{"jsonrpc":"2.0","jsonrpc":"2.0","method":"x"}"#.as_slice(),
            br#"{"jsonrpc":"2.0","method":"x","method":"y"}"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":1,"id":2,"result":null}"#.as_slice(),
            br#"{"jsonrpc":"2.0","method":"x","params":{},"params":[]}"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":1,"result":null,"result":null}"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":1,"error":{"code":1,"message":"x"},"error":{"code":1,"message":"x"}}"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":1,"error":{"code":1,"code":2,"message":"x"}}"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":1,"error":{"code":1,"message":"x","message":"y"}}"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":1,"error":{"code":1,"message":"x","data":null,"data":null}}"#.as_slice(),
        ] {
            assert!(post_requires_response_permit(duplicate));
        }
    }
}
