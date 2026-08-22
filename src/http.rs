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
        header::{CONTENT_TYPE, RETRY_AFTER, WWW_AUTHENTICATE},
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use http_body::{Frame, SizeHint};
use rmcp::ServerHandler;
use rmcp::transport::{
    StreamableHttpServerConfig, StreamableHttpService,
    streamable_http_server::{
        session::local::LocalSessionManager, tower::validate_streamable_http_request_headers,
    },
};
use serde::de::{self, Deserialize, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::{
    auth::{JwtAuthenticationFailure, JwtAuthenticator, ProtectedResourceMetadata},
    server::OzonMcp,
};

/// Security metadata required by the shared hardened MCP HTTP surface.
///
/// Implementations remain responsible for their own tool-level authorization.
/// The transport uses this trait only to authenticate every HTTP request and
/// publish the corresponding OAuth protected-resource document.
pub trait HttpMcpServer: ServerHandler + Clone {
    fn protected_resource_metadata(&self) -> Option<ProtectedResourceMetadata>;
    fn transport_authenticator(&self) -> Option<&JwtAuthenticator>;
}

impl HttpMcpServer for OzonMcp {
    fn protected_resource_metadata(&self) -> Option<ProtectedResourceMetadata> {
        self.protected_resource_metadata()
    }

    fn transport_authenticator(&self) -> Option<&JwtAuthenticator> {
        self.transport_authenticator()
    }
}

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

/// Default period with no MCP protocol activity before a legacy session is
/// reclaimed. In-flight requests suspend this countdown.
pub const MCP_SESSION_IDLE_TIMEOUT_DEFAULT: Duration = Duration::from_secs(120);

const DEV_MCP_ALLOWED_HOSTS: &[&str] = &["localhost", "127.0.0.1", "::1"];

/// Browser origins permitted by the single-user development mode.
///
/// The vendored transport treats a portless loopback allowlist entry as an
/// explicit any-port rule. That keeps local Inspector/Vite-style browser
/// clients working even when they choose an ephemeral port, without extending
/// the exception to a non-loopback host.
const DEV_MCP_ALLOWED_ORIGINS: &[&str] = &[
    "http://localhost",
    "http://127.0.0.1",
    "http://[::1]",
    "https://localhost",
    "https://127.0.0.1",
    "https://[::1]",
];

#[derive(Clone)]
struct McpHttpLimits {
    auth_request_permits: Arc<Semaphore>,
    auth_stream_permits: Arc<Semaphore>,
    request_permits: Arc<Semaphore>,
    post_response_permits: Arc<Semaphore>,
    stream_permits: Arc<Semaphore>,
    body_read_timeout: Duration,
    transport_config: Arc<StreamableHttpServerConfig>,
    authenticator: Option<JwtAuthenticator>,
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
        if matches!(&frame, Poll::Ready(None | Some(Err(_)))) {
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
    fn production(
        transport_config: StreamableHttpServerConfig,
        authenticator: Option<JwtAuthenticator>,
    ) -> Self {
        Self {
            auth_request_permits: Arc::new(Semaphore::new(MCP_MAX_IN_FLIGHT_REQUESTS)),
            auth_stream_permits: Arc::new(Semaphore::new(MCP_MAX_IN_FLIGHT_STREAMS)),
            request_permits: Arc::new(Semaphore::new(MCP_MAX_IN_FLIGHT_REQUESTS)),
            post_response_permits: Arc::new(Semaphore::new(MCP_MAX_IN_FLIGHT_POST_RESPONSES)),
            stream_permits: Arc::new(Semaphore::new(MCP_MAX_IN_FLIGHT_STREAMS)),
            body_read_timeout: MCP_REQUEST_BODY_READ_TIMEOUT,
            transport_config: Arc::new(transport_config),
            authenticator,
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
            auth_request_permits: Arc::new(Semaphore::new(requests)),
            auth_stream_permits: Arc::new(Semaphore::new(streams)),
            request_permits: Arc::new(Semaphore::new(requests)),
            post_response_permits: Arc::new(Semaphore::new(post_responses)),
            stream_permits: Arc::new(Semaphore::new(streams)),
            body_read_timeout,
            transport_config: Arc::new(
                StreamableHttpServerConfig::default()
                    .with_max_request_body_bytes(MCP_REQUEST_BODY_LIMIT_BYTES)
                    .with_allowed_origins(DEV_MCP_ALLOWED_ORIGINS.iter().copied()),
            ),
            authenticator: None,
        }
    }

    fn try_enter_request(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.request_permits).try_acquire_owned().ok()
    }

    fn try_enter_auth(&self, method: &Method) -> Option<OwnedSemaphorePermit> {
        let permits = if method == Method::GET {
            &self.auth_stream_permits
        } else {
            &self.auth_request_permits
        };
        Arc::clone(permits).try_acquire_owned().ok()
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
    let minimum_length = body.size_hint().lower();
    if minimum_length > MCP_REQUEST_BODY_LIMIT_BYTES as u64 {
        return Err(McpBodyReadFailure::TooLarge);
    }
    let capacity = usize::try_from(minimum_length).map_err(|_| McpBodyReadFailure::TooLarge)?;
    let mut bytes = Vec::with_capacity(capacity);
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
#[derive(Clone, Copy)]
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
#[derive(Clone, Copy)]
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
// Each flag records whether a distinct JSON-RPC field was observed so duplicate
// and mutually exclusive fields can be rejected without retaining their data.
#[allow(clippy::struct_excessive_bools)]
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
) -> Result<(Request, bool), Box<Response>> {
    let (parts, body) = request.into_parts();
    match tokio::time::timeout(timeout, read_bounded_mcp_body(body)).await {
        Err(_) => Err(Box::new(body_failure_response(
            StatusCode::REQUEST_TIMEOUT,
            "MCP request body deadline exceeded",
        ))),
        Ok(Err(McpBodyReadFailure::TooLarge)) => Err(Box::new(body_failure_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "MCP request body too large",
        ))),
        Ok(Err(McpBodyReadFailure::Transport)) => Err(Box::new(body_failure_response(
            StatusCode::BAD_REQUEST,
            "MCP request body could not be read",
        ))),
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

fn authentication_failure_response(
    authenticator: &JwtAuthenticator,
    failure: JwtAuthenticationFailure,
) -> Response {
    let status = match failure {
        JwtAuthenticationFailure::MissingCredentials
        | JwtAuthenticationFailure::InvalidToken
        | JwtAuthenticationFailure::ExpiredToken
        | JwtAuthenticationFailure::WrongAudience => StatusCode::UNAUTHORIZED,
        JwtAuthenticationFailure::InsufficientScope | JwtAuthenticationFailure::AccessDenied => {
            StatusCode::FORBIDDEN
        }
        JwtAuthenticationFailure::VerifierUnavailable => StatusCode::SERVICE_UNAVAILABLE,
    };
    let mut response = (status, failure.public_message()).into_response();
    if let Some(challenge) = authenticator.challenge(&failure) {
        response.headers_mut().insert(
            WWW_AUTHENTICATE,
            HeaderValue::from_str(&challenge)
                .expect("validated OAuth metadata and scopes form a safe challenge"),
        );
    }
    response
}

fn is_sse_response(response: &Response) -> bool {
    response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("text/event-stream"))
}

fn exact_origin_from_resource_url(resource_url: &str) -> Option<String> {
    let parsed = reqwest::Url::parse(resource_url).ok()?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return None;
    }
    let scheme = parsed.scheme();
    let host = parsed
        .host_str()?
        .trim_start_matches('[')
        .trim_end_matches(']');
    let port = parsed.port_or_known_default()?;
    let authority_host = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_owned()
    };
    Some(format!("{scheme}://{authority_host}:{port}"))
}

fn allowed_mcp_origins(protected_resource_url: Option<&str>) -> Vec<String> {
    match protected_resource_url {
        Some(resource_url) => vec![
            // AppConfig validates production resource URLs. Retaining one
            // deliberately invalid entry if a programmatic caller bypasses
            // that boundary keeps Origin-bearing requests fail-closed rather
            // than accidentally disabling validation with an empty list.
            exact_origin_from_resource_url(resource_url).unwrap_or_default(),
        ],
        None => DEV_MCP_ALLOWED_ORIGINS
            .iter()
            .map(|origin| (*origin).to_owned())
            .collect(),
    }
}

fn exact_host_from_resource_url(resource_url: &str) -> Option<String> {
    let parsed = reqwest::Url::parse(resource_url).ok()?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return None;
    }
    // Host validation prevents DNS rebinding and therefore binds the hostname,
    // not the listener-facing port. A reverse proxy may legitimately forward
    // the public hostname to a different internal port. Browser ports remain
    // constrained independently by the exact Origin policy above.
    Some(parsed.host_str()?.to_owned())
}

fn allowed_mcp_hosts(protected_resource_url: Option<&str>) -> Vec<String> {
    match protected_resource_url {
        Some(resource_url) => vec![
            // Keep a non-empty, non-matching policy when a programmatic caller
            // bypasses AppConfig validation. An empty allowlist would disable
            // rmcp's Host validation entirely.
            exact_host_from_resource_url(resource_url).unwrap_or_default(),
        ],
        None => DEV_MCP_ALLOWED_HOSTS
            .iter()
            .map(|host| (*host).to_owned())
            .collect(),
    }
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
    mut request: Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    // The transport performs this exact preflight again as its own security
    // boundary. Sharing its validator here avoids reading a potentially
    // unbounded-latency body for a request whose headers already guarantee a
    // rejection, without maintaining a second set of subtly divergent rules.
    if let Err(response) = validate_streamable_http_request_headers(
        request.method(),
        request.uri(),
        request.headers(),
        limits.transport_config.as_ref(),
        false,
    ) {
        let (parts, body) = response.into_parts();
        return Response::from_parts(parts, Body::new(body));
    }

    // The MCP authorization specification requires the access token on every
    // HTTP request. Authenticate before body polling or session lookup and
    // carry the exact registry snapshot used for OIDC mapping into the request
    // so downstream RBAC cannot observe a different reload. A dedicated gate
    // bounds JWT/JWKS futures without letting unauthenticated work occupy the
    // subsequent MCP execution/stream budget.
    if let Some(authenticator) = &limits.authenticator {
        let Some(auth_permit) = limits.try_enter_auth(&method) else {
            return capacity_exhausted_response("MCP authentication capacity exhausted");
        };
        match authenticator
            .authenticate_with_registry(request.headers())
            .await
        {
            Ok(access) => {
                request.extensions_mut().insert(access.actor);
                request.extensions_mut().insert(access.registry);
            }
            Err(failure) => return authentication_failure_response(authenticator, failure),
        }
        drop(auth_permit);
    }

    let permit = if method == Method::GET {
        let Some(permit) = limits.try_enter_stream() else {
            return capacity_exhausted_response("MCP stream capacity exhausted");
        };
        permit
    } else {
        let Some(permit) = limits.try_enter_request() else {
            return capacity_exhausted_response("MCP request capacity exhausted");
        };
        permit
    };

    if method == Method::GET {
        let response = next.run(request).await;
        if is_sse_response(&response) {
            return hold_permit_through_body(response, permit);
        }
        return response;
    }

    let (request, post_response_permit) = if method == Method::POST {
        match buffer_mcp_post_body(request, limits.body_read_timeout).await {
            Ok((request, true)) => {
                let Some(response_permit) = limits.try_enter_post_response() else {
                    return capacity_exhausted_response("MCP response capacity exhausted");
                };
                (request, Some(response_permit))
            }
            Ok((request, false)) => (request, None),
            Err(response) => return *response,
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

/// Builds the production router with an explicit legacy-session idle policy.
pub fn build_router_with_session_idle_timeout(
    server: OzonMcp,
    max_sessions: NonZeroUsize,
    session_idle_timeout: Duration,
) -> Router {
    build_router_with_cancellation_and_session_idle_timeout(
        server,
        max_sessions,
        session_idle_timeout,
        CancellationToken::new(),
    )
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
    build_router_with_cancellation_and_session_idle_timeout(
        server,
        max_sessions,
        MCP_SESSION_IDLE_TIMEOUT_DEFAULT,
        cancellation_token,
    )
}

/// Builds the complete router with explicit session-idle and cancellation
/// policies. Existing wrappers retain the 120-second safe default.
pub fn build_router_with_cancellation_and_session_idle_timeout(
    server: OzonMcp,
    max_sessions: NonZeroUsize,
    session_idle_timeout: Duration,
    cancellation_token: CancellationToken,
) -> Router {
    build_router_for_server_with_cancellation_and_session_idle_timeout(
        server,
        max_sessions,
        session_idle_timeout,
        cancellation_token,
    )
}

/// Builds the same hardened HTTP surface for another isolated MCP server.
///
/// This is intentionally generic only over the MCP handler. Host/origin
/// validation, per-request authentication, body/session limits and bounded
/// response lifetimes remain identical to the analytics production router.
pub fn build_router_for_server_with_cancellation_and_session_idle_timeout<S>(
    server: S,
    max_sessions: NonZeroUsize,
    session_idle_timeout: Duration,
    cancellation_token: CancellationToken,
) -> Router
where
    S: HttpMcpServer,
{
    let protected_resource_metadata = server.protected_resource_metadata();
    let protected_resource_url = protected_resource_metadata
        .as_ref()
        .map(|metadata| metadata.resource.as_str());
    let allowed_hosts = allowed_mcp_hosts(protected_resource_url);
    let allowed_origins = allowed_mcp_origins(protected_resource_url);
    let transport_config = StreamableHttpServerConfig::default()
        .with_max_request_body_bytes(MCP_REQUEST_BODY_LIMIT_BYTES)
        .with_allowed_hosts(allowed_hosts)
        .with_allowed_origins(allowed_origins)
        .with_cancellation_token(cancellation_token);
    let http_limits = McpHttpLimits::production(
        transport_config.clone(),
        server.transport_authenticator().cloned(),
    );
    let server = Arc::new(server);
    let session_manager = Arc::new(
        LocalSessionManager::default()
            .with_max_sessions(max_sessions)
            .with_session_idle_timeout(session_idle_timeout),
    );
    let service: StreamableHttpService<S, LocalSessionManager> = StreamableHttpService::new(
        move || Ok((*server).clone()),
        session_manager,
        transport_config,
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
        http::{
            Request as HttpRequest,
            header::{ACCEPT, CONTENT_TYPE, HOST, ORIGIN},
        },
    };
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use crate::config::{JwtConfig, RegistrySource};

    use super::*;

    static AUTH_TEST_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

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

    struct OversizedHintBody;

    impl HttpBody for OversizedHintBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            panic!("an already oversized size hint must be rejected before polling")
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::with_exact((MCP_REQUEST_BODY_LIMIT_BYTES + 1) as u64)
        }
    }

    struct OversizedFrameBody;

    impl HttpBody for OversizedFrameBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(Some(Ok(Frame::data(Bytes::from(vec![
                0;
                MCP_REQUEST_BODY_LIMIT_BYTES
                    + 1
            ])))))
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

    fn test_authenticator() -> JwtAuthenticator {
        let sequence = AUTH_TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mcp-ozon-http-auth-{}-{sequence}.json",
            std::process::id()
        ));
        std::fs::write(
            &path,
            serde_json::to_vec(&json!({
                "version": 1,
                "actors": [{
                    "id": "admin",
                    "name": "Administrator",
                    "role": "admin",
                    "oidc": {"username": "admin"}
                }],
                "accounts": []
            }))
            .expect("test registry serializes"),
        )
        .expect("test registry is writable");
        let registry = RegistrySource::new(path).expect("test registry is valid");
        JwtAuthenticator::new(
            JwtConfig {
                issuer: "http://issuer.test/realms/ofk".to_owned(),
                audience: "http://localhost:8788/mcp".to_owned(),
                jwks_url: "http://127.0.0.1:1/certs".to_owned(),
                resource_url: "http://localhost:8788/mcp".to_owned(),
                resource_metadata_url: "http://localhost:8788/.well-known/oauth-protected-resource"
                    .to_owned(),
                required_scopes: vec!["mcp:tools".to_owned()],
                jwks_cache_ttl: Duration::from_secs(300),
            },
            registry,
        )
        .expect("test authenticator builds")
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

    fn middleware_request_at(uri: &str, method: &Method, body: Body) -> HttpRequest<Body> {
        let mut builder = HttpRequest::builder()
            .method(method)
            .uri(uri)
            .header(HOST, "localhost");
        if method == Method::POST {
            builder = builder
                .header(CONTENT_TYPE, "application/json")
                .header(ACCEPT, "application/json, text/event-stream");
        } else if method == Method::GET {
            builder = builder.header(ACCEPT, "text/event-stream");
        }
        builder.body(body).expect("test request builds")
    }

    fn middleware_request(method: &Method, body: Body) -> HttpRequest<Body> {
        middleware_request_at("/mcp", method, body)
    }

    fn control_notification_request(method: &str, params: &Value) -> HttpRequest<Body> {
        control_notification_request_at("/mcp", method, params)
    }

    fn control_notification_request_at(
        uri: &str,
        method: &str,
        params: &Value,
    ) -> HttpRequest<Body> {
        middleware_request_at(
            uri,
            &Method::POST,
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
                &json!({}),
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
                &json!({}),
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
                &json!({}),
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
            .oneshot(middleware_request_at(
                "/mcp?short=1",
                &Method::GET,
                Body::empty(),
            ))
            .await
            .expect("router responds");
        assert_eq!(short.status(), StatusCode::OK);
        drop(short);

        let mut streams = Vec::with_capacity(MCP_MAX_IN_FLIGHT_STREAMS);
        for _ in 0..MCP_MAX_IN_FLIGHT_STREAMS {
            let response = router
                .clone()
                .oneshot(middleware_request(&Method::GET, Body::empty()))
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
            .oneshot(middleware_request(&Method::GET, Body::empty()))
            .await
            .expect("router responds");
        assert_eq!(overloaded.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            overloaded.headers().get(RETRY_AFTER),
            Some(&HeaderValue::from_static("1"))
        );

        let post = router
            .clone()
            .oneshot(middleware_request(&Method::POST, Body::empty()))
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
            .oneshot(middleware_request(&Method::GET, Body::empty()))
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
                .oneshot(middleware_request(&Method::POST, Body::new(PendingBody))),
        )
        .await
        .expect("test deadline is bounded")
        .expect("router responds");
        assert_eq!(timed_out.status(), StatusCode::REQUEST_TIMEOUT);
        assert_eq!(reached.load(Ordering::SeqCst), 0);

        let oversized = router
            .clone()
            .oneshot(middleware_request(
                &Method::POST,
                Body::from(vec![0; MCP_REQUEST_BODY_LIMIT_BYTES + 1]),
            ))
            .await
            .expect("router responds");
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(reached.load(Ordering::SeqCst), 0);

        let failed = router
            .clone()
            .oneshot(middleware_request(&Method::POST, Body::new(FailingBody)))
            .await
            .expect("router responds");
        assert_eq!(failed.status(), StatusCode::BAD_REQUEST);
        assert_eq!(reached.load(Ordering::SeqCst), 0);

        let accepted = router
            .clone()
            .oneshot(middleware_request(&Method::POST, Body::from("abc")))
            .await
            .expect("router responds");
        assert_eq!(accepted.status(), StatusCode::OK);
        let accepted = to_bytes(accepted.into_body(), 16)
            .await
            .expect("response is readable");
        assert_eq!(accepted.as_ref(), b"3");
        assert_eq!(reached.load(Ordering::SeqCst), 1);

        let deleted = router
            .oneshot(middleware_request(&Method::DELETE, Body::empty()))
            .await
            .expect("router responds");
        assert_eq!(deleted.status(), StatusCode::OK);
        assert_eq!(reached.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn invalid_post_headers_are_rejected_without_polling_a_pending_body() {
        let reached = Arc::new(AtomicUsize::new(0));
        let limits = McpHttpLimits::for_test(1, 1, 1, Duration::from_secs(60));
        let router = middleware_test_router(limits.clone(), Arc::clone(&reached));

        let mut cases = Vec::new();

        let mut bad_host = middleware_request(&Method::POST, Body::new(PendingBody));
        bad_host
            .headers_mut()
            .insert(HOST, HeaderValue::from_static("evil.example"));
        cases.push((bad_host, StatusCode::FORBIDDEN));

        let mut bad_origin = middleware_request(&Method::POST, Body::new(PendingBody));
        bad_origin
            .headers_mut()
            .insert(ORIGIN, HeaderValue::from_static("https://evil.example"));
        cases.push((bad_origin, StatusCode::FORBIDDEN));

        let mut bad_accept = middleware_request(&Method::POST, Body::new(PendingBody));
        bad_accept
            .headers_mut()
            .insert(ACCEPT, HeaderValue::from_static("application/json"));
        cases.push((bad_accept, StatusCode::NOT_ACCEPTABLE));

        let mut bad_content_type = middleware_request(&Method::POST, Body::new(PendingBody));
        bad_content_type
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        cases.push((bad_content_type, StatusCode::UNSUPPORTED_MEDIA_TYPE));

        let mut substring_accept = middleware_request(&Method::POST, Body::new(PendingBody));
        substring_accept.headers_mut().insert(
            ACCEPT,
            HeaderValue::from_static("xapplication/json, text/event-stream-evil"),
        );
        cases.push((substring_accept, StatusCode::NOT_ACCEPTABLE));

        let mut prefixed_content_type = middleware_request(&Method::POST, Body::new(PendingBody));
        prefixed_content_type.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/json-malformed"),
        );
        cases.push((prefixed_content_type, StatusCode::UNSUPPORTED_MEDIA_TYPE));

        for value in ["application/json,text/plain", "application/json;"] {
            let mut malformed_content_type =
                middleware_request(&Method::POST, Body::new(PendingBody));
            malformed_content_type
                .headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static(value));
            cases.push((malformed_content_type, StatusCode::UNSUPPORTED_MEDIA_TYPE));
        }

        for (request, expected_status) in cases {
            let response =
                tokio::time::timeout(Duration::from_secs(1), router.clone().oneshot(request))
                    .await
                    .expect("header preflight must not wait for the pending body")
                    .expect("router responds");
            assert_eq!(response.status(), expected_status);
            assert_eq!(limits.request_permits.available_permits(), 1);
        }
        assert_eq!(reached.load(Ordering::SeqCst), 0);

        let mut valid = middleware_request(&Method::POST, Body::from("abc"));
        valid.headers_mut().remove(ACCEPT);
        valid
            .headers_mut()
            .append(ACCEPT, HeaderValue::from_static("Application/JSON; q=1"));
        valid
            .headers_mut()
            .append(ACCEPT, HeaderValue::from_static("TEXT/EVENT-STREAM; q=0.5"));
        valid.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("Application/JSON; charset=utf-8"),
        );
        let response = router.oneshot(valid).await.expect("router responds");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(reached.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn missing_jwt_is_rejected_before_body_polling_or_session_handling() {
        let reached = Arc::new(AtomicUsize::new(0));
        let limits = McpHttpLimits::for_test(1, 1, 1, Duration::from_secs(60));
        let mut protected_limits = limits.clone();
        protected_limits.authenticator = Some(test_authenticator());
        let router = middleware_test_router(protected_limits, Arc::clone(&reached));

        let response = tokio::time::timeout(
            Duration::from_millis(250),
            router.oneshot(middleware_request(&Method::POST, Body::new(PendingBody))),
        )
        .await
        .expect("JWT rejection must not poll the pending body")
        .expect("router responds");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(response.headers().get(WWW_AUTHENTICATE).is_some());
        assert_eq!(limits.auth_request_permits.available_permits(), 1);
        assert_eq!(limits.request_permits.available_permits(), 1);
        assert_eq!(limits.post_response_permits.available_permits(), 1);
        assert_eq!(reached.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn jwt_verification_gate_is_bounded_separately_from_mcp_ingress() {
        let reached = Arc::new(AtomicUsize::new(0));
        let limits = McpHttpLimits::for_test(1, 1, 1, Duration::from_secs(60));
        let mut protected_limits = limits.clone();
        protected_limits.authenticator = Some(test_authenticator());
        let router = middleware_test_router(protected_limits, Arc::clone(&reached));

        let held = limits
            .try_enter_auth(&Method::POST)
            .expect("fresh JWT gate has capacity");
        let response = tokio::time::timeout(
            Duration::from_millis(250),
            router.oneshot(middleware_request(&Method::POST, Body::new(PendingBody))),
        )
        .await
        .expect("JWT gate overload must not poll the pending body")
        .expect("router responds");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(RETRY_AFTER),
            Some(&HeaderValue::from_static("1"))
        );
        assert_eq!(limits.request_permits.available_permits(), 1);
        assert_eq!(reached.load(Ordering::SeqCst), 0);
        drop(held);

        let stream = limits
            .try_enter_auth(&Method::GET)
            .expect("GET uses the independent stream authentication gate");
        assert!(limits.try_enter_auth(&Method::GET).is_none());
        assert!(limits.try_enter_auth(&Method::POST).is_some());
        drop(stream);
    }

    #[tokio::test]
    async fn jwt_failures_map_to_sanitized_http_statuses_and_challenges() {
        let authenticator = test_authenticator();
        for (failure, expected_status, expected_challenge) in [
            (
                JwtAuthenticationFailure::MissingCredentials,
                StatusCode::UNAUTHORIZED,
                true,
            ),
            (
                JwtAuthenticationFailure::InvalidToken,
                StatusCode::UNAUTHORIZED,
                true,
            ),
            (
                JwtAuthenticationFailure::ExpiredToken,
                StatusCode::UNAUTHORIZED,
                true,
            ),
            (
                JwtAuthenticationFailure::WrongAudience,
                StatusCode::UNAUTHORIZED,
                true,
            ),
            (
                JwtAuthenticationFailure::InsufficientScope,
                StatusCode::FORBIDDEN,
                true,
            ),
            (
                JwtAuthenticationFailure::AccessDenied,
                StatusCode::FORBIDDEN,
                false,
            ),
            (
                JwtAuthenticationFailure::VerifierUnavailable,
                StatusCode::SERVICE_UNAVAILABLE,
                false,
            ),
        ] {
            let response = authentication_failure_response(&authenticator, failure);
            assert_eq!(response.status(), expected_status);
            assert_eq!(
                response.headers().contains_key(WWW_AUTHENTICATE),
                expected_challenge
            );
            assert!(response.headers().get("mcp-session-id").is_none());
            let body = to_bytes(response.into_body(), 1_024)
                .await
                .expect("fixed authentication body is readable");
            assert_eq!(body.as_ref(), failure.public_message().as_bytes());
        }
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
    async fn bounded_body_reader_rejects_an_oversized_lower_hint_before_polling() {
        let error = read_bounded_mcp_body(Body::new(OversizedHintBody))
            .await
            .expect_err("an oversized lower bound cannot fit the request budget");
        assert!(matches!(error, McpBodyReadFailure::TooLarge));
    }

    #[tokio::test]
    async fn bounded_body_reader_rejects_a_frame_that_exhausts_the_remaining_budget() {
        let error = read_bounded_mcp_body(Body::new(OversizedFrameBody))
            .await
            .expect_err("an oversized frame cannot fit the request budget");
        assert!(matches!(error, McpBodyReadFailure::TooLarge));
    }

    #[test]
    #[should_panic(expected = "an already oversized size hint must be rejected before polling")]
    fn oversized_hint_body_guard_panics_if_polled() {
        let mut body = OversizedHintBody;
        let mut context = Context::from_waker(std::task::Waker::noop());
        let _ = Pin::new(&mut body).poll_frame(&mut context);
    }

    #[test]
    fn protected_resource_origins_are_reduced_to_an_exact_effective_port() {
        assert_eq!(
            allowed_mcp_origins(Some("https://mcp.example/mcp")),
            vec!["https://mcp.example:443"]
        );
        assert_eq!(
            allowed_mcp_origins(Some("http://localhost:8788/mcp?ignored=true")),
            vec!["http://localhost:8788"]
        );
        assert_eq!(
            allowed_mcp_origins(Some("https://[::1]/mcp")),
            vec!["https://[::1]:443"]
        );

        let invalid = allowed_mcp_origins(Some("not a resource URL"));
        assert_eq!(invalid.len(), 1);
        assert!(invalid[0].is_empty());

        let unsupported_scheme = allowed_mcp_origins(Some("ftp://mcp.example/mcp"));
        assert_eq!(unsupported_scheme.len(), 1);
        assert!(unsupported_scheme[0].is_empty());
    }

    #[test]
    fn protected_resource_hosts_are_reduced_to_one_fail_closed_hostname() {
        assert_eq!(
            allowed_mcp_hosts(Some("https://MCP.example:4443/mcp")),
            vec!["mcp.example"]
        );
        assert_eq!(allowed_mcp_hosts(Some("https://[::1]/mcp")), vec!["[::1]"]);
        assert_eq!(
            allowed_mcp_hosts(None),
            vec!["localhost", "127.0.0.1", "::1"]
        );

        for invalid in ["not a resource URL", "ftp://mcp.example/mcp"] {
            let allowed = allowed_mcp_hosts(Some(invalid));
            assert_eq!(allowed.len(), 1);
            assert!(allowed[0].is_empty());
        }
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
                &Method::POST,
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
            .oneshot(middleware_request(&Method::POST, Body::from("not json")))
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
                control_notification_request("notifications/initialized", &json!({}))
            } else {
                control_notification_request(
                    "notifications/cancelled",
                    &json!({"requestId": index, "reason": "test"}),
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
