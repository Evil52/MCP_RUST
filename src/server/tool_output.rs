//! Construction and classification of tool results before they leave the server.

use std::{any::Any, future::Future, io, str::FromStr};

use rmcp::{
    handler::server::tool::IntoCallToolResult,
    model::{CallToolResponse, CallToolResult, ContentBlock},
};
use serde::Serialize;
use serde_json::Value;

use super::{OzonMcp, OzonResult, WbResult, WbSellerWarehouseStocksResult};
use crate::{reporting::mcp_read::SourceSnapshotResult, tool_telemetry::ToolCallOutcome};

/// Text block that replaces the JSON mirror in [`ToolTextContent::Summary`].
const STRUCTURED_CONTENT_POINTER: &str = "Результат находится в structuredContent.";

/// Same limits as rmcp's `Json<T>` (`vendor/rmcp/src/handler/server/wrapper/json.rs`);
/// `tests::limits_match_rmcp_json` fails if they drift apart.
const MAX_STRUCTURED_CONTENT_BYTES: usize = (2 * 1024 * 1024) + (64 * 1024);
const MAX_SERIALIZED_CALL_TOOL_RESULT_BYTES: usize = (3 * 2 * 1024 * 1024) + (64 * 1024);

/// What the text block of a successful structured tool result carries.
///
/// rmcp mirrors every `structuredContent` object into `content[0].text`, as the
/// MCP specification recommends for clients that predate structured output.
/// `ChatGPT` puts both fields into the model transcript, so the mirror doubles
/// the tokens of every result. Claude Desktop and Claude Code read only
/// `content`, so the mirror stays the default and the compact form is opt-in.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ToolTextContent {
    /// Full JSON copy of `structuredContent`.
    #[default]
    Json,
    /// A short pointer to `structuredContent`.
    Summary,
}

tokio::task_local! {
    static TOOL_TEXT_CONTENT: ToolTextContent;
}

impl FromStr for ToolTextContent {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "json" => Ok(Self::Json),
            "summary" => Ok(Self::Summary),
            _ => anyhow::bail!("MCP_TOOL_TEXT_CONTENT должен быть json или summary"),
        }
    }
}

impl ToolTextContent {
    /// Makes this mode visible to every [`Json`] result built inside `future`.
    pub(super) fn scope<F: Future>(self, future: F) -> impl Future<Output = F::Output> {
        TOOL_TEXT_CONTENT.scope(self, future)
    }

    /// Outside a tool call the result keeps the specification default.
    fn current() -> Self {
        TOOL_TEXT_CONTENT.try_with(|mode| *mode).unwrap_or_default()
    }
}

impl OzonMcp {
    #[must_use]
    pub const fn with_tool_text_content(mut self, tool_text_content: ToolTextContent) -> Self {
        self.tool_text_content = tool_text_content;
        self
    }
}

/// Structured tool output.
///
/// It replaces rmcp's `Json<T>` for this server and keeps its name because the
/// `#[tool]` macro derives the output schema only from a return type named
/// `Json<T>`. rmcp serializes the result to bytes, parses them into a second
/// `Value` tree and always builds the text mirror. This wrapper moves an
/// existing tree into `structuredContent` and builds the text block for the
/// current [`ToolTextContent`] only, with the same size limits and errors.
pub struct Json<T>(pub T);

impl<T: Serialize + 'static> IntoCallToolResult for Json<T> {
    fn into_call_tool_result(self) -> Result<CallToolResponse, rmcp::ErrorData> {
        let mut inner = self.0;
        let structured_bytes = serialized_len(&inner, MAX_STRUCTURED_CONTENT_BYTES)?;
        let moved = take_owned_trees(&mut inner);
        let value = with_moved_fields(serde_json::to_value(&inner), moved)
            .map_err(|_| serialization_error(false))?;
        drop(inner);
        let mirror = ToolTextContent::current() == ToolTextContent::Json
            || structured_bytes <= STRUCTURED_CONTENT_POINTER.len();
        let text = if mirror {
            value.to_string()
        } else {
            STRUCTURED_CONTENT_POINTER.to_owned()
        };
        let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
        result.structured_content = Some(value);
        // Without the mirror the result is the capped structured content plus
        // a fixed pointer, far below the whole-result cap.
        if mirror {
            serialized_len(&result, MAX_SERIALIZED_CALL_TOOL_RESULT_BYTES)?;
        }
        Ok(result.into())
    }
}

/// Takes the marketplace and snapshot `Value` trees out of the results that
/// own one, so they are moved into `structuredContent` rather than copied.
/// Every other result is small and typed, and is copied by `to_value`.
fn take_owned_trees(result: &mut dyn Any) -> Vec<(&'static str, Option<Value>)> {
    if let Some(result) = result.downcast_mut::<OzonResult>() {
        return vec![("data", Some(std::mem::take(&mut result.data)))];
    }
    if let Some(result) = result.downcast_mut::<WbResult>() {
        return vec![("data", Some(std::mem::take(&mut result.data)))];
    }
    if let Some(result) = result.downcast_mut::<WbSellerWarehouseStocksResult>() {
        return vec![("data", Some(std::mem::take(&mut result.source.data)))];
    }
    if let Some(result) = result.downcast_mut::<SourceSnapshotResult>() {
        return vec![
            ("rows", Some(Value::Array(std::mem::take(&mut result.rows)))),
            ("latest_collection", result.latest_collection.take()),
        ];
    }
    Vec::new()
}

/// Puts the fields taken out before serialization back into `object`.
///
/// A field that held `None` is left as serialized, so `skip_serializing_if`
/// and `null` behave exactly as for an untouched value.
fn with_moved_fields(
    object: serde_json::Result<Value>,
    moved: Vec<(&'static str, Option<Value>)>,
) -> serde_json::Result<Value> {
    let mut object = object?;
    if moved.is_empty() {
        return Ok(object);
    }
    let Value::Object(fields) = &mut object else {
        return Err(serde::ser::Error::custom(
            "structured result must be an object",
        ));
    };
    for (name, value) in moved {
        if let Some(value) = value {
            fields.insert(name.to_owned(), value);
        }
    }
    Ok(object)
}

/// Counts serialized bytes without allocating, failing past `limit`.
struct CountingWriter {
    written: usize,
    limit: usize,
    exceeded: bool,
}

impl io::Write for CountingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self.written.checked_add(buffer.len()) {
            Some(written) if written <= self.limit => {
                self.written = written;
                Ok(buffer.len())
            }
            _ => {
                self.exceeded = true;
                Err(io::Error::other(
                    "structured tool result size limit exceeded",
                ))
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialized_len(value: &impl Serialize, limit: usize) -> Result<usize, rmcp::ErrorData> {
    let mut writer = CountingWriter {
        written: 0,
        limit,
        exceeded: false,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| serialization_error(writer.exceeded))?;
    Ok(writer.written)
}

fn serialization_error(limit_exceeded: bool) -> rmcp::ErrorData {
    let message = if limit_exceeded {
        "Structured tool result exceeds the response size limit"
    } else {
        "Failed to serialize structured content"
    };
    rmcp::ErrorData::internal_error(message, None)
}

pub(super) fn classify_tool_call_result(
    result: &Result<CallToolResponse, rmcp::ErrorData>,
) -> (ToolCallOutcome, Option<&'static str>) {
    let Ok(response) = result else {
        return (ToolCallOutcome::Failed, Some("MCP_PROTOCOL_ERROR"));
    };
    let CallToolResponse::Complete(result) = response else {
        return (ToolCallOutcome::Succeeded, None);
    };
    if !result.is_error.unwrap_or(false) {
        return (ToolCallOutcome::Succeeded, None);
    }
    match result
        .structured_content
        .as_ref()
        .and_then(|value| value.pointer("/kind"))
        .and_then(Value::as_str)
    {
        Some("cancelled") => (ToolCallOutcome::Cancelled, Some("MCP_CANCELLED")),
        Some("local_overloaded") => (ToolCallOutcome::Overloaded, Some("MCP_LOCAL_OVERLOADED")),
        _ => (ToolCallOutcome::Failed, Some("MCP_TOOL_FAILURE")),
    }
}

#[cfg(test)]
mod tests;
