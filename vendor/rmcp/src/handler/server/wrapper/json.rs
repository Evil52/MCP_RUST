use std::{
    borrow::Cow,
    io::{self, Write},
};

use schemars::JsonSchema;
use serde::Serialize;

use crate::{
    handler::server::tool::IntoCallToolResult,
    model::{CallToolResponse, CallToolResult},
};

/// Json wrapper for structured output
///
/// When used with tools, this wrapper indicates that the value should be
/// serialized as structured JSON content with an associated schema.
/// The framework will place the JSON in the `structured_content` field
/// of the tool result rather than the regular `content` field.
#[expect(clippy::exhaustive_structs, reason = "intentionally exhaustive")]
pub struct Json<T>(pub T);

// Marketplace clients cap their raw JSON at 2 MiB. Keep a small, fixed amount
// of headroom for the account/endpoint metadata added by the tool wrapper.
const MAX_STRUCTURED_CONTENT_BYTES: usize = (2 * 1024 * 1024) + (64 * 1024);

// MCP compatibility requires structured output in both `structuredContent`
// and a JSON-encoded text block. In the worst case, JSON-string escaping can
// double the fallback copy, so bound the serialized CallToolResult itself as
// well as the input used to construct it.
const MAX_SERIALIZED_CALL_TOOL_RESULT_BYTES: usize = (3 * 2 * 1024 * 1024) + (64 * 1024);

struct BoundedWriter<W> {
    inner: W,
    written: usize,
    limit: usize,
    limit_exceeded: bool,
}

impl<W> BoundedWriter<W> {
    const fn new(inner: W, limit: usize) -> Self {
        Self {
            inner,
            written: 0,
            limit,
            limit_exceeded: false,
        }
    }

    fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write> Write for BoundedWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let Some(new_len) = self.written.checked_add(buffer.len()) else {
            self.limit_exceeded = true;
            return Err(io::Error::other(
                "structured tool result size limit exceeded",
            ));
        };
        if new_len > self.limit {
            self.limit_exceeded = true;
            return Err(io::Error::other(
                "structured tool result size limit exceeded",
            ));
        }

        let written = self.inner.write(buffer)?;
        self.written += written;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn serialization_error(limit_exceeded: bool) -> crate::ErrorData {
    let message = if limit_exceeded {
        "Structured tool result exceeds the response size limit"
    } else {
        "Failed to serialize structured content"
    };
    crate::ErrorData::internal_error(message, None)
}

// Implement JsonSchema for Json<T> to delegate to T's schema
impl<T: JsonSchema> JsonSchema for Json<T> {
    fn schema_name() -> Cow<'static, str> {
        T::schema_name()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        T::json_schema(generator)
    }
}

// Implementation for Json<T> to create structured content
impl<T: Serialize + JsonSchema + 'static> IntoCallToolResult for Json<T> {
    fn into_call_tool_result(self) -> Result<CallToolResponse, crate::ErrorData> {
        let inner = self.0;
        let mut writer = BoundedWriter::new(Vec::new(), MAX_STRUCTURED_CONTENT_BYTES);

        if serde_json::to_writer(&mut writer, &inner).is_err() {
            return Err(serialization_error(writer.limit_exceeded));
        }

        let serialized = writer.into_inner();
        drop(inner);

        let value = serde_json::from_slice(&serialized).map_err(|_| {
            crate::ErrorData::internal_error("Failed to serialize structured content", None)
        })?;
        drop(serialized);

        let result = CallToolResult::structured(value);
        let mut writer = BoundedWriter::new(
            io::sink(),
            MAX_SERIALIZED_CALL_TOOL_RESULT_BYTES,
        );
        if serde_json::to_writer(&mut writer, &result).is_err() {
            return Err(serialization_error(writer.limit_exceeded));
        }

        Ok(result.into())
    }
}
