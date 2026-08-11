use std::{
    borrow::Cow,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use rmcp::{
    ErrorData,
    handler::server::{tool::IntoCallToolResult, wrapper::Json},
    model::{CallToolResponse, ErrorCode},
};
use schemars::JsonSchema;
use serde::{Serialize, Serializer, ser::SerializeSeq};

const STRUCTURED_CONTENT_LIMIT_BYTES: usize = (2 * 1024 * 1024) + (64 * 1024);
const SERIALIZED_CALL_TOOL_RESULT_LIMIT_BYTES: usize = (3 * 2 * 1024 * 1024) + (64 * 1024);

struct CountedString {
    payload: String,
    serialization_count: Arc<AtomicUsize>,
}

impl Serialize for CountedString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.serialization_count.fetch_add(1, Ordering::Relaxed);
        serializer.serialize_str(&self.payload)
    }
}

impl JsonSchema for CountedString {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("CountedString")
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        String::json_schema(generator)
    }
}

#[test]
fn structured_json_at_limit_preserves_the_existing_output_shape() {
    let payload = "a".repeat(STRUCTURED_CONTENT_LIMIT_BYTES - 2);
    let serialization_count = Arc::new(AtomicUsize::new(0));
    let counted_payload = CountedString {
        payload: payload.clone(),
        serialization_count: Arc::clone(&serialization_count),
    };

    let response = Json(counted_payload)
        .into_call_tool_result()
        .expect("a JSON string exactly at the serialized limit must be accepted");
    let CallToolResponse::Complete(result) = response else {
        panic!("Json<T> must produce a complete tool result");
    };

    assert_eq!(result.structured_content, Some(payload.clone().into()));
    assert_eq!(result.content.len(), 1);
    let content = serde_json::to_value(&result.content[0]).expect("content must serialize");
    assert_eq!(content["type"], "text");
    assert_eq!(content["text"], format!("\"{payload}\""));
    assert_eq!(
        serialization_count.load(Ordering::Relaxed),
        1,
        "the accepted value must be serialized once into the bounded buffer"
    );
}

#[test]
fn structured_json_over_limit_returns_a_payload_free_internal_error() {
    let serialization_count = Arc::new(AtomicUsize::new(0));
    let payload = CountedString {
        payload: "a".repeat(STRUCTURED_CONTENT_LIMIT_BYTES - 1),
        serialization_count: Arc::clone(&serialization_count),
    };

    let error = Json(payload)
        .into_call_tool_result()
        .expect_err("a serialized JSON string one byte over the limit must be rejected");

    assert_eq!(
        error,
        ErrorData::new(
            ErrorCode::INTERNAL_ERROR,
            "Structured tool result exceeds the response size limit",
            None,
        )
    );
    assert_eq!(
        serialization_count.load(Ordering::Relaxed),
        1,
        "the oversized value must be rejected before conversion into a JSON Value"
    );
}

#[test]
fn quote_heavy_json_enforces_the_final_serialized_result_limit() {
    // For a JSON string containing `a` plain bytes and `q` quote bytes, the
    // complete CallToolResult is `2*a + 6*q + 106` bytes. These counts put the
    // accepted result exactly at the final limit while keeping its inner JSON
    // one byte below the separate structured-content limit.
    const QUOTE_BYTES: usize = 1_015_758;
    const PLAIN_BYTES: usize = 131_169;

    let mut payload = "\"".repeat(QUOTE_BYTES);
    payload.push_str(&"a".repeat(PLAIN_BYTES));

    let response = Json(payload)
        .into_call_tool_result()
        .expect("a final CallToolResult exactly at the wire-memory limit must be accepted");
    let CallToolResponse::Complete(result) = response else {
        panic!("Json<T> must produce a complete tool result");
    };
    let serialized = serde_json::to_vec(&result).expect("the accepted result must serialize");

    assert_eq!(serialized.len(), SERIALIZED_CALL_TOOL_RESULT_LIMIT_BYTES);

    drop(serialized);
    drop(result);

    let mut payload = "\"".repeat(QUOTE_BYTES);
    payload.push_str(&"a".repeat(PLAIN_BYTES + 1));

    let error = Json(payload)
        .into_call_tool_result()
        .expect_err("a final CallToolResult over the wire-memory limit must be rejected");
    assert_eq!(
        error,
        ErrorData::new(
            ErrorCode::INTERNAL_ERROR,
            "Structured tool result exceeds the response size limit",
            None,
        )
    );
}

struct StreamingStrings {
    item: String,
    item_count: usize,
    serialized_items: Arc<AtomicUsize>,
}

impl Serialize for StreamingStrings {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.item_count))?;
        for _ in 0..self.item_count {
            self.serialized_items.fetch_add(1, Ordering::Relaxed);
            sequence.serialize_element(&self.item)?;
        }
        sequence.end()
    }
}

impl JsonSchema for StreamingStrings {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("StreamingStrings")
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        Vec::<String>::json_schema(generator)
    }
}

#[test]
fn structured_json_stops_streaming_when_the_bounded_buffer_is_full() {
    let serialized_items = Arc::new(AtomicUsize::new(0));
    let item_count = 10_000;
    let payload = StreamingStrings {
        item: "a".repeat(1024),
        item_count,
        serialized_items: Arc::clone(&serialized_items),
    };

    let error = Json(payload)
        .into_call_tool_result()
        .expect_err("an oversized streamed value must be rejected at the byte limit");

    assert_eq!(
        error,
        ErrorData::new(
            ErrorCode::INTERNAL_ERROR,
            "Structured tool result exceeds the response size limit",
            None,
        )
    );
    assert!(
        serialized_items.load(Ordering::Relaxed) < item_count,
        "serialization must stop before traversing the complete oversized value"
    );
}
