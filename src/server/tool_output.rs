//! Post-processing of a finished tool call before it leaves the server.

use std::str::FromStr;

use rmcp::model::{CallToolResponse, ContentBlock};
use serde_json::Value;

use super::OzonMcp;
use crate::tool_telemetry::ToolCallOutcome;

/// Text block that replaces the JSON mirror in [`ToolTextContent::Summary`].
const STRUCTURED_CONTENT_POINTER: &str = "Результат находится в structuredContent.";

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
    /// Replaces the JSON mirror of a successful structured result.
    ///
    /// Error results keep their text because it is the public failure message.
    /// A mirror shorter than the pointer is kept as well.
    pub(super) fn apply(self, result: &mut Result<CallToolResponse, rmcp::ErrorData>) {
        let (Self::Summary, Ok(CallToolResponse::Complete(result))) = (self, result) else {
            return;
        };
        if result.is_error == Some(true) || result.structured_content.is_none() {
            return;
        }
        if let [ContentBlock::Text(text)] = result.content.as_mut_slice()
            && text.text.len() > STRUCTURED_CONTENT_POINTER.len()
        {
            STRUCTURED_CONTENT_POINTER.clone_into(&mut text.text);
        }
    }
}

impl OzonMcp {
    #[must_use]
    pub const fn with_tool_text_content(mut self, tool_text_content: ToolTextContent) -> Self {
        self.tool_text_content = tool_text_content;
        self
    }
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
mod tests {
    use rmcp::{
        handler::server::{tool::IntoCallToolResult, wrapper::Json},
        model::CallToolResult,
    };
    use serde_json::json;

    use super::*;

    fn structured(value: Value) -> Result<CallToolResponse, rmcp::ErrorData> {
        Json(value).into_call_tool_result()
    }

    fn text_and_structured(
        response: &Result<CallToolResponse, rmcp::ErrorData>,
    ) -> (&str, Option<&Value>) {
        let Ok(CallToolResponse::Complete(result)) = response else {
            panic!("the fixture must be a complete tool result");
        };
        let [ContentBlock::Text(text)] = result.content.as_slice() else {
            panic!("the fixture must carry exactly one text block");
        };
        (&text.text, result.structured_content.as_ref())
    }

    fn large_value() -> Value {
        json!({"rows": (0..50).map(|sku| json!({"sku": sku, "stock": sku * 3})).collect::<Vec<_>>()})
    }

    #[test]
    fn mode_parses_only_the_documented_values() {
        assert_eq!(
            "json".parse::<ToolTextContent>().unwrap(),
            ToolTextContent::Json
        );
        assert_eq!(
            "summary".parse::<ToolTextContent>().unwrap(),
            ToolTextContent::Summary
        );
        for rejected in ["", "JSON", "none", " summary"] {
            assert!(rejected.parse::<ToolTextContent>().is_err(), "{rejected:?}");
        }
        assert_eq!(ToolTextContent::default(), ToolTextContent::Json);
    }

    #[test]
    fn json_mode_keeps_the_full_mirror() {
        let value = large_value();
        let mut response = structured(value.clone());
        ToolTextContent::Json.apply(&mut response);

        let (text, structured) = text_and_structured(&response);
        assert_eq!(serde_json::from_str::<Value>(text).unwrap(), value);
        assert_eq!(structured, Some(&value));
    }

    #[test]
    fn summary_mode_replaces_the_mirror_and_keeps_structured_content() {
        let value = large_value();
        let mut response = structured(value.clone());
        let mirrored_bytes = text_and_structured(&response).0.len();
        ToolTextContent::Summary.apply(&mut response);

        let (text, structured) = text_and_structured(&response);
        assert_eq!(text, STRUCTURED_CONTENT_POINTER);
        assert!(text.len() * 10 < mirrored_bytes, "{mirrored_bytes}");
        assert_eq!(structured, Some(&value));
    }

    #[test]
    fn summary_mode_keeps_a_mirror_shorter_than_the_pointer() {
        let mut response = structured(json!({"ok": true}));
        ToolTextContent::Summary.apply(&mut response);

        assert_eq!(text_and_structured(&response).0, r#"{"ok":true}"#);
    }

    #[test]
    fn summary_mode_keeps_error_and_unstructured_results() {
        let message = "x".repeat(200);
        let mut error = Ok(CallToolResult::structured_error(json!({"message": message})).into());
        let mut plain = Ok(CallToolResult::success(vec![ContentBlock::text(message)]).into());
        let mut protocol_error = Err(rmcp::ErrorData::invalid_params("bad request", None));
        for response in [&mut error, &mut plain, &mut protocol_error] {
            let before = format!("{response:?}");
            ToolTextContent::Summary.apply(response);
            assert_eq!(format!("{response:?}"), before);
        }
    }
}
