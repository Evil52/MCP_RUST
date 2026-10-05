//! Server construction and initial capability configuration.

use super::{
    Arc, Duration, JwtAuthenticator, MAX_IN_FLIGHT_TOOL_CALLS, OzonClient, OzonMcp,
    PerformanceClient, RefreshRequestService, RegistrySource, ReportingReader, Semaphore,
    ToolTelemetryService, ToolTextContent, WbClient,
};
use crate::reporting::advertising_history::HistoryRepository;

impl OzonMcp {
    #[must_use]
    pub fn new(client: OzonClient, actor_id: String, registry: RegistrySource) -> Self {
        Self {
            reporting_only: false,
            client,
            performance_client: PerformanceClient::empty(Duration::from_secs(30)),
            wb_client: WbClient::empty(Duration::from_secs(30)),
            default_actor_id: Some(actor_id),
            authenticator: None,
            registry,
            reporting_reader: ReportingReader::disabled(),
            advertising_history: HistoryRepository::default(),
            refresh_requests: RefreshRequestService::disabled(),
            tool_telemetry: ToolTelemetryService::disabled(),
            tool_router: Self::default_tool_router(None),
            tool_call_slots: Arc::new(Semaphore::new(MAX_IN_FLIGHT_TOOL_CALLS)),
            tool_text_content: ToolTextContent::Json,
        }
    }

    #[must_use]
    pub fn new_authenticated(
        client: OzonClient,
        registry: RegistrySource,
        authenticator: JwtAuthenticator,
    ) -> Self {
        let tool_router = Self::default_tool_router(Some(&authenticator));
        Self {
            reporting_only: false,
            client,
            performance_client: PerformanceClient::empty(Duration::from_secs(30)),
            wb_client: WbClient::empty(Duration::from_secs(30)),
            default_actor_id: None,
            authenticator: Some(authenticator),
            registry,
            reporting_reader: ReportingReader::disabled(),
            advertising_history: HistoryRepository::default(),
            refresh_requests: RefreshRequestService::disabled(),
            tool_telemetry: ToolTelemetryService::disabled(),
            tool_router,
            tool_call_slots: Arc::new(Semaphore::new(MAX_IN_FLIGHT_TOOL_CALLS)),
            tool_text_content: ToolTextContent::Json,
        }
    }
}
