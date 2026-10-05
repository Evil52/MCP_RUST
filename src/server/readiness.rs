use super::OzonMcp;

impl OzonMcp {
    /// Verifies only deployment-owned dependencies used by the request path.
    /// Marketplace APIs are intentionally excluded from readiness.
    pub(crate) async fn readiness(&self) -> Result<(), ()> {
        if let Err(error) = self.registry.load_async().await {
            tracing::warn!(%error, "MCP readiness failed: access registry is invalid");
            return Err(());
        }
        if let Err(error) = self.reporting_reader.probe().await {
            tracing::warn!(%error, "MCP readiness failed: reporting reader is unavailable");
            return Err(());
        }
        if self.refresh_requests.is_enabled()
            && let Err(error) = self.refresh_requests.probe().await
        {
            tracing::warn!(%error, "MCP readiness failed: report refresh queue is unavailable");
            return Err(());
        }
        if let Err(error) = self.advertising_history.probe().await {
            tracing::warn!(%error, "MCP readiness failed: advertising history is unavailable");
            return Err(());
        }
        if let Err(error) = self.tool_telemetry.probe().await {
            tracing::warn!(%error, "MCP readiness failed: tool telemetry is unavailable");
            return Err(());
        }
        Ok(())
    }
}
