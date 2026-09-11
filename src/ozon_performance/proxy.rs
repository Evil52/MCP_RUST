use super::{
    BTreeMap, Duration, MIN_REQUEST_INTERVAL, PERFORMANCE_API_BASE_URL, PerformanceClient,
    PerformanceClientBuildError, PerformanceCredentials, PerformanceRequestPacer, StoreId,
};

impl PerformanceClient {
    /// Builds a client using one deployment-owned HTTPS forward proxy.
    ///
    /// Ambient proxy variables remain disabled. This constructor is reserved
    /// for the isolated report collector whose network namespace can reach
    /// only the fixed egress gateway.
    pub fn new_with_https_proxy(
        timeout: Duration,
        credentials: BTreeMap<StoreId, PerformanceCredentials>,
        proxy_url: &str,
    ) -> Result<Self, PerformanceClientBuildError> {
        Self::build(
            PERFORMANCE_API_BASE_URL.to_owned(),
            timeout,
            MIN_REQUEST_INTERVAL,
            credentials,
            concat!("mcp-ozon/", env!("CARGO_PKG_VERSION")),
            Some(proxy_url),
            None,
        )
    }

    /// Builds the isolated read client with a pacing boundary shared by every
    /// configured credential. Guard runtimes currently bind exactly one
    /// account, so sharing one arbiter is both exact and fail-closed if that
    /// invariant ever broadens.
    pub(crate) fn new_with_https_proxy_and_pacer(
        timeout: Duration,
        credentials: BTreeMap<StoreId, PerformanceCredentials>,
        proxy_url: &str,
        pacing: &PerformanceRequestPacer,
    ) -> Result<Self, PerformanceClientBuildError> {
        Self::build(
            PERFORMANCE_API_BASE_URL.to_owned(),
            timeout,
            MIN_REQUEST_INTERVAL,
            credentials,
            concat!("mcp-ozon/", env!("CARGO_PKG_VERSION")),
            Some(proxy_url),
            Some(pacing),
        )
    }
}
