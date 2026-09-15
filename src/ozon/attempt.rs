//! One Ozon Seller API attempt: permits, shared quota, response cooldowns and
//! the retry decision for that single request.

use super::{
    ANALYTICS_DATA_PATH, AnalyticsPacingMode, AttemptFailure, AttemptInput, Duration, Instant,
    OzonClient, OzonError, OzonErrorKind, RETRY_POLICY, RateLimiter, RequestTrace, RetryOwner,
    StatusCode, StoreCredentials, Utc, Value, analytics_queued_retry_plan, classify_http_error,
    classify_transport_error, decode_response, is_retriable_transport, parse_retry_after, policy,
    quota, read_bounded_diagnostic_body, retry_delay, retry_plan, safe_request_id,
    shared_retry_cooldown, trace_response, trace_transport_failure,
};

impl OzonClient {
    pub(super) async fn send_attempt(
        &self,
        input: AttemptInput<'_>,
    ) -> Result<Value, AttemptFailure> {
        let AttemptInput {
            limiter,
            credentials,
            store,
            path,
            payload,
            attempt,
            pacing_mode,
            retry_owner,
        } = input;
        let can_retry = retry_owner == RetryOwner::Client
            && RETRY_POLICY.allows_attempt(attempt)
            && !policy::is_search_path(path);
        let queue_analytics = pacing_mode == AnalyticsPacingMode::Queue || attempt > 1;
        let _permits = self
            .acquire_request_permits(limiter, path, queue_analytics)
            .await
            .map_err(AttemptFailure::terminal)?;
        let request_trace = RequestTrace {
            store,
            endpoint: path,
            started_at: Instant::now(),
            attempt,
        };
        self.admit_shared_request(&credentials.client_id, path)
            .await
            .map_err(AttemptFailure::terminal)?;
        let response = self
            .http
            .post(format!("{}{path}", self.base_url))
            .header("Client-Id", &credentials.client_id)
            .header("Api-Key", &credentials.api_key)
            .json(payload)
            .send()
            .await;

        let mut response = match response {
            Ok(response) => response,
            Err(source) => {
                return Err(transport_attempt_failure(
                    source,
                    &request_trace,
                    attempt,
                    can_retry,
                ));
            }
        };
        let status = response.status();
        let request_id = safe_request_id(response.headers());
        let retry_after = parse_retry_after(response.headers(), Utc::now());
        let vendor_retry_after = retry_after.duration();
        let local_cooldown = self
            .record_response_cooldowns(limiter, credentials, path, status, vendor_retry_after)
            .await?;
        let enforced_retry_after = local_cooldown.max(vendor_retry_after);
        let planned_retry = if can_retry {
            analytics_queued_retry_plan(path, status, pacing_mode, attempt, enforced_retry_after)
                .or_else(|| retry_plan(path, status, attempt, retry_after))
        } else {
            None
        };

        if path != ANALYTICS_DATA_PATH
            && let Some(delay) = shared_retry_cooldown(status, retry_after)
        {
            // Install a vendor-directed cooldown before `_permits` is
            // released, closing the window in which a same-Client-Id sibling
            // could leave during Retry-After.
            limiter.extend_cooldown(delay).await;
        }

        if let Some((delay, kind)) = planned_retry {
            trace_response(&request_trace, status, request_id.as_deref(), true, kind);
            let diagnostic = read_bounded_diagnostic_body(&mut response).await;
            let error = classify_http_error(
                status,
                request_id,
                vendor_retry_after,
                local_cooldown,
                diagnostic,
            );
            return Err(AttemptFailure {
                error,
                retry_delay: Some(delay),
            });
        }

        let result = decode_response(
            &mut response,
            status,
            request_id.clone(),
            vendor_retry_after,
            local_cooldown,
        )
        .await;
        if path == ANALYTICS_DATA_PATH && result.is_ok() {
            limiter.clear_analytics_rate_limit().await;
        }
        let kind = result
            .as_ref()
            .err()
            .map_or(OzonErrorKind::Http, OzonError::kind);
        // Receiving a successful status does not mean the complete JSON body
        // reached us. A proxy or upstream can close the stream between chunks;
        // all Ozon routes exposed by this client are read-only, so replaying
        // that interrupted attempt is safe and prevents partial analytics from
        // surfacing to browser clients.
        let will_retry = result
            .as_ref()
            .is_err_and(|error| is_retriable_transport(error.kind()) && can_retry);
        trace_response(
            &request_trace,
            status,
            request_id.as_deref(),
            will_retry,
            kind,
        );
        result.map_err(|error| AttemptFailure {
            error,
            retry_delay: will_retry.then(|| retry_delay(attempt, None)),
        })
    }

    /// Records every cooldown a response imposes before its permits are
    /// released: the local analytics rate limit, the search window and the
    /// shared quota deferral. Returns the local analytics cooldown, if any.
    async fn record_response_cooldowns(
        &self,
        limiter: &RateLimiter,
        credentials: &StoreCredentials,
        path: &'static str,
        status: StatusCode,
        vendor_retry_after: Option<Duration>,
    ) -> Result<Option<Duration>, AttemptFailure> {
        let local_cooldown =
            if path == ANALYTICS_DATA_PATH && status == StatusCode::TOO_MANY_REQUESTS {
                Some(
                    limiter
                        .record_analytics_rate_limit(vendor_retry_after)
                        .await,
                )
            } else {
                None
            };
        if policy::is_search_path(path)
            && let Some(delay) = quota::response_cooldown(status, vendor_retry_after, None)
        {
            let mut next = limiter.search_next_allowed.lock().await;
            // Bound untrusted header arithmetic, as the WB client does. The
            // uncapped vendor delay is still returned and passed to shared quota.
            *next = (*next).max(Instant::now() + delay.min(Duration::from_hours(24)));
        }
        if let Some(delay) = quota::response_cooldown(status, vendor_retry_after, local_cooldown) {
            self.defer_shared_request(&credentials.client_id, path, delay)
                .await
                .map_err(AttemptFailure::terminal)?;
        }
        Ok(local_cooldown)
    }
}

/// A request that failed before response headers: classify, trace, and plan a
/// retry only for a retriable transport failure the client owns.
fn transport_attempt_failure(
    source: reqwest::Error,
    request_trace: &RequestTrace<'_>,
    attempt: usize,
    can_retry: bool,
) -> AttemptFailure {
    let error = classify_transport_error(source, None);
    let kind = error.kind();
    let will_retry = is_retriable_transport(kind) && can_retry;
    trace_transport_failure(request_trace, kind, will_retry);
    AttemptFailure {
        error,
        retry_delay: will_retry.then(|| retry_delay(attempt, None)),
    }
}
