use super::{
    AccountState, CachedToken, Duration, HeaderValue, Instant, MAX_ACCESS_TOKEN_BYTES,
    MAX_TOKEN_BODY_BYTES, MAX_TOKEN_LIFETIME, PerformanceClient, PerformanceError,
    PerformanceErrorKind, TOKEN_EXPIRY_SKEW, TOKEN_FAILURE_COOLDOWN, TOKEN_PATH, TokenCooldown,
    TokenResponse, TokenSlot, classify_status, classify_transport, json, read_body,
    safe_request_id,
};

/// Dropping a cancelled refresh must suppress the next single-flight waiter
/// just as a returned transport error would. Start the delay at completion.
struct TokenRefresh<'a> {
    slot: &'a mut TokenSlot,
    failure: Option<&'static str>,
}

impl Drop for TokenRefresh<'_> {
    fn drop(&mut self) {
        if let Some(previous) = self.failure {
            self.slot.cooldown = Some(TokenCooldown {
                until: Instant::now() + TOKEN_FAILURE_COOLDOWN,
                previous,
            });
        }
    }
}

impl PerformanceClient {
    pub(super) async fn invalidate_token_if_current(&self, state: &AccountState, used_token: &str) {
        let mut slot = state.token.lock().await;
        if slot
            .cached
            .as_ref()
            .is_some_and(|cached| cached.value == used_token)
        {
            slot.cached = None;
        }
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "the token mutex deliberately provides a single-flight OAuth exchange"
    )]
    pub(super) async fn access_token(
        &self,
        state: &AccountState,
    ) -> Result<String, PerformanceError> {
        let mut slot = state.token.lock().await;
        let now = Instant::now();
        if let Some(token) = slot.cached.as_ref()
            && token.refresh_at > now
        {
            return Ok(token.value.clone());
        }
        // Preserve the vendor delay, including when it exceeds the fallback
        // token-failure cooldown or shared PostgreSQL coordination is disabled.
        state.oauth_cooldown.check().await?;
        // Holding the lock across the request is what makes this single-flight,
        // so a queued caller must not simply repeat an attempt that just failed.
        if let Some(cooldown) = slot.cooldown.as_ref() {
            if cooldown.until > now {
                return Err(PerformanceError::TokenUnavailable {
                    previous: cooldown.previous,
                });
            }
            slot.cooldown = None;
        }
        // Single-flight waiters hold no network permit. Only the caller that
        // actually refreshes participates in the separate OAuth capacity limit.
        let _permit = self
            .oauth_in_flight
            .try_acquire()
            .map_err(|_| PerformanceError::Overloaded)?;
        let mut refresh = TokenRefresh {
            slot: &mut slot,
            failure: Some(PerformanceErrorKind::Timeout.code()),
        };
        match self.request_token(state).await {
            Ok((value, refresh_at)) => {
                refresh.slot.cached = Some(CachedToken {
                    value: value.clone(),
                    refresh_at,
                });
                refresh.slot.cooldown = None;
                refresh.failure = None;
                Ok(value)
            }
            Err(error) => {
                refresh.failure = Some(error.kind().code());
                Err(error)
            }
        }
    }

    /// Performs one OAuth client-credentials exchange and validates it.
    ///
    /// Returns the token together with the instant it must be refreshed, so
    /// the caller can cache both without re-deriving the lifetime.
    async fn request_token(
        &self,
        state: &AccountState,
    ) -> Result<(String, Instant), PerformanceError> {
        self.admit_shared_request(state, "oauth", TOKEN_FAILURE_COOLDOWN)
            .await?;
        let response = self
            .http
            .post(format!("{}{}", self.base_url, TOKEN_PATH))
            .json(&json!({
                "client_id": state.credentials.client_id,
                "client_secret": state.credentials.client_secret,
                "grant_type": "client_credentials",
            }))
            .send()
            .await
            .map_err(|error| classify_transport(&error))?;
        self.defer_shared_response(state, "oauth", &response)
            .await?;
        let request_id = safe_request_id(response.headers());
        let status = response.status();
        if !status.is_success() {
            return Err(classify_status(
                status,
                request_id,
                crate::ozon::retry_after_duration(response.headers()),
            ));
        }
        let bytes = read_body(response, MAX_TOKEN_BODY_BYTES, request_id.clone()).await?;
        let token: TokenResponse = serde_json::from_slice(&bytes)
            .map_err(|source| PerformanceError::InvalidJson { request_id, source })?;
        if token.access_token.is_empty()
            || token.access_token.len() > MAX_ACCESS_TOKEN_BYTES
            || token.expires_in == 0
            || Duration::from_secs(token.expires_in) > MAX_TOKEN_LIFETIME
            || !token.token_type.eq_ignore_ascii_case("bearer")
        {
            return Err(PerformanceError::InvalidToken);
        }
        let mut authorization = HeaderValue::from_str(&format!("Bearer {}", token.access_token))
            .map_err(|_| PerformanceError::InvalidToken)?;
        authorization.set_sensitive(true);
        let lifetime = Duration::from_secs(token.expires_in);
        let refresh_after = lifetime.saturating_sub(TOKEN_EXPIRY_SKEW);
        let refresh_at = Instant::now()
            .checked_add(refresh_after)
            .ok_or(PerformanceError::InvalidToken)?;
        Ok((token.access_token, refresh_at))
    }
}
