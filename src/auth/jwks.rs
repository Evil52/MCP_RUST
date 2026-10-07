//! JWKS retrieval, bounded parsing and the verified-key cache.

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use jsonwebtoken::{
    DecodingKey,
    jwk::{AlgorithmParameters, JwkSet},
};

use super::{JwtAuthenticationFailure, JwtAuthenticator, read_bounded};

#[derive(Debug)]
pub(super) struct CachedJwks {
    fetched_at: Instant,
    keys: JwkSet,
    /// Keys parsed once per fetch instead of once per request, for the first
    /// JWK of each `kid` exactly as `JwkSet::find` selects it. `None` marks a
    /// `kid` whose key material is unusable.
    decoding_keys: BTreeMap<String, Option<DecodingKey>>,
}

impl CachedJwks {
    fn new(fetched_at: Instant, keys: JwkSet) -> Self {
        let mut decoding_keys = BTreeMap::new();
        for jwk in &keys.keys {
            if let Some(kid) = jwk.common.key_id.as_ref()
                && !decoding_keys.contains_key(kid)
            {
                decoding_keys.insert(kid.clone(), DecodingKey::from_jwk(jwk).ok());
            }
        }
        Self {
            fetched_at,
            keys,
            decoding_keys,
        }
    }

    /// `None` when the set has no such `kid`.
    fn decoding_key(&self, kid: &str) -> Option<Result<DecodingKey, JwtAuthenticationFailure>> {
        let key = self.decoding_keys.get(kid)?;
        Some(
            key.clone()
                .ok_or(JwtAuthenticationFailure::VerifierUnavailable),
        )
    }
}

#[derive(Debug, Default)]
pub(super) struct JwksCacheState {
    cache: Option<CachedJwks>,
    pub(super) last_unknown_kid_refresh_at: Option<Instant>,
    pub(super) last_failed_refresh_at: Option<Instant>,
}

pub(super) const UNKNOWN_KID_REFRESH_COOLDOWN: Duration = Duration::from_secs(30);
pub(super) const FAILED_REFRESH_COOLDOWN: Duration = Duration::from_secs(5);
pub(super) const MAX_JWKS_BODY_BYTES: usize = 1024 * 1024;
pub(super) const MAX_JWKS_KEYS: usize = 64;
pub(super) const MAX_JWK_STRING_BYTES: usize = 16 * 1024;

fn jwks_strings_are_bounded(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(value) => value.len() <= MAX_JWK_STRING_BYTES,
        serde_json::Value::Array(values) => values.iter().all(jwks_strings_are_bounded),
        serde_json::Value::Object(values) => values.iter().all(|(name, value)| {
            name.len() <= MAX_JWK_STRING_BYTES && jwks_strings_are_bounded(value)
        }),
        _ => true,
    }
}

pub(super) fn parse_bounded_jwks(
    body: &[u8],
) -> std::result::Result<JwkSet, JwtAuthenticationFailure> {
    let value = serde_json::from_slice::<serde_json::Value>(body)
        .map_err(|_| JwtAuthenticationFailure::VerifierUnavailable)?;
    let keys = value
        .get("keys")
        .and_then(serde_json::Value::as_array)
        .ok_or(JwtAuthenticationFailure::VerifierUnavailable)?;
    if keys.is_empty() || keys.len() > MAX_JWKS_KEYS || !jwks_strings_are_bounded(&value) {
        return Err(JwtAuthenticationFailure::VerifierUnavailable);
    }
    let jwks = serde_json::from_value::<JwkSet>(value)
        .map_err(|_| JwtAuthenticationFailure::VerifierUnavailable)?;
    if jwks
        .keys
        .iter()
        .any(|jwk| matches!(&jwk.algorithm, AlgorithmParameters::Other(_)))
    {
        return Err(JwtAuthenticationFailure::VerifierUnavailable);
    }
    Ok(jwks)
}

impl JwtAuthenticator {
    pub(super) async fn fetch_jwks(&self) -> std::result::Result<JwkSet, JwtAuthenticationFailure> {
        let mut response = self
            .client
            .get(&self.config.jwks_url)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|_| JwtAuthenticationFailure::VerifierUnavailable)?;
        if !response.status().is_success() {
            return Err(JwtAuthenticationFailure::VerifierUnavailable);
        }
        let body = read_bounded(&mut response, MAX_JWKS_BODY_BYTES)
            .await
            .map_err(|_| JwtAuthenticationFailure::VerifierUnavailable)?;
        parse_bounded_jwks(&body)
    }

    fn cached_decoding_key(
        &self,
        state: &JwksCacheState,
        kid: &str,
    ) -> std::result::Result<Option<DecodingKey>, JwtAuthenticationFailure> {
        let fresh_cache = state
            .cache
            .as_ref()
            .filter(|cache| cache.fetched_at.elapsed() < self.config.jwks_cache_ttl);
        if let Some(key) = fresh_cache.and_then(|cache| cache.decoding_key(kid)) {
            return key.map(Some);
        }
        if state
            .last_failed_refresh_at
            .is_some_and(|at| at.elapsed() < FAILED_REFRESH_COOLDOWN)
        {
            return Err(JwtAuthenticationFailure::VerifierUnavailable);
        }
        if fresh_cache.is_some()
            && state
                .last_unknown_kid_refresh_at
                .is_some_and(|at| at.elapsed() < UNKNOWN_KID_REFRESH_COOLDOWN)
        {
            return Err(JwtAuthenticationFailure::InvalidToken);
        }
        Ok(None)
    }

    pub(super) async fn decoding_key(
        &self,
        kid: &str,
    ) -> std::result::Result<DecodingKey, JwtAuthenticationFailure> {
        let cached_key = {
            let state = self.cache.read().await;
            self.cached_decoding_key(&state, kid)?
        };
        if let Some(key) = cached_key {
            return Ok(key);
        }

        // Only one task may fetch JWKS. Every waiter re-checks the cache after
        // acquiring the gate, so concurrent misses are coalesced into one fetch.
        let _refresh_guard = self.refresh_gate.lock().await;
        let cached_key = {
            let state = self.cache.read().await;
            self.cached_decoding_key(&state, kid)?
        };
        if let Some(key) = cached_key {
            return Ok(key);
        }

        let refresh_is_for_unknown_kid =
            self.cache.read().await.cache.as_ref().is_some_and(|cache| {
                cache.fetched_at.elapsed() < self.config.jwks_cache_ttl
                    && cache.keys.find(kid).is_none()
            });
        let keys = match self.fetch_jwks().await {
            Ok(keys) => keys,
            Err(error) => {
                let failed_at = Instant::now();
                let mut state = self.cache.write().await;
                state.last_failed_refresh_at = Some(failed_at);
                if refresh_is_for_unknown_kid {
                    state.last_unknown_kid_refresh_at = Some(failed_at);
                }
                drop(state);
                return Err(error);
            }
        };
        let fetched_at = Instant::now();
        let fetched = CachedJwks::new(fetched_at, keys);
        let key = fetched.decoding_key(kid).transpose();
        let missing_after_refresh = matches!(key, Ok(None));
        let mut state = self.cache.write().await;
        state.cache = Some(fetched);
        state.last_failed_refresh_at = None;
        state.last_unknown_kid_refresh_at =
            (refresh_is_for_unknown_kid || missing_after_refresh).then_some(fetched_at);
        drop(state);
        key?.ok_or(JwtAuthenticationFailure::InvalidToken)
    }
}
