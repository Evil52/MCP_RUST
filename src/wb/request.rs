//! One enforced dispatch boundary for every WB read.

use super::{
    EndpointPolicy, Method, RequestClass, StatusCode, TokenLimiter, TokioInstant, Url, Value,
    WbClient, WbError, bearer_authorization,
};

use tokio::sync::SemaphorePermit;

impl WbClient {
    pub(super) async fn request(
        &self,
        account: &str,
        method: Method,
        path: &str,
        query: Option<Vec<(&'static str, String)>>,
        payload: Option<Value>,
    ) -> Result<Value, WbError> {
        self.request_document(account, method, path, query, payload)
            .await?
            .ok_or_else(|| WbError::Api {
                status: StatusCode::NO_CONTENT,
                request_id: None,
                diagnostic: String::new(),
            })
    }

    pub(super) async fn request_document(
        &self,
        account: &str,
        method: Method,
        path: &str,
        query: Option<Vec<(&'static str, String)>>,
        payload: Option<Value>,
    ) -> Result<Option<Value>, WbError> {
        // Enforced here, at the only point where a WB request can leave the
        // process, so the read-only guarantee does not depend on callers.
        let Some(endpoint_policy) = EndpointPolicy::for_request(&method, path) else {
            return Err(WbError::EndpointNotAllowed {
                method,
                path: path.to_owned(),
            });
        };
        let endpoint = endpoint_policy.label;
        let request_class = endpoint_policy.request_class;
        let base_url = self.base_urls.base_url(endpoint_policy.host);
        let mut url = Url::parse(&format!("{base_url}{path}"))
            .expect("static production or validated test WB base URL");
        if let Some(query) = query {
            url.query_pairs_mut().extend_pairs(query);
        }
        let url = url.to_string();
        let credentials = self
            .accounts
            .get(account)
            .ok_or_else(|| WbError::MissingCredentials(account.to_owned()))?;
        let limiter = self
            .limiters
            .get(account)
            .expect("configured WB account has a limiter");
        let authorization = bearer_authorization(&credentials.token)?;

        let deadline = TokioInstant::now() + self.logical_timeout;
        self.request_with_retries(
            account,
            method,
            endpoint,
            request_class,
            limiter,
            url,
            authorization,
            payload,
            deadline,
        )
        .await
    }
    pub(super) async fn acquire_request_permits<'a>(
        &'a self,
        limiter: &'a TokenLimiter,
        request_class: RequestClass,
        retry: bool,
        deadline: TokioInstant,
    ) -> Result<(SemaphorePermit<'a>, SemaphorePermit<'a>), WbError> {
        let interval = if request_class == RequestClass::FinanceReport {
            limiter
                .finance_access
                .interval(self.policy.finance_interval)
                .await
        } else {
            self.policy.interval(request_class)
        };
        loop {
            // Readiness does not consume quota. Queued callers therefore hold
            // no network capacity, and a fail-fast permit rejection cannot
            // postpone the next real WB request by 20 or 60s.
            limiter
                .wait_until_ready(request_class, retry, deadline)
                .await?;
            let global_permit = self
                .global_in_flight
                .try_acquire()
                .map_err(|_| WbError::Overloaded)?;
            let Ok(token_permit) = limiter.in_flight.try_acquire() else {
                drop(global_permit);
                return Err(WbError::Overloaded);
            };
            if limiter.try_claim(request_class, interval).await.is_ok() {
                return Ok((global_permit, token_permit));
            }

            // Another ready caller claimed the departure between our readiness
            // check and permit acquisition. Never queue while reserving scarce
            // HTTP capacity; retry the readiness phase.
            drop(token_permit);
            drop(global_permit);
        }
    }
}
