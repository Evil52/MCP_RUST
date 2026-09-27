//! One enforced dispatch boundary for every WB read.

use std::time::Duration;

use crate::marketplace_quota::QuotaError;

use super::{
    AUTHORIZATION, AttemptContext, AttemptOutcome, ClientPolicy, EndpointPolicy, Instant, Method,
    RequestClass, StatusCode, TokenLimiter, TokioInstant, Url, Value, WbClient, WbError,
    bearer_authorization, read_quota_error, transport_failure_outcome,
};

use tokio::sync::SemaphorePermit;
use tokio::time::sleep;

const SHORT_SHARED_QUOTA_WAIT: Duration = Duration::from_millis(500);
const SHARED_QUOTA_WAIT_MARGIN: Duration = Duration::from_millis(5);
const MAX_SHORT_SHARED_QUOTA_RETRIES: usize = 3;

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
    pub(super) async fn request_attempt(
        &self,
        context: AttemptContext<'_>,
        attempt: usize,
        retry: bool,
    ) -> Result<AttemptOutcome, WbError> {
        let quota_key = self.shared_quota_key(context)?;
        let mut short_quota_retries = 0;
        // Another process or a small clock difference can leave the shared
        // quota briefly behind the local limiter. Release both HTTP permits before waiting, then claim both
        // limits again. Longer vendor cooldowns remain fail-fast.
        let (_global_permit, _token_permit) = loop {
            let permits = self
                .acquire_request_permits(
                    context.limiter,
                    context.request_class,
                    retry,
                    context.deadline,
                )
                .await?;
            let Some(key) = quota_key.as_ref() else {
                break permits;
            };
            match self
                .shared_quota
                .admit(
                    key,
                    ClientPolicy::production(self.logical_timeout).interval(context.request_class),
                )
                .await
            {
                Ok(()) => break permits,
                Err(QuotaError::Limited { retry_after })
                    if retry_after <= SHORT_SHARED_QUOTA_WAIT
                        && short_quota_retries < MAX_SHORT_SHARED_QUOTA_RETRIES =>
                {
                    let delay = retry_after + SHARED_QUOTA_WAIT_MARGIN;
                    if TokioInstant::now() + delay >= context.deadline {
                        return Err(WbError::LocalRateLimited { retry_after });
                    }
                    short_quota_retries += 1;
                    drop(permits);
                    sleep(delay).await;
                }
                Err(error) => return Err(read_quota_error(error)),
            }
        };
        let mut request = self
            .http
            .request(context.method.clone(), context.url)
            .header(AUTHORIZATION, context.authorization.clone());
        if let Some(payload) = context.payload {
            request = request.json(payload);
        }

        let started = Instant::now();
        match request.send().await {
            Ok(response) => {
                self.response_outcome(context, attempt, started, response, quota_key.as_ref())
                    .await
            }
            Err(source) => {
                transport_failure_outcome(context, attempt, started, source, &self.policy)
            }
        }
    }
}
