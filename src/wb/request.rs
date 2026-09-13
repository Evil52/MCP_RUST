//! One enforced dispatch boundary for every WB read.

use super::{
    EndpointPolicy, Method, StatusCode, TokioInstant, Url, Value, WbClient, WbError,
    bearer_authorization,
};

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
}
