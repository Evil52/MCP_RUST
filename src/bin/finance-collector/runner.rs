//! Follow only durable deferrals. Authorization and all I/O remain in execute.
use std::{future::Future, time::Duration};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tokio::time::{Instant, sleep, timeout_at};

pub async fn run<F, Fut>(follow: bool, max_run_seconds: u64, mut execute: F) -> Result<Value>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Value>>,
{
    if !follow {
        return execute().await;
    }
    let deadline = Instant::now() + Duration::from_secs(max_run_seconds);
    loop {
        if Instant::now() >= deadline {
            return Ok(paused());
        }
        let result = match timeout_at(deadline, execute()).await {
            Ok(result) => result?,
            Err(_) => return Ok(paused()),
        };
        if result["status"] != "deferred" {
            return Ok(result);
        }
        let next = result
            .get("next_request_at")
            .and_then(Value::as_str)
            .context("deferred collection lacks its durable next-request time")?;
        let next = DateTime::parse_from_rfc3339(next)
            .context("invalid next-request time")?
            .with_timezone(&Utc);
        // A local quantum may be due already. Never spin; reopen the journal
        // after waiting and let its persisted gate decide whether HTTP is due.
        let delay = (next - Utc::now())
            .to_std()
            .unwrap_or_default()
            .max(Duration::from_secs(1))
            .min(Duration::from_secs(60));
        eprintln!(
            "financial collection deferred; completed_reports={}; total_reports={}",
            result
                .get("published_reports")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            result
                .get("total_reports")
                .and_then(Value::as_u64)
                .unwrap_or(0)
        );
        if timeout_at(deadline, sleep(delay)).await.is_err() {
            return Ok(paused());
        }
    }
}

fn paused() -> Value {
    json!({"status":"collection_paused","reason":"run_budget_exhausted",
        "collection_complete":false,"resume_same_observation":true,"profit":null})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn follows_deferred_work_and_stops_at_completion_without_repeating_errors() {
        let mut calls = 0;
        let start = Instant::now();
        let result = run(true, 30, || {
            calls += 1;
            let result = if calls == 1 {
                json!({"status":"deferred","next_request_at":Utc::now() + chrono::Duration::seconds(5)})
            } else { json!({"status":"reports_published_complete"}) };
            std::future::ready(Ok(result))
        }).await.unwrap();
        assert_eq!(calls, 2);
        assert_eq!(result["status"], "reports_published_complete");
        assert!(start.elapsed() >= Duration::from_secs(4));
        calls = 0;
        assert!(
            run(true, 30, || {
                calls += 1;
                std::future::ready(Err(anyhow::anyhow!("access revoked")))
            })
            .await
            .is_err()
        );
        assert_eq!(calls, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn budget_expiry_keeps_work_incomplete_and_one_shot_never_waits() {
        let mut calls = 0;
        let result=run(true,2,|| {
            calls+=1;
            std::future::ready(Ok(json!({"status":"deferred","next_request_at":Utc::now()+chrono::Duration::hours(12)})))
        }).await.unwrap();
        assert_eq!(calls, 1);
        assert_eq!(result["collection_complete"], false);
        assert_eq!(result["status"], "collection_paused");
        let result = run(false, 2, || {
            std::future::ready(Ok(json!({"status":"deferred"})))
        })
        .await
        .unwrap();
        assert_eq!(result["status"], "deferred");
        assert!(
            run(true, 2, || std::future::ready(Ok(
                json!({"status":"deferred"})
            )))
            .await
            .is_err()
        );
    }
}
