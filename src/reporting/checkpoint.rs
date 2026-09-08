//! Durable replay of normalized pages, never raw marketplace responses.
use std::{fmt::Write, future::Future, pin::Pin, sync::Arc};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

pub type Checkpoints = Option<Arc<dyn PageJournal>>;
pub type JournalFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, CheckpointError>> + Send + 'a>>;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointError {
    #[error("collection yielded after its page quantum")]
    Deferred,
    #[error("collection checkpoint is unavailable or its lease was lost")]
    Unavailable,
    #[error("collection checkpoint exceeds its bound or has an invalid format")]
    Invalid,
}

impl CheckpointError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Deferred => "checkpoint_deferred",
            Self::Unavailable => "checkpoint_unavailable",
            Self::Invalid => "checkpoint_invalid",
        }
    }
}

/// Implementations must fence every operation with the live source-job lease.
/// `admit` reserves a departure before I/O, including across process restarts.
pub trait PageJournal: Send + Sync {
    fn load<'a>(&'a self, key: &'a str) -> JournalFuture<'a, Option<Value>>;
    fn admit(&self) -> JournalFuture<'_, ()>;
    fn save<'a>(&'a self, key: &'a str, page: Value) -> JournalFuture<'a, ()>;
}

pub async fn checkpointed<T, E, F, Fut>(
    journal: &Checkpoints,
    request_identity: Value,
    fetch_normalized: F,
) -> Result<T, E>
where
    T: Serialize + DeserializeOwned,
    E: From<CheckpointError>,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let Some(journal) = journal else {
        return fetch_normalized().await;
    };
    // Bump this namespace whenever a normalizer's persisted page contract changes.
    let digest = Sha256::digest(
        serde_json::to_vec(&(1, request_identity)).map_err(|_| CheckpointError::Invalid)?,
    );
    let mut key = String::with_capacity(64);
    for byte in digest {
        let _ = write!(key, "{byte:02x}");
    }
    if let Some(page) = journal.load(&key).await? {
        return serde_json::from_value(page).map_err(|_| CheckpointError::Invalid.into());
    }
    journal.admit().await?;
    let normalized = fetch_normalized().await?;
    journal
        .save(
            &key,
            serde_json::to_value(&normalized).map_err(|_| CheckpointError::Invalid)?,
        )
        .await?;
    Ok(normalized)
}

/// Round upward so a fractional vendor delay is never shortened.
#[must_use]
pub fn delay_seconds(delay: std::time::Duration) -> u64 {
    delay
        .as_secs()
        .saturating_add(u64::from(delay.subsec_nanos() > 0))
        .max(1)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::{
        collections::BTreeMap,
        sync::{
            Mutex,
            atomic::{AtomicBool, Ordering},
        },
    };
    pub type MemoryPages = Arc<Mutex<BTreeMap<String, Value>>>;
    struct MemoryJournal {
        pages: MemoryPages,
        admitted: AtomicBool,
    }
    #[allow(
        clippy::unnecessary_wraps,
        reason = "fixture mirrors the optional production checkpoint API"
    )]
    pub fn journal(pages: &MemoryPages) -> Checkpoints {
        Some(Arc::new(MemoryJournal {
            pages: Arc::clone(pages),
            admitted: AtomicBool::new(false),
        }))
    }
    impl PageJournal for MemoryJournal {
        fn load<'a>(&'a self, key: &'a str) -> JournalFuture<'a, Option<Value>> {
            Box::pin(async move { Ok(self.pages.lock().unwrap().get(key).cloned()) })
        }
        fn admit(&self) -> JournalFuture<'_, ()> {
            Box::pin(async move {
                if self.admitted.swap(true, Ordering::SeqCst) {
                    Err(CheckpointError::Deferred)
                } else {
                    Ok(())
                }
            })
        }
        fn save<'a>(&'a self, key: &'a str, page: Value) -> JournalFuture<'a, ()> {
            Box::pin(async move {
                self.pages.lock().unwrap().insert(key.to_owned(), page);
                Ok(())
            })
        }
    }
    #[tokio::test]
    async fn failed_pages_are_not_cached_and_request_identity_is_not_reused() {
        let pages = MemoryPages::default();
        let result = checkpointed(
            &journal(&pages),
            serde_json::json!(["source", 0]),
            || async { Err::<Vec<u64>, _>(CheckpointError::Unavailable) },
        )
        .await;
        assert_eq!(result, Err(CheckpointError::Unavailable));
        assert!(pages.lock().unwrap().is_empty());
        let resumed = journal(&pages);
        assert_eq!(
            checkpointed(&resumed, serde_json::json!(["source", 0]), || async {
                Ok::<_, CheckpointError>(vec![7_u64])
            })
            .await
            .unwrap(),
            vec![7]
        );
        assert_eq!(
            checkpointed(&resumed, serde_json::json!(["source", 1]), || async {
                Ok::<_, CheckpointError>(vec![8_u64])
            })
            .await,
            Err(CheckpointError::Deferred)
        );
        assert_eq!(delay_seconds(std::time::Duration::from_millis(1500)), 2);
    }
}
