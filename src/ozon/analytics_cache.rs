//! Short-lived, coalesced cache for identical Seller Analytics requests.
//!
//! A single response is bounded at 2 MiB on the wire, but the parsed `Value`
//! tree is several times larger. Counting entries alone therefore allowed
//! hundreds of megabytes, so the cache also keeps an approximate byte budget.

use std::{
    collections::BTreeMap,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use mcp_marketplace_types::StoreId;
use serde_json::Value;
use tokio::sync::Mutex;

const TTL: Duration = Duration::from_secs(300);
const MAX_ENTRIES: usize = 256;
const MAX_BYTES: usize = 64 * 1024 * 1024;

pub(super) fn key(store: &StoreId, payload: &Value) -> String {
    format!("{store}\n{payload}")
}

#[derive(Debug, Clone)]
struct Entry {
    value: Value,
    expires_at: Instant,
    bytes: usize,
}

#[derive(Debug, Default)]
pub(super) struct AnalyticsCache {
    entries: Mutex<BTreeMap<String, Entry>>,
    in_flight: Mutex<BTreeMap<String, Weak<Mutex<()>>>>,
}

impl AnalyticsCache {
    pub(super) async fn get(&self, key: &str) -> Option<Value> {
        let now = Instant::now();
        let mut entries = self.entries.lock().await;
        entries.retain(|_, entry| entry.expires_at > now);
        entries.get(key).map(|entry| entry.value.clone())
    }

    pub(super) async fn coalescing_lock(&self, key: &str) -> Arc<Mutex<()>> {
        let mut in_flight = self.in_flight.lock().await;
        in_flight.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = in_flight.get(key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        in_flight.insert(key.to_owned(), Arc::downgrade(&lock));
        lock
    }

    /// Stores a fresh response, evicting the entries closest to expiry until
    /// both the entry count and the approximate byte budget hold. A response
    /// larger than the whole budget is simply not cached.
    pub(super) async fn insert(&self, key: String, value: Value) {
        let bytes = approximate_bytes(&value).saturating_add(key.len());
        if bytes > MAX_BYTES {
            return;
        }
        let now = Instant::now();
        let mut entries = self.entries.lock().await;
        entries.retain(|_, entry| entry.expires_at > now);
        entries.remove(&key);
        let mut total = entries
            .values()
            .fold(0_usize, |total, entry| total.saturating_add(entry.bytes));
        while entries.len() >= MAX_ENTRIES || total.saturating_add(bytes) > MAX_BYTES {
            let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, entry)| entry.expires_at)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            if let Some(evicted) = entries.remove(&oldest) {
                total = total.saturating_sub(evicted.bytes);
            }
        }
        entries.insert(
            key,
            Entry {
                value,
                expires_at: now + TTL,
                bytes,
            },
        );
    }
}

/// Approximate heap footprint of a parsed JSON tree: one `Value` per node plus
/// owned string and key bytes. Parsing already bounds the nesting depth.
fn approximate_bytes(value: &Value) -> usize {
    const NODE: usize = std::mem::size_of::<Value>();
    let children = match value {
        Value::String(text) => text.len(),
        Value::Array(items) => items.iter().fold(0_usize, |total, item| {
            total.saturating_add(approximate_bytes(item))
        }),
        Value::Object(fields) => fields.iter().fold(0_usize, |total, (name, item)| {
            total
                .saturating_add(name.len())
                .saturating_add(NODE)
                .saturating_add(approximate_bytes(item))
        }),
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
    };
    NODE.saturating_add(children)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn the_oldest_entry_is_evicted_at_the_entry_cap() {
        let cache = AnalyticsCache::default();
        for index in 0..=MAX_ENTRIES {
            cache.insert(format!("key-{index:03}"), json!(index)).await;
        }
        assert!(cache.get("key-000").await.is_none());
        assert_eq!(
            cache.get(&format!("key-{MAX_ENTRIES:03}")).await,
            Some(json!(MAX_ENTRIES))
        );
    }

    #[tokio::test]
    async fn large_responses_are_bounded_by_bytes_not_only_by_count() {
        let cache = AnalyticsCache::default();
        // Three values just under a third of the budget fit; the fourth must
        // evict the oldest even though the entry cap is far away.
        let third = "x".repeat(MAX_BYTES / 3 - 1024);
        for index in 0..4 {
            cache.insert(format!("large-{index}"), json!(third)).await;
        }
        assert!(cache.get("large-0").await.is_none());
        assert!(cache.get("large-3").await.is_some());
        let total = cache
            .entries
            .lock()
            .await
            .values()
            .map(|entry| entry.bytes)
            .sum::<usize>();
        assert!(total <= MAX_BYTES);

        let oversized = "x".repeat(MAX_BYTES + 1);
        cache.insert("oversized".to_owned(), json!(oversized)).await;
        assert!(cache.get("oversized").await.is_none());
        assert!(
            cache.get("large-3").await.is_some(),
            "a refused insert evicts nothing"
        );
    }

    #[tokio::test]
    async fn reinserting_a_key_replaces_its_accounting() {
        let cache = AnalyticsCache::default();
        cache
            .insert("same".to_owned(), json!("a".repeat(1024)))
            .await;
        cache.insert("same".to_owned(), json!("b")).await;
        let entries = cache.entries.lock().await;
        assert_eq!(entries.len(), 1);
        assert!(entries["same"].bytes < 1024);
        drop(entries);
        assert_eq!(cache.get("same").await, Some(json!("b")));
    }

    #[test]
    fn footprint_counts_nodes_strings_and_keys() {
        let node = std::mem::size_of::<Value>();
        assert_eq!(approximate_bytes(&json!(null)), node);
        assert_eq!(approximate_bytes(&json!("abcd")), node + 4);
        assert_eq!(approximate_bytes(&json!([1, true])), 3 * node);
        assert_eq!(approximate_bytes(&json!({"ab": 1})), node + 2 + node + node);
        assert_eq!(
            key(&StoreId::from("ofk"), &json!({"limit": 10})),
            "ofk\n{\"limit\":10}"
        );
    }
}
