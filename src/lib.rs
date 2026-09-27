#![forbid(unsafe_code)]

/// Every runtime image is built on Alpine, and musl's allocator serializes
/// allocation across threads. Tool calls build and drop `serde_json::Value`
/// trees of up to ~150,000 nodes, so under 16 concurrent calls musl fell to
/// half its two-thread throughput. The images set `MIMALLOC_PURGE_DELAY=0`,
/// otherwise mimalloc keeps freed pages for a second and a burst of large
/// responses stays resident near the container memory limit.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod auth;
mod bounded_body;
pub mod config;
pub mod control;
pub mod http;
pub mod marketplace_quota;
pub mod ozon;
pub mod ozon_performance;
pub mod ozon_posting_sales;
pub mod position_collector;
pub mod postgres;
pub mod reporting;
mod retry;
pub mod runtime;
pub mod server;
pub mod tool_telemetry;
pub mod wb;

#[cfg(test)]
pub(crate) mod test_support;
