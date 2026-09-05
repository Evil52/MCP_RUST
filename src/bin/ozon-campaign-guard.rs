#![forbid(unsafe_code)]

use anyhow::Result;
use mcp_ozon::{control::run_ozon_campaign_guard, runtime::print_runtime_version_if_requested};

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if print_runtime_version_if_requested("ozon-campaign-guard", &arguments)? {
        return Ok(());
    }
    run_ozon_campaign_guard(&arguments).await
}
