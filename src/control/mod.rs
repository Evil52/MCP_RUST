//! Fail-closed scaffold for a future advertising control MCP.
//!
//! This module deliberately contains no marketplace HTTP client, endpoint, or
//! credential lookup. The first milestone exposes only local policy/status
//! inspection while every marketplace mutation remains unavailable.

mod config;
mod policy;
mod server;

pub use config::{ControlAppConfig, ControlAuthConfig};
pub use policy::{ControlMode, ControlPolicy};
pub use server::ControlMcp;
