#![cfg_attr(not(test), forbid(unsafe_code))]

pub mod auth;
pub mod config;
pub mod http;
pub mod ozon;
pub mod ozon_performance;
pub mod server;
pub mod wb;

#[cfg(test)]
pub(crate) mod test_support;
