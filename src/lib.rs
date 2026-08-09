#![cfg_attr(not(test), forbid(unsafe_code))]

pub mod auth;
pub mod config;
pub mod ozon;
pub mod server;
pub mod wb;

#[cfg(test)]
pub(crate) mod test_support;
