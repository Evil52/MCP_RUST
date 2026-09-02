#[cfg(test)]
use jwt::{CONTROL_INTERNAL_JWKS_URL, CONTROL_REQUIRED_SCOPE};
pub use model::{
    ControlAppConfig, ControlAuthConfig, ControlOzonRuntimeConfig, ControlPolicyDatabaseConfig,
    ControlWbRuntimeConfig,
};
use ozon_runtime::load_ozon_runtime;
#[cfg(test)]
use wb_runtime::{
    MAX_CONTROL_CREDENTIAL_BYTES, WB_PROMOTION_BIT, WB_READ_ONLY_BIT, load_policy_database,
    normalize_control_token_bytes, validate_proxy_url,
};
pub(in crate::control) use wb_runtime::{
    read_control_token, validate_wb_reader_token, validate_wb_writer_token,
};

mod jwt;
mod loader;
mod model;
mod ozon_runtime;
mod validation;
mod wb_runtime;

#[cfg(test)]
mod tests;
