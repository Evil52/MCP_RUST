//! Private server-side address routing for scheduled reports.
//!
//! The committed reporting policy contains only bounded symbolic names. This
//! module resolves those names from one private, read-only JSON file and
//! requires its key set to match the policy exactly. Chat input, account data,
//! environment values and report artifacts cannot choose a recipient.

use std::{collections::BTreeMap, fmt, fs, path::Path};

use serde::Deserialize;

use super::{mail::validate_address, policy::DailyReportPolicy};

const ROUTING_VERSION: u32 = 1;
const MAX_ROUTING_BYTES: u64 = 64 * 1024;
const MAX_ROUTES: usize = 65;

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum MailRoutingError {
    #[error("daily report mail routing file is invalid")]
    InvalidFile,
    #[error("daily report mail routing document is invalid")]
    InvalidDocument,
    #[error("daily report recipient is outside the approved routing policy")]
    UnknownAudience,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutingDocument {
    version: u32,
    routes: Vec<RouteEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RouteEntry {
    name: String,
    address: String,
}

#[derive(Clone, PartialEq, Eq)]
pub struct MailRoute {
    sender: String,
    recipient: String,
}

impl fmt::Debug for MailRoute {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MailRoute")
            .field("sender", &"<redacted>")
            .field("recipient", &"<redacted>")
            .finish()
    }
}

impl MailRoute {
    #[must_use]
    pub fn sender(&self) -> &str {
        &self.sender
    }

    #[must_use]
    pub fn recipient(&self) -> &str {
        &self.recipient
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct MailRouting {
    sender: String,
    recipients: BTreeMap<String, String>,
}

impl fmt::Debug for MailRouting {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MailRouting")
            .field("sender", &"<redacted>")
            .field("recipient_count", &self.recipients.len())
            .finish()
    }
}

impl MailRouting {
    /// Loads one private regular file without following a symlink.
    pub fn load(
        path: impl AsRef<Path>,
        policy: &DailyReportPolicy,
    ) -> Result<Self, MailRoutingError> {
        let path = path.as_ref();
        let metadata = fs::symlink_metadata(path).map_err(|_| MailRoutingError::InvalidFile)?;
        if !metadata.file_type().is_file()
            || !private_permissions(&metadata)
            || metadata.len() == 0
            || metadata.len() > MAX_ROUTING_BYTES
        {
            return Err(MailRoutingError::InvalidFile);
        }
        let bytes = fs::read(path).map_err(|_| MailRoutingError::InvalidFile)?;
        Self::from_slice(&bytes, policy)
    }

    /// Parses a bounded strict document and binds it to one validated policy.
    pub fn from_slice(bytes: &[u8], policy: &DailyReportPolicy) -> Result<Self, MailRoutingError> {
        if bytes.is_empty() || bytes.len() as u64 > MAX_ROUTING_BYTES {
            return Err(MailRoutingError::InvalidDocument);
        }
        let document: RoutingDocument =
            serde_json::from_slice(bytes).map_err(|_| MailRoutingError::InvalidDocument)?;
        if document.version != ROUTING_VERSION
            || document.routes.is_empty()
            || document.routes.len() > MAX_ROUTES
        {
            return Err(MailRoutingError::InvalidDocument);
        }
        let mut by_name = BTreeMap::new();
        for route in document.routes {
            if validate_address(&route.address).is_err()
                || by_name.insert(route.name, route.address).is_some()
            {
                return Err(MailRoutingError::InvalidDocument);
            }
        }

        let mut expected = BTreeMap::new();
        expected.insert(policy.sender_email_env.as_str(), None);
        for audience in &policy.audiences {
            expected.insert(audience.email_env.as_str(), Some(audience.id.as_str()));
        }
        if by_name.len() != expected.len()
            || !by_name
                .keys()
                .map(String::as_str)
                .eq(expected.keys().copied())
        {
            return Err(MailRoutingError::InvalidDocument);
        }
        let sender = by_name
            .remove(&policy.sender_email_env)
            .ok_or(MailRoutingError::InvalidDocument)?;
        let mut recipients = BTreeMap::new();
        for audience in &policy.audiences {
            let address = by_name
                .remove(&audience.email_env)
                .ok_or(MailRoutingError::InvalidDocument)?;
            recipients.insert(audience.id.clone(), address);
        }
        debug_assert!(by_name.is_empty());
        Ok(Self { sender, recipients })
    }

    pub fn resolve(&self, audience_id: &str) -> Result<MailRoute, MailRoutingError> {
        let recipient = self
            .recipients
            .get(audience_id)
            .ok_or(MailRoutingError::UnknownAudience)?;
        Ok(MailRoute {
            sender: self.sender.clone(),
            recipient: recipient.clone(),
        })
    }
}

#[cfg(unix)]
fn private_permissions(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt as _;

    metadata.permissions().mode().trailing_zeros() >= 6
}

#[cfg(not(unix))]
fn private_permissions(_metadata: &fs::Metadata) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use serde_json::{Value, json};

    use crate::config::AccessRegistry;

    use super::*;

    static NEXT_FILE: AtomicU64 = AtomicU64::new(1);

    fn policy() -> DailyReportPolicy {
        let registry: AccessRegistry = serde_json::from_value(json!({
            "version": 1,
            "actors": [
                {"id":"diana","name":"Diana","role":"manager","oidc":{"username":"diana"}},
                {"id":"anna","name":"Anna","role":"manager","oidc":{"username":"anna"}}
            ],
            "accounts": [
                {"id":"ozon","organization":"Ozon","marketplace":"ozon","seller_client_id":"1","manager_id":"diana","ozon":{"store_id":"1","client_id_env":"OZON_ID","api_key_env":"OZON_KEY"}},
                {"id":"wb","organization":"WB","marketplace":"wildberries","seller_client_id":"2","manager_id":"anna","wildberries":{"api_token_env":"WB_TOKEN"}}
            ]
        }))
        .unwrap();
        DailyReportPolicy::from_slice(
            br#"{"version":1,"enabled":false,"timezone":"Asia/Yekaterinburg","sender_email_env":"SENDER","audiences":[{"id":"diana_report","email_env":"DIANA_EMAIL","managers":[{"actor_id":"diana","account_ids":["ozon"]}]},{"id":"anna_report","email_env":"ANNA_EMAIL","managers":[{"actor_id":"anna","account_ids":["wb"]}]}]}"#,
            &registry,
        )
        .unwrap()
    }

    fn document() -> Value {
        json!({
            "version": 1,
            "routes": [
                {"name":"SENDER","address":"reports@example.test"},
                {"name":"DIANA_EMAIL","address":"diana@example.test"},
                {"name":"ANNA_EMAIL","address":"anna@example.test"}
            ]
        })
    }

    fn private_file(value: &[u8]) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "mcp-ozon-mail-routing-{}-{}",
            std::process::id(),
            NEXT_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&path, value).unwrap();
        set_mode(&path, 0o600);
        path
    }

    #[cfg(unix)]
    fn set_mode(path: &Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt as _;

        fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    }

    #[cfg(not(unix))]
    fn set_mode(_path: &Path, _mode: u32) {}

    #[test]
    fn exact_policy_routes_resolve_and_debug_never_exposes_addresses() {
        let routing =
            MailRouting::from_slice(&serde_json::to_vec(&document()).unwrap(), &policy()).unwrap();
        let diana = routing.resolve("diana_report").unwrap();
        assert_eq!(diana.sender(), "reports@example.test");
        assert_eq!(diana.recipient(), "diana@example.test");
        assert_eq!(
            routing.resolve("missing"),
            Err(MailRoutingError::UnknownAudience)
        );
        for debug in [format!("{routing:?}"), format!("{diana:?}")] {
            assert!(!debug.contains("reports@example.test"));
            assert!(!debug.contains("diana@example.test"));
            assert!(!debug.contains("anna@example.test"));
        }
    }

    #[test]
    fn document_shape_key_set_duplicates_and_addresses_fail_closed() {
        let valid = document();
        let mut cases = Vec::new();
        let mut wrong_version = valid.clone();
        wrong_version["version"] = json!(2);
        cases.push(wrong_version);
        let mut unknown = valid.clone();
        unknown["unexpected"] = json!(true);
        cases.push(unknown);
        let mut empty = valid.clone();
        empty["routes"] = json!([]);
        cases.push(empty);
        let mut missing = valid.clone();
        missing["routes"].as_array_mut().unwrap().pop();
        cases.push(missing);
        let mut extra = valid.clone();
        extra["routes"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name":"EXTRA","address":"extra@example.test"}));
        cases.push(extra);
        let mut duplicate = valid.clone();
        duplicate["routes"][2]["name"] = json!("DIANA_EMAIL");
        cases.push(duplicate);
        let mut invalid_address = valid;
        invalid_address["routes"][1]["address"] = json!("bad address");
        cases.push(invalid_address);

        for case in cases {
            assert_eq!(
                MailRouting::from_slice(&serde_json::to_vec(&case).unwrap(), &policy()),
                Err(MailRoutingError::InvalidDocument)
            );
        }
        assert_eq!(
            MailRouting::from_slice(&[], &policy()),
            Err(MailRoutingError::InvalidDocument)
        );
        assert_eq!(
            MailRouting::from_slice(
                &vec![
                    b' ';
                    usize::try_from(MAX_ROUTING_BYTES).expect("routing limit fits usize") + 1
                ],
                &policy(),
            ),
            Err(MailRoutingError::InvalidDocument)
        );
        let mut too_many = document();
        too_many["routes"] = Value::Array(
            (0..=MAX_ROUTES)
                .map(|index| {
                    json!({"name":format!("ROUTE_{index}"),"address":format!("u{index}@example.test")})
                })
                .collect(),
        );
        assert_eq!(
            MailRouting::from_slice(&serde_json::to_vec(&too_many).unwrap(), &policy()),
            Err(MailRoutingError::InvalidDocument)
        );
    }

    #[test]
    fn private_regular_file_is_required_and_loaded_exactly_once() {
        let bytes = serde_json::to_vec(&document()).unwrap();
        let path = private_file(&bytes);
        let routing = MailRouting::load(&path, &policy()).unwrap();
        assert_eq!(
            routing.resolve("anna_report").unwrap().recipient(),
            "anna@example.test"
        );
        fs::remove_file(&path).unwrap();
        assert_eq!(
            MailRouting::load(&path, &policy()),
            Err(MailRoutingError::InvalidFile)
        );

        let oversized_length =
            usize::try_from(MAX_ROUTING_BYTES).expect("routing limit fits usize") + 1;
        for value in [Vec::new(), vec![b'x'; oversized_length]] {
            let path = private_file(&value);
            assert_eq!(
                MailRouting::load(&path, &policy()),
                Err(MailRoutingError::InvalidFile)
            );
            fs::remove_file(path).unwrap();
        }

        let path = private_file(&bytes);
        set_mode(&path, 0o644);
        assert_eq!(
            MailRouting::load(&path, &policy()),
            Err(MailRoutingError::InvalidFile)
        );
        fs::remove_file(path).unwrap();

        let directory = std::env::temp_dir().join(format!(
            "mcp-ozon-mail-routing-directory-{}-{}",
            std::process::id(),
            NEXT_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        assert_eq!(
            MailRouting::load(&directory, &policy()),
            Err(MailRoutingError::InvalidFile)
        );
        fs::remove_dir(directory).unwrap();
    }
}
