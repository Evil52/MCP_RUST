#[path = "../src/bin/finance-collector/access.rs"]
mod access;

use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::json;

mod checkpoint {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;

    pub fn sha256(bytes: &[u8]) -> String {
        let mut output = String::new();
        for byte in Sha256::digest(bytes) {
            let _ = write!(output, "{byte:02x}");
        }
        output
    }
}

#[derive(Clone, Copy)]
enum Egress {
    Direct,
    CollectorProxy,
}

struct Arguments {
    registry: PathBuf,
    actor: String,
    account: String,
    credentials_dir: PathBuf,
    egress: Egress,
}

const SID: &str = "123e4567-e89b-42d3-a456-426614174000";
const OTHER_SID: &str = "123e4567-e89b-42d3-a456-426614174001";

struct Fixture {
    root: PathBuf,
    arguments: Arguments,
}

impl Fixture {
    fn new(label: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "finance-access-{label}-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let credentials_dir = root.join("credentials");
        fs::create_dir(&credentials_dir).unwrap();
        fs::set_permissions(&credentials_dir, fs::Permissions::from_mode(0o700)).unwrap();
        let arguments = Arguments {
            registry: root.join("registry.json"),
            actor: "operator".to_owned(),
            account: "wb-pilot".to_owned(),
            credentials_dir,
            egress: Egress::Direct,
        };
        let fixture = Self { root, arguments };
        fixture.registry(Some(SID), "finance");
        fixture.token(SID, 0);
        fixture
    }

    fn registry(&self, sid: Option<&str>, role: &str) {
        let registry = json!({"version": 1, "actors": [
            {"id":"operator", "name":"Finance operator", "role":role, "account_ids":["wb-pilot"]}
        ], "accounts":[{"id":"wb-pilot", "organization":"Fixture", "marketplace":"wildberries",
            "seller_client_id":"fixture", "manager_id":"operator",
            "wildberries":{"api_token_env":"WB_TEST_TOKEN", "seller_sid":sid}}]});
        fs::write(
            &self.arguments.registry,
            serde_json::to_vec(&registry).unwrap(),
        )
        .unwrap();
    }

    fn token(&self, sid: &str, marker: i64) {
        let payload = json!({"acc":3, "s": (1_u64 << 30) | (1_u64 << 13),
            "exp":chrono::Utc::now().timestamp() + 3600, "sid":sid, "marker":marker});
        let token = format!(
            "{}.{}.{}",
            URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#),
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap()),
            URL_SAFE_NO_PAD.encode(b"fixture-signature")
        );
        let path = self.arguments.credentials_dir.join("WB_TEST_TOKEN");
        fs::write(&path, token).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn financial_credentials_require_an_explicit_matching_seller_binding() {
    let mut fixture = Fixture::new("binding");
    fixture.registry(None, "finance");
    assert!(access::resolve(&fixture.arguments).is_err());
    fixture.registry(Some(OTHER_SID), "finance");
    assert!(access::resolve(&fixture.arguments).is_err());
    fixture.registry(Some(SID), "finance");
    let scoped = access::resolve(&fixture.arguments).unwrap();
    assert!(scoped.client.is_configured("wb-pilot"));
    assert_eq!(
        scoped.identity.seller_scope,
        checkpoint::sha256(SID.as_bytes())
    );
    fixture.arguments.egress = Egress::CollectorProxy;
    assert!(access::resolve(&fixture.arguments).is_ok());
    fixture.token(OTHER_SID, 0);
    assert!(access::resolve(&fixture.arguments).is_err());
}

#[test]
fn fresh_resolution_detects_revocation_and_same_seller_key_rotation_without_network() {
    let fixture = Fixture::new("revocation");
    let before = access::resolve(&fixture.arguments).unwrap();
    fixture.token(SID, 1);
    let rotated = access::resolve(&fixture.arguments).unwrap();
    assert!(before.identity != rotated.identity);
    assert_ne!(
        before.identity.fingerprint(),
        rotated.identity.fingerprint()
    );
    fixture.registry(Some(SID), "manager");
    fs::remove_file(fixture.arguments.credentials_dir.join("WB_TEST_TOKEN")).unwrap();
    let error = access::resolve(&fixture.arguments).err().unwrap();
    assert_eq!(error.to_string(), "finance or admin role is required");
}
