//! Pin the SDK negotiation change reviewed during the 3.3.0 import.
use std::borrow::Cow;

use rmcp::{
    ServerHandler,
    model::{
        ClientCapabilities, ErrorCode, Implementation, InitializeRequestParams, ProtocolVersion,
        ServerInfo,
    },
};

struct CompatibleServer;
impl ServerHandler for CompatibleServer {}

struct SessionlessServer;
impl ServerHandler for SessionlessServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::default().with_protocol_version(ProtocolVersion::V_2026_07_28)
    }

    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(&[ProtocolVersion::V_2026_07_28])
    }
}

fn request(version: ProtocolVersion) -> InitializeRequestParams {
    let mut request = InitializeRequestParams::new(
        ClientCapabilities::default(),
        Implementation::new("sdk-upgrade-test", "1"),
    );
    request.protocol_version = version;
    request
}

#[test]
fn legacy_initialize_preserves_supported_versions_and_never_negotiates_sessionless() {
    for version in ProtocolVersion::known_up_to(&ProtocolVersion::V_2025_11_25) {
        let result = CompatibleServer
            .negotiate_initialize(&request(version.clone()))
            .unwrap();
        assert_eq!(&result.protocol_version, version);
    }
    let result = CompatibleServer
        .negotiate_initialize(&request(ProtocolVersion::V_2026_07_28))
        .unwrap();
    assert_eq!(result.protocol_version, ProtocolVersion::V_2025_11_25);
    assert_eq!(
        result.capabilities,
        CompatibleServer.get_info().capabilities
    );
}

#[test]
fn a_sessionless_only_server_cannot_silently_open_a_legacy_session() {
    let error = SessionlessServer
        .negotiate_initialize(&request(ProtocolVersion::V_2026_07_28))
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::UNSUPPORTED_PROTOCOL_VERSION);
}
