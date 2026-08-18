//! `SECURITY.md` invariant 1: ambient HTTP proxies are disabled on every
//! outbound client.
//!
//! A `HTTP(S)_PROXY` variable inherited from the host or the container image is
//! enough to route marketplace traffic through a third party. Ozon requests
//! carry `Client-Id` and `Api-Key` headers, so a proxied request hands over
//! live seller credentials; the JWKS fetch is worse still, because whoever
//! answers it chooses the signing keys this process will trust and can mint
//! tokens for any actor. Both clients therefore call `.no_proxy()`.
//!
//! This lives in its own test binary on purpose. The test has to mutate
//! process-wide environment variables, and doing that inside the shared library
//! test binary would intermittently reroute the loopback HTTP that other tests
//! depend on. A separate binary is a separate process, so the mutation cannot
//! reach them.

use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    thread,
    time::Duration,
};

use axum::http::{HeaderMap, HeaderValue, header::AUTHORIZATION};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use mcp_ozon::{
    auth::JwtAuthenticator,
    config::{JwtConfig, RegistrySource, StoreCredentials, StoreId},
    ozon::OzonClient,
    reporting::{
        ozon_adapter::product_page_request,
        ozon_source::{OzonClientReportTransport, OzonReportTransport},
    },
};
use serde_json::json;

/// Accepts up to `accepts` connections, answers each with a small JSON body and
/// reports every request line it saw.
fn mock_http(accepts: usize) -> (String, mpsc::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("mock binds");
    let address = listener.local_addr().expect("mock has an address");
    let (sender, receiver) = mpsc::channel();

    thread::spawn(move || {
        for _ in 0..accepts {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let request_line = read_request_line(&stream);
            if sender.send(request_line).is_err() {
                return;
            }
            let body = r#"{"keys":[]}"#;
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });

    (format!("http://{address}"), receiver)
}

fn read_request_line(stream: &TcpStream) -> String {
    let mut reader = BufReader::new(stream.try_clone().expect("stream clones"));
    let mut first = String::new();
    let _ = reader.read_line(&mut first);
    let mut content_length = 0_usize;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            content_length = value.trim().parse().unwrap_or(0);
        }
    }
    let mut body = vec![0_u8; content_length];
    let _ = reader.read_exact(&mut body);
    first.trim_end().to_owned()
}

fn saw_request(receiver: &mpsc::Receiver<String>) -> bool {
    receiver.recv_timeout(Duration::from_millis(1_500)).is_ok()
}

fn stores() -> BTreeMap<StoreId, StoreCredentials> {
    BTreeMap::from([(
        StoreId::from("shop"),
        StoreCredentials {
            client_id: "proxy-test-client".to_owned(),
            api_key: "proxy-test-key".to_owned(),
        },
    )])
}

async fn exercise_real_report_transport_overload() {
    const HELD_REQUESTS: usize = 16;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("overload mock binds");
    let base_url = format!(
        "http://{}",
        listener.local_addr().expect("overload mock has an address")
    );
    let (accepted_tx, mut accepted_rx) = tokio::sync::mpsc::channel(HELD_REQUESTS);
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut streams = Vec::with_capacity(HELD_REQUESTS);
        for _ in 0..HELD_REQUESTS {
            let (stream, _) = listener.accept().await.expect("held request connects");
            streams.push(stream);
            accepted_tx
                .send(())
                .await
                .expect("the test still observes accepted requests");
        }
        release_rx.await.expect("the test releases held requests");
    });

    let client = OzonClient::new(base_url, Duration::from_secs(5), stores())
        .expect("the overload client builds");
    let transport = OzonClientReportTransport::new(client, StoreId::from("shop"));
    let mut held = tokio::task::JoinSet::new();
    for _ in 0..HELD_REQUESTS {
        let transport = transport.clone();
        held.spawn(async move {
            transport
                .post(product_page_request("/v4/product/info/stocks", None).unwrap())
                .await
        });
    }
    for _ in 0..HELD_REQUESTS {
        tokio::time::timeout(Duration::from_secs(2), accepted_rx.recv())
            .await
            .expect("all held requests reach the mock promptly")
            .expect("the mock reports every held request");
    }

    let error = tokio::time::timeout(
        Duration::from_secs(2),
        transport.post(product_page_request("/v4/product/info/stocks", None).unwrap()),
    )
    .await
    .expect("the bounded local-overload retry completes")
    .expect_err("the saturated real client remains locally overloaded");
    assert_eq!(error.code(), "local_overloaded");

    release_tx
        .send(())
        .expect("the overload mock is still waiting");
    held.abort_all();
    while held.join_next().await.is_some() {}
    server.await.expect("the overload mock exits cleanly");
}

fn registry() -> RegistrySource {
    let path = std::env::temp_dir().join(format!("mcp-ozon-proxy-{}.json", std::process::id()));
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&json!({
            "version": 1,
            "actors": [{"id": "admin", "name": "Administrator", "role": "admin"}],
            "accounts": [],
        }))
        .expect("registry fixture serializes"),
    )
    .expect("registry fixture is written");
    RegistrySource::new(path).expect("registry fixture is valid")
}

fn jwt_config(jwks_url: String) -> JwtConfig {
    JwtConfig {
        issuer: "http://issuer.test/realms/ofk".to_owned(),
        audience: "ozonofk-mcp".to_owned(),
        jwks_url,
        resource_url: "http://localhost:8788/mcp".to_owned(),
        resource_metadata_url: "http://localhost:8788/.well-known/oauth-protected-resource"
            .to_owned(),
        required_scopes: vec!["mcp:tools".to_owned()],
        jwks_cache_ttl: Duration::from_secs(300),
    }
}

/// A token whose header parses as RS256 with a `kid`, which is all that is
/// needed to drive `authenticate` as far as the JWKS fetch. The signature is
/// deliberately junk: this test observes which server is contacted, not whether
/// verification succeeds.
fn header_only_bearer() -> HeaderMap {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256","typ":"JWT","kid":"proxy-probe"}"#);
    let claims = URL_SAFE_NO_PAD.encode(b"{}");
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {header}.{claims}.signature"))
            .expect("bearer header is valid"),
    );
    headers
}

/// Removes the proxy variables on the way out, including while a panic unwinds,
/// so one failed assertion cannot leave the rest of the binary proxied.
struct AmbientProxy;

impl AmbientProxy {
    fn set(url: &str) -> Self {
        // SAFETY: this binary contains exactly one test, so no other thread is
        // reading the environment while it is being modified.
        unsafe {
            std::env::set_var("HTTP_PROXY", url);
            std::env::set_var("ALL_PROXY", url);
        }
        Self
    }
}

impl Drop for AmbientProxy {
    fn drop(&mut self) {
        // SAFETY: as above.
        unsafe {
            std::env::remove_var("HTTP_PROXY");
            std::env::remove_var("ALL_PROXY");
        }
    }
}

/// One test function, so the environment mutation is never concurrent with
/// another test in this binary.
#[tokio::test]
async fn no_outbound_client_follows_an_ambient_http_proxy() {
    let (proxy_url, proxy_requests) = mock_http(3);
    let (ozon_url, ozon_requests) = mock_http(1);
    let (jwks_url, jwks_requests) = mock_http(1);

    let _ambient_proxy = AmbientProxy::set(&proxy_url);

    // Control. Without this, the test would pass vacuously the moment the HTTP
    // stack stopped honouring proxy variables at all — the assertions below
    // would hold for the wrong reason and the invariant would be unguarded.
    let unprotected = reqwest::Client::builder()
        .build()
        .expect("a default client builds");
    let _ = unprotected
        .post(format!("{ozon_url}/v1/rating/summary"))
        .json(&json!({}))
        .send()
        .await;
    assert!(
        saw_request(&proxy_requests),
        "a client that has not opted out must reach the proxy; if this fails the \
         HTTP stack no longer honours HTTP_PROXY/ALL_PROXY and the assertions \
         below prove nothing — fix this test before trusting them"
    );
    assert!(
        !saw_request(&ozon_requests),
        "the control request must have been diverted to the proxy"
    );

    // The Ozon client must reach the marketplace directly, credentials and all.
    let client = OzonClient::new(ozon_url, Duration::from_secs(3), stores())
        .expect("the Ozon client builds");
    let transport = OzonClientReportTransport::new(client, StoreId::from("shop"));
    let _ = transport
        .post(
            product_page_request("/v4/product/info/stocks", None)
                .expect("the read-only report request builds"),
        )
        .await;
    assert!(
        saw_request(&ozon_requests),
        "the Ozon client must contact the marketplace directly"
    );
    assert!(
        !saw_request(&proxy_requests),
        "Ozon requests carry Client-Id and Api-Key and must never traverse an ambient proxy"
    );

    // Drive the production report adapter through the local admission-control
    // retry as well as the successful request above. This is deliberately a
    // real OzonClient: the generic retry has a distinct production coverage
    // instantiation that policy-only unit tests cannot execute.
    exercise_real_report_transport_overload().await;

    // The JWKS fetch is a trust anchor: whoever answers it decides which keys
    // this process accepts, so it must not be interceptable either.
    let authenticator =
        JwtAuthenticator::new(jwt_config(jwks_url), registry()).expect("the authenticator builds");
    let _ = authenticator.authenticate(&header_only_bearer()).await;
    assert!(
        saw_request(&jwks_requests),
        "the JWKS fetch must contact the issuer directly"
    );
    assert!(
        !saw_request(&proxy_requests),
        "an ambient proxy must never be able to substitute JWT signing keys"
    );
}
