use std::{num::NonZeroUsize, sync::Arc};

use rmcp::{
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage},
    transport::{
        Transport,
        streamable_http_server::session::{
            SessionManager,
            local::{LocalSessionManager, LocalSessionManagerError},
        },
    },
};
use serde_json::json;

fn client_message(value: serde_json::Value) -> ClientJsonRpcMessage {
    serde_json::from_value(value).expect("test client message is valid")
}

fn server_message(value: serde_json::Value) -> ServerJsonRpcMessage {
    serde_json::from_value(value).expect("test server message is valid")
}

async fn complete_initialize(
    manager: Arc<LocalSessionManager>,
    id: rmcp::transport::streamable_http_server::SessionId,
    transport: &mut rmcp::transport::streamable_http_server::session::local::SessionTransport,
) {
    let initialize = client_message(json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "idle-test", "version": "0"}
        }
    }));
    let initialize_task =
        tokio::spawn(async move { manager.initialize_session(&id, initialize).await });
    let received = transport
        .receive()
        .await
        .expect("worker forwards initialize to its handler");
    assert!(matches!(received, ClientJsonRpcMessage::Request(_)));
    transport
        .send(server_message(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "serverInfo": {"name": "idle-test", "version": "0"}
            }
        })))
        .await
        .expect("handler sends initialize response");
    initialize_task
        .await
        .expect("initialize task does not panic")
        .expect("manager receives initialize response");
}

#[tokio::test]
async fn local_session_limit_rejects_n_plus_one_and_reclaims_terminated_handles() {
    assert_eq!(
        LocalSessionManager::default().max_sessions().get(),
        256,
        "the safe default must stay bounded"
    );
    let limit = NonZeroUsize::new(2).expect("test limit is non-zero");
    let manager = LocalSessionManager::default().with_max_sessions(limit);
    assert_eq!(manager.max_sessions(), limit);

    let (_, mut first) = manager
        .create_session()
        .await
        .expect("first session must fit");
    let (_, second) = manager
        .create_session()
        .await
        .expect("second session must fit");

    match manager.create_session().await {
        Err(LocalSessionManagerError::SessionCapacityExhausted { limit }) => {
            assert_eq!(limit, 2);
        }
        Ok(_) => panic!("N+1 session must be rejected"),
        Err(error) => panic!("unexpected session error: {error}"),
    }
    assert_eq!(manager.sessions.read().await.len(), 2);

    first
        .close()
        .await
        .expect("closing the transport must terminate its worker");
    let (_, replacement) = manager
        .create_session()
        .await
        .expect("a terminated handle must be pruned before the capacity check");
    assert_eq!(manager.sessions.read().await.len(), 2);

    drop((second, replacement));
}

#[tokio::test]
async fn concurrent_session_creation_cannot_race_past_the_hard_limit() {
    const LIMIT: usize = 8;
    const ATTEMPTS: usize = 32;

    let manager = Arc::new(
        LocalSessionManager::default()
            .with_max_sessions(NonZeroUsize::new(LIMIT).expect("test limit is non-zero")),
    );
    let barrier = Arc::new(tokio::sync::Barrier::new(ATTEMPTS));
    let mut tasks = Vec::with_capacity(ATTEMPTS);

    for _ in 0..ATTEMPTS {
        let manager = manager.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            manager.create_session().await
        }));
    }

    let mut accepted = Vec::new();
    let mut rejected = 0;
    for task in tasks {
        match task.await.expect("session creation task must not panic") {
            Ok((_id, transport)) => accepted.push(transport),
            Err(LocalSessionManagerError::SessionCapacityExhausted { limit }) => {
                assert_eq!(limit, LIMIT);
                rejected += 1;
            }
            Err(error) => panic!("unexpected session error: {error}"),
        }
    }

    assert_eq!(accepted.len(), LIMIT);
    assert_eq!(rejected, ATTEMPTS - LIMIT);
    assert_eq!(manager.sessions.read().await.len(), LIMIT);
}

#[tokio::test(start_paused = true)]
async fn an_abandoned_initialized_session_expires_and_releases_its_slot() {
    let idle_timeout = std::time::Duration::from_secs(10);
    let manager = Arc::new(
        LocalSessionManager::default()
            .with_max_sessions(NonZeroUsize::new(1).unwrap())
            .with_session_idle_timeout(idle_timeout),
    );
    assert_eq!(manager.session_idle_timeout(), Some(idle_timeout));

    let (id, mut transport) = manager.create_session().await.unwrap();
    complete_initialize(Arc::clone(&manager), id.clone(), &mut transport).await;
    assert!(manager.has_session(&id).await.unwrap());

    tokio::time::advance(
        idle_timeout
            .checked_sub(std::time::Duration::from_millis(1))
            .unwrap(),
    )
    .await;
    tokio::task::yield_now().await;
    assert!(manager.has_session(&id).await.unwrap());

    tokio::time::advance(std::time::Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert!(!manager.has_session(&id).await.unwrap());

    let (_replacement_id, replacement) = manager
        .create_session()
        .await
        .expect("expired handle is pruned before the capacity check");
    assert_eq!(manager.sessions.read().await.len(), 1);
    drop((transport, replacement));
}

#[tokio::test(start_paused = true)]
async fn in_flight_request_suspends_idle_expiry_then_restarts_the_countdown() {
    let idle_timeout = std::time::Duration::from_secs(10);
    let manager = Arc::new(
        LocalSessionManager::default()
            .with_max_sessions(NonZeroUsize::new(1).unwrap())
            .with_session_idle_timeout(idle_timeout),
    );
    let (id, mut transport) = manager.create_session().await.unwrap();
    complete_initialize(Arc::clone(&manager), id.clone(), &mut transport).await;

    let response_stream = manager
        .create_stream(
            &id,
            client_message(json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list",
                "params": {}
            })),
        )
        .await
        .expect("request enters the session");
    let request = transport
        .receive()
        .await
        .expect("handler receives the active request");
    assert!(matches!(request, ClientJsonRpcMessage::Request(_)));

    tokio::time::advance(idle_timeout * 3).await;
    tokio::task::yield_now().await;
    assert!(
        manager.has_session(&id).await.unwrap(),
        "an operation deadline must not be replaced by the session idle policy"
    );

    transport
        .send(server_message(json!({
            "jsonrpc": "2.0",
            "id": 2,
            "result": {"tools": []}
        })))
        .await
        .expect("handler completes the request");
    tokio::task::yield_now().await;

    tokio::time::advance(
        idle_timeout
            .checked_sub(std::time::Duration::from_millis(1))
            .unwrap(),
    )
    .await;
    tokio::task::yield_now().await;
    assert!(manager.has_session(&id).await.unwrap());
    tokio::time::advance(std::time::Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert!(!manager.has_session(&id).await.unwrap());

    drop((response_stream, transport));
}
