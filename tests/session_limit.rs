use std::{num::NonZeroUsize, sync::Arc};

use rmcp::transport::{
    Transport,
    streamable_http_server::session::{
        SessionManager,
        local::{LocalSessionManager, LocalSessionManagerError},
    },
};

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
