//! Cross-process ownership for an Ozon Performance executor identity.
//!
//! Static and PostgreSQL-backed guard modes can otherwise be started at the
//! same time with one Client-Id. A session advisory lock keeps that identity
//! exclusive across processes and hosts that share the Control database. The
//! process treats loss of the dedicated database session as fatal.

use tokio::{sync::watch, task::JoinHandle};
use tokio_postgres::{Client, Config, NoTls};

use thiserror::Error;

use crate::postgres::harden;

const EXECUTOR_ROLE: &str = "ozon_control_executor";
const LOCK_NAMESPACE: &str = "mcp-ozon/executor-identity/v1";

/// Failure to establish exclusive ownership of an executor identity.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum OzonExecutorLeaseError {
    #[error("Ozon executor Client-Id fingerprint is not canonical")]
    InvalidFingerprint,
    #[error("Ozon executor lease database role is invalid")]
    InvalidRole,
    #[error("another Ozon runtime already owns this executor identity")]
    Busy,
    #[error("no Ozon runtime owns this executor identity")]
    NotHeld,
    #[error("Ozon executor lease database is unavailable")]
    Unavailable,
}

/// A process-lifetime PostgreSQL session lease for one executor Client-Id.
#[derive(Debug)]
pub struct OzonExecutorLease {
    client: Client,
    connection_task: JoinHandle<()>,
    lost: watch::Receiver<bool>,
}

impl OzonExecutorLease {
    /// Verifies that another live PostgreSQL session owns this identity lock.
    ///
    /// A health probe is a separate process. If it can acquire the lock, the
    /// real executor has lost ownership; the probe immediately drops the
    /// temporary session and reports unhealthy.
    pub async fn verify_held(
        database: &Config,
        executor_client_id_sha256: &str,
    ) -> Result<(), OzonExecutorLeaseError> {
        match Self::acquire(database, executor_client_id_sha256).await {
            Err(OzonExecutorLeaseError::Busy) => Ok(()),
            Ok(lease) => {
                drop(lease);
                Err(OzonExecutorLeaseError::NotHeld)
            }
            Err(error) => Err(error),
        }
    }

    /// Acquires the identity lock on a dedicated database session.
    pub async fn acquire(
        database: &Config,
        executor_client_id_sha256: &str,
    ) -> Result<Self, OzonExecutorLeaseError> {
        validate_fingerprint(executor_client_id_sha256)?;
        let mut database = database.clone();
        harden(&mut database, "mcp-ozon-control-executor-lease");
        let (client, connection) = database
            .connect(NoTls)
            .await
            .map_err(|_| OzonExecutorLeaseError::Unavailable)?;
        let (lost_sender, lost) = watch::channel(false);
        let connection_task = tokio::spawn(async move {
            let _ = connection.await;
            let _ = lost_sender.send(true);
        });
        let lock_identity = format!("{LOCK_NAMESPACE}/{executor_client_id_sha256}");
        let Ok(row) = client
            .query_one(
                "SELECT current_user::text, \
                 pg_try_advisory_lock(hashtextextended($1::text, 0))",
                &[&lock_identity],
            )
            .await
        else {
            drop(client);
            connection_task.abort();
            return Err(OzonExecutorLeaseError::Unavailable);
        };
        let role: String = row.get(0);
        let acquired: bool = row.get(1);
        if role != EXECUTOR_ROLE {
            drop(client);
            connection_task.abort();
            return Err(OzonExecutorLeaseError::InvalidRole);
        }
        if !acquired {
            drop(client);
            connection_task.abort();
            return Err(OzonExecutorLeaseError::Busy);
        }
        Ok(Self {
            client,
            connection_task,
            lost,
        })
    }

    /// Completes only if the database session (and therefore its lock) dies.
    pub async fn lost(&self) {
        let mut signal = self.lost.clone();
        if self.client.is_closed() || *signal.borrow() {
            return;
        }
        while signal.changed().await.is_ok() {
            if *signal.borrow_and_update() {
                return;
            }
        }
    }
}

impl Drop for OzonExecutorLease {
    fn drop(&mut self) {
        self.connection_task.abort();
    }
}

fn validate_fingerprint(value: &str) -> Result<(), OzonExecutorLeaseError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(OzonExecutorLeaseError::InvalidFingerprint)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::control::plan::CONTROL_DB_TEST_LOCK;

    #[test]
    fn fingerprint_is_exact_lowercase_sha256_and_cannot_change_the_lock_key_shape() {
        assert!(validate_fingerprint(&"a".repeat(64)).is_ok());
        for value in [
            "a".repeat(63),
            "a".repeat(65),
            "A".repeat(64),
            format!("{}g", "a".repeat(63)),
            format!("{}../", "a".repeat(61)),
        ] {
            assert_eq!(
                validate_fingerprint(&value),
                Err(OzonExecutorLeaseError::InvalidFingerprint)
            );
        }
    }

    #[tokio::test]
    async fn postgres_session_lease_rejects_a_second_connection_for_the_same_identity() {
        let Ok(database_url) = std::env::var("OZON_EXECUTOR_TEST_DATABASE_URL") else {
            return;
        };
        let _database_guard = CONTROL_DB_TEST_LOCK.lock().await;
        let database = database_url.parse::<Config>().unwrap();
        let fingerprint = "a".repeat(64);
        if let Ok(planner_url) = std::env::var("OZON_CONTROL_TEST_DATABASE_URL") {
            let planner_database = planner_url.parse::<Config>().unwrap();
            assert_eq!(
                OzonExecutorLease::acquire(&planner_database, &fingerprint)
                    .await
                    .unwrap_err(),
                OzonExecutorLeaseError::InvalidRole
            );
        }
        let first = OzonExecutorLease::acquire(&database, &fingerprint)
            .await
            .unwrap();
        OzonExecutorLease::verify_held(&database, &fingerprint)
            .await
            .unwrap();
        assert_eq!(
            OzonExecutorLease::acquire(&database, &fingerprint)
                .await
                .unwrap_err(),
            OzonExecutorLeaseError::Busy
        );
        let second_identity = OzonExecutorLease::acquire(&database, &"b".repeat(64))
            .await
            .unwrap();
        drop(second_identity);
        drop(first);

        let reacquired = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match OzonExecutorLease::acquire(&database, &fingerprint).await {
                    Ok(lease) => break lease,
                    Err(OzonExecutorLeaseError::Busy) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => panic!("unexpected executor lease error: {error}"),
                }
            }
        })
        .await
        .expect("executor session lock was not released");
        if let Ok(admin_url) = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL") {
            let (admin, admin_connection) = admin_url
                .parse::<Config>()
                .unwrap()
                .connect(NoTls)
                .await
                .unwrap();
            let admin_task = tokio::spawn(admin_connection);
            let backend_pid: i32 = reacquired
                .client
                .query_one("SELECT pg_backend_pid()", &[])
                .await
                .unwrap()
                .get(0);
            let terminated: bool = admin
                .query_one("SELECT pg_terminate_backend($1)", &[&backend_pid])
                .await
                .unwrap()
                .get(0);
            assert!(terminated);
            tokio::time::timeout(Duration::from_secs(3), reacquired.lost())
                .await
                .expect("executor lease did not report its lost database session");
            drop(admin);
            admin_task.await.unwrap().unwrap();
        }
        drop(reacquired);
    }
}
