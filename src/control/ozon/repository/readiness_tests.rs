use super::{COMPONENT, OzonPlanRepository, OzonPlanStoreError, SupervisedClient};
use std::sync::Arc;

use crate::control::plan::CONTROL_DB_TEST_LOCK;

#[tokio::test]
async fn production_audit_ozon_probe_rechecks_schema_guards_privileges_and_recovers() {
    let Ok(admin_url) = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL") else {
        return;
    };
    let _database_guard = CONTROL_DB_TEST_LOCK.lock().await;
    for role in ["ozon_control_planner", "ozon_control_executor"] {
        let (client, connection) = tokio_postgres::connect(&admin_url, tokio_postgres::NoTls)
            .await
            .unwrap();
        let connection_task = tokio::spawn(connection);
        // Keep the disposable database owner's session for transactional DDL,
        // but execute every probe as the actual restricted application role.
        client
            .batch_execute(&format!(
                "SET ROLE {role}; SET default_transaction_read_only = on; \
                 SET statement_timeout = '2s'; \
                 SET idle_in_transaction_session_timeout = '10s'"
            ))
            .await
            .unwrap();
        let repository = OzonPlanRepository {
            client: Arc::new(SupervisedClient::preconnected(client, COMPONENT)),
        };
        repository.probe().await.unwrap();

        for drift in [
            "ALTER TABLE control.ozon_campaign_guards \
             RENAME TO production_audit_hidden_guards"
                .to_owned(),
            "ALTER TABLE control.ozon_campaign_plans \
             RENAME COLUMN status TO production_audit_hidden_status"
                .to_owned(),
            "ALTER TABLE control.ozon_campaign_guards \
             DISABLE TRIGGER ozon_guards_transition_guard"
                .to_owned(),
            format!("REVOKE SELECT ON control.ozon_campaign_plans FROM {role}"),
            format!("GRANT DELETE ON control.ozon_campaign_plans TO {role}"),
            "SET LOCAL statement_timeout = 0".to_owned(),
            "SET LOCAL idle_in_transaction_session_timeout = 0".to_owned(),
        ] {
            {
                repository
                    .client
                    .acquire()
                    .await
                    .unwrap()
                    .batch_execute(&format!(
                        "BEGIN READ WRITE; SET LOCAL ROLE position_admin; {drift}; \
                         SET LOCAL ROLE {role}; SET LOCAL transaction_read_only = on"
                    ))
                    .await
                    .unwrap();
            }
            // A live connection is insufficient: readiness must notice the
            // runtime contract changed after a successful startup probe.
            repository.client.probe().await.unwrap();
            let readiness = repository.probe().await;
            // Restore before assertions. Uncommitted DDL/grants are never
            // visible to concurrent test sessions; no business rows are read.
            repository
                .client
                .acquire()
                .await
                .unwrap()
                .batch_execute("ROLLBACK")
                .await
                .unwrap();
            assert_eq!(
                readiness,
                Err(OzonPlanStoreError::Unavailable),
                "{role} readiness accepted runtime drift: {drift}"
            );
            assert_eq!(
                repository.probe().await,
                Ok(()),
                "{role} readiness failed to recover after restoring: {drift}"
            );
        }
        drop(repository);
        connection_task.await.unwrap().unwrap();
    }
}
