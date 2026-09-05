use std::str::FromStr;

use chrono::{Duration, TimeZone, Utc};
use mcp_ozon::control::{OzonBidPositionReadError, OzonBidPositionReader, OzonPositionSignal};
use tokio_postgres::{Client, Config, NoTls};

async fn connect(url: &str) -> Client {
    let config = Config::from_str(url).expect("test database URL must be valid");
    let (client, connection) = config
        .connect(NoTls)
        .await
        .expect("test database must connect");
    tokio::spawn(async move {
        connection
            .await
            .expect("test database connection must remain healthy");
    });
    client
}

async fn insert_monitor(
    admin: &Client,
    store_id: &str,
    product_id: u64,
    search_phrase: &str,
) -> i64 {
    admin
        .query_one(
            "INSERT INTO search_position.monitors (\
                store_id, product_id, search_phrase, region_code, region_name, \
                interval_minutes, max_position, active\
             ) VALUES ($1, $2, $3, 'moscow', 'Москва', 30, 100, true) \
             RETURNING id",
            &[&store_id, &product_id.to_string(), &search_phrase],
        )
        .await
        .unwrap()
        .get(0)
}

async fn insert_run(
    admin: &Client,
    slot: chrono::DateTime<Utc>,
    observed_at: chrono::DateTime<Utc>,
    digest_byte: char,
) -> i64 {
    let digest = digest_byte.to_string();
    admin
        .query_one(
            "INSERT INTO search_position.collection_runs (\
                source, scheduled_for, started_at, status, monitors_planned, \
                queries_planned, collector_version, payload_digest\
             ) VALUES (\
                'ozon_public_search', $1, $2, 'running', 1, 1, \
                'ozon-bid-reader-test', repeat($3, 64)\
             ) RETURNING id",
            &[&slot, &observed_at, &digest],
        )
        .await
        .unwrap()
        .get(0)
}

async fn finish_run(admin: &Client, run_id: i64, observed_at: chrono::DateTime<Utc>, status: &str) {
    admin
        .execute(
            "UPDATE search_position.collection_runs SET \
                finished_at = $2, status = $3, monitors_attempted = 1, \
                monitors_succeeded = 1, queries_attempted = 1, queries_succeeded = 1 \
             WHERE id = $1",
            &[&run_id, &(observed_at + Duration::minutes(1)), &status],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn position_reader_accepts_only_one_published_complete_monitor() {
    let Ok(admin_url) = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL") else {
        return;
    };
    let reader_url = std::env::var("POSITION_REPOSITORY_TEST_READER_URL")
        .expect("reader URL accompanies the admin URL");
    let admin = connect(&admin_url).await;
    let suffix = std::process::id();
    let store_id = format!("ozon-bid-reader-{suffix}");
    let product_id = u64::from(suffix) + 8_000_000_000;
    let monitor_id = insert_monitor(&admin, &store_id, product_id, "мебельные ручки").await;
    let slot = Utc.with_ymd_and_hms(2035, 1, 1, 0, 0, 0).unwrap();
    let observed_at = slot + Duration::minutes(5);
    let run_id = insert_run(&admin, slot, observed_at, 'a').await;
    admin
        .execute(
            "INSERT INTO search_position.measurements (\
                run_id, monitor_id, observed_at, outcome, overall_position, placement\
             ) VALUES ($1, $2, $3, 'found', 44, 'unknown')",
            &[&run_id, &monitor_id, &observed_at],
        )
        .await
        .unwrap();
    finish_run(&admin, run_id, observed_at, "succeeded").await;

    let reader = OzonBidPositionReader::connect(&reader_url).await.unwrap();
    reader.verify_runtime_contract().await.unwrap();
    assert_eq!(
        reader
            .latest_position(&store_id, product_id, "Москва")
            .await,
        Ok(Some(OzonPositionSignal {
            observed_at,
            position: 44,
        }))
    );
    assert_eq!(
        reader
            .latest_position(&store_id, product_id + 2, "Москва")
            .await,
        Ok(None)
    );

    let not_found_product_id = product_id + 1;
    let not_found_monitor_id =
        insert_monitor(&admin, &store_id, not_found_product_id, "золотые ручки").await;
    let not_found_slot = slot + Duration::minutes(30);
    let not_found_observed_at = not_found_slot + Duration::minutes(5);
    let not_found_run_id = insert_run(&admin, not_found_slot, not_found_observed_at, 'b').await;
    admin
        .execute(
            "INSERT INTO search_position.measurements (\
                run_id, monitor_id, observed_at, outcome\
             ) VALUES ($1, $2, $3, 'not_found')",
            &[
                &not_found_run_id,
                &not_found_monitor_id,
                &not_found_observed_at,
            ],
        )
        .await
        .unwrap();
    finish_run(&admin, not_found_run_id, not_found_observed_at, "succeeded").await;
    assert_eq!(
        reader
            .latest_position(&store_id, not_found_product_id, "Москва")
            .await,
        Ok(Some(OzonPositionSignal {
            observed_at: not_found_observed_at,
            position: 101,
        }))
    );

    let unmeasured_product_id = product_id + 3;
    insert_monitor(&admin, &store_id, unmeasured_product_id, "ручки без замера").await;
    assert_eq!(
        reader
            .latest_position(&store_id, unmeasured_product_id, "Москва")
            .await,
        Ok(None)
    );

    let partial_product_id = product_id + 4;
    let partial_monitor_id =
        insert_monitor(&admin, &store_id, partial_product_id, "частичный замер").await;
    let partial_slot = slot + Duration::minutes(60);
    let partial_observed_at = partial_slot + Duration::minutes(5);
    let partial_run_id = insert_run(&admin, partial_slot, partial_observed_at, 'c').await;
    admin
        .execute(
            "INSERT INTO search_position.measurements (\
                run_id, monitor_id, observed_at, outcome, overall_position, placement\
             ) VALUES ($1, $2, $3, 'found', 20, 'unknown')",
            &[&partial_run_id, &partial_monitor_id, &partial_observed_at],
        )
        .await
        .unwrap();
    finish_run(&admin, partial_run_id, partial_observed_at, "partial").await;
    assert_eq!(
        reader
            .latest_position(&store_id, partial_product_id, "Москва")
            .await,
        Err(OzonBidPositionReadError::InvalidSnapshot)
    );

    let invalid_outcome_product_id = product_id + 5;
    let invalid_outcome_monitor_id = insert_monitor(
        &admin,
        &store_id,
        invalid_outcome_product_id,
        "блокированный замер",
    )
    .await;
    let invalid_outcome_slot = slot + Duration::minutes(90);
    let invalid_outcome_observed_at = invalid_outcome_slot + Duration::minutes(5);
    let invalid_outcome_run_id = insert_run(
        &admin,
        invalid_outcome_slot,
        invalid_outcome_observed_at,
        'd',
    )
    .await;
    admin
        .execute(
            "INSERT INTO search_position.measurements (\
                run_id, monitor_id, observed_at, outcome\
             ) VALUES ($1, $2, $3, 'blocked')",
            &[
                &invalid_outcome_run_id,
                &invalid_outcome_monitor_id,
                &invalid_outcome_observed_at,
            ],
        )
        .await
        .unwrap();
    finish_run(
        &admin,
        invalid_outcome_run_id,
        invalid_outcome_observed_at,
        "succeeded",
    )
    .await;
    assert_eq!(
        reader
            .latest_position(&store_id, invalid_outcome_product_id, "Москва")
            .await,
        Err(OzonBidPositionReadError::InvalidSnapshot)
    );

    admin
        .execute(
            "INSERT INTO search_position.monitors (\
                store_id, product_id, search_phrase, region_code, region_name, \
                interval_minutes, max_position, active\
             ) VALUES ($1, $2, 'ручки для мебели', 'moscow', 'Москва', 30, 100, true)",
            &[&store_id, &product_id.to_string()],
        )
        .await
        .unwrap();
    assert_eq!(
        reader
            .latest_position(&store_id, product_id, "Москва")
            .await,
        Err(OzonBidPositionReadError::AmbiguousTarget)
    );

    let admin_reader = OzonBidPositionReader::from_client(admin);
    assert_eq!(
        admin_reader.verify_runtime_contract().await,
        Err(OzonBidPositionReadError::Unavailable)
    );

    let admin = connect(&admin_url).await;
    admin
        .execute(
            "REVOKE SELECT ON search_position.published_measurements FROM position_reader",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        reader.verify_runtime_contract().await,
        Err(OzonBidPositionReadError::Unavailable)
    );
}
