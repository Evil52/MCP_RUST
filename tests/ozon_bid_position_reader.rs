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
    let monitor_id: i64 = admin
        .query_one(
            "INSERT INTO search_position.monitors (\
                store_id, product_id, search_phrase, region_code, region_name, \
                interval_minutes, max_position, active\
             ) VALUES ($1, $2, 'мебельные ручки', 'moscow', 'Москва', 30, 100, true) \
             RETURNING id",
            &[&store_id, &product_id.to_string()],
        )
        .await
        .unwrap()
        .get(0);
    let slot = Utc.with_ymd_and_hms(2035, 1, 1, 0, 0, 0).unwrap();
    let observed_at = slot + Duration::minutes(5);
    let run_id: i64 = admin
        .query_one(
            "INSERT INTO search_position.collection_runs (\
                source, scheduled_for, started_at, status, monitors_planned, \
                queries_planned, collector_version, payload_digest\
             ) VALUES (\
                'ozon_public_search', $1, $2, 'running', 1, 1, \
                'ozon-bid-reader-test', repeat('a', 64)\
             ) RETURNING id",
            &[&slot, &observed_at],
        )
        .await
        .unwrap()
        .get(0);
    admin
        .execute(
            "INSERT INTO search_position.measurements (\
                run_id, monitor_id, observed_at, outcome, overall_position, placement\
             ) VALUES ($1, $2, $3, 'found', 44, 'unknown')",
            &[&run_id, &monitor_id, &observed_at],
        )
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE search_position.collection_runs SET \
                finished_at = $2, status = 'succeeded', monitors_attempted = 1, \
                monitors_succeeded = 1, queries_attempted = 1, queries_succeeded = 1 \
             WHERE id = $1",
            &[&run_id, &(observed_at + Duration::minutes(1))],
        )
        .await
        .unwrap();

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
    let not_found_monitor_id: i64 = admin
        .query_one(
            "INSERT INTO search_position.monitors (\
                store_id, product_id, search_phrase, region_code, region_name, \
                interval_minutes, max_position, active\
             ) VALUES ($1, $2, 'золотые ручки', 'moscow', 'Москва', 30, 100, true) \
             RETURNING id",
            &[&store_id, &not_found_product_id.to_string()],
        )
        .await
        .unwrap()
        .get(0);
    let not_found_slot = slot + Duration::minutes(30);
    let not_found_observed_at = not_found_slot + Duration::minutes(5);
    let not_found_run_id: i64 = admin
        .query_one(
            "INSERT INTO search_position.collection_runs (\
                source, scheduled_for, started_at, status, monitors_planned, \
                queries_planned, collector_version, payload_digest\
             ) VALUES (\
                'ozon_public_search', $1, $2, 'running', 1, 1, \
                'ozon-bid-reader-test', repeat('b', 64)\
             ) RETURNING id",
            &[&not_found_slot, &not_found_observed_at],
        )
        .await
        .unwrap()
        .get(0);
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
    admin
        .execute(
            "UPDATE search_position.collection_runs SET \
                finished_at = $2, status = 'succeeded', monitors_attempted = 1, \
                monitors_succeeded = 1, queries_attempted = 1, queries_succeeded = 1 \
             WHERE id = $1",
            &[
                &not_found_run_id,
                &(not_found_observed_at + Duration::minutes(1)),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        reader
            .latest_position(&store_id, not_found_product_id, "Москва")
            .await,
        Ok(Some(OzonPositionSignal {
            observed_at: not_found_observed_at,
            position: 101,
        }))
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
}
