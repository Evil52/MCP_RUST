//! Run only against the disposable database produced by with-position-test-db.sh.
//! A unique sibling database tests the real 032 -> 035 upgrade with old rows.

use std::fmt::Write;

use chrono::NaiveDate;
use mcp_ozon::reporting::finance_ledger::PostgresFinanceLedgerReader;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, Config, NoTls, error::SqlState};

const INVOKE: &str = "SELECT batch_id,content_sha256,already_present \
    FROM daily_reporting.publish_wb_financial_ledger($1,$2,$3,$4,$5,$6)";

async fn connect(config: &Config) -> Client {
    let (client, connection) = config.connect(NoTls).await.unwrap();
    std::mem::drop(tokio::spawn(async move {
        let _ = connection.await;
    }));
    client
}

fn hash(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[tokio::test]
async fn precision_upgrade_preserves_old_values_hashes_grants_and_idempotent_replay() {
    let (Ok(admin_url), Ok(writer_url), Ok(reader_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
        std::env::var("POSITION_REPOSITORY_TEST_READER_URL"),
    ) else {
        return;
    };
    let mut admin_config = admin_url.parse::<Config>().unwrap();
    let mut writer_config = writer_url.parse::<Config>().unwrap();
    let mut reader_config = reader_url.parse::<Config>().unwrap();
    let coordinator = connect(&admin_config).await;
    let database = format!(
        "wb_precision_upgrade_{}_{}",
        std::process::id(),
        chrono::Utc::now().timestamp_micros()
    );
    coordinator
        .batch_execute(&format!("CREATE DATABASE {database}"))
        .await
        .unwrap();
    admin_config.dbname(&database);
    writer_config.dbname(&database);
    reader_config.dbname(&database);
    let admin = connect(&admin_config).await;
    admin.batch_execute("CREATE SCHEMA daily_reporting; GRANT USAGE ON SCHEMA daily_reporting TO report_collector,position_reader,report_worker").await.unwrap();
    admin
        .batch_execute(include_str!(
            "../position-monitor/initdb/032_financial_ledger.sql"
        ))
        .await
        .unwrap();
    let writer = connect(&writer_config).await;
    let date = NaiveDate::from_ymd_opt(2025, 9, 1).unwrap();
    let mut normalized = json!([{"rrd_id":1,"report_id":2,"business_date":"2025-09-01",
        "sku":7,"currency":"RUB","document_type":"Продажа","operation_type":null,"quantity":1,
        "amounts":{"forPay":{"units":112_345,"scale":3},"vw":{"units":-1,"scale":9}}}]);
    let old_payload = serde_json::to_string(&normalized).unwrap();
    let old_hash = hash(old_payload.as_bytes());
    let original = writer
        .query_one(
            INVOKE,
            &[
                &"legacy-account",
                &date,
                &date,
                &old_payload,
                &old_hash,
                &204_i32,
            ],
        )
        .await
        .unwrap();
    let batch_id: i64 = original.get(0);
    assert!(!original.get::<_, bool>(2));
    admin
        .batch_execute(include_str!(
            "../position-monitor/initdb/035_wb_financial_precision.sql"
        ))
        .await
        .unwrap();
    for amount in normalized[0]["amounts"]
        .as_object_mut()
        .unwrap()
        .values_mut()
    {
        amount["units"] = json!(amount["units"].as_i64().unwrap().to_string());
    }
    let new_payload = serde_json::to_string(&normalized).unwrap();
    let new_hash = hash(new_payload.as_bytes());
    assert_ne!(new_hash, old_hash);
    let replay = writer
        .query_one(
            INVOKE,
            &[
                &"legacy-account",
                &date,
                &date,
                &new_payload,
                &new_hash,
                &204_i32,
            ],
        )
        .await
        .unwrap();
    assert_eq!(replay.get::<_, i64>(0), batch_id);
    assert_eq!(replay.get::<_, String>(1), old_hash);
    assert!(replay.get::<_, bool>(2));
    let reader = PostgresFinanceLedgerReader::connect(&reader_config)
        .await
        .unwrap();
    let stored = reader
        .read_wb_rows("legacy-account", batch_id, 0, 100)
        .await
        .unwrap();
    assert_eq!(stored[0].amounts["forPay"].units, 112_345);
    assert_eq!(stored[0].amounts["forPay"].scale, 3);
    assert_eq!(stored[0].amounts["vw"].units, -1);
    assert_eq!(stored[0].amounts["vw"].scale, 9);
    let recorded = admin.query_one("SELECT content_sha256,payload_encoding FROM daily_reporting.financial_ledger_batches WHERE id=$1", &[&batch_id]).await.unwrap();
    assert_eq!(recorded.get::<_, String>(0), old_hash);
    assert_eq!(recorded.get::<_, String>(1), "i64_number_v1");
    normalized[0]["amounts"]["vw"] = json!({"units":"-9999123456789012345678","scale":18});
    let precise_payload = serde_json::to_string(&normalized).unwrap();
    let precise_hash = hash(precise_payload.as_bytes());
    let changed = writer
        .query_one(
            INVOKE,
            &[
                &"legacy-account",
                &date,
                &date,
                &precise_payload,
                &precise_hash,
                &204_i32,
            ],
        )
        .await
        .unwrap_err();
    assert_eq!(changed.code(), Some(&SqlState::UNIQUE_VIOLATION));
    let precise = writer
        .query_one(
            INVOKE,
            &[
                &"precise-account",
                &date,
                &date,
                &precise_payload,
                &precise_hash,
                &204_i32,
            ],
        )
        .await
        .unwrap();
    let exact = reader
        .read_wb_rows("precise-account", precise.get(0), 0, 100)
        .await
        .unwrap();
    assert_eq!(
        exact[0].amounts["vw"].units,
        -9_999_123_456_789_012_345_678_i128
    );
    assert_eq!(exact[0].amounts["vw"].scale, 18);
    assert_eq!(
        serde_json::to_value(&exact).unwrap()[0]["amounts"]["vw"]["units"],
        "-9999123456789012345678"
    );
    let raw_reader = connect(&reader_config).await;
    for connection in [&writer, &raw_reader] {
        let error = connection
            .batch_execute("SELECT * FROM daily_reporting.financial_ledger_amounts")
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE));
    }
    for invalid_units in [
        json!(1),
        json!("01"),
        json!("-0"),
        json!("1e20"),
        json!("170141183460469231731687303715884105728"),
    ] {
        normalized[0]["amounts"]["vw"]["units"] = invalid_units;
        let invalid = serde_json::to_string(&normalized).unwrap();
        let error = writer
            .query_one(
                INVOKE,
                &[
                    &"invalid-account",
                    &date,
                    &date,
                    &invalid,
                    &hash(invalid.as_bytes()),
                    &204_i32,
                ],
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error.code(),
            Some(&SqlState::INVALID_PARAMETER_VALUE | &SqlState::CHECK_VIOLATION)
        ));
    }
    drop(raw_reader);
    drop(reader);
    drop(writer);
    drop(admin);
    coordinator
        .batch_execute(&format!("DROP DATABASE {database} WITH (FORCE)"))
        .await
        .unwrap();
}
