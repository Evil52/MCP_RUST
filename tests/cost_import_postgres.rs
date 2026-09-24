use std::str::FromStr;

use chrono::{Duration, NaiveDate, Utc};
use mcp_ozon::reporting::{
    cost_import::{
        CostImportError, CostImportScope, CostVatTreatment, PostgresCostRepository,
        ValidatedCostBatch,
    },
    snapshot::{AccountScope, Marketplace},
};
use serde_json::{Value, json};
use tokio_postgres::{Config, NoTls, error::SqlState};

fn payload(account: &str, export: &str, sku: u64, from: &str, to: &str) -> Value {
    json!({
        "version":1,"account_id":account,"marketplace":"ozon","source_id":"one_c",
        "export_id":export,"exported_at":"2026-09-13T10:00:00Z",
        "rows":[{"source_row_id":"row_1","sku":sku,"amount_minor":12345,"currency":"RUB",
            "allocation":"per_unit","vat_treatment":"included","vat_rate_bps":2000,
            "effective_from":from,"effective_to":to}]
    })
}

fn validated(value: &Value, scope: &CostImportScope) -> ValidatedCostBatch {
    ValidatedCostBatch::parse_json(&serde_json::to_vec(value).unwrap(), scope).unwrap()
}

#[tokio::test]
async fn costs_are_atomic_immutable_scoped_and_do_not_rewrite_historical_report_knowledge() {
    let (Ok(admin_url), Ok(worker_url), Ok(collector_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("REPORT_OUTBOX_TEST_WORKER_URL"),
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
    ) else {
        return;
    };
    // Only the disposable integration-test database activates the dormant role.
    let admin_config = Config::from_str(&admin_url).unwrap();
    let (admin, connection) = admin_config.connect(NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    admin
        .batch_execute(
            "ALTER ROLE report_cost_importer LOGIN PASSWORD 'cost-import-disposable-test'",
        )
        .await
        .unwrap();
    let mut importer_config = admin_config;
    importer_config
        .user("report_cost_importer")
        .password("cost-import-disposable-test");
    let repository = PostgresCostRepository::connect(&importer_config)
        .await
        .unwrap();
    repository.verify_import_contract().await.unwrap();
    let (raw, connection) = importer_config.connect(NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    for query in [
        "UPDATE daily_reporting.cost_import_entries SET amount_minor=0",
        "DELETE FROM daily_reporting.cost_import_batches",
        "TRUNCATE daily_reporting.cost_import_entries",
        "SELECT * FROM daily_reporting.source_snapshots LIMIT 1",
    ] {
        assert_eq!(
            raw.batch_execute(query).await.unwrap_err().code(),
            Some(&SqlState::INSUFFICIENT_PRIVILEGE)
        );
    }
    let collector = PostgresCostRepository::connect(&Config::from_str(&collector_url).unwrap())
        .await
        .unwrap();
    assert_eq!(
        collector.verify_import_contract().await,
        Err(CostImportError::Unavailable)
    );
    let account = format!("cost_integration_{}", std::process::id());
    let scope = CostImportScope::new(
        AccountScope::new(account.clone(), Marketplace::Ozon).unwrap(),
        "one_c".to_owned(),
        [101, 102, 103, 104].into_iter().collect(),
        "finance_original".to_owned(),
    )
    .unwrap();
    let first = payload(&account, "export_1", 102, "2026-09-01", "2026-09-30");
    let receipt = repository.import(&validated(&first, &scope)).await.unwrap();
    assert!(!receipt.already_imported);
    assert_eq!(receipt.imported_by, "finance_original");
    let retry_scope = CostImportScope::new(
        AccountScope::new(account.clone(), Marketplace::Ozon).unwrap(),
        "one_c".to_owned(),
        [101, 102, 103, 104].into_iter().collect(),
        "finance_retry".to_owned(),
    )
    .unwrap();
    let retry = repository
        .import(&validated(&first, &retry_scope))
        .await
        .unwrap();
    assert!(retry.already_imported);
    assert_eq!(retry.batch_id, receipt.batch_id);
    assert_eq!(retry.imported_by, "finance_original");
    assert_eq!(retry.sha256, receipt.sha256);
    let mut changed = first.clone();
    changed["rows"][0]["amount_minor"] = json!(12346);
    assert_eq!(
        repository.import(&validated(&changed, &scope)).await,
        Err(CostImportError::Conflict)
    );
    let mut conflict = payload(&account, "atomic_failure", 101, "2026-09-01", "2026-09-30");
    let mut conflicting_row = first["rows"][0].clone();
    conflicting_row["source_row_id"] = json!("row_2");
    conflict["rows"]
        .as_array_mut()
        .unwrap()
        .push(conflicting_row);
    assert_eq!(
        repository.import(&validated(&conflict, &scope)).await,
        Err(CostImportError::Conflict)
    );
    let date = NaiveDate::from_ymd_opt(2026, 9, 15).unwrap();
    assert!(
        repository
            .lookup(&scope, 101, date, Utc::now())
            .await
            .unwrap()
            .is_none()
    );
    let failed: i64 = admin.query_one("SELECT count(*) FROM daily_reporting.cost_import_batches WHERE account_id=$1 AND export_id='atomic_failure'", &[&account]).await.unwrap().get(0);
    assert_eq!(failed, 0);
    let worker = PostgresCostRepository::connect(&Config::from_str(&worker_url).unwrap())
        .await
        .unwrap();
    assert_eq!(
        worker.verify_import_contract().await,
        Err(CostImportError::Unavailable)
    );
    assert!(
        worker
            .lookup(
                &scope,
                102,
                date,
                receipt.imported_at - Duration::seconds(1)
            )
            .await
            .unwrap()
            .is_none()
    );
    let cost = worker
        .lookup(&scope, 102, date, Utc::now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cost.row.amount_minor, 12345);
    assert_eq!(cost.sha256, receipt.sha256);
    assert_eq!(cost.imported_by, "finance_original");
    assert_eq!(
        worker.lookup(&scope, 999, date, Utc::now()).await,
        Err(CostImportError::ScopeDenied)
    );
    let other_scope = CostImportScope::new(
        AccountScope::new("other_cost_account".to_owned(), Marketplace::Ozon).unwrap(),
        "one_c".to_owned(),
        std::iter::once(102).collect(),
        "finance_original".to_owned(),
    )
    .unwrap();
    assert!(
        worker
            .lookup(&other_scope, 102, date, Utc::now())
            .await
            .unwrap()
            .is_none()
    );
    let adjacent = payload(&account, "adjacent", 102, "2026-10-01", "2026-10-31");
    repository
        .import(&validated(&adjacent, &scope))
        .await
        .unwrap();
    let sealed = raw.execute("INSERT INTO daily_reporting.cost_import_entries \
        (batch_id,account_id,marketplace,source_row_id,sku,amount_minor,currency,allocation,vat_treatment,effective_from,effective_to) \
        VALUES ($1,$2,'ozon','late_row',103,1,'RUB','per_unit','not_applicable','2026-09-01','2026-09-30')", &[&receipt.batch_id, &account]).await.unwrap_err();
    assert_eq!(sealed.code(), Some(&SqlState::CHECK_VIOLATION));
    drop(raw);
    // A multi-row batch is inserted by one statement, including NULL VAT rates.
    let bulk_skus = 1_000..2_000_u64;
    let bulk_scope = CostImportScope::new(
        AccountScope::new(account.clone(), Marketplace::Ozon).unwrap(),
        "one_c".to_owned(),
        bulk_skus.clone().collect(),
        "finance_original".to_owned(),
    )
    .unwrap();
    let mut bulk = payload(&account, "bulk", 1_000, "2026-09-01", "2026-09-30");
    bulk["rows"] = bulk_skus
        .clone()
        .map(|sku| {
            let (vat_treatment, vat_rate_bps) = if sku % 2 == 0 {
                ("included", json!(2000))
            } else {
                ("not_applicable", Value::Null)
            };
            json!({"source_row_id": format!("row_{sku}"), "sku": sku, "amount_minor": sku,
                "currency": "RUB", "allocation": "per_unit", "vat_treatment": vat_treatment,
                "vat_rate_bps": vat_rate_bps, "effective_from": "2026-09-01",
                "effective_to": "2026-09-30"})
        })
        .collect();
    let bulk_receipt = repository
        .import(&validated(&bulk, &bulk_scope))
        .await
        .unwrap();
    assert_eq!(bulk_receipt.row_count, 1_000);
    let stored: i64 = admin
        .query_one(
            "SELECT count(*) FROM daily_reporting.cost_import_entries WHERE batch_id=$1",
            &[&bulk_receipt.batch_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored, 1_000);
    for (sku, vat_treatment, vat_rate_bps) in [
        (1_000, CostVatTreatment::Included, Some(2000)),
        (1_999, CostVatTreatment::NotApplicable, None),
    ] {
        let cost = worker
            .lookup(&bulk_scope, sku, date, Utc::now())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cost.row.source_row_id, format!("row_{sku}"));
        assert_eq!(cost.row.amount_minor, i64::try_from(sku).unwrap());
        assert_eq!(cost.row.vat_treatment, vat_treatment);
        assert_eq!(cost.row.vat_rate_bps, vat_rate_bps);
        assert_eq!(cost.batch_id, bulk_receipt.batch_id);
    }
    let contender = PostgresCostRepository::connect(&importer_config)
        .await
        .unwrap();
    let race_a = validated(
        &payload(&account, "race_a", 104, "2026-09-01", "2026-09-30"),
        &scope,
    );
    let race_b = validated(
        &payload(&account, "race_b", 104, "2026-09-30", "2026-10-31"),
        &scope,
    );
    let (a, b) = tokio::join!(repository.import(&race_a), contender.import(&race_b));
    assert!(matches!(
        (a, b),
        (Ok(_), Err(CostImportError::Conflict)) | (Err(CostImportError::Conflict), Ok(_))
    ));
}
