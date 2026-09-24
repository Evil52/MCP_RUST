#![expect(
    clippy::significant_drop_tightening,
    reason = "PostgreSQL transactions borrow the supervised guard until commit"
)]

use chrono::{DateTime, NaiveDate, Utc};
use tokio_postgres::{Config, Row, error::SqlState};

use crate::postgres::SupervisedClient;

use super::{
    CostAllocation, CostCurrency, CostImportError, CostImportReceipt, CostImportRow,
    CostImportScope, CostVatTreatment, StoredCost, ValidatedCostBatch, valid_date,
};
use crate::reporting::snapshot::Marketplace;

pub struct PostgresCostRepository {
    client: SupervisedClient,
}

impl PostgresCostRepository {
    pub async fn connect(config: &Config) -> Result<Self, CostImportError> {
        let client = SupervisedClient::connect(config, "mcp-ozon-cost-import")
            .await
            .map_err(|_| CostImportError::Unavailable)?;
        Ok(Self { client })
    }

    pub async fn verify_import_contract(&self) -> Result<(), CostImportError> {
        self.client
            .verify_session_bounds()
            .await
            .map_err(|_| CostImportError::Unavailable)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| CostImportError::Unavailable)?;
        let row = client.query_one(
            "SELECT current_user = 'report_cost_importer' \
             AND NOT r.rolsuper AND NOT r.rolcreaterole AND NOT r.rolcreatedb AND NOT r.rolbypassrls \
             AND has_table_privilege(current_user, 'daily_reporting.cost_import_batches', 'SELECT') \
             AND has_column_privilege(current_user, 'daily_reporting.cost_import_batches', 'export_id', 'INSERT') \
             AND has_column_privilege(current_user, 'daily_reporting.cost_import_batches', 'imported_by', 'INSERT') \
             AND NOT has_column_privilege(current_user, 'daily_reporting.cost_import_batches', 'imported_at', 'INSERT') \
             AND NOT has_column_privilege(current_user, 'daily_reporting.cost_import_batches', 'import_transaction', 'INSERT') \
             AND has_table_privilege(current_user, 'daily_reporting.cost_import_entries', 'SELECT,INSERT') \
             AND NOT has_table_privilege(current_user, 'daily_reporting.cost_import_entries', 'UPDATE,DELETE,TRUNCATE') \
             AND NOT has_table_privilege(current_user, 'daily_reporting.cost_import_batches', 'UPDATE,DELETE,TRUNCATE') \
             AND NOT has_schema_privilege(current_user, 'daily_reporting', 'CREATE') \
             AND NOT EXISTS (SELECT 1 FROM pg_auth_members WHERE member = r.oid) \
             AND NOT EXISTS (SELECT 1 FROM information_schema.table_privileges \
                 WHERE grantee = current_user AND table_schema NOT IN ('pg_catalog','information_schema') \
                 AND NOT (table_schema = 'daily_reporting' \
                     AND table_name IN ('cost_import_batches','cost_import_entries'))) \
             FROM pg_roles r WHERE r.rolname = current_user", &[],
        ).await.map_err(db_error)?;
        if !row.get::<_, bool>(0) {
            return Err(CostImportError::Unavailable);
        }
        Ok(())
    }

    /// Batch insertion is all-or-nothing. Existing history is never updated.
    pub async fn import(
        &self,
        batch: &ValidatedCostBatch,
    ) -> Result<CostImportReceipt, CostImportError> {
        self.verify_import_contract().await?;
        let envelope = &batch.envelope;
        let marketplace = marketplace_name(envelope.marketplace);
        let count = i32::try_from(batch.row_count()).map_err(|_| CostImportError::LimitExceeded)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| CostImportError::Unavailable)?;
        let transaction = client.transaction().await.map_err(db_error)?;
        // Serialize export retries before looking up the immutable idempotency key.
        let lock_key = format!("cost-import/{}/{marketplace}", envelope.account_id);
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 33))",
                &[&lock_key],
            )
            .await
            .map_err(db_error)?;
        if let Some(existing) = transaction.query_opt(
            "SELECT id, sha256, row_count, imported_at, imported_by FROM daily_reporting.cost_import_batches \
             WHERE account_id=$1 AND marketplace=$2 AND source_id=$3 AND export_id=$4",
            &[&envelope.account_id, &marketplace, &envelope.source_id, &envelope.export_id],
        ).await.map_err(db_error)? {
            if existing.get::<_, String>(1) != batch.sha256 || existing.get::<_, i32>(2) != count {
                return Err(CostImportError::Conflict);
            }
            let receipt = receipt(batch, &existing, true);
            transaction.commit().await.map_err(db_error)?;
            return Ok(receipt);
        }
        let saved = transaction.query_one(
            "INSERT INTO daily_reporting.cost_import_batches \
             (version, account_id, marketplace, source_id, export_id, sha256, row_count, exported_at, imported_by) \
             VALUES (1,$1,$2,$3,$4,$5,$6,$7,$8) RETURNING id, sha256, row_count, imported_at, imported_by",
            &[&envelope.account_id, &marketplace, &envelope.source_id, &envelope.export_id,
                &batch.sha256, &count, &envelope.exported_at, &batch.imported_by],
        ).await.map_err(db_error)?;
        let batch_id: i64 = saved.get(0);
        // One round trip for the whole batch; every row still passes the
        // per-row triggers and the deferred row-count check at commit.
        let rows = &envelope.rows;
        let source_row_ids: Vec<&str> = rows.iter().map(|row| row.source_row_id.as_str()).collect();
        let skus = rows
            .iter()
            .map(|row| i64::try_from(row.sku))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| CostImportError::InvalidInput)?;
        let amounts: Vec<i64> = rows.iter().map(|row| row.amount_minor).collect();
        let vat_treatments: Vec<&str> = rows.iter().map(|row| row.vat_treatment.as_str()).collect();
        let vat_rates: Vec<Option<i32>> = rows
            .iter()
            .map(|row| row.vat_rate_bps.map(i32::from))
            .collect();
        let effective_from: Vec<NaiveDate> = rows.iter().map(|row| row.effective_from).collect();
        let effective_to: Vec<NaiveDate> = rows.iter().map(|row| row.effective_to).collect();
        transaction
            .execute(
                "INSERT INTO daily_reporting.cost_import_entries \
                 (batch_id,account_id,marketplace,source_row_id,sku,amount_minor,currency,allocation, \
                  vat_treatment,vat_rate_bps,effective_from,effective_to) \
                 SELECT $1,$2,$3,batch.source_row_id,batch.sku,batch.amount_minor,'RUB','per_unit', \
                  batch.vat_treatment,batch.vat_rate_bps,batch.effective_from,batch.effective_to \
                 FROM unnest($4::text[],$5::bigint[],$6::bigint[],$7::text[],$8::integer[], \
                  $9::date[],$10::date[]) \
                 AS batch(source_row_id,sku,amount_minor,vat_treatment,vat_rate_bps, \
                  effective_from,effective_to)",
                &[
                    &batch_id,
                    &envelope.account_id,
                    &marketplace,
                    &source_row_ids,
                    &skus,
                    &amounts,
                    &vat_treatments,
                    &vat_rates,
                    &effective_from,
                    &effective_to,
                ],
            )
            .await
            .map_err(db_error)?;
        let receipt = receipt(batch, &saved, false);
        transaction.commit().await.map_err(db_error)?;
        Ok(receipt)
    }

    /// Returns only costs whose server import time is within the knowledge cutoff.
    ///
    /// Missing costs remain `None` (N/D). A frozen report must also record the
    /// selected batch ID or explicit absence, because a concurrent transaction
    /// may commit after this read with an earlier transaction import timestamp.
    pub async fn lookup(
        &self,
        scope: &CostImportScope,
        sku: u64,
        date: NaiveDate,
        knowledge_cutoff: DateTime<Utc>,
    ) -> Result<Option<StoredCost>, CostImportError> {
        if !scope.allowed_skus.contains(&sku) {
            return Err(CostImportError::ScopeDenied);
        }
        if !valid_date(date) {
            return Err(CostImportError::InvalidInput);
        }
        let sku = i64::try_from(sku).map_err(|_| CostImportError::InvalidInput)?;
        let marketplace = marketplace_name(scope.account.marketplace());
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| CostImportError::Unavailable)?;
        let row = client.query_opt(
            "SELECT e.source_row_id,e.sku,e.amount_minor,e.vat_treatment,e.vat_rate_bps, \
                e.effective_from,e.effective_to,b.id,b.source_id,b.export_id,b.sha256,b.imported_at,b.imported_by \
             FROM daily_reporting.cost_import_entries e \
             JOIN daily_reporting.cost_import_batches b ON b.id=e.batch_id \
             WHERE e.account_id=$1 AND e.marketplace=$2 AND e.sku=$3 \
             AND e.effective_from <= $4 AND e.effective_to >= $4 \
             AND b.imported_at <= $5 AND b.source_id=$6",
            &[&scope.account.account_id(), &marketplace, &sku, &date, &knowledge_cutoff, &scope.source_id],
        ).await.map_err(db_error)?;
        row.as_ref().map(stored_cost).transpose()
    }
}

fn receipt(batch: &ValidatedCostBatch, row: &Row, already_imported: bool) -> CostImportReceipt {
    CostImportReceipt {
        batch_id: row.get(0),
        account_id: batch.envelope.account_id.clone(),
        source_id: batch.envelope.source_id.clone(),
        export_id: batch.envelope.export_id.clone(),
        sha256: batch.sha256.clone(),
        row_count: batch.row_count(),
        imported_at: row.get(3),
        imported_by: row.get(4),
        already_imported,
    }
}

fn stored_cost(row: &Row) -> Result<StoredCost, CostImportError> {
    let vat_treatment = match row.get::<_, &str>(3) {
        "included" => CostVatTreatment::Included,
        "excluded" => CostVatTreatment::Excluded,
        "not_applicable" => CostVatTreatment::NotApplicable,
        _ => return Err(CostImportError::Unavailable),
    };
    let vat_rate_bps = row
        .get::<_, Option<i32>>(4)
        .map(u16::try_from)
        .transpose()
        .map_err(|_| CostImportError::Unavailable)?;
    Ok(StoredCost {
        row: CostImportRow {
            source_row_id: row.get(0),
            sku: u64::try_from(row.get::<_, i64>(1)).map_err(|_| CostImportError::Unavailable)?,
            amount_minor: row.get(2),
            currency: CostCurrency::RUB,
            allocation: CostAllocation::PerUnit,
            vat_treatment,
            vat_rate_bps,
            effective_from: row.get(5),
            effective_to: row.get(6),
        },
        batch_id: row.get(7),
        source_id: row.get(8),
        export_id: row.get(9),
        sha256: row.get(10),
        imported_at: row.get(11),
        imported_by: row.get(12),
    })
}

const fn marketplace_name(value: Marketplace) -> &'static str {
    match value {
        Marketplace::Ozon => "ozon",
        Marketplace::Wildberries => "wildberries",
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "map_err consumes and sanitizes the database error"
)]
fn db_error(error: tokio_postgres::Error) -> CostImportError {
    match error.code() {
        Some(&SqlState::UNIQUE_VIOLATION | &SqlState::EXCLUSION_VIOLATION) => {
            CostImportError::Conflict
        }
        _ => CostImportError::Unavailable,
    }
}
