use std::{collections::BTreeSet, str::FromStr};

use chrono::{Duration, NaiveDate};
use mcp_ozon::reporting::{
    collector_plan::CollectionTarget,
    mcp_read::{DataState, ManagerActionKind, ReportingReader},
    postgres_collector::{
        CollectedAdvertisingFact, CollectedFacts, CollectedPriceFact, CollectedSalesFact,
        CollectedSnapshot, CollectedStockFact, PostgresSnapshotWriter,
    },
    snapshot::{AccountScope, Marketplace, SnapshotSource, SnapshotStatus},
};
use tokio_postgres::Config;

#[tokio::test]
async fn wb_manager_actions_scope_stock_signals_and_withhold_legacy_mixed_inventory() {
    let (Ok(reader_url), Ok(collector_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_READER_URL"),
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
    ) else {
        return;
    };
    let reader = ReportingReader::connect_optional(Some(&reader_url))
        .await
        .unwrap();
    let writer = PostgresSnapshotWriter::connect(&Config::from_str(&collector_url).unwrap())
        .await
        .unwrap();
    let date = NaiveDate::from_ymd_opt(2098, 9, 17).unwrap();
    let start = date.and_hms_opt(0, 0, 0).unwrap().and_utc() - Duration::hours(5);
    let end = start + Duration::days(1);
    let cutoff = end + Duration::hours(8);
    let observed = cutoff - Duration::minutes(1);
    for legacy in [false, true] {
        let account = AccountScope::new(
            format!("wb_stock_actions_{}_{legacy}", std::process::id()),
            Marketplace::Wildberries,
        )
        .unwrap();
        let target = CollectionTarget {
            account_id: account.account_id().to_owned(),
            marketplace: Marketplace::Wildberries,
            sources: vec![
                SnapshotSource::Sales,
                SnapshotSource::Advertising,
                SnapshotSource::Stocks,
                SnapshotSource::Prices,
            ],
        };
        let claim = writer
            .claim_target(&target, cutoff, "wb-stock-scope-test")
            .await
            .unwrap()
            .unwrap();
        let facts = vec![
            CollectedFacts::Sales(
                (1..=3)
                    .map(|sku| CollectedSalesFact {
                        business_date: date,
                        sku,
                        ordered_units: 4,
                        operational_gmv_minor: 100_000,
                        cancelled_units: Some(0),
                        returned_units: Some(0),
                    })
                    .collect(),
            ),
            CollectedFacts::Advertising(vec![CollectedAdvertisingFact {
                business_date: date,
                campaign_id: 10,
                sku: 2,
                impressions: 0,
                clicks: 0,
                spend_minor: 10_000,
                attributed_orders: 4,
                attributed_revenue_minor: 100_000,
                basket_additions: 0,
                model_attributed_orders: 0,
                model_attributed_revenue_minor: 0,
                product_price_minor: 0,
                average_cpc_minor: None,
                cpm_minor: None,
                cpl_minor: None,
            }]),
            CollectedFacts::Stocks(
                (1..=3)
                    .map(|sku| CollectedStockFact {
                        sku,
                        warehouse_id: if legacy { "wb:seller:1:1" } else { "wb:1" }.to_owned(),
                        sellable_units: u64::from(sku == 3),
                    })
                    .collect(),
            ),
            CollectedFacts::Prices(Vec::<CollectedPriceFact>::new()),
        ];
        let snapshots = facts
            .into_iter()
            .map(|facts| {
                let period = if matches!(
                    &facts,
                    CollectedFacts::Stocks(_) | CollectedFacts::Prices(_)
                ) {
                    (observed, observed)
                } else {
                    (start, end)
                };
                CollectedSnapshot::new(
                    account.account_id().to_owned(),
                    Marketplace::Wildberries,
                    cutoff,
                    observed,
                    period.0,
                    period.1,
                    SnapshotStatus::Succeeded,
                    true,
                    "wb-stock-scope-test".to_owned(),
                    facts,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        writer
            .persist_claimed_batch(&claim, &snapshots)
            .await
            .unwrap();
        let result = reader
            .manager_actions(&account, Some(cutoff))
            .await
            .unwrap();
        assert_eq!(result.state, DataState::Complete);
        assert!(result.recommendations_allowed);
        if legacy {
            assert!(result.actions.is_empty());
        } else {
            assert_eq!(result.actions.len(), 3);
            assert_eq!(
                result
                    .actions
                    .iter()
                    .map(|a| a.sku.as_str())
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from(["1", "2", "3"])
            );
            assert_eq!(result.actions[0].kind, ManagerActionKind::WbFbwStockout);
            assert_eq!(
                result.actions[1].kind,
                ManagerActionKind::WbFbwStockoutWithAdSpend
            );
            assert_eq!(
                result.actions[2].kind,
                ManagerActionKind::WbFbwLowStockCover
            );
        }
    }
}
