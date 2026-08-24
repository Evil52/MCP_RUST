use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    config::{Marketplace, RegistrySource},
    reporting::{
        business_date,
        postgres_collector::CollectedAdvertisingFact,
        wb_adapter::{parse_promotion_stats, parse_stock_page},
    },
    wb::{WbClient, WbCredentials},
};

use super::{
    automation::{
        WbAutomationDecision, WbAutomationObservation, WbAutomationPolicy,
        WbAutomationSkuObservation, evaluate_wb_automation, validate_wb_automation_policy,
    },
    config::{read_control_token, validate_wb_reader_token},
};

const SNAPSHOT_SCHEMA_VERSION: u32 = 1;
const MAX_POLICY_BYTES: u64 = 64 * 1024;
const MAX_SNAPSHOT_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WbAutomationStateView {
    pub paused_by_automation: bool,
    pub actions_today: u32,
    pub last_action_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WbAutomationSnapshot {
    pub schema_version: u32,
    pub policy_sha256: String,
    pub previous_business_date: NaiveDate,
    pub observation: WbAutomationObservation,
    pub decision: WbAutomationDecision,
}

pub struct WbAutomationObserver {
    policy: WbAutomationPolicy,
    policy_sha256: String,
    client: WbClient,
}

impl std::fmt::Debug for WbAutomationObserver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WbAutomationObserver")
            .field("account_id", &self.policy.account_id)
            .field("campaign_id", &self.policy.campaign_id)
            .field("policy_sha256", &self.policy_sha256)
            .finish_non_exhaustive()
    }
}

impl WbAutomationObserver {
    pub fn from_files(
        policy_path: &Path,
        registry_path: &Path,
        reader_token_path: &Path,
        allow_broad_reader: bool,
        timeout: Duration,
        proxy_url: Option<&str>,
    ) -> Result<Self> {
        ensure!(
            !timeout.is_zero() && timeout <= Duration::from_secs(60),
            "WB automation timeout должен быть от 1 до 60 секунд"
        );
        let policy_bytes = read_regular_file(policy_path, MAX_POLICY_BYTES, "automation policy")?;
        let policy = serde_json::from_slice::<WbAutomationPolicy>(&policy_bytes)
            .context("WB automation policy содержит неверный JSON")?;
        validate_wb_automation_policy(&policy).map_err(|_| {
            anyhow::anyhow!("WB automation policy не прошёл fail-closed validation")
        })?;

        let registry = RegistrySource::new(registry_path)
            .context("WB automation access registry path неверен")?
            .load()
            .context("WB automation access registry недоступен")?;
        let account = registry
            .accounts
            .iter()
            .find(|account| account.id == policy.account_id)
            .context("WB automation account отсутствует в access registry")?;
        ensure!(
            account.marketplace == Marketplace::Wildberries,
            "WB automation account должен быть Wildberries"
        );
        let seller_sid = account
            .wildberries
            .as_ref()
            .and_then(|binding| binding.seller_sid.as_deref())
            .context("WB automation требует reviewed seller_sid")?;
        let token = read_control_token(reader_token_path, "WB_AUTOMATION_READ_TOKEN_FILE")?;
        validate_wb_reader_token(&token, seller_sid, allow_broad_reader)?;
        let accounts = BTreeMap::from([(policy.account_id.clone(), WbCredentials { token })]);
        let client = match proxy_url {
            Some(proxy) => WbClient::new_with_https_proxy(timeout, accounts, proxy)
                .context("WB automation proxy configuration неверна"),
            None => Ok(WbClient::new(timeout, accounts)),
        }?;
        let policy_sha256 = sha256_hex(
            &serde_json::to_vec(&policy).context("WB automation policy нельзя сериализовать")?,
        );
        Ok(Self {
            policy,
            policy_sha256,
            client,
        })
    }

    #[must_use]
    pub const fn policy(&self) -> &WbAutomationPolicy {
        &self.policy
    }

    #[must_use]
    pub fn policy_sha256(&self) -> &str {
        &self.policy_sha256
    }

    pub async fn observe(
        &self,
        observed_at: DateTime<Utc>,
        state: WbAutomationStateView,
    ) -> Result<WbAutomationSnapshot> {
        let current_date = business_date(observed_at);
        let previous_date = current_date
            .pred_opt()
            .context("WB automation business date вышла за диапазон")?;
        let details = self
            .client
            .promotion_campaign_details(
                &self.policy.account_id,
                vec![self.policy.campaign_id],
                Vec::new(),
                None,
            )
            .await
            .context("WB automation campaign details недоступны")?;
        let campaign = parse_campaign(&details, &self.policy)?;
        let budget = self
            .client
            .promotion_campaign_budget(&self.policy.account_id, self.policy.campaign_id)
            .await
            .context("WB automation campaign budget недоступен")?;
        let budget_remaining_minor = parse_budget_minor(&budget)?;
        let stats_response = self
            .client
            .promotion_stats(
                &self.policy.account_id,
                vec![self.policy.campaign_id],
                previous_date.format("%Y-%m-%d").to_string(),
                current_date.format("%Y-%m-%d").to_string(),
            )
            .await
            .context("WB automation campaign stats недоступны")?;
        let advertising = parse_promotion_stats(&stats_response)
            .map_err(|_| anyhow::anyhow!("WB automation campaign stats имеют неверную форму"))?;
        let stocks = self
            .client
            .warehouse_stocks(
                &self.policy.account_id,
                serde_json::json!({
                    "nmIds": self.policy.nm_ids,
                    "chrtIds": [],
                    "limit": 100,
                    "offset": 0
                }),
            )
            .await
            .context("WB automation stock snapshot недоступен")?;
        let (stocks, source_stock_rows) = parse_stock_page(&stocks)
            .map_err(|_| anyhow::anyhow!("WB automation stock snapshot имеет неверную форму"))?;
        ensure!(
            source_stock_rows <= 100,
            "WB automation stock response неожиданно требует pagination"
        );
        let observation = build_observation(
            &self.policy,
            observed_at,
            current_date,
            previous_date,
            &campaign,
            budget_remaining_minor,
            &advertising,
            &stocks,
            state,
        )?;
        let decision = evaluate_wb_automation(&self.policy, &observation)
            .map_err(|_| anyhow::anyhow!("WB automation observation отклонён движком"))?;
        Ok(WbAutomationSnapshot {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            policy_sha256: self.policy_sha256.clone(),
            previous_business_date: previous_date,
            observation,
            decision,
        })
    }
}

#[derive(Debug)]
struct CampaignObservation {
    status: i32,
    bids: BTreeMap<u64, u64>,
}

fn parse_campaign(response: &Value, policy: &WbAutomationPolicy) -> Result<CampaignObservation> {
    let adverts = response
        .get("adverts")
        .and_then(Value::as_array)
        .context("WB campaign details не содержит adverts")?;
    let matching = adverts
        .iter()
        .filter(|advert| advert.get("id").and_then(Value::as_u64) == Some(policy.campaign_id))
        .collect::<Vec<_>>();
    ensure!(
        matching.len() == 1,
        "WB automation не нашёл ровно одну разрешённую campaign"
    );
    let advert = matching[0];
    ensure!(
        advert.pointer("/settings/name").and_then(Value::as_str)
            == Some(policy.campaign_name.as_str())
            && advert.get("bid_type").and_then(Value::as_str) == Some("manual")
            && advert
                .pointer("/settings/payment_type")
                .and_then(Value::as_str)
                == Some("cpc")
            && advert
                .pointer("/settings/placements/search")
                .and_then(Value::as_bool)
                == Some(true)
            && advert
                .pointer("/settings/placements/recommendations")
                .and_then(Value::as_bool)
                == Some(false),
        "WB automation campaign contract изменился"
    );
    let status = advert
        .get("status")
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .context("WB automation campaign status неверен")?;
    let nm_settings = advert
        .get("nm_settings")
        .and_then(Value::as_array)
        .context("WB automation campaign не содержит nm_settings")?;
    let mut bids = BTreeMap::new();
    for setting in nm_settings {
        let nm_id = setting
            .get("nm_id")
            .and_then(Value::as_u64)
            .context("WB automation campaign nm_id неверен")?;
        let bid = setting
            .pointer("/bids_kopecks/search")
            .and_then(Value::as_u64)
            .context("WB automation campaign search bid отсутствует")?;
        let recommendations = setting
            .pointer("/bids_kopecks/recommendations")
            .and_then(Value::as_u64)
            .context("WB automation campaign recommendations bid отсутствует")?;
        ensure!(
            recommendations == 0 && bids.insert(nm_id, bid).is_none(),
            "WB automation campaign содержит неожиданную ставку или duplicate SKU"
        );
    }
    let expected = policy.nm_ids.iter().copied().collect::<BTreeSet<_>>();
    ensure!(
        bids.keys().copied().collect::<BTreeSet<_>>() == expected,
        "WB automation campaign SKU scope изменился"
    );
    Ok(CampaignObservation { status, bids })
}

fn parse_budget_minor(response: &Value) -> Result<u64> {
    let total_rubles = response
        .get("total")
        .and_then(Value::as_u64)
        .context("WB automation budget total неверен")?;
    total_rubles
        .checked_mul(100)
        .context("WB automation budget overflow")
}

#[allow(clippy::too_many_arguments)]
fn build_observation(
    policy: &WbAutomationPolicy,
    observed_at: DateTime<Utc>,
    current_date: NaiveDate,
    previous_date: NaiveDate,
    campaign: &CampaignObservation,
    budget_remaining_minor: u64,
    advertising: &[CollectedAdvertisingFact],
    stocks: &[crate::reporting::postgres_collector::CollectedStockFact],
    state: WbAutomationStateView,
) -> Result<WbAutomationObservation> {
    let allowed = policy.nm_ids.iter().copied().collect::<BTreeSet<_>>();
    ensure!(
        advertising.iter().all(|fact| {
            fact.campaign_id == policy.campaign_id
                && (fact.sku == 0 || allowed.contains(&fact.sku))
                && matches!(fact.business_date, date if date == current_date || date == previous_date)
        }),
        "WB automation stats вышли за campaign/date/SKU scope"
    );
    let mut daily_spend_minor = 0_u64;
    let mut previous = BTreeMap::<u64, &CollectedAdvertisingFact>::new();
    let mut campaign_level_previous = false;
    for fact in advertising {
        if fact.business_date == current_date {
            daily_spend_minor = daily_spend_minor
                .checked_add(fact.spend_minor)
                .context("WB automation daily spend overflow")?;
        } else if fact.sku == 0 {
            campaign_level_previous = true;
        } else {
            ensure!(
                previous.insert(fact.sku, fact).is_none(),
                "WB automation stats содержат duplicate SKU"
            );
        }
    }
    let mut stock_totals = BTreeMap::<u64, u64>::new();
    for stock in stocks.iter().filter(|stock| allowed.contains(&stock.sku)) {
        let total = stock_totals.entry(stock.sku).or_default();
        *total = total
            .checked_add(stock.sellable_units)
            .context("WB automation stock overflow")?;
    }
    let skus = policy
        .nm_ids
        .iter()
        .map(|nm_id| {
            let fact = previous.get(nm_id).copied();
            Ok(WbAutomationSkuObservation {
                nm_id: *nm_id,
                current_bid_kopecks: *campaign
                    .bids
                    .get(nm_id)
                    .context("WB automation campaign bid исчез")?,
                sellable_stock: stock_totals.get(nm_id).copied().unwrap_or(0),
                impressions: fact.map_or(0, |value| value.impressions),
                clicks: fact.map_or(0, |value| value.clicks),
                spend_minor: fact.map_or(0, |value| value.spend_minor),
                attributed_orders: fact.map_or(0, |value| value.attributed_orders),
                attributed_revenue_minor: fact.map_or(0, |value| value.attributed_revenue_minor),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(WbAutomationObservation {
        observed_at,
        campaign_status: campaign.status,
        paused_by_automation: state.paused_by_automation,
        budget_remaining_minor,
        daily_spend_minor,
        actions_today: state.actions_today,
        last_action_at: state.last_action_at,
        attribution_complete: !campaign_level_previous,
        skus,
    })
}

pub fn persist_wb_automation_snapshot(
    directory: &Path,
    snapshot: &WbAutomationSnapshot,
) -> Result<PathBuf> {
    validate_private_directory(directory)?;
    let bytes = serde_json::to_vec_pretty(snapshot)
        .context("WB automation snapshot нельзя сериализовать")?;
    ensure!(
        bytes.len() <= MAX_SNAPSHOT_BYTES,
        "WB automation snapshot превышает 1 MiB"
    );
    let stamp = snapshot
        .observation
        .observed_at
        .to_rfc3339_opts(SecondsFormat::Nanos, true)
        .replace([':', '-'], "")
        .replace('.', "_");
    let history = directory.join(format!("snapshot-{stamp}.json"));
    write_new_file(&history, &bytes)?;
    write_atomic_file(directory, Path::new("latest.json"), &bytes)?;
    Ok(history)
}

fn validate_private_directory(path: &Path) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).context("WB automation state directory недоступен")?;
    ensure!(
        metadata.is_dir()
            && !metadata.file_type().is_symlink()
            && metadata.permissions().mode().is_multiple_of(0o100),
        "WB automation state directory должен быть доступен только владельцу"
    );
    Ok(())
}

fn write_new_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .context("WB automation history snapshot уже существует или недоступен")?;
    write_and_sync(&mut file, bytes)
}

fn write_atomic_file(directory: &Path, name: &Path, bytes: &[u8]) -> Result<()> {
    let temporary = directory.join(format!(".latest-{}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .context("WB automation temporary snapshot недоступен")?;
    if let Err(error) = write_and_sync(&mut file, bytes) {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    fs::rename(&temporary, directory.join(name))
        .context("WB automation latest snapshot нельзя опубликовать")?;
    File::open(directory)
        .and_then(|directory| directory.sync_all())
        .context("WB automation state directory нельзя синхронизировать")
}

fn write_and_sync(file: &mut File, bytes: &[u8]) -> Result<()> {
    file.write_all(bytes)
        .context("WB automation snapshot нельзя записать")?;
    file.write_all(b"\n")
        .context("WB automation snapshot нельзя завершить")?;
    file.sync_all()
        .context("WB automation snapshot нельзя синхронизировать")
}

fn read_regular_file(path: &Path, maximum: u64, label: &str) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path).with_context(|| format!("{label} недоступен"))?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink() && metadata.len() <= maximum,
        "{label} должен быть обычным bounded-файлом"
    );
    fs::read(path).with_context(|| format!("{label} нельзя прочитать"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    digest
        .finalize()
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
            output
        })
}
