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
        postgres_collector::CollectedAdvertisingFact,
        wb_adapter::{parse_promotion_stats, parse_stock_page},
    },
    wb::{WbClient, WbCredentials},
};

use super::{
    automation::{
        WbAutomationCampaignMetrics, WbAutomationDecision, WbAutomationObservation,
        WbAutomationPolicy, WbAutomationSkuObservation, evaluate_wb_automation,
        validate_wb_automation_policy, wb_automation_business_date,
    },
    config::{read_control_token, validate_wb_reader_token},
};

// Version 4 adds explicit current-day spend completeness and the versioned
// WB ADS ROBOT v1 policy contract. Missing current-day delivery is no longer
// serialized as a trusted zero that could authorize a write.
const SNAPSHOT_SCHEMA_VERSION: u32 = 5;
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

    #[cfg(test)]
    pub(super) fn replace_client_for_test(&mut self, client: WbClient) {
        self.client = client;
    }

    pub async fn observe(
        &self,
        observed_at: DateTime<Utc>,
        state: WbAutomationStateView,
    ) -> Result<WbAutomationSnapshot> {
        let current_date = wb_automation_business_date(observed_at);
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
        let minimum_bids = self
            .client
            .promotion_minimum_bids(
                &self.policy.account_id,
                self.policy.campaign_id,
                self.policy.nm_ids.clone(),
                self.policy.payment_type.clone(),
                vec![self.policy.placement.clone()],
            )
            .await
            .context("WB automation minimum CPC bids недоступны")?;
        validate_minimum_bids(&minimum_bids, &self.policy)?;
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

fn validate_minimum_bids(response: &Value, policy: &WbAutomationPolicy) -> Result<()> {
    let envelope = response
        .as_object()
        .context("WB automation minimum CPC bids имеют неверную форму")?;
    ensure!(
        envelope.len() == 1,
        "WB automation minimum CPC bids имеют неверный envelope"
    );
    let rows = envelope
        .get("bids")
        .and_then(Value::as_array)
        .context("WB automation minimum CPC bids не содержат bids")?;
    let mut minimums = BTreeMap::new();
    for row in rows {
        let row = row
            .as_object()
            .context("WB automation minimum CPC bid имеет неверную форму")?;
        ensure!(
            row.len() == 2,
            "WB automation minimum CPC bid имеет unexpected fields"
        );
        let nm_id = row
            .get("nm_id")
            .and_then(Value::as_u64)
            .context("WB automation minimum CPC bid nm_id неверен")?;
        let bids = row
            .get("bids")
            .and_then(Value::as_array)
            .context("WB automation minimum CPC bid не содержит bids")?;
        let search = bids
            .iter()
            .filter(|bid| bid.get("type").and_then(Value::as_str) == Some("search"))
            .collect::<Vec<_>>();
        let search_bid = search.first().and_then(|bid| bid.as_object());
        ensure!(
            search.len() == 1
                && bids.len() == 1
                && search_bid.is_some_and(|bid| {
                    bid.len() == 3 && bid.get("currency").and_then(Value::as_str) == Some("RUB")
                })
                && minimums
                    .insert(
                        nm_id,
                        search[0]
                            .get("value")
                            .and_then(Value::as_u64)
                            .context("WB automation minimum CPC bid value неверен")?,
                    )
                    .is_none(),
            "WB automation minimum CPC bids содержат duplicate или unexpected placement"
        );
    }
    let expected = policy.nm_ids.iter().copied().collect::<BTreeSet<_>>();
    ensure!(
        minimums.keys().copied().collect::<BTreeSet<_>>() == expected
            && minimums
                .values()
                .all(|minimum| *minimum == policy.min_bid_kopecks),
        "WB automation minimum CPC bid contract изменился"
    );
    Ok(())
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
    let mut current_sku_spend_minor = 0_u64;
    let mut current_sku_rows = 0_u64;
    let mut current_campaign = None;
    let mut previous = BTreeMap::<u64, &CollectedAdvertisingFact>::new();
    let mut campaign_level_previous = None;
    for fact in advertising {
        if fact.business_date == current_date {
            if fact.sku == 0 {
                ensure!(
                    current_campaign.replace(fact).is_none(),
                    "WB automation stats содержат duplicate campaign total"
                );
            } else {
                current_sku_rows = current_sku_rows
                    .checked_add(1)
                    .context("WB automation current stats row count overflow")?;
                current_sku_spend_minor = current_sku_spend_minor
                    .checked_add(fact.spend_minor)
                    .context("WB automation daily spend overflow")?;
            }
        } else if fact.sku == 0 {
            ensure!(
                campaign_level_previous.replace(fact).is_none(),
                "WB automation stats содержат duplicate campaign total"
            );
        } else {
            ensure!(
                previous.insert(fact.sku, fact).is_none(),
                "WB automation stats содержат duplicate SKU"
            );
        }
    }
    ensure!(
        current_campaign.is_none() || current_sku_spend_minor == 0,
        "WB automation stats смешивают campaign и SKU totals за текущую дату"
    );
    ensure!(
        campaign_level_previous.is_none() || previous.is_empty(),
        "WB automation stats смешивают campaign и SKU totals за предыдущую дату"
    );
    let daily_spend_complete = current_campaign.is_some() || current_sku_rows > 0;
    let daily_spend_minor =
        current_campaign.map_or(current_sku_spend_minor, |fact| fact.spend_minor);
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
        daily_spend_complete,
        actions_today: state.actions_today,
        last_action_at: state.last_action_at,
        // One present SKU row is enough for exact attribution because WB omits
        // SKUs that genuinely did not deliver. A campaign-level row is retained
        // separately for symmetric decisions and is never copied into SKU
        // facts. No row in either scope remains missing evidence and holds.
        attribution_complete: campaign_level_previous.is_none() && !previous.is_empty(),
        campaign_level_metrics: campaign_level_previous.map(|fact| WbAutomationCampaignMetrics {
            impressions: fact.impressions,
            clicks: fact.clicks,
            spend_minor: fact.spend_minor,
            attributed_orders: fact.attributed_orders,
            attributed_revenue_minor: fact.attributed_revenue_minor,
        }),
        current_campaign_metrics: current_campaign.map(|fact| WbAutomationCampaignMetrics {
            impressions: fact.impressions,
            clicks: fact.clicks,
            spend_minor: fact.spend_minor,
            attributed_orders: fact.attributed_orders,
            attributed_revenue_minor: fact.attributed_revenue_minor,
        }),
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
    write_atomic_file_with(directory, name, bytes, write_and_sync)
}

fn write_atomic_file_with(
    directory: &Path,
    name: &Path,
    bytes: &[u8],
    write: fn(&mut File, &[u8]) -> Result<()>,
) -> Result<()> {
    let temporary = directory.join(format!(".latest-{}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .context("WB automation temporary snapshot недоступен")?;
    if let Err(error) = write(&mut file, bytes) {
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
    read_regular_file_with(path, maximum, label, read_file)
}

fn read_file(path: &Path) -> std::io::Result<Vec<u8>> {
    fs::read(path)
}

fn read_regular_file_with(
    path: &Path,
    maximum: u64,
    label: &str,
    read: fn(&Path) -> std::io::Result<Vec<u8>>,
) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path).with_context(|| format!("{label} недоступен"))?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink() && metadata.len() <= maximum,
        "{label} должен быть обычным bounded-файлом"
    );
    read(path).with_context(|| format!("{label} нельзя прочитать"))
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

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::control::automation::{WbAutomationAction, WbAutomationHoldReason};
    use crate::test_support::mock_http;

    const TEST_SELLER_SID: &str = "123e4567-e89b-42d3-a456-426614174000";
    static FIXTURE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    struct Fixture {
        root: PathBuf,
        policy: PathBuf,
        registry: PathBuf,
        reader_token: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let id = FIXTURE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "mcp-wb-automation-observer-{}-{id}",
                std::process::id()
            ));
            fs::create_dir(&root).unwrap();
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            let policy = root.join("policy.json");
            let registry = root.join("access.json");
            let reader_token = root.join("reader.token");
            fs::write(
                &policy,
                serde_json::to_vec_pretty(&policy_fixture()).unwrap(),
            )
            .unwrap();
            fs::write(
                &registry,
                serde_json::to_vec(&serde_json::json!({
                    "version": 1,
                    "actors": [{
                        "id": "manager",
                        "name": "Manager",
                        "role": "manager",
                        "oidc": {"username": "manager"}
                    }],
                    "accounts": [{
                        "id": "ip_domnyshev_wb",
                        "organization": "Test WB",
                        "marketplace": "wildberries",
                        "seller_client_id": "seller",
                        "manager_id": "manager",
                        "wildberries": {
                            "api_token_env": "UNUSED_WB_TOKEN",
                            "seller_sid": TEST_SELLER_SID
                        }
                    }]
                }))
                .unwrap(),
            )
            .unwrap();
            fs::write(&reader_token, wb_token((1_u64 << 6) | (1_u64 << 30))).unwrap();
            fs::set_permissions(&reader_token, fs::Permissions::from_mode(0o600)).unwrap();
            Self {
                root,
                policy,
                registry,
                reader_token,
            }
        }

        fn observer(&self, proxy_url: Option<&str>) -> WbAutomationObserver {
            WbAutomationObserver::from_files(
                &self.policy,
                &self.registry,
                &self.reader_token,
                false,
                Duration::from_secs(2),
                proxy_url,
            )
            .unwrap()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn policy_fixture() -> WbAutomationPolicy {
        let mut policy = serde_json::from_str::<WbAutomationPolicy>(include_str!(
            "../../config/wb-automation-robot.json"
        ))
        .unwrap();
        policy.write_enabled = true;
        policy.bid_writes_enabled = true;
        policy
    }

    fn wb_token(scope: u64) -> String {
        let expires = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3_600;
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"ES256","typ":"JWT"}"#);
        let claims = serde_json::json!({
            "acc": 3,
            "for": "self",
            "t": false,
            "s": scope,
            "exp": expires,
            "sid": TEST_SELLER_SID
        });
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
        let signature = URL_SAFE_NO_PAD.encode([0_u8; 64]);
        format!("{header}.{payload}.{signature}")
    }

    fn install_test_client(observer: &mut WbAutomationObserver, base_url: &str) {
        let account_id = observer.policy.account_id.clone();
        observer.replace_client_for_test(WbClient::new_for_test(
            Duration::from_secs(2),
            BTreeMap::from([(
                account_id,
                WbCredentials {
                    token: "test-token".to_owned(),
                },
            )]),
            base_url,
            base_url,
        ));
    }

    fn campaign_response() -> Value {
        serde_json::json!({
            "adverts": [{
                "id": 39_682_633,
                "status": 9,
                "bid_type": "manual",
                "settings": {
                    "name": "Робот",
                    "payment_type": "cpc",
                    "placements": {"search": true, "recommendations": false}
                },
                "nm_settings": [
                    {"nm_id": 449_627_598_u64, "bids_kopecks": {"search": 102, "recommendations": 0}},
                    {"nm_id": 449_627_015_u64, "bids_kopecks": {"search": 102, "recommendations": 0}},
                    {"nm_id": 497_424_314_u64, "bids_kopecks": {"search": 102, "recommendations": 0}}
                ]
            }]
        })
    }

    fn minimum_bids_response() -> Value {
        serde_json::json!({
            "bids": [
                {"nm_id": 449_627_598_u64, "bids": [{"currency": "RUB", "type": "search", "value": 102}]},
                {"nm_id": 449_627_015_u64, "bids": [{"currency": "RUB", "type": "search", "value": 102}]},
                {"nm_id": 497_424_314_u64, "bids": [{"currency": "RUB", "type": "search", "value": 102}]}
            ]
        })
    }

    fn stats_response() -> Value {
        serde_json::json!([{
            "advertId": 39_682_633,
            "stats": [
                {"date": "2026-08-24", "nm_id": 449_627_598_u64, "views": 100, "clicks": 10, "sum": 2, "orders": 1, "sumPrice": 100},
                {"date": "2026-08-24", "nm_id": 449_627_015_u64, "views": 80, "clicks": 8, "sum": 1, "orders": 1, "sumPrice": 80},
                {"date": "2026-08-24", "nm_id": 497_424_314_u64, "views": 60, "clicks": 6, "sum": 1, "orders": 1, "sumPrice": 60},
                {"date": "2026-08-25", "nm_id": 449_627_598_u64, "views": 5, "clicks": 1, "sum": 1.5, "orders": 0, "sumPrice": 0}
            ]
        }])
    }

    fn campaign_level_stats_response() -> Value {
        serde_json::json!([{
            "advertId": 39_682_633,
            "days": [
                {
                    "date": "2026-08-24",
                    "views": 30,
                    "clicks": 3,
                    "sum": 3.06,
                    "orders": 0,
                    "sumPrice": 0,
                    "apps": [
                        {"appType": 1},
                        {"appType": 2},
                        {"appType": 64}
                    ]
                },
                {
                    "date": "2026-08-25",
                    "views": 10,
                    "clicks": 1,
                    "sum": 1.02,
                    "orders": 0,
                    "sumPrice": 0,
                    "apps": [
                        {"appType": 1},
                        {"appType": 2},
                        {"appType": 64}
                    ]
                }
            ]
        }])
    }

    fn stocks_response() -> Value {
        serde_json::json!({"data": {"items": [
            {"nmId": 449_627_598_u64, "warehouseId": 1, "quantity": 10},
            {"nmId": 449_627_015_u64, "warehouseId": 1, "quantity": 12},
            {"nmId": 497_424_314_u64, "warehouseId": 1, "quantity": 8}
        ]}})
    }

    #[tokio::test]
    async fn files_network_observation_and_snapshot_persistence_form_one_closed_path() {
        let fixture = Fixture::new();
        let mut observer = fixture.observer(None);
        let (base_url, requests) = mock_http(vec![
            (200, campaign_response().to_string()),
            (200, minimum_bids_response().to_string()),
            (200, serde_json::json!({"total": 1_000}).to_string()),
            (200, stats_response().to_string()),
            (200, stocks_response().to_string()),
        ]);
        observer.replace_client_for_test(WbClient::new_for_test(
            Duration::from_secs(2),
            BTreeMap::from([(
                observer.policy.account_id.clone(),
                WbCredentials {
                    token: "test-token".to_owned(),
                },
            )]),
            &base_url,
            &base_url,
        ));
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 25, 12, 0, 0).unwrap();
        let snapshot = observer
            .observe(
                observed_at,
                WbAutomationStateView {
                    paused_by_automation: false,
                    actions_today: 1,
                    last_action_at: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(observer.policy().campaign_id, 39_682_633);
        assert_eq!(observer.policy_sha256().len(), 64);
        assert!(format!("{observer:?}").contains("ip_domnyshev_wb"));
        assert_eq!(snapshot.previous_business_date.to_string(), "2026-08-24");
        assert_eq!(snapshot.observation.budget_remaining_minor, 100_000);
        assert_eq!(snapshot.observation.daily_spend_minor, 150);
        assert!(snapshot.observation.current_campaign_metrics.is_none());
        assert!(snapshot.observation.attribution_complete);
        assert_eq!(snapshot.observation.skus[0].sellable_stock, 10);

        let history = persist_wb_automation_snapshot(&fixture.root, &snapshot).unwrap();
        assert!(history.is_file());
        assert_eq!(
            serde_json::from_slice::<WbAutomationSnapshot>(
                &fs::read(fixture.root.join("latest.json")).unwrap()
            )
            .unwrap(),
            snapshot
        );
        for _ in 0..5 {
            assert!(
                requests
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .contains("authorization: Bearer test-token")
            );
        }
    }

    #[tokio::test]
    async fn real_cpc_campaign_totals_hold_without_per_sku_attribution() {
        let fixture = Fixture::new();
        let mut observer = fixture.observer(None);
        let (base_url, _requests) = mock_http(vec![
            (200, campaign_response().to_string()),
            (200, minimum_bids_response().to_string()),
            (200, serde_json::json!({"total": 995}).to_string()),
            (200, campaign_level_stats_response().to_string()),
            (200, stocks_response().to_string()),
        ]);
        install_test_client(&mut observer, &base_url);

        let snapshot = observer
            .observe(
                Utc.with_ymd_and_hms(2026, 8, 25, 12, 0, 0).unwrap(),
                WbAutomationStateView::default(),
            )
            .await
            .unwrap();

        assert!(!snapshot.observation.attribution_complete);
        assert_eq!(
            snapshot.observation.campaign_level_metrics,
            Some(WbAutomationCampaignMetrics {
                impressions: 30,
                clicks: 3,
                spend_minor: 306,
                attributed_orders: 0,
                attributed_revenue_minor: 0,
            })
        );
        assert_eq!(snapshot.observation.daily_spend_minor, 102);
        assert_eq!(
            snapshot.observation.current_campaign_metrics,
            Some(WbAutomationCampaignMetrics {
                impressions: 10,
                clicks: 1,
                spend_minor: 102,
                attributed_orders: 0,
                attributed_revenue_minor: 0,
            })
        );
        assert!(snapshot.observation.skus.iter().all(|sku| {
            sku.impressions == 0
                && sku.clicks == 0
                && sku.spend_minor == 0
                && sku.attributed_orders == 0
                && sku.attributed_revenue_minor == 0
        }));
        assert_eq!(
            snapshot.decision.action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::AttributionIncomplete,
            }
        );
    }

    #[test]
    fn proxy_construction_and_parser_guards_are_exercised() {
        let fixture = Fixture::new();
        assert_eq!(
            fixture
                .observer(Some("http://127.0.0.1:3128"))
                .policy()
                .account_id,
            "ip_domnyshev_wb"
        );
        let policy = policy_fixture();
        assert_eq!(
            parse_campaign(&campaign_response(), &policy)
                .unwrap()
                .status,
            9
        );
        assert!(parse_campaign(&serde_json::json!({"adverts": []}), &policy).is_err());
        assert_eq!(
            parse_budget_minor(&serde_json::json!({"total": 7})).unwrap(),
            700
        );
        assert!(parse_budget_minor(&serde_json::json!({"total": u64::MAX})).is_err());
        assert!(validate_minimum_bids(&minimum_bids_response(), &policy).is_ok());
        for invalid in [
            serde_json::json!({}),
            serde_json::json!({"bids": [], "unexpected": true}),
            serde_json::json!({"bids": [{"bids": []}]}),
            serde_json::json!({"bids": [{"nm_id": 449_627_598_u64}]}),
            serde_json::json!({"bids": [{"nm_id": 449_627_598_u64, "bids": [], "unexpected": true}]}),
            serde_json::json!({"bids": [{"nm_id": 449_627_598_u64, "bids": [{"currency": "RUB", "type": "search"}]}]}),
            serde_json::json!({"bids": [{"nm_id": 449_627_598_u64, "bids": [{"currency": "RUB", "type": "recommendation", "value": 102}]}]}),
        ] {
            assert!(validate_minimum_bids(&invalid, &policy).is_err());
        }
        for invalid_bid in [
            serde_json::json!({"type": "search", "value": 102}),
            serde_json::json!({"currency": "USD", "type": "search", "value": 102}),
            serde_json::json!({"currency": "RUB", "type": "search", "value": 102, "unexpected": true}),
        ] {
            let mut invalid = minimum_bids_response();
            invalid["bids"][0]["bids"][0] = invalid_bid;
            assert!(validate_minimum_bids(&invalid, &policy).is_err());
        }
        let mut changed_minimum = minimum_bids_response();
        changed_minimum["bids"][0]["bids"][0]["value"] = serde_json::json!(103);
        assert!(validate_minimum_bids(&changed_minimum, &policy).is_err());
        let mut duplicate_minimum = minimum_bids_response();
        let duplicate_row = duplicate_minimum["bids"][0].clone();
        duplicate_minimum["bids"]
            .as_array_mut()
            .unwrap()
            .push(duplicate_row);
        assert!(validate_minimum_bids(&duplicate_minimum, &policy).is_err());
        assert_eq!(sha256_hex(b"test").len(), 64);
        assert!(read_regular_file(&fixture.policy, MAX_POLICY_BYTES, "policy").is_ok());
        assert!(
            read_regular_file(
                &fixture.root.join("missing-policy"),
                MAX_POLICY_BYTES,
                "policy"
            )
            .is_err()
        );
        assert!(
            read_regular_file_with(&fixture.policy, MAX_POLICY_BYTES, "policy", |_| Err(
                std::io::Error::other("injected read failure")
            ))
            .is_err()
        );

        let campaign = parse_campaign(&campaign_response(), &policy).unwrap();
        let advertising = parse_promotion_stats(&serde_json::json!([{
            "advertId": 39_682_633,
            "stats": [{
                "date": "2026-08-24",
                "views": 1,
                "clicks": 0,
                "sum": 0,
                "orders": 0,
                "sumPrice": 0
            }]
        }]))
        .unwrap();
        let (stocks, _) = parse_stock_page(&stocks_response()).unwrap();
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 25, 12, 0, 0).unwrap();
        assert!(
            !build_observation(
                &policy,
                observed_at,
                observed_at.date_naive(),
                observed_at.date_naive().pred_opt().unwrap(),
                &campaign,
                100_000,
                &advertising,
                &stocks,
                WbAutomationStateView::default()
            )
            .unwrap()
            .attribution_complete
        );

        assert!(
            WbAutomationObserver::from_files(
                &fixture.policy,
                &fixture.registry,
                &fixture.reader_token,
                false,
                Duration::from_secs(2),
                Some("http://[::1"),
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn invalid_policy_observation_and_atomic_write_fail_closed() {
        let fixture = Fixture::new();
        let mut invalid_policy = policy_fixture();
        invalid_policy.allow_budget_top_up = true;
        fs::write(
            &fixture.policy,
            serde_json::to_vec(&invalid_policy).unwrap(),
        )
        .unwrap();
        assert!(
            WbAutomationObserver::from_files(
                &fixture.policy,
                &fixture.registry,
                &fixture.reader_token,
                false,
                Duration::from_secs(2),
                None,
            )
            .is_err()
        );
        fs::write(
            &fixture.policy,
            serde_json::to_vec(&policy_fixture()).unwrap(),
        )
        .unwrap();

        let mut observer = fixture.observer(None);
        let (base_url, _) = mock_http(vec![
            (200, campaign_response().to_string()),
            (200, minimum_bids_response().to_string()),
            (200, serde_json::json!({"total": 1_000}).to_string()),
            (
                200,
                serde_json::json!([{
                    "advertId": 1,
                    "stats": [{"date": "2026-08-24", "nm_id": 449_627_598_u64, "views": 1, "clicks": 0, "sum": 0, "orders": 0, "sumPrice": 0}]
                }])
                .to_string(),
            ),
            (200, stocks_response().to_string()),
        ]);
        let account_id = observer.policy.account_id.clone();
        observer.replace_client_for_test(WbClient::new_for_test(
            Duration::from_secs(2),
            BTreeMap::from([(
                account_id,
                WbCredentials {
                    token: "test-token".to_owned(),
                },
            )]),
            &base_url,
            &base_url,
        ));
        assert!(
            observer
                .observe(
                    Utc.with_ymd_and_hms(2026, 8, 25, 12, 0, 0).unwrap(),
                    WbAutomationStateView::default()
                )
                .await
                .is_err()
        );

        assert!(
            write_atomic_file_with(
                &fixture.root,
                Path::new("never-published.json"),
                b"snapshot",
                |_, _| Err(anyhow::anyhow!("injected write failure"))
            )
            .is_err()
        );
        assert!(!fixture.root.join("never-published.json").exists());
    }

    #[tokio::test]
    async fn response_shape_and_decision_error_mappers_are_executed() {
        let fixture = Fixture::new();

        let mut invalid_stats = fixture.observer(None);
        let (stats_url, _) = mock_http(vec![
            (200, campaign_response().to_string()),
            (200, minimum_bids_response().to_string()),
            (200, serde_json::json!({"total": 1_000}).to_string()),
            (200, serde_json::json!({}).to_string()),
        ]);
        install_test_client(&mut invalid_stats, &stats_url);
        assert!(
            invalid_stats
                .observe(
                    Utc.with_ymd_and_hms(2026, 8, 25, 12, 0, 0).unwrap(),
                    WbAutomationStateView::default()
                )
                .await
                .is_err()
        );

        let mut invalid_stocks = fixture.observer(None);
        let (stocks_url, _) = mock_http(vec![
            (200, campaign_response().to_string()),
            (200, minimum_bids_response().to_string()),
            (200, serde_json::json!({"total": 1_000}).to_string()),
            (200, serde_json::Value::Null.to_string()),
            (200, serde_json::json!({}).to_string()),
        ]);
        install_test_client(&mut invalid_stocks, &stocks_url);
        assert!(
            invalid_stocks
                .observe(
                    Utc.with_ymd_and_hms(2026, 8, 25, 12, 1, 0).unwrap(),
                    WbAutomationStateView::default()
                )
                .await
                .is_err()
        );

        let mut invalid_campaign_bid = campaign_response();
        invalid_campaign_bid["adverts"][0]["nm_settings"][0]["bids_kopecks"]["search"] =
            serde_json::json!(101);
        let mut invalid_decision = fixture.observer(None);
        let (decision_url, _) = mock_http(vec![
            (200, invalid_campaign_bid.to_string()),
            (200, minimum_bids_response().to_string()),
            (200, serde_json::json!({"total": 1_000}).to_string()),
            (200, serde_json::Value::Null.to_string()),
            (200, stocks_response().to_string()),
        ]);
        install_test_client(&mut invalid_decision, &decision_url);
        assert!(
            invalid_decision
                .observe(
                    Utc.with_ymd_and_hms(2026, 8, 25, 12, 2, 0).unwrap(),
                    WbAutomationStateView::default()
                )
                .await
                .is_err()
        );
    }

    /// A WB `fullstats` response can legitimately contain no rows for the
    /// previous business date (delivery gap, late aggregation, or an API
    /// hiccup). `build_observation` then reports every SKU with zero
    /// impressions and zero clicks while `attribution_complete` stays true,
    /// because only a campaign-level previous row lowers that flag. The
    /// decision engine reads those zeros as genuine low exposure and raises
    /// every bid, so missing evidence turns into a spend increase.
    #[tokio::test]
    async fn absent_previous_day_stats_must_not_be_read_as_low_exposure() {
        let fixture = Fixture::new();
        let mut observer = fixture.observer(None);
        // Identical to `stats_response`, minus every previous-day (2026-08-24)
        // row: the campaign reported only current-day delivery.
        let stats_without_previous_day = serde_json::json!([{
            "advertId": 39_682_633,
            "stats": [
                {"date": "2026-08-25", "nm_id": 449_627_598_u64, "views": 5, "clicks": 1, "sum": 1.5, "orders": 0, "sumPrice": 0}
            ]
        }]);
        let (base_url, _requests) = mock_http(vec![
            (200, campaign_response().to_string()),
            (200, minimum_bids_response().to_string()),
            (200, serde_json::json!({"total": 1_000}).to_string()),
            (200, stats_without_previous_day.to_string()),
            (200, stocks_response().to_string()),
        ]);
        observer.replace_client_for_test(WbClient::new_for_test(
            Duration::from_secs(2),
            BTreeMap::from([(
                observer.policy.account_id.clone(),
                WbCredentials {
                    token: "test-token".to_owned(),
                },
            )]),
            &base_url,
            &base_url,
        ));
        let snapshot = observer
            .observe(
                Utc.with_ymd_and_hms(2026, 8, 25, 12, 0, 0).unwrap(),
                WbAutomationStateView::default(),
            )
            .await
            .unwrap();

        assert!(
            snapshot
                .observation
                .skus
                .iter()
                .all(|sku| sku.impressions == 0 && sku.clicks == 0),
            "absent previous-day rows collapse to zero evidence: {:?}",
            snapshot.observation.skus
        );
        assert!(
            !snapshot.observation.attribution_complete,
            "a business date with no advertising rows at all is missing \
             attribution evidence and must hold, but the observation reported \
             attribution_complete=true and produced {:?}",
            snapshot.decision.action
        );
        assert!(snapshot.observation.campaign_level_metrics.is_none());
        assert_eq!(
            snapshot.decision.action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::AttributionIncomplete,
            }
        );
    }

    #[tokio::test]
    async fn absent_current_day_stats_are_not_a_trusted_zero_spend() {
        let fixture = Fixture::new();
        let mut observer = fixture.observer(None);
        let previous_only = serde_json::json!([{
            "advertId": 39_682_633,
            "stats": [{
                "date": "2026-08-24",
                "nm_id": 449_627_598_u64,
                "views": 10,
                "clicks": 1,
                "sum": 1,
                "orders": 0,
                "sumPrice": 0
            }]
        }]);
        let (base_url, _) = mock_http(vec![
            (200, campaign_response().to_string()),
            (200, minimum_bids_response().to_string()),
            (200, serde_json::json!({"total": 1_000}).to_string()),
            (200, previous_only.to_string()),
            (200, stocks_response().to_string()),
        ]);
        install_test_client(&mut observer, &base_url);

        let snapshot = observer
            .observe(
                Utc.with_ymd_and_hms(2026, 8, 25, 12, 0, 0).unwrap(),
                WbAutomationStateView::default(),
            )
            .await
            .unwrap();

        assert_eq!(snapshot.observation.daily_spend_minor, 0);
        assert!(!snapshot.observation.daily_spend_complete);
        assert_eq!(
            snapshot.decision.action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::SpendDataIncomplete,
            }
        );
    }
}
