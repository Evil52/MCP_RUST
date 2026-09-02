use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::config::{
    Actor, Marketplace, MarketplaceAccount, OidcIdentity, OzonAccount, OzonPerformanceAccount,
    Role, StoreId, WildberriesAccount,
};

static POLICY_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn registry() -> AccessRegistry {
    AccessRegistry {
        version: 1,
        actors: vec![
            Actor {
                id: "manager".to_owned(),
                name: "Manager".to_owned(),
                role: Role::Manager,
                account_ids: BTreeSet::new(),
                oidc: Some(OidcIdentity {
                    username: Some("manager".to_owned()),
                    ..OidcIdentity::default()
                }),
            },
            Actor {
                id: "approver".to_owned(),
                name: "Approver".to_owned(),
                role: Role::Finance,
                account_ids: ["wb_one".to_owned(), "ozon_one".to_owned()]
                    .into_iter()
                    .collect(),
                oidc: Some(OidcIdentity {
                    username: Some("approver".to_owned()),
                    ..OidcIdentity::default()
                }),
            },
        ],
        accounts: vec![
            MarketplaceAccount {
                id: "ozon_one".to_owned(),
                organization: "Example".to_owned(),
                marketplace: Marketplace::Ozon,
                seller_client_id: "seller".to_owned(),
                manager_id: "manager".to_owned(),
                ozon: Some(OzonAccount {
                    store_id: StoreId::from("store_one"),
                    client_id_env: "UNUSED_CLIENT_ID".to_owned(),
                    api_key_env: "UNUSED_API_KEY".to_owned(),
                    performance: Some(OzonPerformanceAccount {
                        client_id_env: "UNUSED_PERF_ID".to_owned(),
                        client_secret_env: "UNUSED_PERF_SECRET".to_owned(),
                    }),
                }),
                wildberries: None,
            },
            MarketplaceAccount {
                id: "wb_one".to_owned(),
                organization: "WB Example".to_owned(),
                marketplace: Marketplace::Wildberries,
                seller_client_id: "wb-seller".to_owned(),
                manager_id: "manager".to_owned(),
                ozon: None,
                wildberries: Some(WildberriesAccount {
                    api_token_env: "UNUSED_WB_TOKEN".to_owned(),
                    seller_sid: Some("123e4567-e89b-42d3-a456-426614174000".to_owned()),
                }),
            },
        ],
    }
}

fn valid_policy() -> serde_json::Value {
    serde_json::json!({
        "version": 1,
        "revision": 1,
        "mode": "disabled",
        "actors": [{
            "actor_id": "manager",
            "targets": [{
                "account_id": "ozon_one",
                "campaign_id": 42,
                "skus": [1001, 1002],
                "bid_limits": {
                    "min_minor": 100,
                    "max_minor": 5000,
                    "max_delta_percent": 5
                }
            }]
        }]
    })
}

fn valid_wb_policy() -> serde_json::Value {
    let mut value = valid_policy();
    value["mode"] = serde_json::json!("plan_only");
    value["actors"][0]["wb_promotion_bid_targets"] = serde_json::json!([{
        "account_id": "wb_one",
        "seller_sid": "123e4567-e89b-42d3-a456-426614174000",
        "advert_id": 77,
        "nm_ids": [1001],
        "placements": ["search"],
        "bid_limits_kopecks": {
            "min_minor": 500,
            "max_minor": 5000,
            "max_delta_percent": 10
        },
        "approver_actor_ids": ["approver"],
        "action_limits": {
            "max_actions_per_hour": 4,
            "max_actions_per_day": 12,
            "cooldown_seconds": 900,
            "max_cumulative_abs_delta_kopecks_per_day": 5000
        }
    }]);
    value
}

fn valid_ozon_launch_policy() -> serde_json::Value {
    let mut value = valid_policy();
    value["mode"] = serde_json::json!("plan_only");
    value["actors"][0]["ozon_campaign_launch_targets"] = serde_json::json!([{
        "account_id": "ozon_one",
        "skus": [
            3_457_585_933_u64,
            3_624_640_796_u64,
            3_625_930_192_u64,
            2_978_114_773_u64,
            3_026_611_133_u64
        ],
        "weekly_budget_microrubles": 10_000_000_000_u64,
        "per_sku_spend_cap_microrubles": 2_000_000_000_u64,
        "initial_cpc_bid_microrubles": 7_000_000_u64,
        "max_cpc_bid_microrubles": 12_000_000_u64,
        "target_drr_percent": 15,
        "target_position": 10,
        "approver_actor_ids": ["approver"]
    }]);
    value
}

fn parse(value: &serde_json::Value) -> Result<ControlPolicy> {
    parse_with_registry(value, &registry())
}

fn parse_with_registry(
    value: &serde_json::Value,
    registry: &AccessRegistry,
) -> Result<ControlPolicy> {
    ControlPolicy::from_slice(
        &serde_json::to_vec(value).expect("test policy serializes"),
        Path::new("test-control-policy.json"),
        registry,
    )
}

#[test]
fn valid_disabled_policy_is_accepted() {
    let policy = parse(&valid_policy()).expect("valid policy");
    assert_eq!(policy.mode, ControlMode::Disabled);
    assert_eq!(policy.revision, 1);
    assert_eq!(policy.digest().len(), 64);
    assert_eq!(policy.actor_policy("manager").unwrap().targets.len(), 1);
    assert!(policy.actor_policy("absent").is_none());
}

#[test]
fn policy_revision_is_required_and_digest_binds_exact_document() {
    let mut missing_revision = valid_policy();
    missing_revision.as_object_mut().unwrap().remove("revision");
    assert!(parse(&missing_revision).is_err());

    let mut zero_revision = valid_policy();
    zero_revision["revision"] = serde_json::json!(0);
    assert!(parse(&zero_revision).is_err());

    let first = parse(&valid_policy()).unwrap();
    let mut changed = valid_policy();
    changed["revision"] = serde_json::json!(2);
    let second = parse(&changed).unwrap();
    assert_ne!(first.digest(), second.digest());
}

#[test]
fn wb_bid_scope_is_explicit_and_bounded() {
    let mut value = valid_policy();
    value["mode"] = serde_json::json!("plan_only");
    value["actors"][0]["wb_promotion_bid_targets"] = serde_json::json!([{
        "account_id": "wb_one",
        "seller_sid": "123e4567-e89b-42d3-a456-426614174000",
        "advert_id": 77,
        "nm_ids": [1001, 1002],
        "placements": ["search", "recommendations"],
        "bid_limits_kopecks": {
            "min_minor": 500,
            "max_minor": 5000,
            "max_delta_percent": 10
        },
        "approver_actor_ids": ["approver"],
        "action_limits": {
            "max_actions_per_hour": 4,
            "max_actions_per_day": 12,
            "cooldown_seconds": 900,
            "max_cumulative_abs_delta_kopecks_per_day": 5000
        }
    }]);
    let policy = parse(&value).expect("valid WB policy");
    assert_eq!(policy.mode, ControlMode::PlanOnly);
    assert_eq!(
        policy
            .actor_policy("manager")
            .unwrap()
            .wb_promotion_bid_targets[0]
            .advert_id,
        77
    );

    let mut missing_sid = value.clone();
    missing_sid["actors"][0]["wb_promotion_bid_targets"][0]
        .as_object_mut()
        .unwrap()
        .remove("seller_sid");
    assert!(parse(&missing_sid).is_err());

    let mut mismatched_sid = value.clone();
    mismatched_sid["actors"][0]["wb_promotion_bid_targets"][0]["seller_sid"] =
        serde_json::json!("22222222-2222-4222-8222-222222222222");
    assert!(parse(&mismatched_sid).is_err());

    let mut rebound_registry = registry();
    rebound_registry.accounts[1]
        .wildberries
        .as_mut()
        .unwrap()
        .seller_sid = Some("22222222-2222-4222-8222-222222222222".to_owned());
    assert!(parse_with_registry(&value, &rebound_registry).is_err());

    value["actors"][0]["wb_promotion_bid_targets"][0]["nm_ids"] = serde_json::json!([1001, 1001]);
    assert!(parse(&value).is_err());
}

#[test]
fn wb_bid_scope_requires_distinct_authorized_approver_and_bounded_actions() {
    let mut value = valid_policy();
    value["mode"] = serde_json::json!("plan_only");
    value["actors"][0]["wb_promotion_bid_targets"] = serde_json::json!([{
        "account_id": "wb_one",
        "seller_sid": "123e4567-e89b-42d3-a456-426614174000",
        "advert_id": 77,
        "nm_ids": [1001],
        "placements": ["search"],
        "bid_limits_kopecks": {
            "min_minor": 500,
            "max_minor": 5000,
            "max_delta_percent": 10
        },
        "approver_actor_ids": ["manager"],
        "action_limits": {
            "max_actions_per_hour": 4,
            "max_actions_per_day": 12,
            "cooldown_seconds": 900,
            "max_cumulative_abs_delta_kopecks_per_day": 5000
        }
    }]);
    assert!(parse(&value).is_err());

    value["actors"][0]["wb_promotion_bid_targets"][0]["approver_actor_ids"] =
        serde_json::json!(["approver"]);
    value["actors"][0]["wb_promotion_bid_targets"][0]["action_limits"]["max_actions_per_day"] =
        serde_json::json!(2);
    assert!(parse(&value).is_err());
}

#[test]
fn ozon_launch_scope_binds_exact_budget_skus_and_distinct_approver() {
    let value = valid_ozon_launch_policy();
    let policy = parse(&value).expect("valid Ozon launch policy");
    let target = &policy
        .actor_policy("manager")
        .unwrap()
        .ozon_campaign_launch_targets[0];
    assert_eq!(target.weekly_budget_microrubles, 10_000_000_000);
    assert_eq!(target.per_sku_spend_cap_microrubles, 2_000_000_000);
    assert_eq!(target.initial_cpc_bid_microrubles, 7_000_000);
    assert_eq!(target.max_cpc_bid_microrubles, 12_000_000);
    assert_eq!(target.target_drr_percent, 15);
    assert_eq!(target.target_position, 10);

    let mut wrong_total = value.clone();
    wrong_total["actors"][0]["ozon_campaign_launch_targets"][0]["weekly_budget_microrubles"] =
        serde_json::json!(9_999_000_000_u64);
    assert!(parse(&wrong_total).is_err());

    let mut duplicate_sku = value.clone();
    duplicate_sku["actors"][0]["ozon_campaign_launch_targets"][0]["skus"] =
        serde_json::json!([3_457_585_933_u64, 3_457_585_933_u64]);
    duplicate_sku["actors"][0]["ozon_campaign_launch_targets"][0]["weekly_budget_microrubles"] =
        serde_json::json!(4_000_000_000_u64);
    assert!(parse(&duplicate_sku).is_err());

    let mut self_approval = value;
    self_approval["actors"][0]["ozon_campaign_launch_targets"][0]["approver_actor_ids"] =
        serde_json::json!(["manager"]);
    assert!(parse(&self_approval).is_err());
}

#[test]
fn policy_rejects_credentials_and_unsafe_or_duplicate_targets() {
    let mut with_secret = valid_policy();
    with_secret["api_token"] = serde_json::json!("must-not-be-accepted");
    assert!(parse(&with_secret).is_err());

    let mut duplicate = valid_policy();
    let target = duplicate["actors"][0]["targets"][0].clone();
    duplicate["actors"][0]["targets"]
        .as_array_mut()
        .unwrap()
        .push(target);
    assert!(parse(&duplicate).is_err());

    let mut duplicate_sku = valid_policy();
    duplicate_sku["actors"][0]["targets"][0]["skus"] = serde_json::json!([1001, 1001]);
    assert!(parse(&duplicate_sku).is_err());
}

#[test]
fn policy_bounds_actor_and_target_collections_and_rejects_duplicate_actors() {
    let actor = serde_json::json!({ "actor_id": "manager", "targets": [] });

    let mut too_many_actors = valid_policy();
    too_many_actors["actors"] =
        serde_json::Value::Array((0..=MAX_ACTORS).map(|_| actor.clone()).collect());
    assert!(parse(&too_many_actors).is_err());

    let mut duplicate_actor = valid_policy();
    let repeated_actor = duplicate_actor["actors"][0].clone();
    duplicate_actor["actors"]
        .as_array_mut()
        .unwrap()
        .push(repeated_actor);
    assert!(parse(&duplicate_actor).is_err());

    let mut too_many_targets = valid_policy();
    let target = too_many_targets["actors"][0]["targets"][0].clone();
    too_many_targets["actors"][0]["targets"] = serde_json::Value::Array(
        (0..=MAX_TARGETS_PER_ACTOR)
            .map(|_| target.clone())
            .collect(),
    );
    assert!(parse(&too_many_targets).is_err());
}

#[test]
fn policy_requires_performance_binding_and_explicit_actor_access() {
    let mut without_performance = registry();
    without_performance.accounts[0]
        .ozon
        .as_mut()
        .unwrap()
        .performance = None;
    assert!(parse_with_registry(&valid_policy(), &without_performance).is_err());

    let mut wrong_marketplace = registry();
    wrong_marketplace.accounts[0].marketplace = Marketplace::Wildberries;
    assert!(parse_with_registry(&valid_policy(), &wrong_marketplace).is_err());

    let mut denied_registry = registry();
    denied_registry.actors.push(Actor {
        id: "viewer".to_owned(),
        name: "Viewer".to_owned(),
        role: Role::Manager,
        account_ids: BTreeSet::new(),
        oidc: None,
    });
    let mut denied_policy = valid_policy();
    denied_policy["actors"][0]["actor_id"] = serde_json::json!("viewer");
    assert!(parse_with_registry(&denied_policy, &denied_registry).is_err());
}

#[test]
fn policy_rejects_invalid_identifiers_and_oversized_files() {
    for invalid_actor in ["", "../manager", &"a".repeat(MAX_IDENTIFIER_BYTES + 1)] {
        let mut value = valid_policy();
        value["actors"][0]["actor_id"] = serde_json::json!(invalid_actor);
        assert!(parse(&value).is_err(), "actor {invalid_actor:?} must fail");
    }

    let mut invalid_account = valid_policy();
    invalid_account["actors"][0]["targets"][0]["account_id"] = serde_json::json!("ozon account");
    assert!(parse(&invalid_account).is_err());

    let id = POLICY_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "mcp-control-oversized-policy-{}-{id}.json",
        std::process::id()
    ));
    let oversized_length =
        usize::try_from(CONTROL_POLICY_MAX_BYTES + 1).expect("control policy limit fits usize");
    std::fs::write(&path, vec![b' '; oversized_length]).unwrap();
    let result = read_policy_bytes(&path);
    std::fs::remove_file(path).unwrap();
    assert!(result.is_err());
}

#[test]
fn policy_file_io_errors_are_reported_without_panicking() {
    let id = POLICY_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let missing = std::env::temp_dir().join(format!(
        "mcp-control-missing-policy-{}-{id}.json",
        std::process::id()
    ));
    assert!(read_policy_bytes(&missing).is_err());

    let directory = std::env::temp_dir().join(format!(
        "mcp-control-policy-directory-{}-{id}",
        std::process::id()
    ));
    std::fs::create_dir(&directory).unwrap();
    let result = read_policy_bytes(&directory);
    std::fs::remove_dir(directory).unwrap();
    assert!(result.is_err());
}

#[test]
fn policy_rejects_invalid_version_actor_account_campaign_and_limits() {
    enum Mutation {
        Version,
        Actor,
        Account,
        Campaign,
        Sku,
        Minimum,
        Range,
        Delta,
    }

    let mutations = [
        ("version", Mutation::Version, serde_json::json!(2)),
        ("actor", Mutation::Actor, serde_json::json!("unknown")),
        ("account", Mutation::Account, serde_json::json!("unknown")),
        ("campaign", Mutation::Campaign, serde_json::json!(0)),
        ("sku", Mutation::Sku, serde_json::json!([])),
        ("min", Mutation::Minimum, serde_json::json!(0)),
        ("range", Mutation::Range, serde_json::json!(50)),
        ("delta", Mutation::Delta, serde_json::json!(0)),
    ];
    for (label, kind, replacement) in mutations {
        let mut value = valid_policy();
        match kind {
            Mutation::Version => value["version"] = replacement,
            Mutation::Actor => value["actors"][0]["actor_id"] = replacement,
            Mutation::Account => value["actors"][0]["targets"][0]["account_id"] = replacement,
            Mutation::Campaign => value["actors"][0]["targets"][0]["campaign_id"] = replacement,
            Mutation::Sku => value["actors"][0]["targets"][0]["skus"] = replacement,
            Mutation::Minimum => {
                value["actors"][0]["targets"][0]["bid_limits"]["min_minor"] = replacement;
            }
            Mutation::Range => {
                value["actors"][0]["targets"][0]["bid_limits"]["max_minor"] = replacement;
            }
            Mutation::Delta => {
                value["actors"][0]["targets"][0]["bid_limits"]["max_delta_percent"] = replacement;
            }
        }
        assert!(parse(&value).is_err(), "mutation {label} must fail");
    }
}

#[test]
fn every_wb_placement_has_the_exact_wire_name() {
    assert_eq!(WbBidPlacement::Combined.as_api_str(), "combined");
    assert_eq!(WbBidPlacement::Search.as_api_str(), "search");
    assert_eq!(
        WbBidPlacement::Recommendations.as_api_str(),
        "recommendations"
    );
}

#[test]
fn wb_policy_rejects_every_unsafe_scope_shape_and_limit() {
    let valid = valid_wb_policy();
    parse(&valid).expect("WB baseline policy");

    let mut too_many_targets = valid.clone();
    let target = too_many_targets["actors"][0]["wb_promotion_bid_targets"][0].clone();
    too_many_targets["actors"][0]["wb_promotion_bid_targets"] = serde_json::Value::Array(
        (0..=MAX_TARGETS_PER_ACTOR)
            .map(|_| target.clone())
            .collect(),
    );
    assert!(parse(&too_many_targets).is_err());

    let mut invalid_advert = valid.clone();
    invalid_advert["actors"][0]["wb_promotion_bid_targets"][0]["advert_id"] =
        serde_json::json!(u64::MAX);
    assert!(parse(&invalid_advert).is_err());

    let mut duplicate_target = valid.clone();
    duplicate_target["actors"][0]["wb_promotion_bid_targets"]
        .as_array_mut()
        .unwrap()
        .push(target);
    assert!(parse(&duplicate_target).is_err());

    let mut unknown_account = valid.clone();
    unknown_account["actors"][0]["wb_promotion_bid_targets"][0]["account_id"] =
        serde_json::json!("missing_wb");
    assert!(parse(&unknown_account).is_err());

    let mut wrong_marketplace = registry();
    wrong_marketplace.accounts[1].marketplace = Marketplace::Ozon;
    assert!(parse_with_registry(&valid, &wrong_marketplace).is_err());

    let mut missing_binding = registry();
    missing_binding.accounts[1].wildberries = None;
    assert!(parse_with_registry(&valid, &missing_binding).is_err());

    let mut missing_registry_sid = registry();
    missing_registry_sid.accounts[1]
        .wildberries
        .as_mut()
        .unwrap()
        .seller_sid = None;
    assert!(parse_with_registry(&valid, &missing_registry_sid).is_err());

    let mut denied_actor = registry();
    denied_actor.accounts[1].manager_id = "approver".to_owned();
    assert!(parse_with_registry(&valid, &denied_actor).is_err());

    let mut empty_nm_ids = valid.clone();
    empty_nm_ids["actors"][0]["wb_promotion_bid_targets"][0]["nm_ids"] = serde_json::json!([]);
    assert!(parse(&empty_nm_ids).is_err());

    let mut empty_placements = valid.clone();
    empty_placements["actors"][0]["wb_promotion_bid_targets"][0]["placements"] =
        serde_json::json!([]);
    assert!(parse(&empty_placements).is_err());

    let mut duplicate_placements = valid.clone();
    duplicate_placements["actors"][0]["wb_promotion_bid_targets"][0]["placements"] =
        serde_json::json!(["search", "search"]);
    assert!(parse(&duplicate_placements).is_err());

    let mut invalid_bid_range = valid.clone();
    invalid_bid_range["actors"][0]["wb_promotion_bid_targets"][0]["bid_limits_kopecks"]["min_minor"] =
        serde_json::json!(0);
    assert!(parse(&invalid_bid_range).is_err());

    let mut invalid_delta = valid.clone();
    invalid_delta["actors"][0]["wb_promotion_bid_targets"][0]["bid_limits_kopecks"]["max_delta_percent"] =
        serde_json::json!(0);
    assert!(parse(&invalid_delta).is_err());

    let mut empty_approvers = valid.clone();
    empty_approvers["actors"][0]["wb_promotion_bid_targets"][0]["approver_actor_ids"] =
        serde_json::json!([]);
    assert!(parse(&empty_approvers).is_err());

    let mut duplicate_approvers = valid.clone();
    duplicate_approvers["actors"][0]["wb_promotion_bid_targets"][0]["approver_actor_ids"] =
        serde_json::json!(["approver", "approver"]);
    assert!(parse(&duplicate_approvers).is_err());

    let mut unknown_approver = valid.clone();
    unknown_approver["actors"][0]["wb_promotion_bid_targets"][0]["approver_actor_ids"] =
        serde_json::json!(["unknown"]);
    assert!(parse(&unknown_approver).is_err());

    let mut denied_approver = registry();
    denied_approver.actors[1].account_ids.clear();
    assert!(parse_with_registry(&valid, &denied_approver).is_err());

    let mut invalid_hourly = valid.clone();
    invalid_hourly["actors"][0]["wb_promotion_bid_targets"][0]["action_limits"]["max_actions_per_hour"] =
        serde_json::json!(0);
    assert!(parse(&invalid_hourly).is_err());

    let mut invalid_cooldown = valid.clone();
    invalid_cooldown["actors"][0]["wb_promotion_bid_targets"][0]["action_limits"]["cooldown_seconds"] =
        serde_json::json!(29);
    assert!(parse(&invalid_cooldown).is_err());

    let mut invalid_cumulative_delta = valid;
    invalid_cumulative_delta["actors"][0]["wb_promotion_bid_targets"][0]["action_limits"]["max_cumulative_abs_delta_kopecks_per_day"] =
        serde_json::json!(0);
    assert!(parse(&invalid_cumulative_delta).is_err());
}

#[test]
fn wb_policy_upper_boundaries_are_inclusive_and_overflow_is_rejected() {
    let mut boundary_registry = registry();
    let mut approver_ids = vec!["approver".to_owned()];
    for index in 1..MAX_APPROVERS_PER_TARGET {
        let actor_id = format!("approver_{index}");
        boundary_registry.actors.push(Actor {
            id: actor_id.clone(),
            name: format!("Approver {index}"),
            role: Role::Finance,
            account_ids: std::iter::once("wb_one".to_owned()).collect(),
            oidc: Some(OidcIdentity {
                username: Some(actor_id.clone()),
                ..OidcIdentity::default()
            }),
        });
        approver_ids.push(actor_id);
    }

    let mut boundary = valid_wb_policy();
    let target = &mut boundary["actors"][0]["wb_promotion_bid_targets"][0];
    target["advert_id"] = serde_json::json!(MAX_WB_SIGNED_ID);
    let mut maximum_nm_ids = (1..MAX_WB_NM_IDS_PER_TARGET as u64).collect::<Vec<_>>();
    maximum_nm_ids.push(MAX_WB_SIGNED_ID);
    target["nm_ids"] = serde_json::json!(maximum_nm_ids);
    target["placements"] = serde_json::json!(["combined", "search", "recommendations"]);
    target["bid_limits_kopecks"]["max_minor"] = serde_json::json!(MAX_WB_SIGNED_ID);
    target["bid_limits_kopecks"]["max_delta_percent"] = serde_json::json!(100);
    target["approver_actor_ids"] = serde_json::json!(approver_ids);
    target["action_limits"] = serde_json::json!({
        "max_actions_per_hour": MAX_ACTIONS_PER_HOUR,
        "max_actions_per_day": MAX_ACTIONS_PER_DAY,
        "cooldown_seconds": 86_400,
        "max_cumulative_abs_delta_kopecks_per_day":
            MAX_CUMULATIVE_ABS_DELTA_KOPECKS_PER_DAY
    });
    parse_with_registry(&boundary, &boundary_registry).expect("inclusive WB upper boundaries");

    let mut zero_advert = valid_wb_policy();
    zero_advert["actors"][0]["wb_promotion_bid_targets"][0]["advert_id"] = serde_json::json!(0);
    assert!(parse(&zero_advert).is_err());

    let mut too_many_nm_ids = valid_wb_policy();
    too_many_nm_ids["actors"][0]["wb_promotion_bid_targets"][0]["nm_ids"] =
        serde_json::json!((1..=MAX_WB_NM_IDS_PER_TARGET as u64 + 1).collect::<Vec<_>>());
    assert!(parse(&too_many_nm_ids).is_err());

    for nm_id in [0, MAX_WB_SIGNED_ID + 1] {
        let mut invalid_nm_id = valid_wb_policy();
        invalid_nm_id["actors"][0]["wb_promotion_bid_targets"][0]["nm_ids"] =
            serde_json::json!([nm_id]);
        assert!(parse(&invalid_nm_id).is_err(), "nm_id {nm_id} must fail");
    }

    let mut reversed_bid_range = valid_wb_policy();
    reversed_bid_range["actors"][0]["wb_promotion_bid_targets"][0]["bid_limits_kopecks"]["max_minor"] =
        serde_json::json!(499);
    assert!(parse(&reversed_bid_range).is_err());

    let mut oversized_bid = valid_wb_policy();
    oversized_bid["actors"][0]["wb_promotion_bid_targets"][0]["bid_limits_kopecks"]["max_minor"] =
        serde_json::json!(MAX_WB_SIGNED_ID + 1);
    assert!(parse(&oversized_bid).is_err());

    let mut excessive_bid_delta = valid_wb_policy();
    excessive_bid_delta["actors"][0]["wb_promotion_bid_targets"][0]["bid_limits_kopecks"]["max_delta_percent"] =
        serde_json::json!(101);
    assert!(parse(&excessive_bid_delta).is_err());

    let mut too_many_approvers = valid_wb_policy();
    too_many_approvers["actors"][0]["wb_promotion_bid_targets"][0]["approver_actor_ids"] =
        serde_json::json!(vec!["approver"; MAX_APPROVERS_PER_TARGET + 1]);
    assert!(parse(&too_many_approvers).is_err());

    let mut invalid_approver_id = valid_wb_policy();
    invalid_approver_id["actors"][0]["wb_promotion_bid_targets"][0]["approver_actor_ids"] =
        serde_json::json!(["../approver"]);
    assert!(parse(&invalid_approver_id).is_err());

    for (field, value) in [
        ("max_actions_per_hour", u64::from(MAX_ACTIONS_PER_HOUR) + 1),
        ("max_actions_per_day", u64::from(MAX_ACTIONS_PER_DAY) + 1),
        ("cooldown_seconds", 86_401),
        (
            "max_cumulative_abs_delta_kopecks_per_day",
            MAX_CUMULATIVE_ABS_DELTA_KOPECKS_PER_DAY + 1,
        ),
    ] {
        let mut invalid_limit = valid_wb_policy();
        invalid_limit["actors"][0]["wb_promotion_bid_targets"][0]["action_limits"][field] =
            serde_json::json!(value);
        assert!(parse(&invalid_limit).is_err(), "limit {field} must fail");
    }
}
