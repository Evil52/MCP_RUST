//! Package integrity checks, not a substitute for model behavioral evaluation.

use std::{collections::BTreeSet, fs, path::PathBuf};

use serde_json::Value;

fn suite_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("plugins/ozonofk-suite")
}

#[test]
fn package_is_skills_only_with_two_real_workflows_and_resolvable_references() {
    let root = suite_root().canonicalize().unwrap();
    let manifest: Value =
        serde_json::from_slice(&fs::read(root.join(".codex-plugin/plugin.json")).unwrap()).unwrap();
    assert_eq!(manifest["name"], "ozonofk-suite");
    assert_eq!(manifest["skills"], "./skills/");
    for deferred in ["apps", "mcpServers", "hooks"] {
        assert!(manifest.get(deferred).is_none());
    }
    let skills = fs::read_dir(root.join("skills"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        skills,
        [
            "ozon-daily-manager-report".to_owned(),
            "ozonofk-marketplace-analytics".to_owned()
        ]
        .into_iter()
        .collect()
    );

    let mut directories = vec![root.clone()];
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            assert!(!kind.is_symlink(), "package must be self-contained");
            let path = entry.path();
            if kind.is_dir() {
                directories.push(path);
            } else if path.extension().is_some_and(|ext| ext == "md") {
                let body = fs::read_to_string(&path).unwrap();
                for suffix in body.split("](").skip(1) {
                    let target = suffix.split(')').next().unwrap();
                    if target.starts_with("https://") || target.starts_with('#') {
                        continue;
                    }
                    let referenced = path
                        .parent()
                        .unwrap()
                        .join(target)
                        .canonicalize()
                        .unwrap_or_else(|_| {
                            panic!("missing reference {target} in {}", path.display())
                        });
                    assert!(referenced.starts_with(&root), "reference escapes package");
                }
            }
        }
    }
}

#[test]
fn behavioral_case_inventory_is_well_formed_and_has_unique_identifiers() {
    let inventory: Value =
        serde_json::from_slice(&fs::read(suite_root().join("evals/scenarios.json")).unwrap())
            .unwrap();
    assert_eq!(inventory["version"], 1);
    let cases = inventory["cases"].as_array().unwrap();
    let mut ids = BTreeSet::new();
    for case in cases {
        assert!(ids.insert(case["id"].as_str().unwrap()));
        assert!(!case["prompt"].as_str().unwrap().is_empty());
        assert!(case["fixture"].is_object());
        assert!(!case["required"].as_array().unwrap().is_empty());
        assert!(!case["forbidden"].as_array().unwrap().is_empty());
    }
    assert_eq!(ids.len(), 9);
}
