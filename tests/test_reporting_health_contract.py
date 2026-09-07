"""Reporting scope and health integration tests without marketplace traffic."""

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "reporting_health_contract", ROOT / "scripts/reporting-health-contract.py"
)
CONTRACT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONTRACT)


class ReportingHealthContractTests(unittest.TestCase):
    def setUp(self):
        self.policy = {
            "version": 1, "enabled": True, "timezone": "Asia/Yekaterinburg",
            "account_ids": ["ozon_one", "wb_one"],
        }
        self.registry = {"version": 1, "accounts": [
            {"id": "ozon_one", "marketplace": "ozon", "manager_id": "owner"},
            {"id": "wb_one", "marketplace": "wildberries", "manager_id": "owner"},
        ]}

    def test_exact_enabled_scope_and_disabled_policy(self):
        self.assertEqual(CONTRACT.build_scope(self.policy, self.registry), [
            {"account_id": "ozon_one", "marketplace": "ozon"},
            {"account_id": "wb_one", "marketplace": "wildberries"},
        ])
        self.policy["enabled"] = False
        self.assertEqual(CONTRACT.build_scope(self.policy, self.registry), [])

    def test_legacy_scope_keeps_registry_ownership_and_rejects_duplicates(self):
        legacy = {"version": 1, "enabled": True, "timezone": "Asia/Yekaterinburg",
                  "sender_email_env": "SENDER", "audiences": [
                      {"id": "owner", "email_env": "RECIPIENT", "managers": [
                          {"actor_id": "owner", "account_ids": ["ozon_one", "wb_one"]}
                      ]}
                  ]}
        self.assertEqual(CONTRACT.build_scope(legacy, self.registry),
                         CONTRACT.build_scope(self.policy, self.registry))
        legacy["audiences"][0]["managers"][0]["actor_id"] = "someone_else"
        with self.assertRaises(ValueError):
            CONTRACT.build_scope(legacy, self.registry)

    def test_ambiguous_or_unbounded_scope_is_rejected(self):
        for key, value in [("enabled", "false"), ("version", True), ("version", 2),
                           ("timezone", "UTC"), ("account_ids", []),
                           ("account_ids", ["ozon_one", "ozon_one"]),
                           ("account_ids", ["unknown"]),
                           ("account_ids", ["x' ); SELECT 'injected"]),
                           ("audiences", [])]:
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                CONTRACT.build_scope(dict(self.policy, **{key: value}), self.registry)

    def test_private_inputs_are_bounded_and_symlinks_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "policy.json"
            path.write_text('{"enabled":true,"enabled":false}')
            with self.assertRaises(ValueError):
                CONTRACT.load_document(path)
            path.write_bytes(b" " * (CONTRACT.MAX_BYTES + 1))
            with self.assertRaises(ValueError):
                CONTRACT.load_document(path)
            path.write_text("{}")
            link = Path(directory) / "link"
            link.symlink_to(path)
            with self.assertRaises(OSError):
                CONTRACT.load_document(link)

    def test_monitor_only_queries_enabled_scope_and_propagates_reporting_findings(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            backup = root / "backups/20260907T000000Z"
            backup.mkdir(parents=True)
            for name in ("offsite-complete.json", "restore-verified.json"):
                (backup / name).touch()
            for name in ("position.env", "ready"):
                (root / name).touch()
            policy_path, registry_path = root / "policy.json", root / "registry.json"
            registry_path.write_text(json.dumps(self.registry))
            docker = root / "docker"
            docker.write_text('''#!/bin/bash
set -euo pipefail
case "$1" in
 info) exit 0 ;;
 ps) printf 'running|Up (healthy)\\n' ;;
 container) printf 'running|healthy\\n' ;;
 run)
   cat > "$TEST_REPORTING_CAPTURE"
   printf 'cycle_age|0\\n'
   if grep -q 'PREPARE reporting_health' "$TEST_REPORTING_CAPTURE"; then
     printf '%s\\n' "$TEST_REPORTING_ROWS"
   fi ;;
 *) exit 2 ;;
esac
''')
            docker.chmod(0o700)
            env = {key: value for key, value in os.environ.items()
                   if not key.startswith("MCP_")}
            env.update({
                "DOCKER_BIN": str(docker), "MCP_HEALTH_POSITION_ENV": str(root / "position.env"),
                "MCP_BACKUP_DIR": str(root / "backups"),
                "MCP_HEALTH_MCP_READY_URL": (root / "ready").as_uri(),
                "MCP_HEALTH_SKIP_LAUNCH_AGENT_CHECK": "true",
                "TEST_REPORTING_CAPTURE": str(root / "captured.sql"),
                "TEST_REPORTING_ROWS": "reporting|ozon_one|ozon|cutoff_incomplete|missing=finance",
            })
            for mode in ("unset", "disabled", "enabled", "invalid",
                         "required_missing", "required_disabled", "required_enabled"):
                with self.subTest(mode=mode):
                    current = dict(env)
                    if mode.startswith("required_"):
                        current["MCP_HEALTH_REQUIRED_SERVICES"] = "position-db,ozon-egress,report-collector"
                    if mode not in ("unset", "required_missing"):
                        policy_path.write_text(json.dumps(dict(
                            self.policy, enabled=mode not in ("disabled", "required_disabled"))))
                        current.update(MCP_HEALTH_REPORTING_POLICY=str(policy_path),
                                       MCP_HEALTH_REPORTING_REGISTRY=str(registry_path))
                    if mode == "invalid":
                        policy_path.write_text('{"enabled":"private-input-never-echo"}')
                    result = subprocess.run([str(ROOT / "scripts/check-runtime-health.sh")],
                                            env=current, text=True, capture_output=True, check=False)
                    output = result.stdout + result.stderr
                    self.assertNotIn("private-input-never-echo", output)
                    if mode in ("unset", "disabled"):
                        self.assertEqual(result.returncode, 0, output)
                        self.assertNotIn("PREPARE reporting_health", (root / "captured.sql").read_text())
                    else:
                        self.assertEqual(result.returncode, 1, output)
                        self.assertNotIn("health check: clean", output)
                    if mode in ("enabled", "required_enabled"):
                        self.assertIn("daily report collection requires attention", output)
                        self.assertIn("missing=finance", output)
                    if mode in ("required_missing", "required_disabled"):
                        self.assertIn("required report-collector has no enabled reporting health scope", output)


if __name__ == "__main__":
    unittest.main()
