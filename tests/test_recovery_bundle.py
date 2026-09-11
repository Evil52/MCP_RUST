#!/usr/bin/env python3
"""Offline tests with ephemeral age identities; never opens live recovery inputs."""

import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/recovery-bundle.py"
sys.dont_write_bytecode = True
SPEC = importlib.util.spec_from_file_location("recovery_bundle", SCRIPT)
BUNDLE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUNDLE)
SENSITIVE = b"test-sensitive-payload-must-never-reach-output"


@unittest.skipUnless(shutil.which("age") and shutil.which("age-keygen"), "real age tools required")
class RecoveryBundleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="mcp-recovery-bundle-test-", dir="/private/tmp" if sys.platform == "darwin" else "/tmp")
        self.root = Path(self.temporary.name).resolve()
        self.root.chmod(0o700)
        self.inputs = self.root / "inputs"
        self.inputs.mkdir(mode=0o700)
        self.identity = self.root / "identity"
        generated = subprocess.run(["age-keygen", "--output", str(self.identity)], capture_output=True, check=False)
        self.assertEqual(generated.returncode, 0)
        self.identity.chmod(0o600)
        recipient = subprocess.run(["age-keygen", "-y", str(self.identity)], capture_output=True, check=True).stdout
        self.recipients = self.write("recipients", recipient)
        self.record = {"schema_version": 1, "escrow": {"status": "pending", "reference": None},
                       "runtime": {"git_sha": "a" * 40,
                                   "server_image": "ghcr.io/evil52/mcp-rust-runtime@sha256:" + "b" * 64},
                       "files": {}}
        for slot in sorted(BUNDLE.REQUIRED):
            path = self.inputs / slot
            path.write_bytes(SENSITIVE + b"\n" if slot == "server_env" else (slot + " fixture\n").encode())
            path.chmod(0o600)
            self.record["files"][slot] = str(path)
        lock = {"schema_version": 1, "git_sha": "a" * 40, "repository": "Evil52/MCP_RUST",
                "images": {"server": {"reference": self.record["runtime"]["server_image"]}}}
        encoded = BUNDLE.json_bytes(lock)
        Path(self.record["files"]["release_images"]).write_bytes(encoded)
        evidence = {"schema_version": 2, "git_sha": "a" * 40, "source_tree": "c" * 40,
                    "repository": "Evil52/MCP_RUST", "workflow_path": ".github/workflows/release.yml",
                    "run_id": 1, "image_lock_sha256": BUNDLE.sha256(encoded)}
        Path(self.record["files"]["release_evidence"]).write_bytes(BUNDLE.json_bytes(evidence))
        self.manifest = self.write("inputs.json", BUNDLE.json_bytes(self.record))

    def tearDown(self):
        self.temporary.cleanup()

    def write(self, name, data):
        path = self.root / name
        path.write_bytes(data)
        path.chmod(0o600)
        return path

    def cli(self, *args):
        result = subprocess.run([sys.executable, str(SCRIPT), *map(str, args)], capture_output=True, timeout=30, check=False)
        self.assertNotIn(SENSITIVE, result.stdout + result.stderr)
        self.assertNotIn(b"AGE-SECRET-KEY-", result.stdout + result.stderr)
        return result

    def create(self, name="bundle.age"):
        output = self.root / name
        result = self.cli("create", "--manifest", self.manifest, "--recipients", self.recipients, "--output", output)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        return output, json.loads(result.stdout)

    def test_real_age_round_trip_preserves_bytes_and_private_extraction(self):
        Path(self.record["files"]["ensure_runtime_script"]).chmod(0o755)
        Path(self.record["files"]["access_registry"]).chmod(0o644)
        archive, result = self.create()
        self.assertEqual(stat.S_IMODE(archive.stat().st_mode), 0o600)
        self.assertNotIn(SENSITIVE, archive.read_bytes())
        self.assertEqual(result["escrow_status"], "pending")
        self.assertFalse(result["escrow_independently_verified"])
        self.assertFalse(result["offsite_transfer_performed"])
        self.assertFalse(result["release_attestation_verified_online"])
        verified = self.cli("verify", "--bundle", archive, "--identity", self.identity)
        self.assertEqual(verified.returncode, 0, verified.stderr.decode())
        destination = self.root / "restored"
        restored = self.cli("extract", "--bundle", archive, "--identity", self.identity, "--output-dir", destination)
        self.assertEqual(restored.returncode, 0, restored.stderr.decode())
        self.assertEqual(stat.S_IMODE(destination.stat().st_mode), 0o700)
        for slot, source in self.record["files"].items():
            self.assertEqual((destination / slot).read_bytes(), Path(source).read_bytes())
            self.assertEqual(stat.S_IMODE((destination / slot).stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(Path(self.record["files"]["access_registry"]).stat().st_mode), 0o644)
        self.assertEqual(stat.S_IMODE(Path(self.record["files"]["ensure_runtime_script"]).stat().st_mode), 0o755)

    def test_wrong_key_tamper_and_truncation_do_not_release_plaintext(self):
        archive, _ = self.create()
        wrong = self.root / "wrong-identity"
        subprocess.run(["age-keygen", "--output", str(wrong)], capture_output=True, check=True)
        wrong.chmod(0o600)
        original = archive.read_bytes()
        changed = original[:-1] + bytes([original[-1] ^ 1])
        for name, ciphertext, identity in [("wrong", original, wrong), ("tampered", changed, self.identity),
                                           ("truncated", original[:-32], self.identity)]:
            with self.subTest(name=name):
                candidate = self.write(name + ".age", ciphertext)
                destination = self.root / (name + "-output")
                result = self.cli("extract", "--bundle", candidate, "--identity", identity, "--output-dir", destination)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(destination.exists())

    def test_explicit_allowlist_rejects_missing_unknown_relative_and_traversal(self):
        records = []
        missing = copy.deepcopy(self.record)
        del missing["files"]["server_env"]
        records.append(missing)
        unexpected = copy.deepcopy(self.record)
        unexpected["files"]["whole_home"] = str(self.inputs)
        records.append(unexpected)
        for path in ["relative.env", str(self.inputs) + "/../inputs/server_env", str(self.inputs) + "/./server_env"]:
            item = copy.deepcopy(self.record)
            item["files"]["server_env"] = path
            records.append(item)
        for record in records:
            with self.subTest(record_index=records.index(record)), self.assertRaises(BUNDLE.Refused):
                BUNDLE.validate_contract(record)

    def test_symlink_leaf_parent_fifo_and_unsafe_source_modes_fail(self):
        leaf = self.root / "leaf"
        leaf.symlink_to(self.inputs / "server_env")
        parent = self.root / "parent"
        parent.symlink_to(self.inputs, target_is_directory=True)
        fifo = self.root / "fifo"
        os.mkfifo(fifo, 0o600)
        for path in [leaf, parent / "server_env", fifo]:
            with self.subTest(path=path.name), self.assertRaises((BUNDLE.Refused, OSError)):
                BUNDLE.read_input(str(path))
        source = self.inputs / "server_env"
        source.chmod(0o644)
        with self.assertRaises(BUNDLE.Refused):
            BUNDLE.build_plaintext(self.record)
        self.root.chmod(0o755)
        self.inputs.chmod(0o755)
        source.chmod(0o644)
        with self.assertRaises(BUNDLE.Refused):
            BUNDLE.read_input(str(source), "access_registry")

    def test_private_age_identity_cannot_hide_under_a_permitted_slot(self):
        source = self.inputs / "server_env"
        for content in [self.identity.read_bytes(), b"-----BEGIN OPENSSH " + b"PRIVATE KEY-----\nfixture",
                        b"age-encryption.org/v1\nfixture", b"AGE-PLUGIN-TEST fixture"]:
            source.write_bytes(content)
            with self.subTest(kind=content[:8]), self.assertRaises(BUNDLE.Refused):
                BUNDLE.build_plaintext(self.record)
        source.write_bytes(SENSITIVE)
        self.record["escrow"] = {"status": "operator_confirmed", "reference": "AGE-SECRET-KEY-1TEST"}
        with self.assertRaises(BUNDLE.Refused):
            BUNDLE.build_plaintext(self.record)

    def test_release_hash_or_server_reference_drift_fails(self):
        self.record["runtime"]["server_image"] = "ghcr.io/evil52/mcp-rust-runtime@sha256:" + "d" * 64
        with self.assertRaises(BUNDLE.Refused):
            BUNDLE.build_plaintext(self.record)
        self.record["runtime"]["server_image"] = "ghcr.io/evil52/mcp-rust-runtime@sha256:" + "b" * 64
        with (self.inputs / "release_images").open("ab") as file:
            file.write(b" ")
        with self.assertRaises(BUNDLE.Refused):
            BUNDLE.build_plaintext(self.record)

    def test_duplicate_json_and_input_limits_fail_without_diagnostics_leak(self):
        self.manifest.write_bytes(b'{"secret":"' + SENSITIVE + b'","secret":1}')
        result = self.cli("create", "--manifest", self.manifest, "--recipients", self.recipients,
                          "--output", self.root / "no.age")
        self.assertNotEqual(result.returncode, 0)
        source = self.inputs / "server_env"
        with source.open("wb") as file:
            file.truncate(BUNDLE.MAX_FILE + 1)
        with self.assertRaises(BUNDLE.Refused):
            BUNDLE.build_plaintext(self.record)

    def test_no_clobber_existing_bundle_symlink_or_extraction_destination(self):
        archive, _ = self.create()
        original = archive.read_bytes()
        with self.assertRaises(FileExistsError):
            BUNDLE.publish_ciphertext(str(archive), b"replacement")
        self.assertEqual(archive.read_bytes(), original)
        link = self.root / "link.age"
        link.symlink_to(archive)
        with self.assertRaises(FileExistsError):
            BUNDLE.publish_ciphertext(str(link), b"replacement")
        self.assertEqual(archive.read_bytes(), original)
        self.assertFalse(list(self.root.glob(".recovery-bundle-*")))
        destination = self.root / "existing"
        destination.mkdir()
        sentinel = destination / "sentinel"
        sentinel.write_bytes(SENSITIVE)
        result = self.cli("extract", "--bundle", archive, "--identity", self.identity, "--output-dir", destination)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(sentinel.read_bytes(), SENSITIVE)
        self.assertEqual(list(destination.iterdir()), [sentinel])
        unsafe_parent = self.root / "unsafe-parent"
        unsafe_parent.mkdir(mode=0o777)
        unsafe_parent.chmod(0o777)
        manifest, contents = BUNDLE.decrypt(str(archive), str(self.identity))
        with self.assertRaises(BUNDLE.Refused):
            BUNDLE.extract_new(str(unsafe_parent / "new-output"), manifest, contents)
        self.assertFalse((unsafe_parent / "new-output").exists())

    def test_authenticated_malicious_tar_is_rejected_before_extraction(self):
        plaintext, _ = BUNDLE.build_plaintext(self.record)
        for kind, name in [(tarfile.SYMTYPE, "files/server_env"), (tarfile.LNKTYPE, "files/server_env"),
                           (tarfile.REGTYPE, "../escape"), (tarfile.REGTYPE, "/absolute"),
                           (tarfile.REGTYPE, "manifest.json")]:
            output = io.BytesIO()
            with tarfile.open(fileobj=io.BytesIO(plaintext), mode="r:") as original:
                with tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as modified:
                    for member in original:
                        modified.addfile(member, original.extractfile(member))
                    extra = tarfile.TarInfo(name)
                    extra.mode = 0o600
                    extra.type = kind
                    extra.linkname = "../../escape" if kind != tarfile.REGTYPE else ""
                    extra.size = 1 if kind == tarfile.REGTYPE else 0
                    modified.addfile(extra, io.BytesIO(b"x") if extra.size else None)
            candidate = self.write("malicious.age", BUNDLE.encrypt(output.getvalue(), str(self.recipients)))
            destination = self.root / "malicious-output"
            result = self.cli("extract", "--bundle", candidate, "--identity", self.identity, "--output-dir", destination)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(destination.exists())
        with self.assertRaises(BUNDLE.Refused):
            BUNDLE.parse_plaintext(plaintext + b"hidden trailing payload")

    def test_payload_hash_corruption_and_capture_drift_fail(self):
        plaintext, manifest = BUNDLE.build_plaintext(self.record)
        _, contents = BUNDLE.parse_plaintext(plaintext)
        contents["server_env"] += b"corrupt"
        with self.assertRaises(BUNDLE.Refused):
            BUNDLE.parse_plaintext(BUNDLE.canonical_tar(BUNDLE.json_bytes(manifest), contents))
        original_read = BUNDLE.read_input
        visits = {}

        def changed(path, slot=None, limit=BUNDLE.MAX_FILE):
            data, mode = original_read(path, slot, limit)
            visits[path] = visits.get(path, 0) + 1
            return (data + b"changed" if slot == "server_env" and visits[path] > 1 else data), mode

        with mock.patch.object(BUNDLE, "read_input", side_effect=changed), self.assertRaises(BUNDLE.Refused):
            BUNDLE.build_plaintext(self.record)

    def test_native_keys_only_and_optional_pairs_are_explicit(self):
        plugin = self.write("plugin-recipient", b"age1plugin1test\n")
        with self.assertRaises(BUNDLE.Refused):
            BUNDLE.encrypt(b"data", str(plugin))
        record = copy.deepcopy(self.record)
        record["files"]["operations_notify_script"] = str(self.inputs / "health_script")
        with self.assertRaises(BUNDLE.Refused):
            BUNDLE.validate_contract(record)
        record = copy.deepcopy(self.record)
        record["escrow"] = {"status": "operator_confirmed", "reference": "independent-vault-reference"}
        plaintext, manifest = BUNDLE.build_plaintext(record)
        self.assertEqual(BUNDLE.parse_plaintext(plaintext)[0], manifest)
        self.assertFalse(BUNDLE.summary(manifest, "verified")["escrow_independently_verified"])

    def test_data_manifest_binds_exact_archive_hashes_without_claiming_data_verification(self):
        backup = {"manifest_version": 2, "capture_order": ["position-db", "report-artifacts"],
                  "encryption": {"format": "age", "specification": "v1"},
                  "archives": {"position-db.dump.age": {"sha256": "d" * 64, "bytes": 1024},
                               "report-artifacts.tar.age": {"sha256": "e" * 64, "bytes": 2048}}}
        file = self.write("data-backup-manifest.json", BUNDLE.json_bytes(backup))
        self.record["files"]["data_backup_manifest"] = str(file)
        plaintext, manifest = BUNDLE.build_plaintext(self.record)
        parsed, contents = BUNDLE.parse_plaintext(plaintext)
        self.assertEqual(BUNDLE.decode_json(contents["data_backup_manifest"]), backup)
        self.assertEqual(parsed, manifest)
        summary = BUNDLE.summary(parsed, "verified")
        self.assertTrue(summary["data_backup_manifest_included"])
        self.assertFalse(summary["data_backup_archives_verified"])
        backup["archives"]["position-db.dump.age"]["sha256"] = "invalid"
        file.write_bytes(BUNDLE.json_bytes(backup))
        with self.assertRaises(BUNDLE.Refused):
            BUNDLE.build_plaintext(self.record)

    def test_guard_data_manifest_requires_the_complete_consistent_v3_set(self):
        backup = {"manifest_version": 3,
                  "capture_order": ["position-db", "ozon-guard-state", "report-artifacts"],
                  "guard_state": {"file": "state.json", "consistency": "exclusive-state-lease",
                                  "root_mode": "700", "uid": 10001, "gid": 10001},
                  "encryption": {"format": "age", "specification": "v1"},
                  "archives": {name: {"sha256": "d" * 64, "bytes": 1024} for name in (
                      "position-db.dump.age", "ozon-guard-state.tar.age", "report-artifacts.tar.age")}}
        file = self.write("data-backup-manifest.json", BUNDLE.json_bytes(backup))
        self.record["files"]["data_backup_manifest"] = str(file)
        plaintext, manifest = BUNDLE.build_plaintext(self.record)
        parsed, contents = BUNDLE.parse_plaintext(plaintext)
        self.assertEqual(BUNDLE.decode_json(contents["data_backup_manifest"]), backup)
        self.assertEqual(parsed, manifest)
        self.assertFalse(BUNDLE.summary(parsed, "verified")["data_backup_archives_verified"])
        for field in ("guard_state", "capture_order", "archives"):
            invalid = copy.deepcopy(backup)
            if field == "guard_state":
                invalid[field]["consistency"] = "independent-copy"
            elif field == "capture_order":
                invalid[field].remove("ozon-guard-state")
            else:
                del invalid[field]["ozon-guard-state.tar.age"]
            file.write_bytes(BUNDLE.json_bytes(invalid))
            with self.subTest(field=field), self.assertRaises(BUNDLE.Refused):
                BUNDLE.build_plaintext(self.record)


if __name__ == "__main__":
    unittest.main(verbosity=2)
