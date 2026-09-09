"""Exercise local artifact publication with a disposable Git repo and fake Docker."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest


PROJECT = Path(__file__).resolve().parents[1]
DOCKER = r'''
import json, os, pathlib, sys
a = sys.argv[1:]
with open(os.environ["DOCKER_TEST_LOG"], "a", encoding="utf-8") as log:
    log.write(json.dumps(a) + "\n")
image_id = "sha256:" + "a" * 64
if a[0] == "info":
    print("aarch64")
elif a[0] == "build":
    context = pathlib.Path(a[-1])
    assert not (context / ".env").exists(), "ignored secret entered build context"
    assert (context / "Cargo.toml").is_file()
    assert "--push" not in a
elif a[:2] == ["image", "inspect"]:
    print(image_id)
elif a[0] == "create":
    assert a[-1] == image_id
    print("b" * 64)
elif a[0] == "cp":
    destination = pathlib.Path(a[-1])
    destination.mkdir()
    binary = destination / "mcp-ozon"
    binary.write_bytes(b"fixture executable\n")
    binary.chmod(0o755)
elif a[0] == "rm":
    assert a[-1] == "b" * 64
elif a[0] == "run":
    assert image_id in a
    assert a[a.index("--network") + 1] == "none"
    assert "--read-only" in a and "--env" not in a and "--volume" not in a
    assert a[a.index("--cap-drop") + 1] == "ALL"
    assert a[-1] == "--version"
    print("wrong version" if os.environ.get("BAD_PROBE") else "mcp-ozon 0.2.1")
else:
    raise AssertionError(a)
'''


class LocalArtifactTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "repo"
        self.root.mkdir()
        for directory in ("scripts", "docs", "src"):
            (self.root / directory).mkdir()
        shutil.copyfile(PROJECT / "scripts/build-local-artifact.sh",
                        self.root / "scripts/build-local-artifact.sh")
        (self.root / "docs/local-artifacts.md").write_text("Local fixture artifact\n", encoding="utf-8")
        (self.root / ".gitignore").write_text("target/\n.env\n", encoding="utf-8")
        (self.root / "Cargo.toml").write_text(
            '[package]\nname = "mcp-ozon"\nversion = "0.2.1"\n'
            'edition = "2024"\nrust-version = "1.98.0"\n', encoding="utf-8")
        (self.root / "Cargo.lock").write_text(
            'version = 4\n[[package]]\nname = "mcp-ozon"\nversion = "0.2.1"\n', encoding="utf-8")
        (self.root / "src/main.rs").write_text("fn main() {}\n", encoding="utf-8")
        self.git("init", "--quiet")
        self.git("config", "user.name", "Artifact Test")
        self.git("config", "user.email", "artifact-test@example.invalid")
        self.git("add", ".")
        self.git("-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "fixture")
        self.sha = self.git("rev-parse", "HEAD").strip()
        (self.root / ".env").write_text("PRIVATE_FIXTURE=not-for-artifact\n", encoding="utf-8")
        tool_dir = Path(self.temporary.name) / "tools"
        tool_dir.mkdir()
        docker = tool_dir / "docker"
        docker.write_text(f"#!{sys.executable}\n" + DOCKER, encoding="utf-8")
        docker.chmod(0o755)
        self.log = Path(self.temporary.name) / "docker.jsonl"
        self.env = dict(os.environ, PATH=f"{tool_dir}{os.pathsep}{os.environ['PATH']}",
                        DOCKER_TEST_LOG=str(self.log))
        self.archive = self.root / f"target/local-artifacts/mcp-ozon-{self.sha}-linux-arm64.tar.gz"

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root, text=True, stderr=subprocess.PIPE)

    def build(self, **extra_env):
        return subprocess.run(["bash", "scripts/build-local-artifact.sh"], cwd=self.root,
                              env=dict(self.env, **extra_env), text=True, capture_output=True, check=False)

    def test_archive_has_matching_commit_binary_and_outer_hashes(self):
        result = self.build()
        self.assertEqual(result.returncode, 0, result.stderr)
        with tarfile.open(self.archive) as archive:
            manifest = json.load(archive.extractfile("./manifest.json"))
            binary = archive.extractfile("./bin/mcp-ozon").read()
            self.assertEqual(archive.getmember("./bin/mcp-ozon").mode, 0o755)
            self.assertEqual(archive.getmember("./bin").mode, 0o755)
            self.assertEqual(archive.getmember("./manifest.json").mode, 0o644)
            self.assertFalse(any(".env" in name for name in archive.getnames()))
        self.assertEqual(manifest["git_sha"], self.sha)
        self.assertEqual(manifest["source_tree"], self.git("rev-parse", f"{self.sha}^{{tree}}").strip())
        self.assertFalse(manifest["production_release"])
        self.assertEqual(manifest["binaries"][0]["sha256"], hashlib.sha256(binary).hexdigest())
        outer_hash = Path(str(self.archive) + ".sha256").read_text().split()[0]
        self.assertEqual(outer_hash, hashlib.sha256(self.archive.read_bytes()).hexdigest())
        self.assertIn('["rm",', self.log.read_text())

    def test_dirty_source_fails_before_docker(self):
        (self.root / "src/main.rs").write_text("fn changed() {}\n", encoding="utf-8")
        result = self.build()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("clean committed checkout", result.stderr)
        self.assertFalse(self.log.exists())

    def test_failed_version_probe_cannot_publish_an_archive(self):
        result = self.build(BAD_PROBE="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("version probe failed", result.stderr)
        self.assertFalse(self.archive.exists())

    def test_existing_artifact_is_never_overwritten(self):
        self.archive.parent.mkdir(parents=True)
        self.archive.write_bytes(b"previous artifact")
        result = self.build()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.archive.read_bytes(), b"previous artifact")
        self.assertIn("refusing to overwrite", result.stderr)


if __name__ == "__main__":
    unittest.main()
