#!/usr/bin/env python3
"""Offline encrypted Mac recovery inputs; never installs, executes, or transfers them.

create --manifest INPUTS.json --recipients RECIPIENTS --output NEW.tar.age
verify --bundle BUNDLE.tar.age --identity INDEPENDENT_IDENTITY
extract --bundle BUNDLE.tar.age --identity INDEPENDENT_IDENTITY --output-dir NEW
schema prints the input contract and fixed slots. No input discovery is performed.
"""

import argparse
from contextlib import contextmanager
import datetime as dt
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import uuid

REQUIRED = frozenset({
    "server_env", "position_env", "access_registry", "tunnel_profile",
    "tunnel_runtime_key", "backup_recipients", "runtime_plist", "backup_plist",
    "health_plist", "restore_verify_plist", "ensure_runtime_script", "backup_script",
    "restore_verify_script", "health_script", "reporting_health_contract",
    "reporting_health_sql", "server_compose", "reporting_reader_compose",
    "release_evidence", "release_images",
})
OPTIONAL = frozenset({
    "release_tested_pr", "notify_hook", "notify_config", "offsite_hook",
    "offsite_config", "reporting_policy", "reporting_registry",
    "operations_notify_script", "operations_heartbeat_script", "heartbeat_hook",
    "heartbeat_config", "data_backup_manifest",
})
PRIVATE = frozenset({
    "server_env", "position_env", "tunnel_profile", "tunnel_runtime_key",
    "backup_recipients", "notify_config", "offsite_config", "heartbeat_config",
})
REGISTRIES = frozenset({"access_registry", "reporting_registry"})
SLOTS = REQUIRED | OPTIONAL
MAX_FILE = 8 * 1024 * 1024
MAX_TOTAL = 32 * 1024 * 1024
MAX_ENVELOPE = 40 * 1024 * 1024
MAX_MANIFEST = 256 * 1024
IMAGE = re.compile(r"ghcr\.io/evil52/mcp-rust-runtime@sha256:[0-9a-f]{64}\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")
HASH = re.compile(r"[0-9a-f]{64}\Z")


class Refused(Exception):
    """Only fixed diagnostic text and allowlisted slot names may reach output."""


def require(condition, message):
    if not condition:
        raise Refused(message)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key")
        result[key] = value
    return result


def decode_json(data):
    return json.loads(data, object_pairs_hook=unique_object)


def json_bytes(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def parts(path):
    require(isinstance(path, str) and path.startswith("/") and "\0" not in path,
            "paths must be absolute")
    components = path.split("/")[1:]
    require(components and all(p not in ("", ".", "..") for p in components),
            "empty, dot and traversal path components are forbidden")
    return components


@contextmanager
def parent_fd(path):
    """Walk every component with openat/O_NOFOLLOW, including parent directories."""
    components = parts(path)
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    private_ancestor = False
    try:
        for name in components[:-1]:
            new_fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = new_fd
            info = os.fstat(fd)
            private_ancestor |= info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o700
        yield fd, components[-1], private_ancestor
    finally:
        os.close(fd)


@contextmanager
def input_fd(path, slot=None, limit=MAX_FILE):
    with parent_fd(path) as (directory, name, private_ancestor):
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid(),
                "input must be a regular file owned by the current user")
        mode = stat.S_IMODE(info.st_mode)
        if slot is None or slot in PRIVATE:
            require(mode == 0o600, "private input must have mode 600")
        elif slot in REGISTRIES:
            require(mode == 0o600 or (mode == 0o644 and private_ancestor),
                    "registry needs mode 600 or mode 644 beneath an owned mode-700 directory")
        else:
            require(mode in (0o600, 0o644, 0o700, 0o755), "unsafe metadata/script mode")
        require(0 < info.st_size <= limit, "input size is outside the bounded contract")
        yield fd, info
    finally:
        os.close(fd)


def read_input(path, slot=None, limit=MAX_FILE):
    with input_fd(path, slot, limit) as (fd, before):
        chunks = []
        remaining = limit + 1
        while remaining:
            data = os.read(fd, min(remaining, 65536))
            if not data:
                break
            chunks.append(data)
            remaining -= len(data)
        after = os.fstat(fd)
    content = b"".join(chunks)
    signature = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns, s.st_mode)
    require(len(content) == before.st_size and len(content) <= limit
            and signature(before) == signature(after), "input changed during capture")
    return content, stat.S_IMODE(before.st_mode)


def reject_identity(content):
    # Identity material has no slot. Catch renamed native/SSH/PKCS private keys
    # and age-encrypted identity files as well, without echoing matched bytes.
    require(b"AGE-SECRET-KEY-" not in content and b"AGE-PLUGIN-" not in content
            and not re.search(rb"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----", content)
            and b"age-encryption.org/v1" not in content
            and b"-----BEGIN AGE ENCRYPTED FILE-----" not in content,
            "private key or encrypted age identity/archive cannot be bundled")


def validate_contract(record):
    require(isinstance(record, dict)
            and set(record) == {"schema_version", "escrow", "runtime", "files"}
            and type(record["schema_version"]) is int and record["schema_version"] == 1,
            "invalid input manifest contract")
    escrow = record["escrow"]
    require(isinstance(escrow, dict) and set(escrow) == {"status", "reference"}
            and escrow["status"] in ("pending", "operator_confirmed"), "invalid escrow status")
    require((escrow["status"] == "pending" and escrow["reference"] is None)
            or (escrow["status"] == "operator_confirmed" and isinstance(escrow["reference"], str)
                and 1 <= len(escrow["reference"]) <= 256
                and not any(ord(c) < 32 for c in escrow["reference"])), "invalid escrow reference")
    runtime = record["runtime"]
    require(isinstance(runtime, dict) and set(runtime) == {"git_sha", "server_image"}
            and isinstance(runtime["git_sha"], str) and SHA.fullmatch(runtime["git_sha"])
            and isinstance(runtime["server_image"], str) and IMAGE.fullmatch(runtime["server_image"]),
            "runtime needs exact release SHA and immutable server reference")
    files = record["files"]
    require(isinstance(files, dict) and REQUIRED <= set(files) <= SLOTS,
            "required slot missing or unexpected recovery input")
    for pair in ({"reporting_policy", "reporting_registry"},
                 {"operations_notify_script", "operations_heartbeat_script"}):
        require(not (set(files) & pair) or pair <= set(files), "paired recovery inputs must appear together")
    for path in files.values():
        parts(path)


def validate_release(record, contents):
    evidence = decode_json(contents["release_evidence"])
    lock_bytes = contents["release_images"]
    lock = decode_json(lock_bytes)
    require(isinstance(evidence, dict) and evidence.get("schema_version") == 2
            and evidence.get("workflow_path") == ".github/workflows/release.yml"
            and evidence.get("git_sha") == record["runtime"]["git_sha"]
            and evidence.get("repository") == "Evil52/MCP_RUST"
            and isinstance(evidence.get("source_tree"), str) and SHA.fullmatch(evidence["source_tree"])
            and type(evidence.get("run_id")) is int and evidence["run_id"] > 0
            and evidence.get("image_lock_sha256") == sha256(lock_bytes),
            "release evidence or image-lock hash mismatch")
    require(isinstance(lock, dict) and lock.get("schema_version") == 1
            and lock.get("git_sha") == evidence["git_sha"]
            and lock.get("repository") == evidence["repository"]
            and lock.get("images", {}).get("server", {}).get("reference") == record["runtime"]["server_image"],
            "runtime image is not bound to the packaged release")
    if "data_backup_manifest" in contents:
        backup = decode_json(contents["data_backup_manifest"])
        require(isinstance(backup, dict) and backup.get("manifest_version") == 2
                and backup.get("capture_order") == ["position-db", "report-artifacts"]
                and isinstance(backup.get("encryption"), dict)
                and backup["encryption"].get("format") == "age"
                and backup["encryption"].get("specification") == "v1"
                and isinstance(backup.get("archives"), dict)
                and set(backup["archives"]) == {"position-db.dump.age", "report-artifacts.tar.age"},
                "data backup manifest must describe the authenticated age backup pair")
        for item in backup["archives"].values():
            require(isinstance(item, dict) and isinstance(item.get("sha256"), str)
                    and HASH.fullmatch(item["sha256"]) and type(item.get("bytes")) is int
                    and item["bytes"] > 0, "data backup archive identity is invalid")


def canonical_tar(manifest_bytes, contents):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        entries = [("manifest.json", manifest_bytes)] + [("files/" + k, contents[k]) for k in sorted(contents)]
        for name, content in entries:
            member = tarfile.TarInfo(name)
            member.mode = 0o600
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))
    return output.getvalue()


def build_plaintext(record):
    validate_contract(record)
    contents, metadata = {}, {}
    for slot, path in sorted(record["files"].items()):
        content, mode = read_input(path, slot)
        reject_identity(content)
        contents[slot] = content
        metadata[slot] = {"source_path": path, "source_mode": mode,
                          "bytes": len(content), "sha256": sha256(content)}
        require(sum(map(len, contents.values())) <= MAX_TOTAL, "bundle payload exceeds size limit")
    validate_release(record, contents)
    # Detect config edits/atomic replacements across the multi-file capture.
    for slot, path in record["files"].items():
        content, mode = read_input(path, slot)
        require(sha256(content) == metadata[slot]["sha256"] and mode == metadata[slot]["source_mode"],
                "recovery inputs changed during multi-file capture")
    manifest = {"schema_version": 1, "kind": "mcp-ozon-mac-recovery",
                "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "escrow": record["escrow"], "runtime": record["runtime"], "files": metadata}
    manifest_bytes = json_bytes(manifest)
    reject_identity(manifest_bytes)
    return canonical_tar(manifest_bytes, contents), manifest


def parse_plaintext(data):
    require(len(data) <= MAX_ENVELOPE, "decrypted archive exceeds size limit")
    files = {}
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
        for member in archive:
            require(member.name not in files and member.isreg() and not member.pax_headers
                    and member.mode == 0o600 and 0 < member.size <= MAX_FILE,
                    "archive contains an unsafe or duplicate entry")
            require(member.name == "manifest.json" or member.name in {"files/" + x for x in SLOTS},
                    "archive entry is outside the fixed allowlist")
            require(member.name != "manifest.json" or member.size <= MAX_MANIFEST,
                    "archive manifest exceeds size limit")
            file = archive.extractfile(member)
            require(file is not None, "archive regular file is unavailable")
            files[member.name] = file.read()
            require(sum(map(len, files.values())) <= MAX_TOTAL + MAX_MANIFEST, "archive payload exceeds size limit")
    require("manifest.json" in files, "archive manifest missing")
    manifest_bytes = files.pop("manifest.json")
    reject_identity(manifest_bytes)
    manifest = decode_json(manifest_bytes)
    require(isinstance(manifest, dict) and set(manifest) == {"schema_version", "kind", "created_at", "escrow", "runtime", "files"}
            and manifest["kind"] == "mcp-ozon-mac-recovery"
            and isinstance(manifest["created_at"], str), "invalid archive manifest")
    metadata = manifest["files"]
    require(isinstance(metadata, dict) and REQUIRED <= set(metadata) <= SLOTS, "archive slots mismatch")
    for value in metadata.values():
        require(isinstance(value, dict) and set(value) == {"source_path", "source_mode", "bytes", "sha256"},
                "invalid archive file metadata")
    record = {"schema_version": manifest["schema_version"], "escrow": manifest["escrow"],
              "runtime": manifest["runtime"], "files": {k: v["source_path"] for k, v in metadata.items()}}
    validate_contract(record)
    contents = {k.removeprefix("files/"): v for k, v in files.items()}
    require(set(contents) == set(metadata), "archive file set does not match manifest")
    for slot, content in contents.items():
        value = metadata[slot]
        require(type(value["bytes"]) is int and value["bytes"] == len(content)
                and isinstance(value["sha256"], str) and HASH.fullmatch(value["sha256"])
                and value["sha256"] == sha256(content), "archive content hash or size mismatch")
        require(type(value["source_mode"]) is int and value["source_mode"] in (0o600, 0o644, 0o700, 0o755),
                "invalid recorded source mode")
        require(slot not in PRIVATE or value["source_mode"] == 0o600, "private source was not mode 600")
        reject_identity(content)
    validate_release(record, contents)
    require(data == canonical_tar(manifest_bytes, contents), "archive framing is not canonical or has trailing data")
    return manifest, contents


def age_command(arguments, data, pass_fds=()):
    executable = shutil.which("age")
    require(executable is not None, "age is required")
    result = subprocess.run([executable, *arguments], input=data, capture_output=True,
                            timeout=60, check=False, pass_fds=pass_fds,
                            env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "AGE_NO_TTY": "1"})
    require(result.returncode == 0, "age encryption/authentication failed; diagnostics suppressed")
    require(len(result.stdout) <= MAX_ENVELOPE, "age output exceeds size limit")
    return result.stdout


def encrypt(data, recipients_path):
    content, _ = read_input(recipients_path, limit=16384)
    recipients = [line.strip() for line in content.decode("ascii").splitlines()
                  if line.strip() and not line.lstrip().startswith("#")]
    require(1 <= len(recipients) <= 8 and len(set(recipients)) == len(recipients)
            and all(re.fullmatch(r"age1[0-9a-z]{58}", r) for r in recipients),
            "only explicit native age recipients are supported")
    arguments = ["--encrypt"]
    for recipient in recipients:
        arguments += ["--recipient", recipient]
    return age_command(arguments, data)


def decrypt(bundle_path, identity_path):
    ciphertext, _ = read_input(bundle_path, limit=MAX_ENVELOPE)
    with input_fd(identity_path, limit=16384) as (fd, info):
        identity = os.read(fd, info.st_size + 1)
        lines = [line.strip() for line in identity.decode("ascii").splitlines()
                 if line.strip() and not line.lstrip().startswith("#")]
        require(len(lines) == 1 and re.fullmatch(r"AGE-SECRET-KEY-1[0-9A-Z]+", lines[0]),
                "only one native age identity is supported; plugins and SSH identities are forbidden")
        os.lseek(fd, 0, os.SEEK_SET)
        # The securely opened descriptor prevents identity path substitution.
        plaintext = age_command(["--decrypt", "--identity", "/dev/fd/" + str(fd)], ciphertext, (fd,))
    return parse_plaintext(plaintext)


def publish_ciphertext(path, content):
    require(path.endswith(".age"), "encrypted output must end in .age")
    with parent_fd(path) as (directory, name, _):
        temporary = ".recovery-bundle-" + uuid.uuid4().hex
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
        try:
            with os.fdopen(fd, "wb") as file:
                file.write(content)
                file.flush()
                os.fsync(file.fileno())
            # linkat provides atomic no-clobber publication even if destination
            # appears after validation; never replace an existing archive/link.
            os.link(temporary, name, src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False)
            os.fsync(directory)
        finally:
            os.unlink(temporary, dir_fd=directory)


def extract_new(path, manifest, contents):
    # Called only after whole-file age authentication AND complete tar validation.
    # No archive path or source_path is ever passed to extraction/filesystem APIs.
    with parent_fd(path) as (parent, name, _):
        parent_info = os.fstat(parent)
        require(parent_info.st_uid == os.getuid() and stat.S_IMODE(parent_info.st_mode) == 0o700,
                "plaintext extraction requires an owned mode-700 parent directory")
        os.mkdir(name, 0o700, dir_fd=parent)
        directory = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        try:
            opened = os.fstat(directory)
            entry = os.stat(name, dir_fd=parent, follow_symlinks=False)
            require(opened.st_uid == os.getuid() and stat.S_IMODE(opened.st_mode) == 0o700
                    and (entry.st_dev, entry.st_ino) == (opened.st_dev, opened.st_ino),
                    "new plaintext directory identity changed")
            for slot, content in [("manifest.json", json_bytes(manifest)), *sorted(contents.items())]:
                fd = os.open(slot, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
                with os.fdopen(fd, "wb") as file:
                    file.write(content)
                    file.flush()
                    os.fsync(file.fileno())
            os.fsync(directory)
        except BaseException:
            for child in ["manifest.json", *contents]:
                try:
                    os.unlink(child, dir_fd=directory)
                except FileNotFoundError:
                    pass
            os.rmdir(name, dir_fd=parent)
            raise
        finally:
            os.close(directory)


def summary(manifest, operation):
    return {"operation": operation, "file_count": len(manifest["files"]),
            "git_sha": manifest["runtime"]["git_sha"], "escrow_status": manifest["escrow"]["status"],
            "escrow_independently_verified": False, "offsite_transfer_performed": False,
            "release_attestation_verified_online": False, "live_restore_performed": False,
            "data_backup_manifest_included": "data_backup_manifest" in manifest["files"],
            "data_backup_archives_verified": False}


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create")
    create.add_argument("--manifest", required=True)
    create.add_argument("--recipients", required=True)
    create.add_argument("--output", required=True)
    for name in ("verify", "extract"):
        command = commands.add_parser(name)
        command.add_argument("--bundle", required=True)
        command.add_argument("--identity", required=True)
        if name == "extract":
            command.add_argument("--output-dir", required=True)
    commands.add_parser("schema")
    args = parser.parse_args()
    if args.command == "schema":
        print(json.dumps({"input_template": {
            "schema_version": 1, "escrow": {"status": "pending", "reference": None},
            "runtime": {"git_sha": "40 lowercase hex characters", "server_image": "ghcr.io/evil52/mcp-rust-runtime@sha256:64_lowercase_hex"},
            "files": {k: "/absolute/source/path" for k in sorted(REQUIRED)}},
            "optional_slots": sorted(OPTIONAL)}, indent=2))
        return
    if args.command == "create":
        encoded, _ = read_input(args.manifest, limit=MAX_MANIFEST)
        record = decode_json(encoded)
        plaintext, manifest = build_plaintext(record)
        ciphertext = encrypt(plaintext, args.recipients)
        publish_ciphertext(args.output, ciphertext)
        result = summary(manifest, "encrypted")
        result.update(ciphertext_sha256=sha256(ciphertext), ciphertext_bytes=len(ciphertext))
    else:
        manifest, contents = decrypt(args.bundle, args.identity)
        if args.command == "extract":
            extract_new(args.output_dir, manifest, contents)
        result = summary(manifest, "authenticated_and_" + ("extracted" if args.command == "extract" else "verified"))
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (Refused, OSError, ValueError, KeyError, TypeError, tarfile.TarError, subprocess.SubprocessError) as error:
        print("recovery bundle refused: " + (str(error) if isinstance(error, Refused)
                                            else "input, archive or encryption operation failed; details suppressed"), file=sys.stderr)
        sys.exit(1)
