# Offline Mac recovery bundle

`recovery-bundle.py` packages an explicit set of configuration files alongside
the existing database/artifact backup. It does not discover files, read Docker
state, contact a service, install anything, execute archived scripts, or send a
backup offsite. Python 3.10+ and native `age` are required on macOS/Linux.

Create a mode-600 input manifest from `python3 scripts/recovery-bundle.py schema`
(use its `input_template`; `optional_slots` is documentation). Replace every
placeholder with a reviewed absolute path. Keep `escrow.status` as `pending`
and `reference` as `null` until independent identity escrow has been arranged.
An `operator_confirmed` reference records the operator's claim; this tool never
claims to have independently verified it.

```sh
python3 scripts/recovery-bundle.py create \
  --manifest /absolute/private/recovery-inputs.json \
  --recipients /absolute/private/age-recipients.txt \
  --output /absolute/private/backup-directory/recovery.tar.age
python3 scripts/recovery-bundle.py verify \
  --bundle /absolute/private/backup-directory/recovery.tar.age \
  --identity /absolute/independent-escrow/age-identity.txt
python3 scripts/recovery-bundle.py extract \
  --bundle /absolute/private/backup-directory/recovery.tar.age \
  --identity /absolute/independent-escrow/age-identity.txt \
  --output-dir /absolute/private/NEW-recovery-drill
```

The manifest has exactly four fields:

```json
{
  "schema_version": 1,
  "escrow": {"status": "pending", "reference": null},
  "runtime": {
    "git_sha": "0123456789abcdef0123456789abcdef01234567",
    "server_image": "ghcr.io/evil52/mcp-rust-runtime@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  },
  "files": {"server_env": "/absolute/private/server.env"}
}
```

The example omits most required slots for readability. The exact required set is:
`server_env`, `position_env`, `access_registry`, `tunnel_profile`,
`tunnel_runtime_key`, `backup_recipients`, `runtime_plist`, `backup_plist`,
`health_plist`, `restore_verify_plist`, `ensure_runtime_script`, `backup_script`,
`restore_verify_script`, `health_script`, `reporting_health_contract`,
`reporting_health_sql`, `server_compose`, `reporting_reader_compose`,
`release_evidence`, `release_images`.

Optional slots are `release_tested_pr`, `notify_hook`, `notify_config`,
`offsite_hook`, `offsite_config`, `heartbeat_hook`, `heartbeat_config`,
`reporting_policy`, `reporting_registry`, `operations_notify_script`,
`operations_heartbeat_script`, and `data_backup_manifest`. The reporting
policy/registry and operations notify/heartbeat scripts must each appear as a
pair. Include the operations pair when the installed health script uses them.
All hook dependencies must fit these explicit slots; unsupported providers
require a reviewed extension, not a recursive directory inclusion.

Use `data_backup_manifest` for the existing backup directory's `manifest.json`.
Its two age archive hashes are then covered by the authenticated configuration
bundle. It does not read/verify the data archives: run `verify-position-backup.sh`
separately. A future offsite hook should transfer the complete backup directory,
including `recovery.tar.age`, as one unit.

Secret inputs, recipient/identity files, and the input manifest require mode 600.
Scripts and public metadata can retain mode 600/644/700/755. Registries may be
644 only beneath an owned 700 directory, matching the live container mount.
Every path component is opened without following symlinks; nonregular files,
relative paths, traversal, unexpected slots, and duplicate JSON keys are refused.
Inputs are limited to 8 MiB each and 32 MiB in total. Source modes are never changed.

Only native age recipients and one native age decryption identity are supported;
SSH/plugin/passphrase identities are excluded. Private age/SSH/PKCS key markers
and age-encrypted archives are refused in every input, including metadata. The
identity must be escrowed independently of this bundle and of the Mac being
recovered. A local identity-only verification cannot prove offsite recovery.

The tar stream exists only in memory before encryption and after authenticated
decryption. No plaintext archive is written to disk. `extract` authenticates the
entire ciphertext and validates the full canonical allowlisted archive before
creating a new directory. Extracted files use slot names, never their recorded
original paths. The directory is 700 and all files are 600; executable/source
modes remain metadata for a later controlled restore. The extraction parent
must already be owned by the current user with mode 700, preventing directory
substitution by another local user. Existing destinations
are never replaced. Treat the extracted directory as sensitive and remove it
after a drill. The tool does not activate restored plists/scripts or assign any
marketplace write authority.

Release SHA, immutable server reference and packaged release-image-lock hash
must agree. This is an offline consistency check, not an attestation or image
availability check. Keep this nonsecret tool and its documented release outside
the encrypted bundle too, obtainable from a verified Git checkout, so the bundle
is not the only source of its own recovery instructions. Recheck source/image
provenance, tools, paths, credentials, service health and a fresh tunnel poll in
the separate recovery drill before activating a recovered host.

Offline regression tests generate disposable age identities and fixture inputs:

```sh
python3 -B -m unittest discover -s tests -p test_recovery_bundle.py
```
