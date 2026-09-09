# Local Rust build artifacts (no push)

Run `bash scripts/build-local-artifact.sh` from a clean committed checkout.
It uses the existing pinned Docker `builder` stage on the Docker engine's
native Linux ARM64 or AMD64 architecture; it never enables emulation, pushes
images, creates a PR, restarts a service or invokes a marketplace operation.
Requirements: Docker, the locked host Cargo toolchain/cache, Git, jq, tar and
SHA-256 utilities. Allow enough free disk for the release build/cache first.

The input is `git archive HEAD`, not the working directory. Ignored local
credentials, private configuration and generated reports cannot enter it.
The script refuses a dirty tree, missing/unsafe binaries, a failed version
probe, or an existing destination archive. The existing Docker builder compiles
all runtime binaries; the artifact inventory comes from Cargo metadata rather
than a separately maintained list of seven names.

Output: `target/local-artifacts/mcp-ozon-<full-sha>-linux-<arch>.tar.gz` and
its `.sha256` file. The archive contains:

- `bin/`: Linux release binaries (not native macOS executables).
- `manifest.json`: source commit/tree, platform, declared Rust version, local
  builder image ID, per-binary SHA-256 and successful `--version` probe status.
- This README.

Every version probe runs without network, credentials, volumes or elevated
container capabilities. It checks artifact executability/version, not full
application readiness. Build cache, the local builder image and a uniquely
named `.build-*` work directory remain under the local build environment for
inspection/reuse; the temporary extraction container is removed.

Verify the outer archive before extraction with `shasum -a 256 -c <file>.sha256`
(or `sha256sum -c`) from its containing directory. After extraction, compare
each binary hash with its manifest entry. These hashes establish integrity
relative to a trusted manifest/checksum; they are not a publisher signature.

`production_release` is deliberately `false`. This manifest is **not** a
`release.json` or `release-images.json`, contains no fabricated GitHub run ID
or attestation, and must not bypass `verify-release-source.sh`. Deployment
still requires the existing protected CI/release/canary evidence workflow.
The archive is not a loadable Docker image and is not a full deployment bundle.
