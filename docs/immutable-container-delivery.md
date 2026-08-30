# Immutable container delivery

## Status and fixed decisions

The repository implements the complete source-to-runtime identity chain. The
selected registry is the public GitHub Container Registry package
`ghcr.io/evil52/mcp-rust-runtime`. CI publishes only after all required quality,
security, coverage and container jobs pass. Production never builds release
images locally.

The fixed policy is:

- the package must remain `public`; CI checks visibility before and after every
  publish and fails closed if it cannot prove that state;
- CI uses a dedicated classic PAT stored as the repository Actions secret
  `GHCR_PUBLISH_TOKEN`, scoped only to `write:packages`;
- production pulls immutable image digests anonymously and stores no GHCR
  credential;
- public visibility is irreversible in GitHub Packages: image layers,
  configuration and metadata are visible to anyone. Secrets must only enter at
  runtime through protected environment files or bind mounts and must never be
  baked into an image;
- every release image is built for `linux/amd64` and `linux/arm64`, scanned on
  both platforms and receives a GitHub build-provenance attestation;
- discovery tags are mutable and cannot be deployment input; installers accept
  only references of the form `ghcr.io/evil52/mcp-rust-runtime@sha256:...`;
- release evidence and image locks are retained as GitHub Actions artifacts for
  90 days. Rollback inputs that must outlive that window must be copied to the
  protected operator archive before expiry.

The package is deliberately not associated with the source repository by an
OCI source label. Its public visibility is an explicit delivery decision rather
than an inherited repository permission.

## Required release contract

The implementation preserves these invariants:

1. CI builds every shipped image for `linux/amd64` and `linux/arm64` from one
   tested Git SHA.
2. CI scans the exact multi-platform digest it publishes.
3. Publication produces `release-images.json`, mapping every logical image to a
   pullable immutable digest. The schema rejects missing, extra or mutable image
   references.
4. `release.json` binds the Git SHA, source tree, successful workflow run and
   SHA-256 of `release-images.json`.
5. Installers revalidate both files, query GitHub for the successful workflow,
   verify the GitHub provenance attestation, pull the digest, verify
   `org.opencontainers.image.revision`, and invoke Compose with `--no-build`.
6. Canary and production consume the same digest lock. Rollback selects a
   previously retained lock; it never rebuilds an old checkout.
7. Production has anonymous, read-only registry access. CI's publish identity
   is separate and unavailable to the runtime.

The lock contains the exact twelve deployable image identities: `server`,
`control`, `control-ingress`, `control-auth-egress`, `control-write-egress`,
`mail-egress`, `ozon-egress`, `position-db`, `position-collector`,
`report-collector`, `report-worker`, and `wb-automation`.

## Rust build cache topology

The six Rust runtime Dockerfiles intentionally have an identical `builder`
stage. After the required gates pass, `prime-rust-release-cache` compiles every
Rust binary once for each target platform and exports that completed layer to
the `release-rust-binaries` GitHub Actions cache scope with a cache-only output.
The parallel image jobs import that scope, assemble their separate minimal
runtime images, and continue to publish, scan and attest each immutable digest
independently.

The shared scope is written only by the prime job. Each parallel publisher
writes only its image-specific cache scope, so concurrent jobs cannot overwrite
the common cache. No credentials or runtime configuration enter the builder;
its inputs are the tested `Cargo.toml`, `Cargo.lock`, `vendor/`, and `src/`
tree. `scripts/test-shared-rust-image-builder.sh` fails CI if a Dockerfile's
builder stage or expected runtime artifact drifts from this contract.

## One-time GitHub setup

1. Create a classic personal access token for the CI publisher with only
   `write:packages`. Do not grant repository scopes.
2. Store it as the repository Actions secret `GHCR_PUBLISH_TOKEN`.
3. After the first publish, open the package page, choose **Package settings**,
   then under **Danger Zone** choose **Change visibility** and `Public`. GitHub
   requires the package name as confirmation and does not allow a public package
   to be made private again.
4. Re-run the release workflow. It checks `public` visibility before and after
   every publish, so a missing transition or later policy drift blocks releases.
5. Do not configure a production GHCR token. Public container packages support
   anonymous pulls.

GitHub CLI still needs an authenticated user able to read this repository
because installers execute `gh attestation verify`. That identity does not need
package read permission for the public registry object.

## Deployment and rollback

Download both files from the single release artifact and keep them together:

```bash
sha="$(git rev-parse HEAD)"
release_dir="$(mktemp -d /private/tmp/mcp-ozon-release.XXXXXX)"
gh run download --name "mcp-ozon-release-$sha" --dir "$release_dir"
export MCP_RELEASE_EVIDENCE="$release_dir/release.json"
export MCP_RELEASE_IMAGE_LOCK="$release_dir/release-images.json"
```

Run `scripts/canary-up.sh`, validate `/livez`, `/readyz`, `/health` and
`/metrics`, then run the relevant production installer. The installer rejects a
dirty checkout, a workflow run that is not successful, a mismatched lock hash,
an invalid attestation, an unavailable platform digest, or a revision-label
mismatch.

For rollback, check out the exact historical Git SHA and supply its matching
retained `release.json` plus `release-images.json`. Repeat canary before the
production installer. Never edit a lock or replace its digest with a tag.
