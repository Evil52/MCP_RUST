# Immutable container delivery

## Status and fixed decisions

The repository implements the complete source-to-runtime identity chain. The
selected registry is the private GitHub Container Registry package
`ghcr.io/evil52/mcp-rust-runtime`. CI publishes only after all required quality,
security, coverage and container jobs pass. Production never builds release
images locally.

The fixed policy is:

- the package must remain `private`; CI checks visibility before and after every
  publish and fails closed if it cannot prove that state;
- CI uses a dedicated classic PAT stored as the repository Actions secret
  `GHCR_PUBLISH_TOKEN`, scoped only to `write:packages`;
- the production Mac uses a separate pull-only classic PAT with
  `read:packages`; it is never stored in the repository or a workflow secret;
- every release image is built for `linux/amd64` and `linux/arm64`, scanned on
  both platforms and receives a GitHub build-provenance attestation;
- discovery tags are mutable and cannot be deployment input; installers accept
  only references of the form `ghcr.io/evil52/mcp-rust-runtime@sha256:...`;
- release evidence and image locks are retained as GitHub Actions artifacts for
  90 days. Rollback inputs that must outlive that window must be copied to the
  protected operator archive before expiry.

The package is deliberately not associated with the public source repository by
an OCI source label. Package access and repository visibility therefore remain
separate controls.

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
7. Registry credentials grant pull-only access on the production Mac. CI's
   publish identity is separate and unavailable to the runtime.

The lock contains the exact twelve deployable image identities: `server`,
`control`, `control-ingress`, `control-auth-egress`, `control-write-egress`,
`mail-egress`, `ozon-egress`, `position-db`, `position-collector`,
`report-collector`, `report-worker`, and `wb-automation`.

## One-time GitHub setup

1. Create a classic personal access token for the CI publisher with only
   `write:packages`. Do not grant repository scopes.
2. Store it as the repository Actions secret `GHCR_PUBLISH_TOKEN`.
3. After the first publish, confirm the package visibility is `private`. The
   workflow repeats this check, so a later visibility drift blocks releases.
4. Create a different classic PAT for the production operator with only
   `read:packages`. Authenticate Docker without placing the token on the command
   line or in shell history:

   ```bash
   docker login ghcr.io --username Evil52 --password-stdin < /protected/path/ghcr-read-token
   ```

   The token file must be a non-symlink regular file with mode `600` in a
   protected directory and must not be placed inside this repository.

GitHub CLI also needs an authenticated user able to read this repository and
the private package metadata because installers execute `gh attestation verify`.

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
