# Code review and Sonar verification — 2026-10-05

Reviewed integration source: `1eb8bd7ba139809b967ea9aae6c23fa9863fb286`.
This report records the reviewed scope and observed checks; it is not a claim
that every possible defect or undefined behavior has been excluded.

## Correctness fixes

- Recheck WB write authorization after pacing and quota waits, immediately before
  dispatch. The durable journal and account lock remain held across dispatch.
- Distinguish definite pre-send failure from an ambiguous marketplace result.
  Cancel known unsent actions without opening an incident; preserve ambiguous
  outcomes for reconciliation. Migration 051 restricts the cancellation transition.
- Pin report generation time after loading inputs, using the database clock.
  Migration 050 keeps that timestamp immutable and preserves legacy artifacts.
- Measure scheduler failure backoff from completion and refresh time per batch.

## Refactoring and integration

Resolve 18 Sonar findings by separating validation, collection, normalization
and calculation steps while preserving monetary arithmetic, error precedence,
snapshot provenance, pagination bounds and lock lifetime. Remove redundant
Python exception subclasses. Refresh the OAuth catalog expectation for the
three advertising-history tools.

Integrate WB advertising history (migration 052), reproducible advertising
baseline collection, and the prepared Forgejo workflows. Retain the existing
PR health-probe fix: archive preflight uses one database connection and does
not open a duplicate quota connection.

## Verification

The reviewed snapshot passed 1,804 Rust tests across the workspace, all targets
and features, including ignored tests, with no failures or skips. The Python
suite passed 80 tests. Strict workspace Clippy, schema and restricted-role ACL
checks, formatting and Rust structure budgets passed.

Local Sonar Quality Gate passed with zero open findings, 95.4% overall coverage
and 90.3% new-code coverage. Thresholds and exclusions were unchanged.

The integration differs from the Sonar snapshot only by the existing PR's
health-probe fix and its documentation. On that integration, the three collector
tests and strict collector Clippy passed; formatting and structure checks passed.
Raw logs and account-specific output remain local under the ignored `output/`
directory. Production rollout and remote CI results require separate verification.
