# ADR 0002: Incremental Rust workspace boundaries

- Status: accepted; storage and marketplace value types extracted
- Date: 2026-09-09
- Scope: source organization, build/test coverage; no deployment change

## Decision

Start with a dependency leaf: private `mcp-storage` owns supervised PostgreSQL
sessions, transport bounds and contention measurements. It depends only on
Tokio, tokio-postgres and tracing, never on marketplace or control domains.
`mcp-ozon` depends on it; `mcp_ozon::postgres` remains a compatibility facade.
Repositories retain their SQL, role checks and transaction ownership. This is
not a generic storage repository crate or a pool migration.

Workspace lints are inherited by all internal crates. Default members and explicit
`--workspace` quality/coverage commands include both. Docker builders copy the
new crate, development watchers observe it and Sonar receives its sources.
No third-party version is upgraded by this extraction.

### Second boundary: marketplace value types

`mcp-marketplace-types` now owns `StoreId`, `Marketplace` and the three
credential containers. It has no application, HTTP, database or environment
dependency; only Serde and Schemars (plus JSON test support). Ozon clients
depend directly on these types, no longer on the large application `config`
module. Configuration also no longer imports credentials from the WB client.
Existing `mcp_ozon::config` and `mcp_ozon::wb::WbCredentials` paths re-export
the same types, preserving callers and wire contracts.

Credential loading/validation, registry authorization and secret lifetime are
unchanged. These containers redact `Debug` but do not claim zeroization or
cryptographic storage. Tests check serialization, schema shape and redaction.
All three crates inherit the workspace quality policy. Extraction of full
marketplace clients, auth and reporting/control is still separate work.

Within the application crate, split production responsibilities first:

- `server`: input/output contracts, normalization, validation and eight tool
  router groups, composed into the existing MCP surface.
- `wb`: endpoint policy, argument validation and response/retry handling.
- `reporting::mcp_read`: output model, sales aggregation, fact reads and decoding.
- `control::automation_executor`: feedback/pacing calculations and state files.

Keep existing public type paths, tool names, schemas, access checks and
Analytics/Control separation. Existing tests stay with their owning module;
moving tests solely to inflate coverage or shrink a headline is not a goal.

## Why not six crates immediately?

The desired domains (`mcp-transport`, `auth`, `marketplace-clients`, `reporting`,
`control-domain`, `storage`) are useful targets, not yet independent dependency
leaves. Configuration and credentials refer to marketplace types; MCP and
reporting connect several domain interfaces. Moving directories wholesale
would force dependency cycles or export implementation details prematurely.

Next extract credential/account value types and pure domain contracts, then
clients and auth, then reporting/control, and finally transport/composition.
Each extraction needs a one-way dependency graph and its own contract tests.
Binary composition roots remain in the application package for now; this ADR
does not claim the whole monolith has been decomposed or compilation improved
without a measurement.

## Structural regression gate

`python3 -B scripts/check-rust-structure.py` counts physical lines in first-party
`src`, `crates` and `tests` Rust files, including comments, blank lines and tests.
New files have a 1,000-line ceiling. Larger existing files have explicit current
budgets in `config/rust-structure-budget.json`: growth fails, shrinkage requires
tightening/removing the exception. Budget increases require explicit review;
editing the baseline is not a routine way to make CI green.

This is a review-size guard, not an AST/cyclomatic-complexity metric. The legacy
Clippy `too_many_lines` allowance concerns function bodies, not whole files,
and remains separate debt. New code still inherits all other denied lints.

## Verification

Run workspace tests including PostgreSQL contracts, strict Clippy, rustdoc,
formatting, the structural gate and shared Docker-builder contract. Retain
95.8% line / 95.5% function coverage gates; never lower them to accommodate a
move. A green local check is not production rollout evidence.
