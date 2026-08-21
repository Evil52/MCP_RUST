# Daily manager reports

The daily reporting subsystem is intentionally separate from search-position
monitoring and from the write-capable Control MCP.

## Current implementation

The first disabled-only phase provides:

- immutable morning and evening identities at 08:00 and 17:00
  `Asia/Yekaterinburg`;
- UTC storage and marketplace transport timestamps, with every manager-facing
  HTML, XLSX, email, Dashboard and reporting-MCP timestamp rendered as
  `Asia/Yekaterinburg` (`UTC+05:00`);
- deterministic D-1 and preliminary same-day reporting intervals;
- catch-up after process downtime without duplicate report identities;
- separate delayed morning and evening deliveries after 17:00 when both were
  missed, preserving each report's explicit period and identity;
- automatic-delivery deadlines at 14:00 for a normally scheduled morning
  report and 23:00 for evening or post-outage recovered work;
- a five-attempt bounded delivery state machine with explicit permanent and
  transient error classes;
- artifact SHA-256 and provider-message identities without storing message
  bodies or credentials in the state machine; and
- a strict routing policy validated against the authoritative access registry;
- a transactional PostgreSQL outbox with immutable occurrence coverage,
  bounded attempts, `FOR UPDATE SKIP LOCKED` claims and append-only delivery
  audit; and
- a dedicated least-privilege `report_worker` role that cannot read the raw
  marketplace-history schema; and
- a separate least-privilege `report_collector` role that can append normalized
  sales, advertising, stock and price facts but cannot use the delivery outbox;
- transactional source snapshots that start as `running`, become immutable at
  `succeeded`, `partial` or `failed`, verify their persisted row count and
  publish only terminal successful/partial projections; and
- a least-privilege PostgreSQL snapshot writer that validates bounded normalized
  facts before I/O, appends facts and publishes one source snapshot atomically,
  and rejects duplicate account/source/cutoff identities without overwriting; and
- a separate disabled `report-collector` runtime that validates the exact pilot
  account/source plan and the `report_collector` database role without loading
  marketplace credentials or making network requests; and
- an explicit operator-only `ozon-dry-run` command which first claims the exact
  account/cutoff lease and only then loads that account's policy-scoped Ozon
  Seller and Performance read credentials, reaches the two
  fixed API hosts through the deployment-owned CONNECT proxy, normalizes one
  completed EKB business day against the following day's immutable 08:00 EKB
  morning cutoff and publishes all five Ozon sources atomically; and
- Ozon sales normalization that requests the two supported metrics, revenue
  and ordered units; unavailable cancellation/return metrics remain explicit
  `N/D` values rather than fabricated zeroes;
  Analytics pagination is paced at one request per minute per Client-Id and is
  capped at ten pages (9,999 complete rows) inside the 12-minute dry-run deadline; and
- warehouse-specific Ozon FBO and FBS stock collection that retains each real
  warehouse ID with an `fbo:` or `fbs:` namespace instead of substituting a
  synthetic channel-wide warehouse; and
- an explicit operator-only `wb-dry-run` command which first claims the exact
  account/cutoff lease and only then loads that account's policy-scoped Personal
  WB token, collects the documented daily sales-funnel,
  current warehouse-stock, current price and eligible-campaign statistics
  endpoints through the exact-host CONNECT proxy, and publishes all four
  normalized sources atomically; the tightly limited campaign statistics are
  attempted first, so a busy advertising quota stops before the other three
  APIs are called; and
- Ozon Performance product statistics normalized by the real
  `campaignId + sku + date`, so advertising facts no longer use an
  unavailable-SKU sentinel or fabricated product attribution; and
- Ozon Performance expense JSON that preserves `moneySpent`, `bonusSpent`
  and `prepaymentSpent` independently, plus SKU baskets, model-attributed
  orders/revenue, product price, average CPC and derived CPM/CPL; and
- an Ozon finance source based on the bounded accrual type and by-day ledger
  APIs; unknown accrual types remain visible through an explicit counter and
  are never silently reclassified; and
- a bounded PostgreSQL reader that loads only published source descriptors and
  revalidates the exact marketplace-specific manifest before report generation;
- a bounded published-fact reader that selects rows only by the frozen
  snapshot identities, rechecks every persisted row count and rejects foreign
  account data; and
- an account-aware report dataset that aggregates sales by `account + SKU`,
  advertising by `account + campaign + SKU`, and stock across warehouses
  without merging identical SKU numbers from different stores; and
- deterministic integer KPI formulas for ordered units, operational GMV, CTR,
  CPC, advertising conversion, CPO and advertising-revenue DRR; zero
  denominators remain `N/D` and aggregation overflow fails closed; and
- a bounded deterministic rule engine for stockouts, low stock cover,
  advertising without stock, spend without attributed orders and high DRR;
  incomplete manifests suppress actions and only five ranked problems survive;
- a bounded, dependency-free HTML email renderer with no images or external
  resources; it escapes display data and suppresses actions when source quality
  is not complete;
- a bounded six-sheet XLSX renderer for summary, SKU sales, advertising,
  inventory/prices, recommendations and source quality; calculations remain
  server-side, identifiers are written as literal text and no images are used;
- a single deterministic report-bundle constructor that derives the interval,
  account scope and quality from the immutable report key and frozen dataset,
  renders matching HTML/XLSX output and assigns a content SHA-256 plus a stable
  object key; its dry-run inspection performs no filesystem or network I/O;
- a manual `report-worker preview` path that resolves one manager only through
  the validated audience policy, loads an exact published cutoff, verifies the
  requested morning/evening interval, renders both artifacts and writes them
  with create-new semantics to an existing operator-selected directory;
- a local immutable artifact-store boundary that persists the deterministic
  XLSX and HTML siblings under the stable report identity, verifies content
  hashes on every retry, refuses path traversal and symbolic links, and never
  overwrites different bytes; and
- an idempotent publication operation that validates an artifact against the
  exact outbox recipient/date/version/coverage, commits files before changing
  the batch to `ready`, and safely reuses the same files after an ambiguous
  database response;
- an isolated `report-worker` binary that accepts only a restricted
  `report_worker` PostgreSQL URL, access-registry path and strict report policy;
  it validates both least-privilege database contracts before serving and is
  disabled by default;
- hardened `report-collector` and `report-worker` images in
  `compose.position.yaml`; both run as the established non-root UID, have no
  published ports, mount registry/policy metadata read-only and attach only to
  the internal database network while disabled; only `report-worker` receives
  a dedicated writable artifact volume, and its health check proves that the
  configured root can be written without retaining probe data;
- an idempotent scheduling kernel that reserves separate morning/evening
  outbox identities per audience, treats any existing batch state as covered,
  and permits a missed evening plan after a separate morning plan; and
- a frozen snapshot-manifest contract requiring exactly one sales,
  advertising, stock and price source per scoped account, with source-specific
  freshness limits and fail-closed recommendation suppression for partial or
  stale data.

The pilot example is disabled and scopes the temporary owner report to Diana's
Ozon account and the Vahrusheva/Torsunova Wildberries account. Sender and recipient
addresses are referenced only by environment-variable names. Passwords, OAuth
tokens and actual addresses must not be committed.

## Remaining rollout gates

The snapshot manifest, normalized PostgreSQL storage contract, atomic snapshot
writer, bounded fact reader and report dataset are present. Manual bounded Ozon
and WB adapters are available for policy-scoped canaries, but automatic
marketplace collection remains disabled.
The default Compose stack enables no scheduler, S3 writer or live mail
delivery. Separate opt-in collection and mail profiles are present but require
their canary gates and private deployment inputs. A provider-independent email envelope validates
one server-resolved sender and recipient, one exact claimed report scope, and
the bounded HTML/XLSX artifact without reading environment variables. A
bounded Gmail API transport also exists:
its production constructor has one fixed Gmail send endpoint, one fixed
dedicated mail-egress proxy, disabled redirects and ambient proxies, a bounded
response and payload-free errors. A single-attempt delivery service joins
the strict routing document, validated artifact, OAuth refresh and Gmail send
transports. The primitive outbox coordinator claims at most one ready row,
loads and verifies its immutable artifact, invokes that service once, and
persists only a result whose safety is known. It resolves and
validates the address and report scope before OAuth, refreshes once, sends
once and never retries internally. An explicit provider rate limit and an
OAuth failure before send may be scheduled by the coordinator within the
report deadline; an exhausted safe retry becomes a terminal audited failure.
Any timeout, transport failure, 5xx response, redirect, or malformed receipt
after the Gmail request starts remains an ambiguous `sending` outcome for
operator reconciliation. A failure to persist a post-claim outcome also leaves
the row `sending`; it is never converted into an automatic resend. The outbox
vocabulary reserves distinct permanent
audit classes for invalid artifact scope, invalid private routing and an
explicit provider rejection; these failures are never mislabeled as a
transient network problem. The OAuth transport accepts exactly the private files `client_id`,
`client_secret` and `refresh_token`, requires a private credential directory,
uses only Google's fixed token endpoint through the same dedicated mail-egress
proxy, and accepts only a bounded Bearer token for the minimal
`https://www.googleapis.com/auth/gmail.send` scope. The operator-only credential
mounts and isolated mail-egress canary overlay are present. The binary and
separate `reporting-mail-live` profile contain a gated bounded delivery
scheduler; activation remains an operator rollout decision after a successful
database-backed canary preflight. A strict private routing-file loader
resolves the policy's
symbolic sender and audience names to addresses. It requires one exact route
per configured symbol, refuses extra/missing/duplicate routes and never lets a
prompt, report row or account payload select an address. The routing file is
not mounted by the default Compose service. The separate
`reporting-mail-canary` profile mounts it only for explicit
`REPORT_WORKER_MODE=delivery_canary`, together with an exact private OAuth
directory. No Gmail password or address
belongs in the OAuth directory, repository, Compose environment or logs. An opt-in
`REPORT_WORKER_MODE=dry_run` runtime ticks
once per minute, plans due 08:00/17:00 EKB occurrences and retries only
single-section `planned`/`generating` artifact work inside its delivery window.
It never claims a ready delivery. The isolated
production artifact volume is mounted and health-checked. Manual
HTML/XLSX previews can be generated from an already published complete
marketplace-specific manifest. The immutable local store and outbox-publication
primitive can now be invoked explicitly for one already-planned outbox batch
while delivery remains disabled. Recovery queues a missed morning and evening
as separate single-section artifacts; it never presents one interval as two
reports. The
shipped default Compose service consequently cannot send email. The canary
overlay can perform one operator-invoked `deliver-one` attempt when all private
host paths are supplied. Merely starting its profile runs `healthcheck`, not
delivery. The worker has no general Internet network; its fixed Gmail and OAuth
clients can reach only `gmail.googleapis.com` and `oauth2.googleapis.com`
through the credentialless deny-by-default Squid service. The default and
canary profiles have no automatic delivery loop; only the explicit live profile
can start it. Mail delivery cannot affect a marketplace. Search-position
collection remains disabled.

`report-worker` supports `healthcheck`, disabled idle operation and the
operator-only commands below. The selected actor must belong to the selected
audience; its account scope comes from the validated policy and cannot be
supplied on the command line. Existing output files are never overwritten.

```text
report-worker preview <audience-id> <actor-id> <YYYY-MM-DD> \
  <morning|evening> <cutoff-rfc3339> <existing-output-dir>

report-worker generate <batch-id>

REPORT_WORKER_MODE=delivery_canary report-worker deliver-one

REPORT_WORKER_MODE=scheduled_delivery report-worker mail-preflight <audience-id>

REPORT_WORKER_MODE=dry_run report-worker reconcile-sent \
  <audience-id> <batch-id> <attempt-no> <provider-message-id> \
  --confirm-gmail-sent

REPORT_WORKER_MODE=dry_run report-worker reconcile-suppress \
  <audience-id> <batch-id> <attempt-no> \
  --confirm-provider-outcome-unknown
```

`preview` requires disabled mode and a disabled policy. `generate` accepts
disabled mode with a disabled policy or dry-run mode with an enabled policy;
neither command reads recipient email values, sends mail or calls a
marketplace. `preview` accepts an operator-selected manager and output path but
never creates or changes an outbox occurrence. `generate` accepts only a
positive batch ID: recipient, manager/account scope, report date, kind and
cutoff are loaded from PostgreSQL plus the validated policy. It renders with
the immutable batch creation timestamp, commits both files to the dedicated
artifact volume and only then marks that exact batch ready. Repeating it can
only reproduce or reuse the same bytes. The hardened production container has
a read-only root filesystem plus one dedicated artifact volume. `deliver-one`
requires an enabled policy, strict private routing and OAuth files, claims at
most one ready batch, and is bounded by 60 seconds. It refreshes OAuth once and
sends once; an ambiguous or unpersisted post-claim outcome remains `sending`
for operator reconciliation rather than being retried.

The reconciliation commands require an enabled policy in `dry_run` mode and
never load Gmail OAuth, routing or marketplace credentials. The audience must
exist in that exact policy version, while the batch and active attempt must
match an unresolved `sending` row for the same audience. Use `reconcile-sent`
only after Gmail independently proves delivery and supplies the exact provider
message ID. If delivery cannot be proven, `reconcile-suppress` permanently
closes the attempt as `operator_reconciled_unknown`; it deliberately prefers a
possible missed report to a duplicate email. Both decisions atomically append
an immutable database audit row and move the batch to a terminal state.
Repeating the identical command is idempotent, while a different decision is a
conflict. Never repair an ambiguous send with direct SQL or by returning it to
`ready`.

The one-shot Compose canary is launched only by the guarded wrapper below. It
requires an already healthy `position-db`, builds/starts the dedicated proxy,
waits for its deny-by-default healthcheck, performs exactly one worker command,
and stops the proxy on exit. The four variables are host paths, never secret
values; the routing file and OAuth directory must satisfy the private-mode and
exact-inventory checks described above.

```text
MCP_ACCESS_CONFIG_HOST=/private/path/access.json \
DAILY_REPORT_MAIL_POLICY_HOST=/private/path/daily-report-policy.json \
REPORT_MAIL_ROUTING_HOST=/private/path/mail-routing.json \
REPORT_GMAIL_OAUTH_DIR_HOST=/private/path/gmail-oauth \
scripts/run-report-mail-canary.sh
```

Do not invoke this command until a ready outbox artifact has been manually
reviewed. It may send one real message. A successful canary does not enable the
08:00/17:00 scheduler or any background mail delivery.

After a successful and reconciled canary, `REPORT_WORKER_MODE=scheduled_delivery` can run the
same deterministic planning/generation pass once per minute and then drain at
most 16 ready deliveries. Each provider attempt has its own 60-second bound.
Five consecutive failed ticks terminate the process so its supervisor can
restart a fresh database session. PostgreSQL occurrence uniqueness and the
delivery deadlines make restart catch-up safe: ready morning/evening work is
picked up after an outage while its window is open, already sent work cannot be
recreated, and ambiguous `sending` work is never automatically reclaimed. The
separate `reporting-mail-live` profile supplies the same isolated mail proxy and
mandatory read-only private inputs. It remains inert until an operator invokes:

```text
./scripts/start-report-mail-scheduler.sh --confirm-canary-sent-and-reconciled
```

Set `REPORT_MAIL_CANARY_AUDIENCE_ID` to the non-secret audience identifier used
by the canary. Before starting the services, the wrapper runs a database-backed
`mail-preflight`: that exact audience and policy version must have a successful
provider delivery no more than 24 hours old, and it must have no unresolved
`sending` row. The confirmation records an operational decision, not a security
bypass: an enabled strict policy, exact routing file, OAuth directory and
healthy database are still required before the worker can start. Do not invoke
it before the canary message, recipient, attachment hash and Gmail audit result
are checked.

The scheduled worker re-checks that same activation receipt every time it
starts, so the gate also trips when a whole day of scheduled deliveries has
failed. Because the service runs with `restart: unless-stopped`, the container
then restarts straight back into the refusal: a restart never clears it. The
startup error names the required operator action. Reconcile any ambiguous
`sending` attempt with `report-worker reconcile-sent` or
`report-worker reconcile-suppress`, re-run the `delivery_canary` `deliver-one`
command, and only then start the scheduler again. This is deliberate — an
automatic mail loop must not resume while its last provider-backed proof is
unknown or stale.

The separate collector has two explicit one-account canary commands:

```text
REPORT_COLLECTOR_MODE=ozon_dry_run report-collector \
  ozon-dry-run <account-id> <completed-YYYY-MM-DD>

REPORT_COLLECTOR_MODE=wb_dry_run report-collector \
  wb-dry-run <account-id> <completed-YYYY-MM-DD>
```

The account must already belong to the validated disabled report policy. Startup
loads registry metadata but no marketplace secret values. Each command first
claims the exact account/marketplace/cutoff lease; a busy or completed target
returns before reading any marketplace secret. Only the successful claimant
resolves the selected Ozon account's Seller and Performance bindings or the
selected WB account's Personal token. Secret values must be injected by the
runtime under the environment-variable names from the access registry, never
placed in a command line or committed file. Each invocation is bounded
to twelve minutes and publishes the exact marketplace-specific snapshot set in one
database transaction. The date argument is the completed business day; its
snapshot identity is the following day's fixed 08:00 EKB morning cutoff, so a
successful canary can be consumed by the same report-worker manifest contract.
The actual completion time is retained as `source_as_of`; it is never backdated
to the logical cutoff. To preserve the database freshness boundary, the command
must start after 08:00 EKB and the complete atomic source set must be ready no
later than 08:30 EKB. An early invocation fails before marketplace I/O, while a
collection that completes too late publishes nothing.
A timeout, rate limit or malformed/incomplete source publishes nothing. The
shipped Compose mode remains `disabled`, so neither command runs on a schedule.

The shared collection scheduler contract is deterministic: it opens
only the `08:00–08:30` and `17:00–17:30` Asia/Yekaterinburg completion windows,
returns the same immutable cutoff after a restart inside that window, and
refuses to backdate current state after the window closes. An external timer
may invoke the one-shot command below while the explicitly enabled policy and
mode are active, or the same process can run the bounded minute scheduler:

```text
REPORT_COLLECTOR_MODE=scheduled report-collector collect-due
REPORT_COLLECTOR_MODE=scheduled report-collector run-scheduler
REPORT_COLLECTOR_MODE=scheduled report-collector collection-preflight
```

Both paths perform no marketplace I/O outside a completion window. The
long-running command executes an immediate tick, then once per minute with
missed ticks skipped and without overlapping passes. Five consecutive tick
failures terminate the process so its supervisor can restart a fresh database
session. SIGTERM/Ctrl-C cancels the active target and attempts to release its
lease before exit. Inside a window, a PostgreSQL-backed
preflight now removes exact account/marketplace/cutoff targets that already
have all required terminal published sources. It then processes missing policy
targets sequentially so shared provider quotas are not multiplied. Every
target is claimed idempotently in PostgreSQL before resolving only that
account's credentials or performing marketplace I/O. A failed target releases
its lease and does not prevent later targets from being attempted; the command
returns a failure after the bounded pass so an external timer can retry only
remaining work inside the same window. The database claim is
exclusive for fifteen minutes, can be explicitly released after a bounded
failure, and uses a monotonically increasing fencing generation. All required
source snapshots and claim completion commit together, so an expired owner
cannot publish after a replacement starts. Each target is bounded by twelve
minutes and by the remaining completion window. Startup or idle operation never
performs collection: `collect-due` or `run-scheduler` must be supplied
explicitly. Scheduled mode additionally requires
`REPORT_COLLECTOR_CREDENTIAL_DIR` to name an existing operator-owned directory.
Each access-registry credential name resolves to one regular file directly in
that directory. Names are restricted to uppercase ASCII letters, digits and
underscores, files are bounded, symbolic links are rejected and trailing line
endings are ignored. The directory must be mounted read-only; its path may be
present in process configuration, but marketplace values must never appear in
Compose environment variables, command arguments, images or logs. Opening the
runtime validates only the directory. A value is read after the corresponding
database claim succeeds, so a busy/completed or unrelated account reads no
secret. The shipped Compose mode and policy both remain disabled and do not
mount this directory, so scheduled collection cannot be enabled accidentally.

`collection-preflight` performs no marketplace request. It requires every
target in the enabled policy to share one successful, fully paginated
marketplace-specific cutoff no more
than 24 hours old. A target added to the policy therefore cannot enter the
automatic scheduler until its manual canary has published the same reviewed
occurrence as the other targets. The long-running scheduler repeats this proof
at process startup; later publications provide bounded restart proof.

The repository ships a separate `compose.reporting-live.yaml` overlay, but it
has no defaults for live metadata or credentials and its collector is guarded
by the `reporting-live` profile. After the access registry, enabled policy and
credential directory have been reviewed, render the merged contract first:

```text
MCP_ACCESS_CONFIG_HOST=/absolute/path/access.json \
DAILY_REPORT_POLICY_HOST=/absolute/path/daily-report-policy.json \
REPORT_COLLECTOR_CREDENTIAL_DIR_HOST=/absolute/path/report-credentials \
docker compose --env-file .position.env \
  -f compose.position.yaml -f compose.reporting-live.yaml \
  --profile reporting-live config --quiet
```

Only then may an operator start `report-collector` through the guarded wrapper
with the same explicit files/profile:

```text
./scripts/start-report-collector-scheduler.sh \
  --confirm-canaries-published-and-reconciled
```

The wrapper renders the Compose contract and runs the database-backed
`collection-preflight` before it starts the egress proxy and collector. The
overlay supplies `run-scheduler`; it does not enable the report worker or email
delivery. Omitting any path, the profile, an enabled policy, a recent complete
successful canary occurrence or a valid read-only credential directory fails closed.

Create that directory with the bundled operator command rather than copying the
whole runtime `.env`:

```text
cargo run --locked --bin report-collector -- \
  bootstrap-credentials \
  config/access.json \
  config/daily-report-policy.json \
  .env \
  report-credentials
```

The destination must not already exist. The command accepts only strict
`NAME=value` source lines, validates the enabled policy against the access
registry and copies only marketplace credential names used by policy-selected
accounts. It never loads values into the process environment, expands `$`
references, follows symbolic links, copies unrelated variables, overwrites a
directory or prints secret values. The new directory is mode `0700` and every
credential file is mode `0600`. Re-running after a policy or key change means
creating a different new directory, reviewing it by file names and count, then
atomically switching the host bind; do not edit the mounted directory in place.

Before enabling the scheduler, run each pilot marketplace once through the
operator-only canary overlay. It mounts the same credential directory read-only,
but requires the disabled pilot policy and defaults to the local `healthcheck`
command. Merely starting the profile therefore cannot contact a marketplace.
Only `scripts/run-report-canary.sh` supplies an explicit single-account command;
the script accepts no credential values and the collector reads only the claimed
account's exact files after acquiring its PostgreSQL lease:

```text
MCP_ACCESS_CONFIG_HOST=/absolute/path/access.json \
REPORT_COLLECTOR_CREDENTIAL_DIR_HOST=/absolute/path/report-credentials \
./scripts/run-report-canary.sh ozon furnitura_dlya_doma 2026-08-18 morning

MCP_ACCESS_CONFIG_HOST=/absolute/path/access.json \
REPORT_COLLECTOR_CREDENTIAL_DIR_HOST=/absolute/path/report-credentials \
./scripts/run-report-canary.sh wb ip_domnyshev_wb 2026-08-19 evening
```

The requested date is the business day being collected. `morning` covers that
complete day and runs the next day from 08:00 through 08:30 EKB. `evening`
covers the requested date from midnight through its 17:00 cutoff and runs from
17:00 through 17:30 EKB. Outside the selected window the command fails before
marketplace I/O.
Run Ozon and WB sequentially. A failed, timed-out or partial source set releases
the lease and publishes no snapshot IDs. The canary policy must stay disabled;
the enabled policy is reserved for the separately reviewed live overlay. The
base `position-db` and `ozon-egress` services must already be healthy. The runner
uses `--no-deps`, so a canary never creates or recreates either dependency; an
unavailable dependency makes the canary fail closed.

Dry-run scheduling additionally requires `REPORT_WORKER_MODE=dry_run` and an
enabled validated policy. The shipped Compose value remains `disabled`. Every
tick is bounded to 16 generation candidates; missed timer ticks are skipped,
and an unavailable or incomplete snapshot leaves the batch unmodified for a
later tick. Recovery after a server restart therefore continues within the
existing 14:00/23:00 EKB safety deadlines without creating duplicate report
identities. Email remains impossible because no provider or recipient-value
loader exists.

The persistence layer transactionally stores report occurrences, delivery
coverage and attempts. Its unique occurrence key is
`(local_date, kind, recipient_id, report_version)`. New catch-up work always
keeps morning and evening in separate single-period deliveries. Legacy
multi-period rows remain fail-closed and cannot be rendered or sent. A crash
before a provider call leaves work claimable. A crash after a delivery is
claimed deliberately leaves the batch in `sending` for operator reconciliation:
an ambiguous provider result is never retried automatically.

## Planned pilot

1. Validate a manual Ozon five-source preview for Diana and add the
   corresponding manual WB four-source preview for Vahrusheva/Torsunova.
2. Run the opt-in scheduler dry mode for Diana and Vahrusheva/Torsunova after
   both marketplace collectors publish complete manifests.
3. Connect S3-compatible artifact storage and one service mailbox.
4. Send previews to the temporary owner recipient at 08:00 and 17:00 EKB.
5. Reconcile figures for five to seven days before enabling other managers.

ChatGPT remains the interactive analytics interface. Scheduled collection,
calculation, artifact generation and delivery must remain server-side so they
continue to work when ChatGPT is unavailable.
