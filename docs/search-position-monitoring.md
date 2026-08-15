# Architecture: Ozon search-position monitoring

Status: database boundary plus a provider-independent Rust collector core. No
browser collection or marketplace requests are enabled.

## Goal

Measure the visible organic and sponsored position of selected Ozon products for
fixed search phrases and regions every 30 minutes, retain structured history, and
make that history available to ChatGPT through read-only MCP tools.

An exact live search-position endpoint has not been confirmed in the official
Seller or Performance API contract. The collector must prefer an official API if
Ozon publishes one. Browser collection is a fallback and must stop rather than
bypass CAPTCHA, HTTP 403, HTTP 429, or another access-control response.

## Components

```text
host scheduler (HH:05 and HH:35)
        |
        | claims one logical UTC :00/:30 slot; no catch-up burst
        v
collector core + disabled/provider adapter
        |
        | one request per region+phrase; all tracked SKU; top 100
        v
PostgreSQL (structured measurements only)
        |
        | SELECT-only database role
        v
Rust MCP read tools
        |
        v
ChatGPT analysis and on-demand Excel generation
```

The collector and PostgreSQL are separate from the current MCP containers.
PostgreSQL is reachable only on the named internal Docker network;
it has no host port.

## Thirty-minute policy

Thirty minutes is the fixed cadence, not permission to issue bursts. A day has
48 unique logical slots; repeated runs in one slot never improve coverage.

- Run at `HH:05` and `HH:35`; associate each execution with the preceding exact
  UTC `:00` or `:30` logical slot. The scheduler retains that planned boundary
  and passes it to the core even if the process wakes slightly late; it never
  derives a slot from the imprecise wake timestamp.
- Refuse an early start or a wake more than two minutes after its planned
  boundary; the 20-minute batch budget therefore ends before the next slot.
- Skip missed slots instead of replaying them after restart.
- Hold one PostgreSQL advisory lock for the collection cycle so cycles cannot
  overlap across processes.
- Group all managers and products by `(region, normalized phrase, top-N)` so one
  public result is reused for every tracked SKU in that query.
- Reject a slot with more than 64 unique queries globally or 16 for one region.
- Start sequentially; permit at most two concurrent queries only after canary.
- Keep at least ten seconds between query starts even when responses are fast.
- Start with a maximum of 100 visible positions per phrase.
- Cancel a page observation after the fixed 90-second core timeout.
- Cancel the whole batch after 20 minutes so it cannot reach the next slot.
- Do not automatically retry CAPTCHA, HTTP 403, or HTTP 429.
- Open a circuit breaker after a blocking response and require an operator review.
- Stop the current batch after three consecutive transport failures.
- The first production adapter performs no automatic retries at all.

The scaffold's circuit breaker is intentionally in-process. The persistence
adapter must store the open state so a process restart cannot silently resume
collection before operator review. It must also enforce a durable per-region
and daily request budget; neither guarantee may rely on process memory.

The collector never logs or stores complete URLs with tokens, response bodies,
HTML, screenshots, cookies, browser storage, or Ozon Seller credentials. A public
search collector must not receive Seller or Performance credentials at all.

## Stored data

Each measurement stores only:

- UTC observation time;
- internal store identifier;
- Ozon product identifier and optional seller offer identifier;
- search phrase and fixed region;
- outcome (`found`, `not_found`, `blocked`, or `error`);
- overall position and placement (`organic`, `sponsored`, or honestly
  `unknown`); a miss has no synthetic position `0` or `101`;
- result page, price, original price, delivery time, and availability when visible;
- bounded latency, upstream HTTP status, and a payload-free error class.

Raw pages and generated Excel workbooks are never stored. A 90-day retention
window is the initial recommendation. Backups are encrypted database dumps, not
copies of browser state.

## Database permissions

| Principal | Permissions |
| --- | --- |
| `position_admin` | schema migration and monitor configuration |
| `position_collector` | read monitors; append/update runs; append measurements and alerts |
| `position_reader` | SELECT only; forced read-only transactions |

Only `position_reader` may be configured in the MCP container. Database passwords
are independent and never reuse Ozon, WB, identity-provider, or MCP secrets.

## Planned read-only MCP tools

- `ozon_search_position_latest`: latest observations for explicitly selected
  monitors;
- `ozon_search_position_history`: paginated measurements for at most 31 days;
- `ozon_search_position_summary`: hourly aggregates for charting;
- `ozon_search_position_compare`: bounded comparison of products, phrases, or
  regions;
- `ozon_search_position_alerts`: recent position drops, disappearances, or
  collector blocks.

All inputs use strict schemas, account RBAC, row limits, date limits, query
timeouts, and cancellation. Tools never create or edit monitors and never invoke
the browser. ChatGPT can transform returned rows into an `.xlsx` file on demand.

## Daily visibility product contract

The collected history feeds one manager-facing module named **Daily search
visibility** (`Поисковая видимость за сутки`). This is a presentation and report
contract, not a feature of the provider adapter. The collector records facts;
deterministic aggregation and task rules run after a reporting day is closed;
ChatGPT may explain the resulting facts but does not recalculate them, assign an
owner, or change their priority.

The compact Dashboard view contains only:

- collection status and data freshness;
- unique valid half-hour slots divided by the expected 48 slots; duplicate runs
  never increase coverage;
- visibility rate (`found / (found + not_found)`) without converting a miss to
  position `101`; blocked, failed and missing observations instead reduce
  completeness and are excluded from the visibility denominator;
- changes against the preceding complete day and the median of the preceding
  seven complete days;
- the number of critical products;
- at most five highest-priority problems with numeric evidence;
- open tasks grouped by the responsible manager; and
- a link to the bounded detailed report.

Position statistics are calculated only over found observations and always show
their denominator: latest, median, best and worst position, plus found and valid
counts. Organic, sponsored and unknown placement remain separate. A public
search observation does not prove a causal relationship between a price, bid,
card edit and a position change.

Comparisons use frozen local reporting days in `Asia/Yekaterinburg`. The morning
report at 08:00 contains the preceding complete day; the 17:00 report contains a
preliminary same-day delta and compares only matching elapsed slots. Day-over-day
is unavailable until two complete days exist. The seven-day baseline is
unavailable until seven preceding complete days exist. Missing, stale or partial
data is rendered as `N/D`, never as zero. Expected observations are derived from
the monitors active in each interval. Reporting-day boundaries map to the fixed
UTC collection slots; local report time never changes or recreates those slots.
Period comparisons use only the same monitor cohort: store, product, normalized
phrase, region and top-N. A changed cohort is reported as `scope_changed` rather
than being presented as business growth or decline.

Detailed Excel is generated from the same immutable report run and includes raw
structured measurements, period comparison, data-quality diagnostics, tasks and
action-result checks. It contains no screenshots, product photos, raw HTML,
cookies or credentials. PostgreSQL remains the history source of truth; a new
workbook never replaces previous history.

Tasks are created by versioned deterministic rules and a server-side
responsibility registry (`store/SKU/direction -> manager`). Each task records the
source evidence, severity, owner, due time, status, rule version, and planned
checks after one, three or seven days. Completing an action is distinct from
observing an improvement. Results use only `improved`, `no significant change`,
`worsened`, or `insufficient data`; reports say that a metric changed after an
action, not that the action caused the change.

The initial persistence phase must therefore add immutable daily report runs,
daily aggregates, a responsibility registry, versioned rule evaluations, tasks
and action-result observations. None of these Dashboard/report objects is
implemented by the current disabled-only collector core. Until a reviewed
provider is enabled and valid history exists, any consumer must show
`disabled / awaiting collector`, not zero visibility.

## Delivery phases

1. **Architecture:** schema, roles, network boundary, cadence and safety policy.
2. **Safe core (implemented):** strict target validation, deterministic
   half-hour scheduling, query coalescing, unknown-placement support, local
   overlap guard, disabled provider, no-retry execution, and protection stop.
3. **Persistence contract (schema and pure Rust boundary implemented):** exact slot idempotency,
   non-lossy unknown placement, one-way run finalization, published reader views,
   durable circuit and request-budget claims. Rust now validates and converts a
   batch into a payload-free atomic persistence payload; the disabled repository
   performs no I/O. The PostgreSQL adapter and transactional fixture integration
   remain to be implemented without marketplace traffic.
4. **Collector canary:** one product, one phrase, one region, manually triggered;
   confirm selectors and whether Ozon permits the workflow.
5. **Scheduled pilot:** one product and three to five phrases every 30 minutes;
   observe blocking/error rate for seven days.
6. **MCP reader:** implement and test the five SELECT-only tools against fixture
   data before connecting to the production database.
7. **Daily report projection:** implement complete-day aggregates, data-quality
   rules, deterministic manager tasks and before/after checks against fixture
   history.
8. **Dashboard and export integration:** publish the compact visibility module
   and generate the bounded detailed Excel workbook on demand.
9. **Rollout:** add products gradually, with explicit per-region and daily request
   budgets.

Inputs needed for canary phase 4 are the store identifier, Ozon product ID, optional
seller offer ID, three to five search phrases, region, and desired maximum search
position. No API key should be sent until an official endpoint requiring that key
has been confirmed.
