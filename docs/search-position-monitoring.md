# Architecture: Ozon search-position monitoring

Status: design and database boundary only. No browser collection or marketplace
requests are enabled by this change.

## Goal

Measure the visible organic and sponsored position of selected Ozon products for
fixed search phrases and regions every 15 minutes, retain structured history, and
make that history available to ChatGPT through read-only MCP tools.

An exact live search-position endpoint has not been confirmed in the official
Seller or Performance API contract. The collector must prefer an official API if
Ozon publishes one. Browser collection is a fallback and must stop rather than
bypass CAPTCHA, HTTP 403, HTTP 429, or another access-control response.

## Components

```text
host scheduler (every minute)
        |
        | selects due 15-minute monitors; one advisory lock
        v
isolated collector + ephemeral browser context
        |
        | sequential phrases, fixed region, bounded search depth
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

The collector and PostgreSQL are separate from the current MCP and Keycloak
containers. PostgreSQL is reachable only on the named internal Docker network;
it has no host port.

## Fifteen-minute policy

Fifteen minutes is the target cadence, not permission to issue bursts.

- Add a random start jitter of up to plus or minus two minutes.
- Hold one PostgreSQL advisory lock for the collection cycle so cycles cannot
  overlap across processes.
- Use one browser process and one fixed region per worker.
- Process search phrases sequentially.
- Pause between phrases; the exact safe interval is configured after a canary.
- Stop scanning a phrase as soon as the product is found.
- Start with a maximum of 100 visible positions per phrase.
- Do not automatically retry CAPTCHA, HTTP 403, or HTTP 429.
- Open a circuit breaker after a blocking response and require an operator review.
- Retry only ordinary DNS, connect, or transient 5xx failures, at most once with
  bounded backoff.

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
- organic and sponsored positions;
- result page, price, original price, delivery time, and availability when visible;
- bounded latency and a payload-free error class.

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
are independent and never reuse Ozon, WB, Keycloak, or MCP secrets.

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

## Delivery phases

1. **Architecture (this change):** schema, roles, network boundary, cadence and
   safety policy.
2. **Collector canary:** one product, one phrase, one region, manually triggered;
   confirm selectors and whether Ozon permits the workflow.
3. **Scheduled pilot:** one product and three to five phrases every 15 minutes;
   observe blocking/error rate for seven days.
4. **MCP reader:** implement and test the five SELECT-only tools against fixture
   data before connecting to the production database.
5. **Rollout:** add products gradually, with explicit per-region and daily request
   budgets.

Inputs needed for phase 2 are the store identifier, Ozon product ID, optional
seller offer ID, three to five search phrases, region, and desired maximum search
position. No API key should be sent until an official endpoint requiring that key
has been confirmed.
