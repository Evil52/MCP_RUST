# Ozon: SKU identity and available stock in the fallback collector

The `/v4/product/info/stocks` fallback previously persisted `product_id` in
`CollectedStockFact.sku` and copied `stocks[].present` without removing
reserves. Advertising uses Ozon SKU. Joining these facts by number could miss
existing stock or associate another product's inventory with an advertised
SKU. The ads importer therefore rejects the historical fallback dimensions.

## Corrected contract

Each fact now uses `stocks[].sku` and checked `present - reserved`. Product ID
is validated as a source identity but is never substituted for a missing SKU.
Different fulfillment SKUs belonging to one product remain separate.

The dimensions `sku-fulfillment-v2:fbo`, `sku-fulfillment-v2:fbs` and
`sku-fulfillment-v2:rfbs` distinguish corrected facts from old `FBO`/`FBS`/`RFBS`
records. They describe fulfillment totals, not physical warehouses. No schema
migration or reinterpretation of historical rows is needed.

Missing SKU/quantities, nonpositive or out-of-range IDs, negative or fractional
counts, reserves exceeding present, and an available count outside the storage
integer range reject the page. Unknown fulfillment types and duplicate
SKU/scheme totals also reject it instead of guessing or summing potentially
overlapping totals. Nested rows are bounded. Explicit zero available stock is
retained; an absent SKU does not become zero stock.

The optimizer accepts complete new-format snapshots and sums their distinct
fulfillment dimensions by SKU. It rejects a mixture of physical warehouses
and fulfillment totals, including zero-valued duplicates. A snapshot containing
any old uppercase fallback row still yields unknown stock for recommendations.

## Evidence and limits

A bounded MCP ОФК read on 2026-09-26 returned both `product_id` and `stocks[].sku`
with different values in the same response. After the initial page check,
all 2,120 products in the chosen account were read through 23 sequential
requests, including the terminal empty page. The first/last observations were
11:52:26 / 11:53:01 UTC. The corrected Rust parser accepted 3,072 distinct
SKU/fulfillment rows; 51 source rows had nonzero reserves. No missing SKU,
reserve underflow or repeated SKU/scheme was found. This is evidence for one
account and one observation interval, not all configured accounts or an atomic
point-in-time inventory snapshot.

For one FBS SKU with a nonzero reserve, calls made 13 seconds apart returned
`present=7`, `reserved=1` from `/v4/product/info/stocks` and `free_stock=6`
from `/v2/product/info/stocks-by-warehouse/fbs`, with matching SKU and source
counts. This supports the normalization for the observed case, not a claim of
simultaneous consistency across all Ozon endpoints or all products.

An earlier first-party implementation published on Ozon for dev also computes
available FBO/FBS stock by subtracting reserved from present:
[stock monitoring implementation](https://dev.ozon.ru/case/98-Keis-o-novom-instrumente-dlia-kontrolia-tovarnykh-ostatkov-na-sklade/).
That example uses v3; current v4 shape and the bounded FBS comparison above
come from live MCP responses. The current Seller documentation fetch returned
a redirect loop. No assumption about additional stock fields or shipment types
was taken from an unverified third-party SDK.

Synthetic regression fixtures cover distinct product/SKU identities, reserves,
multiple fulfillment SKUs, zero availability, invalid/missing counts,
duplicates, persistence serialization, and the producer-to-optimizer path.
Private marketplace responses are kept outside Git.

## Durable pages and release

The stock fallback's checkpoint identity now includes
`ozon-sku-fulfillment-v2`. Old normalized pages must be fetched again under the
new contract; otherwise their product IDs and gross inventory would bypass the
corrected parser during recovery. Corrected pages remain resumable without
repeating their requests. Price page identities and native warehouse page
identities retain their contracts.

The regression seeds an old fallback checkpoint, resumes the collector, checks
that a fresh fallback request is issued, and then confirms that the corrected
page is replayed without another request. Old checkpoint rows and historical
snapshots are not deleted or rewritten.

After releasing the collector through the normal immutable-image procedure:

1. Let a new scheduled stock collection complete all pages. An already failed
   source job is not revived by restarting the process.
2. Verify publication, snapshot ID, pagination completeness, observation times
   and `ofk_data_completeness` through MCP ОФК.
3. Read every stock page pinned to that snapshot. Compare chosen advertised
   SKUs against the source; inspect a product with a nonzero reserve.
4. Pass the complete export to `ads-optimizer prepare`, retaining the existing
   campaign coverage, attribution maturity and budget-policy requirements.

Local code/tests and read-only live requests do not establish production
rollout. This change does not schedule or apply advertising budget decisions.
