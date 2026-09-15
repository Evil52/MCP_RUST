# WB inventory collection

WB inventory is collected as two independent sources. Current `stocks` jobs
collect FBW inventory at WB warehouses. `seller_stocks` collects seller warehouses
with every valid positive `deliveryType`, preserving it as `delivery_type` in each
size/warehouse fact. Only `delivery_type=1` is FBS; other values retain their WB
meaning. The optional seller source does not become a required member of the
four-source WB report manifest or add seller inventory to current FBW totals.

PR 102 previously coupled FBW and seller inventory into one `stocks` job and
snapshot. This design is superseded by the independent source and snapshot in
migration 039. Previously published combined snapshots remain immutable. When
reading historical `stocks`, the reader checks the whole snapshot for
`warehouse_id` values beginning with `wb:seller:` and reports
`inventory_scope="mixed"` if any exist, even when the requested page contains
only FBW rows. New seller snapshots have `inventory_scope="seller"`.

Both scheduled collection and an authorized manager refresh enqueue seller
inventory alongside WB stocks. Migration 039 dispatches the optional
`seller_stocks` job independently, with separate leases and publication. The
normalized page journal contains:
warehouse list, current non-trash catalogue cursor pages, then each warehouse
and batch of up to 1000 size IDs. The journal freezes the exact `chrtId` to `nmId`
mapping and warehouse delivery types. Successful batches are replayed locally
after a transient failure. Only one new API request is admitted per quantum,
using the cross-process WB quota and bounded deadline. Catalogue pages contain
up to 100 cards, with and without photos. Trash cards are excluded upstream;
`catalog_scope="active_non_trash_cards"` defines this coverage explicitly.

Collection bounds are 251 catalogue pages including a terminal empty page,
100 seller warehouses, 25000 size identities and 25000 aggregate SKU/warehouse
pairs. Size-level persistence and snapshot reading allow at most 2500000
size/warehouse facts for `seller_stocks`; other source limits remain unchanged.
These are validation and storage bounds, not a throughput guarantee. The
30-minute observation window and rate limits may prevent a very large catalogue
from completing. Repeated cursors or identities, foreign stock rows, invalid
quantities or delivery types and exceeded bounds fail the source.

Seller facts store every requested size/warehouse pair. Explicit `amount: 0`
becomes zero; an omitted size becomes NULL. After every batch has completed, a
snapshot containing NULL quantities is `partial`, with complete pagination.
Unknown quantities are not retried in an unbounded loop. A failed FBW job does
not block seller inventory, and vice versa. A new scheduled observation starts
a new collection; targeted live checks use `wb_seller_warehouse_stocks`.

Read `ofk_source_snapshot` with `source: "seller_stocks"` and the required account.
Pin `snapshot_id` for later pages. Each row includes `delivery_type`; filter
value 1 when interpreting FBS inventory. The reader returns whole-snapshot
known/missing coverage, freshness, actual observation times and partial values.
A zero-row result has `data_state="no_data"`, which does not assert zero physical
inventory. Existing complete snapshots remain addressable by snapshot ID. A
cutoff or retrieval timestamp does not reconstruct inventory on another day.

HTTP 204 on the documented FBW stock endpoint is a successful no-data response,
normalized as an empty page with upstream status and provenance in the live
tool response. Empty or malformed HTTP 200 and unexpected 204 from other
endpoints remain errors. FBW aggregation runs across all pages with checked
addition and rejects overlapping size identities when WB supplies them.

The original stock observation window remains limited to 30 minutes. Migration
038 retains successful checkpoints of failed stock jobs for diagnosis and exact
operator recovery after a repair; it does not automatically retry authentication
or malformed-response failures. See [stock checkpoint recovery](stock-checkpoint-recovery.md).

Apply migrations in order through `038_stock_checkpoint_retention.sql` and
`039_wb_seller_stock_snapshots.sql`, retaining the existing 036–037 migration
history. Migration 037 introduced stock quota groups; 039 extends the contract
for the independent seller source, its size-level facts and reader view. Pause
the previous collector during the schema/code transition so the old coupled
writer cannot run beside the new source design. Deployment also needs the
updated reporting egress image, collector and MCP server. The egress allowlist
includes the exact `marketplace-api.wildberries.ru` host. Database contract
checks require the seller-fact privileges before collection. No live marketplace
stock value is changed by this read-only collection feature.
