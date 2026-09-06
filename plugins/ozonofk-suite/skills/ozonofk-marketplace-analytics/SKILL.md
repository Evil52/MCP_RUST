---
name: ozonofk-marketplace-analytics
description: Produce DB-only Ozon/WB weekly portfolio rankings and explain account coverage or snapshot freshness through OzonOFK. Use for marketplace leader/outsider comparisons, weekly GMV rankings, or missing-data diagnosis. Do not use for marketplace mutations or a detailed Ozon daily manager report.
---

# OFK marketplace analytics

This skill ships with `ozon-daily-manager-report` in OzonOFK Suite. Before data
calls read the shared [data-quality contract](../ozon-daily-manager-report/references/data-quality.md).
Do not install this skill alone without that companion reference.

1. Discover the connected OzonOFK tools and resolve the authenticated actor,
   role and accessible accounts using `list_members` and `marketplace_accounts`.
   Missing connection or unavailable tools are limitations, not an invitation
   to invent data or tool names. Do not access local environment files.
2. For a weekly portfolio comparison use `ofk_weekly_marketplace_ranking` with
   no dates for the previous completed calendar week, or both explicit dates.
   Do not fan out to live Ozon/WB sales tools. This ranking requires admin:
   on denial, explain the restriction without reconstructing another ranking.
3. Enforce the shared period/coverage gate before naming a leader or outsider.
   With partial coverage show only the requested period, complete/expected
   count and missing accounts/dates. Map names and owners only from the account
   inventory; never guess an Ozon/WB manager pairing. Do not label ordered
   units as number of orders or operational GMV as profit.
4. For freshness diagnosis, read the selected accounts' collection quality and
   refresh status. Keep last-refresh failure separate from snapshot quality.
   Status reads are allowed without triggering work. Enqueue only if the user
   explicitly requests current-data refresh, following the shared bounded
   recovery procedure. Do not claim current-day refresh fills a missing week.
5. Return a concise answer in the user's language: exact period, coverage,
   metric and units, then the permitted ranking or missing-data explanation.
   Cite account IDs/cutoffs when helpful. Mention material errors. Offer a
   recovery task when evidence is missing, not a trading recommendation.

For a daily Ozon report, route to the companion `ozon-daily-manager-report`
skill rather than duplicating its report workflow. Spreadsheet/chart output
is optional and requires complete comparable evidence and an available output
capability; lack of such a capability does not prevent a plain-text answer.
