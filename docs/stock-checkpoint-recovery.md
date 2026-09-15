# Failed stock checkpoint recovery

Migration `038_stock_checkpoint_retention.sql` retains normalized pages for
failed `stocks` and `seller_stocks` source jobs. These remain private staging data:
reader tools continue to return the last successful immutable snapshot and the
failed collection status separately. No missing inventory is converted to zero.

The existing limits remain 4 MiB per page, 32 MiB and 4,096 pages per job. Failed
stock pages become eligible for deletion after 24 hours. Each source-claim call
cleans at most four finished jobs (128 MiB); physical cleanup therefore occurs
on subsequent collector ticks. A stopped collector can leave diagnostic pages
on disk longer, but cannot make them eligible for recovery. The small job and
recovery audit records remain available after page cleanup.

`invalid_json`, `unauthorized`, `forbidden`, `missing_credentials`, and
`credentials_unavailable` always leave a source failed, including if the caller
accidentally requests a transient retry. Other terminal errors also stay failed.
Retaining pages does not schedule a retry.

After fixing the recorded cause, a database operator can inspect one exact
collection using a parameterized query:

```sql
SELECT id, account_id, marketplace, source, cutoff_at, generation, status,
       error_class, first_observed_at, last_observed_at, completed_pages,
       cache_bytes, deadline_at
FROM daily_reporting.source_collection_jobs
WHERE account_id = $1 AND marketplace = $2 AND id = $3;
```

Explicit recovery accepts the exact account, marketplace, job ID, failed lease
generation, observed error class, and a short ASCII reason without secrets:

```sql
SELECT daily_reporting.resume_stock_collection($1, $2, $3, $4, $5, $6);
```

Only the function owner/database administrator has this permission. It is not
granted to `report_collector`, `position_reader`, or report workers, and there is
no automatic recovery call in the collector. `false` means the precise failed
state no longer matches, pages are missing, a newer snapshot already exists,
the job's recovery limit is exhausted, or the observation window has expired.

Recovery is allowed at most three times for one collection, only while its
first observation is within 30 minutes and its original deadline has not passed.
It preserves all page timestamps, the cutoff, account/source identity and quota
backoff. It makes the same job ready; its next claim advances the lease
generation so the old worker stays fenced out. Every accepted recovery records
the caller, previous generation/error, time and reason in
`daily_reporting.stock_collection_resumes`.

The operation-specific Content and Seller Inventory quota buckets added by
migration 037 remain in force for both stock source paths. A vendor cooldown
extends the bucket used by that request, including after a process restart.
Migration 036's separate bounded restart policy for overlapping Sales pages is
unchanged by stock recovery.

An expired collection needs a fresh collection with a fresh observation window;
it cannot reuse its old pages as current stock evidence. A retained failed page
is diagnostic evidence and does not establish complete inventory coverage.
