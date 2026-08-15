# OzonOFK Control MCP scaffold

`mcp-ozon-control` is a separate MCP process for the future advertising write
workflow. It does not extend the existing analytics server and cannot weaken
its read-only allowlists.

## Current milestone: disabled scaffold

The current binary intentionally has no marketplace client, marketplace URL,
credential lookup, plan store, or write tool. It exposes exactly two local
read-only tools:

- `ozon_ads_control_status` — confirms that marketplace credentials,
  marketplace egress, persistence, and marketplace writes are disabled;
- `ozon_ads_control_scope` — shows only the account/campaign/SKU bindings
  explicitly listed for the current actor in the local policy.

`ControlAppConfig` reads only variables beginning with `CONTROL_MCP_`. It does
not load `.env`, and it never resolves credential environment names contained
in `config/access.json`.

The example Compose network is `internal: true`, so the disabled scaffold has
no Internet egress. Its port is published only on host loopback. Do not connect
this dev/no-auth instance to a public tunnel.

## Local start without API keys

```bash
cp config/control-policy.example.json config/control-policy.json
chmod 600 config/control-policy.json
docker compose -f compose.control.yaml up -d --build
curl -fsS http://127.0.0.1:8790/health
```

The repo-local Codex plugin at `plugins/ozonofk-control` points to
`http://127.0.0.1:8790/mcp`. No marketplace API key is required or accepted by
this milestone.

## Policy shape

Policy is deny-by-default and contains identifiers and limits only:

```json
{
  "version": 1,
  "mode": "disabled",
  "actors": [
    {
      "actor_id": "manager",
      "targets": [
        {
          "account_id": "example_ozon",
          "campaign_id": 42,
          "skus": [1001],
          "bid_limits": {
            "min_minor": 100,
            "max_minor": 5000,
            "max_delta_percent": 5
          }
        }
      ]
    }
  ]
}
```

Even an analytics `admin` receives no Control scope unless that exact actor is
listed. Policy accounts must already exist in the access registry, belong to
Ozon with a Performance binding, and be accessible to the actor. Unknown fields
— including credential-looking fields — fail startup.

## Next gated milestone

Before any API key or write endpoint is added, the next review must introduce:

1. a dedicated JWT audience and exact `mcp:ads-control` scope;
2. a durable PostgreSQL plan/audit store;
3. `prepare` and `apply(plan_id)` as separate tools;
4. current-state reread, immutable short-lived plans, one-time execution,
   per-campaign/SKU locks, and read-back reconciliation;
5. an exact fixed-host write allowlist with no generic URL/method/body;
6. no automatic retry after an ambiguous marketplace write;
7. ChatGPT approval for every apply call and server-side limits independent of
   the model.

Write credentials must live only in the future Control container. The existing
Analytics container must remain read-only and must never receive them.
