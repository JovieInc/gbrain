# GBrain maintenance runbook (Jovie deployment)

This fork (`JovieInc/gbrain`, branch `master`) is the source of the brain that
serves `memory.timwhite.co` to Claude sessions, Codex, and Summer. It is
upstream `garrytan/gbrain` plus Jovie patches. This file covers operating it.
Upstream docs cover everything else.

## Topology

| Piece | Where | Notes |
|---|---|---|
| MCP HTTP server | ops Mac, launchd `com.gbrain.serve` → `~/.hermes/scripts/gbrain-serve-wrapper.sh` → `~/.hermes/bin/gbrain.real serve --http --port 7801` | Env in the plist. Logs in `/tmp/gbrain_serve_err.log` |
| Public door | launchd `com.gbrain.cloudflared`, tunnel `gbrain-mcp` → `127.0.0.1:7801` | `memory.timwhite.co` (MCP), `memory-health.timwhite.co` (health) |
| Tailnet door | launchd `com.gbrain.tailscale-proxy` | Gem's Codex uses `http://tims-macbook-pro:7801/mcp` |
| Jobs worker | launchd `co.jovie.hermes.gbrain-worker` (`gbrain jobs work`) | Runs minion jobs (facts, backfills) |
| Database | Supabase Postgres (us-east-1) via Supavisor `:6543` (transaction mode) | Small instance: 224MB shared_buffers. Each pooler round trip is about 100-230ms from the Mac |
| Embeddings | Ollama on the Mac, `nomic-embed-text` (768d) | A machine without a reachable Ollama cannot embed. See "Gem" below |

## Health endpoints (Summer owns watching these)

- `GET /health` is liveness. It returns `{"status":"ok"}` after one `SELECT 1`, with a 3s timeout. A 503 means the pool is saturated or the DB is unreachable.
- `GET /health/perf` returns the latency SLO status as JSON. The result is cached for 60s and contains only aggregates. Public URL: `https://memory-health.timwhite.co/health/perf`.

```json
{
  "status": "ok | breach",
  "breaches": ["put_page_lt_10kb", "search", "background_embed"],
  "window_minutes": 60,
  "slos": [
    {"name": "put_page_lt_10kb", "target_p95_ms": 3000, "status": "ok|breach|no_data",
     "n": 42, "p50_ms": 900, "p95_ms": 2100, "error_rate": 0}
  ],
  "background_embed": {"status": "ok|breach", "queued": 0, "oldest_queued_age_ms": null,
                       "failures": 0, "last_error": null},
  "stale_chunks": 0,
  "db_ping_ms": 110
}
```

SLOs, measured over 60 minutes from `mcp_request_log` (server-side latency):

| SLO | Target |
|---|---|
| `put_page` with content < 10KB | p95 < 3s, errors < 5% |
| `get_page` | p95 < 500ms, errors < 5% |
| `search` | p95 < 1.5s, errors < 5% |
| background embedder | oldest queued page < 10 min |

`no_data` means fewer than 5 calls in the window, and it is not an alert.

**Summer's alert rule.** Alert when `status == "breach"` on two consecutive polls 5 minutes apart. Also alert when the endpoint returns non-200 or times out (8s) on two consecutive polls.

## Weekly checks (about 10 minutes)

Run these from the Mac with the brain's config. `scripts/perf/profile-op.ts` profiles any single op.

1. **Latency trend.** Fetch `/health/perf`, then run the 7-day query below and compare with last week. A p50 that is climbing usually means more round trips per op or connection churn.
   ```sql
   select operation, count(*), percentile_cont(0.5) within group (order by latency_ms)::int p50,
          percentile_cont(0.95) within group (order by latency_ms)::int p95,
          count(*) filter (where status='error') errs
     from mcp_request_log where created_at > now() - interval '7 days'
    group by 1 order by 2 desc limit 15;
   ```
2. **Errors by client.** Group the same query by `token_name, status, left(error_message,120)`. A client failing 100% of the time is a misrouted deployment. That is how Gem's broken local server surfaced.
3. **Stale embeddings.** `stale_chunks` in `/health/perf` should stay near 0. If it grows, check `background_embed.last_error` and whether Ollama is up (`curl localhost:11434/api/ps`).
4. **Orphans and contradictions.** Run the MCP tools `find_orphans` and `find_contradictions` (or `gbrain doctor`). Link or retire orphans, and resolve contradictions with `takes_supersede`, or correct the page.
5. **Index and table bloat.**
   ```sql
   select relname, n_live_tup, n_dead_tup, pg_size_pretty(pg_total_relation_size(relid))
     from pg_stat_user_tables order by pg_total_relation_size(relid) desc limit 10;
   ```
   Watch `query_cache` (it has reached about 270MB, mostly TOASTed results) and `content_chunks`. If dead tuples go above about 20% of live tuples, or `last_autovacuum` is more than 7 days old, run `VACUUM (ANALYZE)` on that table.
6. **Hot queries.** Check the top rows of `pg_stat_statements` by `total_exec_time`. Server time is usually small. If a new query shows up with a high mean, profile it.

## Knobs (serve plist `EnvironmentVariables`)

| Env | Serve value | Effect |
|---|---|---|
| `GBRAIN_POOL_SIZE` | 4 | App-side pool. Supavisor transaction mode multiplexes it, so it is cheap. At 2, one slow write starved `/health` |
| `GBRAIN_PG_IDLE_TIMEOUT` | 300 | Seconds before an idle connection closes. At 20, most requests paid a new TLS + pooler handshake |
| `GBRAIN_CONFIG_CACHE_TTL_MS` | default 30000 | Config snapshot cache. `0` disables it |
| `GBRAIN_SERVE_BACKGROUND_EMBED` | default on | `0` puts embedding back inline in put_page (slow; use it only to debug) |

## Deploy

1. Merge to `master` once CI is green.
2. On the Mac, build and swap the binary. Keep the previous one for rollback.
   ```bash
   cd <clone of JovieInc/gbrain master> && bun install --frozen-lockfile && bun run build
   cp ~/.hermes/bin/gbrain.real ~/.hermes/bin/gbrain.real.pre-$(date +%Y%m%d-%H%M)
   cp bin/gbrain ~/.hermes/bin/gbrain.real
   launchctl kickstart -k gui/$(id -u)/com.gbrain.serve
   launchctl kickstart -k gui/$(id -u)/co.jovie.hermes.gbrain-worker
   ```
3. Verify: `curl -s localhost:7801/health`, then `curl -s localhost:7801/health/perf`, then a `put_page` smoke test.
4. To roll back, copy the `.pre-*` binary back and kickstart again.

## Gem

As of 2026-09-26, Gem ran its own stale v0.42 server and worker (systemd user units `gbrain-serve` and `gbrain-worker`) against the same database. Gem has no Ollama, so every put_page through Gem's local CLI failed after three embed retries. That was 1294 failed writes in one week. The Gem CLI (`~/.local/bin/gbrain`) now points at the Mac server over the tailnet, and those units are disabled. Do not run a second server version against the shared database.

## Ownership

| Area | Owner |
|---|---|
| Polling `/health/perf` and alerting | Summer |
| Weekly checks above; fixes, PRs, deploys | the GBrain maintainer agent (coder profile) |
| Supabase plan/instance size, GitHub Actions billing | Tim |
