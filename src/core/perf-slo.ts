/**
 * gbrain#1: latency SLO status for the MCP server, served as JSON at
 * `GET /health/perf` so an operator agent (Summer) can poll one URL.
 *
 * Source of truth is `mcp_request_log` (server-side latency for every MCP
 * tool call). The SLOs:
 *   - put_page, content < 10KB: p95 < 3000ms
 *   - get_page:                 p95 <  500ms
 *   - search:                   p95 < 1500ms
 * plus the background embedder's backlog (a stuck embedder means new pages
 * are keyword-only), and a stale-chunk count.
 *
 * Status per SLO: `ok` (within target), `breach` (p95 over target or error
 * rate over 5%), `no_data` (fewer than MIN_SAMPLES calls in the window).
 * Overall: `breach` if any SLO or the embedder breaches, else `ok`.
 * The payload carries only aggregates — no slugs, params, or tokens.
 */
import type { BrainEngine } from './engine.ts';
import type { BackgroundEmbedderStats } from './background-embed.ts';

export interface SloTarget {
  name: string;
  operation: string;
  p95_ms: number;
  /** Only count calls whose logged approx_bytes is below this. */
  max_bytes?: number;
}

export const SLO_TARGETS: readonly SloTarget[] = [
  { name: 'put_page_lt_10kb', operation: 'put_page', p95_ms: 3000, max_bytes: 10 * 1024 },
  { name: 'get_page', operation: 'get_page', p95_ms: 500 },
  { name: 'search', operation: 'search', p95_ms: 1500 },
];

export const MIN_SAMPLES = 5;
export const MAX_ERROR_RATE = 0.05;
/** Embedder backlog older than this is a breach (new pages not vector-searchable). */
export const MAX_EMBED_QUEUE_AGE_MS = 10 * 60_000;

export interface SloRow {
  operation: string;
  n: number;
  errors: number;
  p50_ms: number | null;
  p95_ms: number | null;
}

export type SloStatus = 'ok' | 'breach' | 'no_data';

export interface SloResult {
  name: string;
  operation: string;
  target_p95_ms: number;
  status: SloStatus;
  n: number;
  p50_ms: number | null;
  p95_ms: number | null;
  error_rate: number | null;
}

export interface PerfStatus {
  status: 'ok' | 'breach';
  generated_at: string;
  window_minutes: number;
  slos: SloResult[];
  background_embed: (BackgroundEmbedderStats & { status: 'ok' | 'breach' | 'off' }) | null;
  stale_chunks: number | null;
  db_ping_ms: number | null;
  breaches: string[];
}

export function evaluateSlo(target: SloTarget, row: SloRow | undefined): SloResult {
  const n = row?.n ?? 0;
  const errorRate = n > 0 ? (row!.errors / n) : null;
  let status: SloStatus = 'no_data';
  if (n >= MIN_SAMPLES) {
    const slow = (row!.p95_ms ?? 0) > target.p95_ms;
    const failing = (errorRate ?? 0) > MAX_ERROR_RATE;
    status = slow || failing ? 'breach' : 'ok';
  }
  return {
    name: target.name,
    operation: target.operation,
    target_p95_ms: target.p95_ms,
    status,
    n,
    p50_ms: row?.p50_ms ?? null,
    p95_ms: row?.p95_ms ?? null,
    error_rate: errorRate === null ? null : Math.round(errorRate * 1000) / 1000,
  };
}

export function evaluateEmbedder(stats: BackgroundEmbedderStats | null): PerfStatus['background_embed'] {
  if (!stats) return null;
  const stuck = (stats.oldest_queued_age_ms ?? 0) > MAX_EMBED_QUEUE_AGE_MS;
  return { ...stats, status: stuck ? 'breach' : 'ok' };
}

export function summarize(
  slos: SloResult[],
  embed: PerfStatus['background_embed'],
  extra: { windowMinutes: number; staleChunks: number | null; dbPingMs: number | null; now?: Date },
): PerfStatus {
  const breaches = slos.filter((s) => s.status === 'breach').map((s) => s.name);
  if (embed?.status === 'breach') breaches.push('background_embed');
  return {
    status: breaches.length > 0 ? 'breach' : 'ok',
    generated_at: (extra.now ?? new Date()).toISOString(),
    window_minutes: extra.windowMinutes,
    slos,
    background_embed: embed,
    stale_chunks: extra.staleChunks,
    db_ping_ms: extra.dbPingMs,
    breaches,
  };
}

/** Per-operation latency aggregates over the window. One indexed query. */
export async function querySloRows(engine: BrainEngine, windowMinutes: number): Promise<Map<string, SloRow>> {
  const rows = await engine.executeRaw<{ name: string; operation: string; n: number; errors: number; p50_ms: number | null; p95_ms: number | null }>(
    `WITH w AS (
       SELECT operation, latency_ms, status,
              COALESCE((params->>'approx_bytes')::bigint, 0) AS approx_bytes
         FROM mcp_request_log
        WHERE created_at > now() - make_interval(mins => $1::int)
          AND operation = ANY($2::text[])
     ), t AS (
       SELECT * FROM unnest($3::text[], $4::text[], $5::bigint[]) AS t(name, operation, max_bytes)
     )
     SELECT t.name, t.operation,
            count(w.*)::int AS n,
            (count(w.*) FILTER (WHERE w.status = 'error'))::int AS errors,
            (percentile_cont(0.5) WITHIN GROUP (ORDER BY w.latency_ms))::int AS p50_ms,
            (percentile_cont(0.95) WITHIN GROUP (ORDER BY w.latency_ms))::int AS p95_ms
       FROM t LEFT JOIN w ON w.operation = t.operation
                         AND (t.max_bytes IS NULL OR w.approx_bytes < t.max_bytes)
      GROUP BY t.name, t.operation`,
    [
      windowMinutes,
      [...new Set(SLO_TARGETS.map((t) => t.operation))],
      SLO_TARGETS.map((t) => t.name),
      SLO_TARGETS.map((t) => t.operation),
      SLO_TARGETS.map((t) => (t.max_bytes ?? null) as unknown as number),
    ],
  );
  return new Map(rows.map((r) => [r.name, { operation: r.operation, n: r.n, errors: r.errors, p50_ms: r.p50_ms, p95_ms: r.p95_ms }]));
}

export async function collectPerfStatus(
  engine: BrainEngine,
  opts: { windowMinutes?: number; embedder?: BackgroundEmbedderStats | null } = {},
): Promise<PerfStatus> {
  const windowMinutes = opts.windowMinutes ?? 60;
  const t0 = performance.now();
  let dbPingMs: number | null = null;
  try {
    await engine.executeRaw('SELECT 1');
    dbPingMs = Math.round(performance.now() - t0);
  } catch { /* reported as null */ }

  const rows = await querySloRows(engine, windowMinutes);
  const slos = SLO_TARGETS.map((t) => evaluateSlo(t, rows.get(t.name)));

  let staleChunks: number | null = null;
  try {
    // Partial index (embedding IS NULL) keeps this cheap.
    const r = await engine.executeRaw<{ n: number }>('SELECT count(*)::int AS n FROM content_chunks WHERE embedding IS NULL');
    staleChunks = r[0]?.n ?? null;
  } catch { /* reported as null */ }

  return summarize(slos, evaluateEmbedder(opts.embedder ?? null), { windowMinutes, staleChunks, dbPingMs });
}
