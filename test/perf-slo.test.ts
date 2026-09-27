/**
 * gbrain#1 — /health/perf SLO evaluation. Pure evaluators plus one PGLite
 * round trip proving the mcp_request_log aggregate query and the <10KB
 * put_page filter.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import {
  SLO_TARGETS, MIN_SAMPLES, MAX_EMBED_QUEUE_AGE_MS,
  evaluateSlo, evaluateEmbedder, summarize, collectPerfStatus,
} from '../src/core/perf-slo.ts';
import type { BackgroundEmbedderStats } from '../src/core/background-embed.ts';

const search = SLO_TARGETS.find((t) => t.name === 'search')!;

describe('evaluateSlo', () => {
  test('no_data below the sample floor', () => {
    expect(evaluateSlo(search, undefined).status).toBe('no_data');
    expect(evaluateSlo(search, { operation: 'search', n: MIN_SAMPLES - 1, errors: 0, p50_ms: 9e9, p95_ms: 9e9 }).status).toBe('no_data');
  });
  test('ok within target, breach over target', () => {
    expect(evaluateSlo(search, { operation: 'search', n: 50, errors: 0, p50_ms: 200, p95_ms: 1400 }).status).toBe('ok');
    expect(evaluateSlo(search, { operation: 'search', n: 50, errors: 0, p50_ms: 200, p95_ms: 1600 }).status).toBe('breach');
  });
  test('error rate over 5% is a breach even when fast', () => {
    const r = evaluateSlo(search, { operation: 'search', n: 100, errors: 6, p50_ms: 10, p95_ms: 20 });
    expect(r.status).toBe('breach');
    expect(r.error_rate).toBe(0.06);
  });
});

describe('evaluateEmbedder / summarize', () => {
  const base: BackgroundEmbedderStats = {
    queued: 0, running: false, embedded_chunks: 0, pages_processed: 0, batches: 0,
    failures: 0, dropped: 0, last_error: null, last_drain_ms: null, oldest_queued_age_ms: null,
  };
  test('a backlog older than the limit breaches', () => {
    expect(evaluateEmbedder(null)).toBeNull();
    expect(evaluateEmbedder({ ...base, queued: 3, oldest_queued_age_ms: 1000 })!.status).toBe('ok');
    expect(evaluateEmbedder({ ...base, queued: 3, oldest_queued_age_ms: MAX_EMBED_QUEUE_AGE_MS + 1 })!.status).toBe('breach');
  });
  test('overall status lists every breach', () => {
    const slos = [
      evaluateSlo(search, { operation: 'search', n: 50, errors: 0, p50_ms: 1, p95_ms: 9999 }),
      evaluateSlo(SLO_TARGETS[1], { operation: 'get_page', n: 50, errors: 0, p50_ms: 1, p95_ms: 10 }),
    ];
    const out = summarize(slos, evaluateEmbedder({ ...base, oldest_queued_age_ms: MAX_EMBED_QUEUE_AGE_MS * 2 }), {
      windowMinutes: 60, staleChunks: 4, dbPingMs: 12, now: new Date(0),
    });
    expect(out.status).toBe('breach');
    expect(out.breaches).toEqual(['search', 'background_embed']);
    expect(out.generated_at).toBe('1970-01-01T00:00:00.000Z');
    expect(summarize([slos[1]], null, { windowMinutes: 60, staleChunks: 0, dbPingMs: 1 }).status).toBe('ok');
  });
});

describe('collectPerfStatus (PGLite)', () => {
  let engine: PGLiteEngine;
  beforeAll(async () => {
    engine = new PGLiteEngine();
    await engine.connect({ database_url: '' });
    await engine.initSchema();
  }, 60_000);
  afterAll(async () => { await engine.disconnect(); });

  test('aggregates mcp_request_log per SLO and filters put_page by size', async () => {
    const insert = (op: string, ms: number, bytes: number, status = 'success') =>
      engine.executeRaw(
        `INSERT INTO mcp_request_log (token_name, agent_name, operation, latency_ms, status, params)
         VALUES ('t', 't', $1, $2, $3, $4::jsonb)`,
        [op, ms, status, JSON.stringify({ approx_bytes: bytes })],
      );
    for (let i = 0; i < 10; i++) await insert('put_page', 500, 1024);
    for (let i = 0; i < 5; i++) await insert('put_page', 60_000, 65_536); // >10KB: excluded
    for (let i = 0; i < 10; i++) await insert('get_page', 2000, 64); // breach
    await insert('search', 100, 64); // below sample floor

    const out = await collectPerfStatus(engine, { windowMinutes: 60 });
    const by = Object.fromEntries(out.slos.map((s) => [s.name, s]));
    expect(by.put_page_lt_10kb.n).toBe(10);
    expect(by.put_page_lt_10kb.p95_ms).toBe(500);
    expect(by.put_page_lt_10kb.status).toBe('ok');
    expect(by.get_page.status).toBe('breach');
    expect(by.search.status).toBe('no_data');
    expect(out.status).toBe('breach');
    expect(out.breaches).toEqual(['get_page']);
    expect(out.stale_chunks).toBe(0);
    expect(typeof out.db_ping_ms).toBe('number');
    expect(out.background_embed).toBeNull();
  });
});
