#!/usr/bin/env bun
/**
 * Profile one MCP operation in-process: wall time plus every SQL round trip
 * the engine issues (count, cumulative wall ms, slowest statements). Built
 * for gbrain#1, where latency was round-trip count x pooler RTT, not server
 * time.
 *
 *   bun scripts/perf/profile-op.ts put_page '{"slug":"ops/bench/prof","content":"x"}'
 *
 * Env:
 *   PROFILE_SERVE=1    apply the `gbrain serve` settings (config snapshot
 *                      cache + background embedder) and report the deferred
 *                      embed separately
 *   PROFILE_SOURCE=id  source to dispatch as (default: default)
 *
 * Uses the configured brain (~/.gbrain/config.json / GBRAIN_DATABASE_URL).
 * Diagnostic only; writes whatever the op writes.
 */
import { loadConfig, toEngineConfig } from '../../src/core/config.ts';
import { createEngine } from '../../src/core/engine-factory.ts';
import { dispatchToolCall } from '../../src/mcp/dispatch.ts';
import { configureGateway } from '../../src/core/ai/gateway.ts';
import { buildGatewayConfig } from '../../src/core/ai/build-gateway-config.ts';
import { startBackgroundEmbedder } from '../../src/core/background-embed.ts';
import { resolveServeConfigCacheTtlMs } from '../../src/commands/serve-http.ts';

const [op, rawParams] = process.argv.slice(2);
if (!op) {
  console.error('usage: profile-op.ts <op> [json-params]');
  process.exit(2);
}
const params = rawParams ? JSON.parse(rawParams) : {};

const config = loadConfig();
if (!config) throw new Error('no gbrain config');
configureGateway(buildGatewayConfig(config));
const engine = await createEngine(toEngineConfig(config));
await engine.connect(toEngineConfig(config));
const serveMode = process.env.PROFILE_SERVE === '1';
const embedder = serveMode ? startBackgroundEmbedder(engine, { sweepIntervalMs: 0 }) : null;
if (serveMode) (engine as { setConfigCacheTtl?: (ms: number) => void }).setConfigCacheTtl?.(resolveServeConfigCacheTtlMs());

const stats = new Map<string, { n: number; ms: number }>();
let total = 0;
let totalMs = 0;

// Time each tagged-template query from first await to resolution.
function wrap(sql: any): any {
  return new Proxy(sql, {
    apply(target, thisArg, args) {
      const pending = Reflect.apply(target, thisArg, args);
      if (!Array.isArray(args[0]) || !pending || typeof pending.then !== 'function') return pending;
      const key = (args[0] as string[]).join('$').replace(/\s+/g, ' ').trim().slice(0, 110);
      const origThen = pending.then.bind(pending);
      let timed = false;
      pending.then = (res: any, rej: any) => {
        if (timed) return origThen(res, rej);
        timed = true;
        const t0 = performance.now();
        return origThen((v: any) => {
          const ms = performance.now() - t0;
          total++;
          totalMs += ms;
          const s = stats.get(key) ?? { n: 0, ms: 0 };
          s.n++;
          s.ms += ms;
          stats.set(key, s);
          return res ? res(v) : v;
        }, rej);
      };
      return pending;
    },
    get(target, prop, recv) {
      const v = Reflect.get(target, prop, recv);
      if (prop === 'begin' && typeof v === 'function') {
        return (...a: any[]) => {
          const fn = a[a.length - 1];
          a[a.length - 1] = (tx: any) => fn(wrap(tx));
          return v.apply(target, a);
        };
      }
      return v;
    },
  });
}

// Instance pools live on _sql; the module-singleton path shadows the getter.
const e = engine as any;
if (e._sql) e._sql = wrap(e._sql);
else Object.defineProperty(e, 'sql', { value: wrap(e.sql), configurable: true });

const t0 = performance.now();
const result = await dispatchToolCall(engine, op, params, {
  remote: true,
  sourceId: process.env.PROFILE_SOURCE ?? 'default',
});
const wall = performance.now() - t0;
if (result.isError) console.error(JSON.stringify(result.content).slice(0, 600));
console.log(JSON.stringify({ op, wall_ms: Math.round(wall), sql_round_trips: total, sql_ms: Math.round(totalMs), isError: !!result.isError }));
for (const [k, s] of [...stats.entries()].sort((a, b) => b[1].ms - a[1].ms).slice(0, 20)) {
  console.log(`${s.ms.toFixed(0).padStart(7)}ms ${String(s.n).padStart(4)}x  ${k}`);
}
if (embedder) {
  const t1 = performance.now();
  await embedder.drain();
  console.log(JSON.stringify({ background_embed_ms: Math.round(performance.now() - t1), ...embedder.stats() }));
}
await engine.disconnect();
process.exit(0);
