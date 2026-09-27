/**
 * gbrain#1 — PostgresEngine getConfig snapshot cache + serve/pool knobs.
 *
 * One remote put_page read ~28 distinct config keys, each a full pooler round
 * trip. With the cache on (serve), a miss loads the whole (tiny) config table
 * once and later reads are served from memory until the TTL lapses. Off by
 * default so workers keep uncached reads of coordination keys.
 *
 * Hermetic: a fake tagged-template `sql` stands in for postgres.js.
 */
import { describe, expect, test } from 'bun:test';
import { PostgresEngine } from '../src/core/postgres-engine.ts';
import { resolveIdleTimeoutSeconds, DEFAULT_PG_IDLE_TIMEOUT_S } from '../src/core/db.ts';
import { resolveServeConfigCacheTtlMs, DEFAULT_SERVE_CONFIG_CACHE_TTL_MS } from '../src/commands/serve-http.ts';

function fakeEngine(initial: Record<string, string>) {
  const table = new Map(Object.entries(initial));
  const queries: string[] = [];
  const sql = (strings: TemplateStringsArray, ...values: unknown[]) => {
    const text = strings.join('$').replace(/\s+/g, ' ').trim();
    queries.push(text);
    if (text === 'SELECT key, value FROM config') {
      return Promise.resolve([...table].map(([key, value]) => ({ key, value })));
    }
    if (text.startsWith('SELECT value FROM config WHERE key =')) {
      const v = table.get(values[0] as string);
      return Promise.resolve(v === undefined ? [] : [{ value: v }]);
    }
    if (text.startsWith('INSERT INTO config')) {
      table.set(values[0] as string, values[1] as string);
      return Promise.resolve([]);
    }
    if (text.startsWith('DELETE FROM config')) {
      const had = table.delete(values[0] as string);
      return Promise.resolve(Object.assign([], { count: had ? 1 : 0 }));
    }
    throw new Error(`unexpected sql: ${text}`);
  };
  const engine = new PostgresEngine();
  (engine as unknown as { _sql: unknown })._sql = sql;
  return { engine, queries, table };
}

describe('PostgresEngine getConfig snapshot cache', () => {
  test('off by default: every read is its own round trip', async () => {
    const { engine, queries } = fakeEngine({ a: '1' });
    expect(await engine.getConfig('a')).toBe('1');
    expect(await engine.getConfig('a')).toBe('1');
    expect(await engine.getConfig('missing')).toBeNull();
    expect(queries.length).toBe(3);
  });

  test('on: many distinct keys (hits and misses) cost one round trip', async () => {
    const { engine, queries } = fakeEngine({ a: '1', b: '2' });
    engine.setConfigCacheTtl(30_000);
    const reads = await Promise.all(['a', 'b', 'missing', 'a', 'c'].map((k) => engine.getConfig(k)));
    expect(reads).toEqual(['1', '2', null, '1', null]);
    expect(queries).toEqual(['SELECT key, value FROM config']);
  });

  test('local setConfig / unsetConfig are visible on the next read', async () => {
    const { engine } = fakeEngine({ a: '1' });
    engine.setConfigCacheTtl(30_000);
    expect(await engine.getConfig('a')).toBe('1');
    await engine.setConfig('a', '2');
    expect(await engine.getConfig('a')).toBe('2');
    await engine.unsetConfig('a');
    expect(await engine.getConfig('a')).toBeNull();
  });

  test('another process\'s write is picked up once the TTL lapses', async () => {
    const { engine, table, queries } = fakeEngine({ a: '1' });
    engine.setConfigCacheTtl(20);
    expect(await engine.getConfig('a')).toBe('1');
    table.set('a', 'from-cli');
    expect(await engine.getConfig('a')).toBe('1'); // within TTL
    await Bun.sleep(30);
    expect(await engine.getConfig('a')).toBe('from-cli');
    expect(queries.length).toBe(2);
  });

  test('setConfigCacheTtl(0) disables and drops the snapshot', async () => {
    const { engine, table } = fakeEngine({ a: '1' });
    engine.setConfigCacheTtl(30_000);
    await engine.getConfig('a');
    table.set('a', '2');
    engine.setConfigCacheTtl(0);
    expect(await engine.getConfig('a')).toBe('2');
  });
});

describe('serve / pool knobs', () => {
  test('resolveServeConfigCacheTtlMs', () => {
    expect(resolveServeConfigCacheTtlMs(undefined)).toBe(DEFAULT_SERVE_CONFIG_CACHE_TTL_MS);
    expect(resolveServeConfigCacheTtlMs('')).toBe(DEFAULT_SERVE_CONFIG_CACHE_TTL_MS);
    expect(resolveServeConfigCacheTtlMs('0')).toBe(0);
    expect(resolveServeConfigCacheTtlMs('5000')).toBe(5000);
    expect(resolveServeConfigCacheTtlMs('nope')).toBe(DEFAULT_SERVE_CONFIG_CACHE_TTL_MS);
    expect(resolveServeConfigCacheTtlMs('-1')).toBe(DEFAULT_SERVE_CONFIG_CACHE_TTL_MS);
  });

  test('resolveIdleTimeoutSeconds', () => {
    expect(resolveIdleTimeoutSeconds({})).toBe(DEFAULT_PG_IDLE_TIMEOUT_S);
    expect(resolveIdleTimeoutSeconds({ GBRAIN_PG_IDLE_TIMEOUT: '300' })).toBe(300);
    expect(resolveIdleTimeoutSeconds({ GBRAIN_PG_IDLE_TIMEOUT: '0' })).toBe(0);
    expect(resolveIdleTimeoutSeconds({ GBRAIN_PG_IDLE_TIMEOUT: '1.5' })).toBe(DEFAULT_PG_IDLE_TIMEOUT_S);
    expect(resolveIdleTimeoutSeconds({ GBRAIN_PG_IDLE_TIMEOUT: 'x' })).toBe(DEFAULT_PG_IDLE_TIMEOUT_S);
  });
});
