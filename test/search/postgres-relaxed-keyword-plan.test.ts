import { describe, expect, test } from 'bun:test';
import { PostgresEngine } from '../../src/core/postgres-engine.ts';

type QueryRecord = { query: string; terms?: string };

function fakeEngine(strictMatches: boolean) {
  const calls: QueryRecord[] = [];
  let currentSeqscan = 'on';
  const row = {
    slug: 'notes/example', page_id: 1, title: 'Example', type: 'note',
    source_id: 'example-source', chunk_id: 2, chunk_index: 0,
    chunk_text: 'alpha beta', chunk_source: 'compiled_truth', score: 1, stale: false,
  };
  const tx = Object.assign(
    async (strings: TemplateStringsArray, ...values: unknown[]) => {
      const query = strings.join('?');
      calls.push({ query });
      if (query === 'SHOW enable_seqscan') return [{ enable_seqscan: currentSeqscan }];
      if (query === 'SET LOCAL enable_seqscan = off') currentSeqscan = 'off';
      if (query.startsWith("SELECT set_config('enable_seqscan'")) currentSeqscan = String(values[0]);
      return [];
    },
    {
      unsafe: async (query: string, params: unknown[]) => {
        calls.push({ query, terms: String(params[0]) });
        if (strictMatches && params[0] === 'alpha beta') return [row];
        if (params[0] === 'alpha OR beta') return [row];
        return [];
      },
    },
  );
  const engine = Object.create(PostgresEngine.prototype) as PostgresEngine;
  Object.defineProperty(engine, 'withScopedReadTransaction', {
    value: async (_sources: unknown, _source: unknown, fn: (client: unknown) => Promise<unknown>) => fn(tx),
  });
  return { engine, calls, getSeqscan: () => currentSeqscan };
}

describe('Postgres relaxed keyword retry plan', () => {
  test('strict hit keeps the ordinary planner and skips the relaxed retry', async () => {
    const { engine, calls, getSeqscan } = fakeEngine(true);
    const hits = await engine.searchKeyword('alpha beta', { sourceId: 'example-source', orFallback: true });
    expect(hits.map((hit) => hit.slug)).toEqual(['notes/example']);
    expect(calls.filter((call) => call.terms).map((call) => call.terms)).toEqual(['alpha beta']);
    expect(calls.some((call) => call.query.includes('enable_seqscan'))).toBe(false);
    expect(getSeqscan()).toBe('on');
  });

  test('OR retry uses indexed plan inside scoped transaction and restores setting', async () => {
    const { engine, calls, getSeqscan } = fakeEngine(false);
    const hits = await engine.searchKeyword('alpha beta', { sourceId: 'example-source', orFallback: true });
    expect(hits.map((hit) => hit.slug)).toEqual(['notes/example']);
    const searches = calls.filter((call) => call.terms);
    expect(searches.map((call) => call.terms)).toEqual(['alpha beta', 'alpha OR beta']);
    expect(searches[0].query).toBe(searches[1].query);
    expect(searches[1].query).toContain('p.source_id = $');
    expect(calls.map((call) => call.query).filter((query) => query.includes('enable_seqscan')))
      .toEqual(['SHOW enable_seqscan', 'SET LOCAL enable_seqscan = off', "SELECT set_config('enable_seqscan', ?, true)"]);
    expect(getSeqscan()).toBe('on');
  });

  test('without opt-in, an empty strict result never changes the planner', async () => {
    const { engine, calls } = fakeEngine(false);
    expect(await engine.searchKeyword('alpha beta', { sourceId: 'example-source' })).toEqual([]);
    expect(calls.filter((call) => call.terms).map((call) => call.terms)).toEqual(['alpha beta']);
    expect(calls.some((call) => call.query.includes('enable_seqscan'))).toBe(false);
  });
});
