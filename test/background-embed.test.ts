/**
 * gbrain#1 — BackgroundEmbedder: remote put_page persists and returns; the
 * serve process embeds the page's NULL chunks off the request path.
 *
 * Hermetic: the embed + stale-sweep seams are injected, no engine I/O.
 */
import { describe, expect, test } from 'bun:test';
import { BackgroundEmbedder, type EmbedPagesFn } from '../src/core/background-embed.ts';
import type { BrainEngine } from '../src/core/engine.ts';

const engine = {} as BrainEngine;

function recorder(impl?: (slugs: string[], sourceId: string) => Promise<void>) {
  const calls: Array<{ slugs: string[]; sourceId: string; sig?: string }> = [];
  const fn: EmbedPagesFn = async (_e, slugs, sourceId, opts) => {
    calls.push({ slugs: [...slugs], sourceId, sig: opts.embeddingSignature });
    if (impl) await impl(slugs, sourceId);
    return { embedded: slugs.length, pagesProcessed: slugs.length, aborted: false };
  };
  return { calls, fn };
}

describe('BackgroundEmbedder', () => {
  test('enqueue returns immediately and the page is embedded in the background', async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const { calls, fn } = recorder(() => gate);
    const bg = new BackgroundEmbedder(engine, { embedPages: fn, sweepIntervalMs: 0, logger: () => {} });

    expect(bg.enqueue('notes/a', 'default')).toBe(true);
    // The embed is still blocked on the gate: enqueue did not wait for it.
    expect(bg.stats().running).toBe(true);
    release();
    await bg.drain();

    expect(calls).toEqual([{ slugs: ['notes/a'], sourceId: 'default', sig: calls[0].sig }]);
    const s = bg.stats();
    expect(s.queued).toBe(0);
    expect(s.embedded_chunks).toBe(1);
    expect(s.pages_processed).toBe(1);
    expect(s.failures).toBe(0);
    bg.stop();
  });

  test('dedupes repeated writes to one slug and batches per source', async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    let first = true;
    const { calls, fn } = recorder(async () => {
      if (first) { first = false; await gate; }
    });
    const bg = new BackgroundEmbedder(engine, { embedPages: fn, sweepIntervalMs: 0, logger: () => {} });

    bg.enqueue('warmup', 'default'); // occupies the runner
    bg.enqueue('a', 'default');
    bg.enqueue('a', 'default');
    bg.enqueue('b', 'default');
    bg.enqueue('c', 'other');
    expect(bg.stats().queued).toBe(3); // warmup already dequeued; 'a' deduped
    release();
    await bg.drain();

    expect(calls.map((c) => [c.sourceId, c.slugs])).toEqual([
      ['default', ['warmup']],
      ['default', ['a', 'b']],
      ['other', ['c']],
    ]);
    bg.stop();
  });

  test('an embed failure is contained, counted, and does not stop the queue', async () => {
    const { calls, fn } = recorder(async (slugs) => {
      if (slugs.includes('bad')) throw new Error('provider down');
    });
    const bg = new BackgroundEmbedder(engine, { embedPages: fn, sweepIntervalMs: 0, batchSize: 1, logger: () => {} });
    bg.enqueue('bad', 'default');
    bg.enqueue('good', 'default');
    await bg.drain();

    expect(calls.map((c) => c.slugs[0])).toEqual(['bad', 'good']);
    const s = bg.stats();
    expect(s.failures).toBe(1);
    expect(s.last_error).toBe('provider down');
    expect(s.pages_processed).toBe(1);
    bg.stop();
  });

  test('queue cap drops overflow (left for the stale sweep)', async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const { fn } = recorder(() => gate);
    const bg = new BackgroundEmbedder(engine, { embedPages: fn, sweepIntervalMs: 0, maxQueue: 2, logger: () => {} });
    expect(bg.enqueue('running', 'default')).toBe(true);
    expect(bg.enqueue('q1', 'default')).toBe(true);
    expect(bg.enqueue('q2', 'default')).toBe(true);
    expect(bg.enqueue('q3', 'default')).toBe(false);
    expect(bg.stats().dropped).toBe(1);
    release();
    await bg.drain();
    bg.stop();
  });

  test('sweep enqueues pages that still carry NULL-embedding chunks', async () => {
    const { calls, fn } = recorder();
    const bg = new BackgroundEmbedder(engine, {
      embedPages: fn,
      sweepIntervalMs: 0,
      findStale: async (limit) => {
        expect(limit).toBe(50);
        return [{ slug: 'crashed/before-embed', source_id: 'default' }];
      },
      logger: () => {},
    });
    expect(await bg.sweep()).toBe(1);
    await bg.drain();
    expect(calls[0].slugs).toEqual(['crashed/before-embed']);
    bg.stop();
  });

  test('stop() refuses new work', () => {
    const { fn } = recorder();
    const bg = new BackgroundEmbedder(engine, { embedPages: fn, sweepIntervalMs: 0, logger: () => {} });
    bg.stop();
    expect(bg.enqueue('x', 'default')).toBe(false);
  });
});
