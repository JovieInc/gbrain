/**
 * gbrain#1 — put_page over a remote transport returns once the page is
 * persisted when the serve-side BackgroundEmbedder is running; chunks land
 * `embedding IS NULL` and are embedded off the request path. Without the
 * embedder (CLI, stdio, tests) put_page keeps embedding inline.
 */
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { resetPgliteState } from './helpers/reset-pglite.ts';
import { dispatchToolCall } from '../src/mcp/dispatch.ts';
import { configureGateway, resetGateway, __setEmbedTransportForTests } from '../src/core/ai/gateway.ts';
import { startBackgroundEmbedder, stopBackgroundEmbedder, getBackgroundEmbedder } from '../src/core/background-embed.ts';

let engine: PGLiteEngine;
let embedCalls = 0;

beforeAll(async () => {
  engine = new PGLiteEngine();
  await engine.connect({ database_url: '' });
  await engine.initSchema();
}, 60_000);

afterAll(async () => {
  await engine.disconnect();
});

beforeEach(async () => {
  await resetPgliteState(engine);
  embedCalls = 0;
  const dims = Number((await engine.getConfig('embedding_dimensions')) ?? 1536);
  configureGateway({
    embedding_model: 'openai:text-embedding-3-small',
    embedding_dimensions: dims,
    env: { OPENAI_API_KEY: 'sk-fake' },
  });
  __setEmbedTransportForTests((async (args: { values: string[] }) => {
    embedCalls++;
    return { embeddings: args.values.map(() => Array.from({ length: dims }, () => 0.01)) };
  }) as never);
});

afterEach(() => {
  stopBackgroundEmbedder();
  __setEmbedTransportForTests(null);
  resetGateway();
});

async function nullChunks(slug: string): Promise<number> {
  const rows = await engine.executeRaw<{ n: number }>(
    `SELECT count(*)::int AS n FROM content_chunks cc JOIN pages p ON p.id = cc.page_id
      WHERE p.slug = $1 AND cc.embedding IS NULL`,
    [slug],
  );
  return rows[0].n;
}

function putPage(slug: string) {
  return dispatchToolCall(
    engine,
    'put_page',
    { slug, content: `---\ntitle: ${slug}\ntype: note\n---\n\nA receipt an agent wrote about ${slug}.\n` },
    { remote: true, sourceId: 'default' },
  );
}

describe('put_page with the serve background embedder', () => {
  test('returns with embedding queued; chunks embed after the response', async () => {
    const bg = startBackgroundEmbedder(engine, { sweepIntervalMs: 0, logger: () => {} });
    // Hold the embed until we have inspected the post-response state.
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    __setEmbedTransportForTests((async (args: { values: string[] }) => {
      await gate;
      embedCalls++;
      const dims = Number((await engine.getConfig('embedding_dimensions')) ?? 1536);
      return { embeddings: args.values.map(() => Array.from({ length: dims }, () => 0.01)) };
    }) as never);

    const res = await putPage('ops/receipts/a');
    expect(res.isError).toBeFalsy();
    const body = JSON.parse(res.content[0].text);
    expect(body.status).toBe('created_or_updated');
    expect(body.embedding).toBe('queued');
    expect(body.chunks).toBeGreaterThan(0);
    // Persisted and readable before any embedding happened.
    expect((await engine.getPage('ops/receipts/a'))?.title).toBe('ops/receipts/a');
    expect(await nullChunks('ops/receipts/a')).toBe(body.chunks);
    expect(embedCalls).toBe(0);

    release();
    await bg.drain();
    expect(embedCalls).toBe(1);
    expect(await nullChunks('ops/receipts/a')).toBe(0);
  });

  test('without the embedder, put_page embeds inline (unchanged behavior)', async () => {
    expect(getBackgroundEmbedder()).toBeNull();
    const res = await putPage('ops/receipts/b');
    const body = JSON.parse(res.content[0].text);
    expect(body.embedding).toBeUndefined();
    expect(embedCalls).toBe(1);
    expect(await nullChunks('ops/receipts/b')).toBe(0);
  });

  test('trusted local callers keep inline embedding even with the embedder running', async () => {
    startBackgroundEmbedder(engine, { sweepIntervalMs: 0, logger: () => {} });
    const res = await dispatchToolCall(
      engine,
      'put_page',
      { slug: 'ops/receipts/c', content: '---\ntitle: c\ntype: note\n---\n\nlocal write.\n' },
      { remote: false, sourceId: 'default' },
    );
    const body = JSON.parse(res.content[0].text);
    expect(body.embedding).toBeUndefined();
    expect(await nullChunks('ops/receipts/c')).toBe(0);
  });
});
