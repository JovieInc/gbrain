/**
 * gbrain#1: in-process background embedder for long-lived servers.
 *
 * `put_page` over MCP used to embed every chunk inline, so an agent's receipt
 * write paid the embedding provider plus the chunk upserts before it could
 * return (p50 ~13s, p95 ~40s against a remote pooler). When `gbrain serve`
 * starts this embedder, remote put_page writes persist the page with
 * `embedding IS NULL` chunks (the #4216 deferEmbeds contract) and enqueue the
 * slug here. The embedder drains the queue serially through
 * `embedStalePages` — the same per-page mechanics the dream synthesize phase
 * uses for its deferred writes, including signature stamping.
 *
 * Durability: the page is committed before the response; embeddings are the
 * only deferred work. A crash or a failed embed leaves NULL chunks, which the
 * periodic sweep below (and the existing `embed --stale` / embed-backfill
 * machinery) picks up. Readers already tolerate NULL chunks: keyword search
 * matches them immediately, vector search skips them until embedded.
 */
import type { BrainEngine } from './engine.ts';
import { embedStalePages } from './embed-stale.ts';
import { currentEmbeddingSignature } from './embedding.ts';
import { resolveActiveEmbeddingColumnFromEngine, quoteIdentifier } from './search/embedding-column.ts';

export type EmbedPagesFn = (
  engine: BrainEngine,
  slugs: string[],
  sourceId: string,
  opts: { signal?: AbortSignal; embeddingSignature?: string },
) => Promise<{ embedded: number; pagesProcessed: number; aborted: boolean }>;

export interface BackgroundEmbedderOptions {
  /** Override the embed implementation (tests). Defaults to embedStalePages. */
  embedPages?: EmbedPagesFn;
  /** Override the stale sweep query (tests). Returns (slug, source_id) pairs. */
  findStale?: (limit: number) => Promise<Array<{ slug: string; source_id: string }>>;
  /** Max slugs per embed call. Default 16. */
  batchSize?: number;
  /** Queue cap; overflow is dropped and left to the sweep. Default 2000. */
  maxQueue?: number;
  /** Stale-sweep interval; 0 disables. Default 5 min. */
  sweepIntervalMs?: number;
  /** Pages enqueued per sweep. Default 50. */
  sweepLimit?: number;
  /** Per-batch time budget. Default 120s. */
  batchTimeoutMs?: number;
  logger?: (msg: string) => void;
}

export interface BackgroundEmbedderStats {
  queued: number;
  running: boolean;
  embedded_chunks: number;
  pages_processed: number;
  batches: number;
  failures: number;
  dropped: number;
  last_error: string | null;
  last_drain_ms: number | null;
  oldest_queued_age_ms: number | null;
}

export class BackgroundEmbedder {
  private queue = new Map<string, { slug: string; sourceId: string; at: number }>();
  private running: Promise<void> | null = null;
  private sweepTimer: ReturnType<typeof setInterval> | null = null;
  private stopped = false;
  private readonly opts: Required<Omit<BackgroundEmbedderOptions, 'embedPages' | 'findStale'>> &
    Pick<BackgroundEmbedderOptions, 'embedPages' | 'findStale'>;
  private readonly counters = {
    embedded_chunks: 0,
    pages_processed: 0,
    batches: 0,
    failures: 0,
    dropped: 0,
    last_error: null as string | null,
    last_drain_ms: null as number | null,
  };

  constructor(private readonly engine: BrainEngine, opts: BackgroundEmbedderOptions = {}) {
    this.opts = {
      batchSize: opts.batchSize ?? 16,
      maxQueue: opts.maxQueue ?? 2000,
      sweepIntervalMs: opts.sweepIntervalMs ?? 5 * 60_000,
      sweepLimit: opts.sweepLimit ?? 50,
      batchTimeoutMs: opts.batchTimeoutMs ?? 120_000,
      logger: opts.logger ?? ((m: string) => process.stderr.write(`${m}\n`)),
      embedPages: opts.embedPages,
      findStale: opts.findStale,
    };
    if (this.opts.sweepIntervalMs > 0) {
      this.sweepTimer = setInterval(() => { void this.sweep(); }, this.opts.sweepIntervalMs);
      (this.sweepTimer as { unref?: () => void }).unref?.();
    }
  }

  /** Queue a page for embedding. Returns false when dropped (full / stopped). */
  enqueue(slug: string, sourceId: string): boolean {
    if (this.stopped) return false;
    const key = `${sourceId}\u0000${slug}`;
    if (!this.queue.has(key)) {
      if (this.queue.size >= this.opts.maxQueue) {
        this.counters.dropped++;
        return false;
      }
      this.queue.set(key, { slug, sourceId, at: Date.now() });
    }
    this.kick();
    return true;
  }

  /** Resolves once the queue is empty (tests, graceful shutdown). */
  async drain(): Promise<void> {
    while (this.running) await this.running;
  }

  stop(): void {
    this.stopped = true;
    if (this.sweepTimer) clearInterval(this.sweepTimer);
    this.sweepTimer = null;
  }

  stats(): BackgroundEmbedderStats {
    let oldest: number | null = null;
    for (const item of this.queue.values()) oldest = oldest === null ? item.at : Math.min(oldest, item.at);
    return {
      queued: this.queue.size,
      running: this.running !== null,
      ...this.counters,
      oldest_queued_age_ms: oldest === null ? null : Date.now() - oldest,
    };
  }

  /** Enqueue pages that still have NULL-embedding chunks. */
  async sweep(): Promise<number> {
    if (this.stopped) return 0;
    try {
      const rows = await (this.opts.findStale ?? ((limit) => defaultFindStale(this.engine, limit)))(this.opts.sweepLimit);
      let n = 0;
      for (const r of rows) if (this.enqueue(r.slug, r.source_id)) n++;
      return n;
    } catch (e) {
      this.counters.last_error = `sweep: ${e instanceof Error ? e.message : String(e)}`;
      return 0;
    }
  }

  private kick(): void {
    if (this.running || this.stopped) return;
    this.running = this.loop().finally(() => { this.running = null; });
  }

  private async loop(): Promise<void> {
    while (this.queue.size > 0 && !this.stopped) {
      const started = Date.now();
      // One source per batch; embedStalePages is source-scoped.
      const first = this.queue.values().next().value!;
      const batch: string[] = [];
      for (const [key, item] of this.queue) {
        if (item.sourceId !== first.sourceId) continue;
        batch.push(item.slug);
        this.queue.delete(key);
        if (batch.length >= this.opts.batchSize) break;
      }
      try {
        const embedPages = this.opts.embedPages ?? (embedStalePages as EmbedPagesFn);
        const sig = currentEmbeddingSignature();
        const res = await embedPages(this.engine, batch, first.sourceId, {
          signal: AbortSignal.timeout(this.opts.batchTimeoutMs),
          ...(sig !== null ? { embeddingSignature: sig } : {}),
        });
        this.counters.embedded_chunks += res.embedded;
        this.counters.pages_processed += res.pagesProcessed;
      } catch (e) {
        // Chunks stay NULL; the sweep retries later. Never rethrow — this
        // runs detached from any request.
        this.counters.failures++;
        this.counters.last_error = e instanceof Error ? e.message : String(e);
        this.opts.logger(`[background-embed] batch of ${batch.length} failed: ${this.counters.last_error}`);
      }
      this.counters.batches++;
      this.counters.last_drain_ms = Date.now() - started;
    }
  }
}

async function defaultFindStale(engine: BrainEngine, limit: number): Promise<Array<{ slug: string; source_id: string }>> {
  const col = quoteIdentifier((await resolveActiveEmbeddingColumnFromEngine(engine, { fallbackToLegacy: true })).name);
  return engine.executeRaw<{ slug: string; source_id: string }>(
    `SELECT DISTINCT p.slug, p.source_id
       FROM content_chunks cc JOIN pages p ON p.id = cc.page_id
      WHERE cc.${col} IS NULL
        AND p.deleted_at IS NULL
        AND NOT jsonb_exists(COALESCE(p.frontmatter, '{}'::jsonb), 'embed_skip')
      LIMIT $1`,
    [limit],
  );
}

let active: BackgroundEmbedder | null = null;

/** Start the process-wide embedder (idempotent). Called by `gbrain serve`. */
export function startBackgroundEmbedder(engine: BrainEngine, opts?: BackgroundEmbedderOptions): BackgroundEmbedder {
  if (!active) active = new BackgroundEmbedder(engine, opts);
  return active;
}

export function getBackgroundEmbedder(): BackgroundEmbedder | null {
  return active;
}

export function stopBackgroundEmbedder(): void {
  active?.stop();
  active = null;
}
