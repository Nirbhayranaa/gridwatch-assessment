import type { Pool } from 'pg';
import { ingest } from './store.ts';
import type { Scope } from './store.ts';
import { InvalidEvent, object, timestamp } from './normalize.ts';

export type Clock = { now(): number; sleep(ms: number): Promise<void> };
export const realClock: Clock = { now: Date.now, sleep: ms => new Promise(resolve => setTimeout(resolve, ms)) };
export type Transport = (url: URL) => Promise<Response>;

export function retryDelay(response: Response, body: Record<string, unknown>, now: number): number {
  const header = response.headers.get('retry-after');
  if (header && /^\d+(\.\d+)?$/.test(header)) return Number(header) * 1000;
  if (header && Number.isFinite(Date.parse(header))) return Math.max(0, Date.parse(header) - now);
  const seconds = body.retry_after_seconds;
  return typeof seconds === 'number' && Number.isFinite(seconds) && seconds >= 0 ? seconds * 1000 : 30_000;
}

export async function pollSite(pool: Pool, scope: Scope, site: string, transport: Transport, clock: Clock = realClock) {
  const owner = await pool.connect();
  const key = JSON.stringify([scope.operator, scope.connection, site]);
  try {
    const lease = await owner.query('SELECT pg_try_advisory_lock(hashtextextended($1,0)) AS owned', [key]);
    if (!lease.rows[0].owned) throw new Error('site already has a poll owner');
    await owner.query(`INSERT INTO poll_checkpoints(operator_id,connection_id,site_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`,
      [scope.operator,scope.connection,site]);
    const saved = await owner.query(`SELECT completed_at FROM poll_checkpoints WHERE operator_id=$1 AND connection_id=$2 AND site_id=$3`,
      [scope.operator,scope.connection,site]);
    const since = new Date(Math.max(0, saved.rows[0].completed_at.getTime() - 300_000)).toISOString();
    let cursor: string | undefined;
    let boundary: string | undefined;
    let failures = 0;
    const seen = new Set<string>();
    const totals = { accepted: 0, duplicates: 0, rejected: 0 };
    for (let page = 0; page < 100;) {
      const slot = await owner.query(`SELECT next_allowed_at FROM poll_checkpoints WHERE operator_id=$1 AND connection_id=$2 AND site_id=$3`,
        [scope.operator,scope.connection,site]);
      await clock.sleep(Math.max(0, slot.rows[0].next_allowed_at.getTime() - clock.now()));
      await owner.query(`UPDATE poll_checkpoints SET next_allowed_at=$4 WHERE operator_id=$1 AND connection_id=$2 AND site_id=$3`,
        [scope.operator,scope.connection,site,new Date(clock.now() + 30_000)]);
      const url = new URL('https://csms.fixture/v1/chargers');
      url.searchParams.set('site_id', site);
      url.searchParams.set('since', since);
      if (cursor) url.searchParams.set('cursor', cursor);
      let response: Response;
      try { response = await transport(url); }
      catch {
        failures++;
        if (failures >= 3) throw new Error('poll transport failed after three attempts');
        await clock.sleep(Math.min(30_000, 1000 * 2 ** (failures - 1)));
        continue;
      }
      console.log(JSON.stringify({ event: 'poll_response', site, status: response.status }));
      if (response.status === 429 || response.status >= 500) {
        const raw = await response.json().catch(() => ({}));
        const body = raw && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : {};
        const wait = response.status === 429 ? retryDelay(response, body, clock.now()) : 1000 * 2 ** failures;
        await owner.query(`UPDATE poll_checkpoints SET next_allowed_at=GREATEST(next_allowed_at,$4::timestamptz)
          WHERE operator_id=$1 AND connection_id=$2 AND site_id=$3`,
        [scope.operator,scope.connection,site,new Date(clock.now() + wait)]);
        if (++failures >= 3) throw new Error(`poll failed after three attempts: HTTP ${response.status}`);
        continue;
      }
      if (!response.ok) throw new Error(`non-retryable vendor response: HTTP ${response.status}`);
      const body = object(await response.json());
      if (body.site_id !== site || !Array.isArray(body.events) || body.events.length > 1000) throw new InvalidEvent('invalid poll page');
      const serverTime = timestamp(body.server_time);
      if (new Date(serverTime).getTime() > clock.now() + 300_000) throw new InvalidEvent('vendor clock is too far ahead');
      if (body.next_cursor !== undefined && (typeof body.next_cursor !== 'string' || !body.next_cursor || body.next_cursor.length > 1000)) throw new InvalidEvent('invalid cursor');
      const next = body.next_cursor as string | undefined;
      if (next && seen.has(next)) throw new InvalidEvent('repeated pagination cursor');
      if (next) seen.add(next);
      boundary ??= serverTime;
      const stored = await ingest(pool, scope, body.events, site, next ? undefined : { site, completed: boundary }, serverTime);
      for (const k of ['accepted','duplicates','rejected'] as const) totals[k] += stored[k];
      if (!next) return totals;
      cursor = next;
      failures = 0;
      page++;
    }
    throw new InvalidEvent('pagination exceeded 100 pages');
  } finally {
    // A dedicated connection owns the lock; closing it also releases ownership on errors.
    owner.release(true);
  }
}
