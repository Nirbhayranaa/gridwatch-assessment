import { readFile } from 'node:fs/promises';
import type { Pool } from 'pg';
import { connections, sign } from './http.ts';
import { ingest } from './store.ts';
import type { Pilot } from './store.ts';
import { pollSite } from './poll.ts';
import type { Clock, Transport } from './poll.ts';

export const pilot: Pilot = JSON.parse(await readFile(new URL('../fixtures/pilot.json',import.meta.url),'utf8'));
export const stubs: Record<string, Record<string, unknown>> = JSON.parse(await readFile(new URL('../fixtures/csms.json',import.meta.url),'utf8'));

export function fixtureClock(start = Date.now()): Clock {
  let current = start;
  return { now: () => current, sleep: async ms => { current += ms; } };
}

export function fixtureTransport(ids: string[], calls: URL[] = []): Transport {
  let position = 0;
  return async url => {
    calls.push(new URL(url));
    const id = ids[position++];
    if (!id) throw new Error('fixture sequence exhausted');
    return new Response(JSON.stringify(stubs[id]), {
      status: id === '1c' ? 429 : id === '1d' ? 500 : 200,
      headers: id === '1c' ? { 'retry-after': '30' } : {},
    });
  };
}

export async function replay(pool: Pool, baseUrl: string) {
  for (const scope of Object.values(connections)) {
    const samples = pilot.telemetry_samples.filter(raw => {
      const charger = pilot.chargers.find(c => c.charger_id === (raw as Record<string, unknown>).charger_id);
      return pilot.sites.find(s => s.site_id === charger?.site_id)?.operator_id === scope.operator;
    });
    await ingest(pool, scope, samples);
  }
  // Only the canned transport uses simulated waits; timestamps and freshness stay real.
  await pollSite(pool, connections.in, 'S-IN-0007', fixtureTransport(['1c','1d','1a','1b']), fixtureClock());
  await pollSite(pool, connections.de, 'S-DE-0003', fixtureTransport(['1e']), fixtureClock());
  for (const [id,scope] of [['2a',connections.de],['2b',connections.de],['2c',connections.in],['2d',connections.in]] as const) {
    const raw = JSON.stringify(stubs[id]);
    const response = await fetch(`${baseUrl}/webhooks/${scope.connection}`, {
      method: 'POST', body: raw, headers: { 'content-type': 'application/json', 'x-csms-signature': sign(raw,scope.secret) },
      signal: AbortSignal.timeout(10_000),
    });
    if (!response.ok) throw new Error(`webhook replay failed: ${id} ${response.status}`);
  }
}
