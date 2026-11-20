import { test } from 'node:test';
import assert from 'node:assert/strict';
import { normalize, timestamp, fingerprint } from '../src/normalize.ts';
import { retryDelay } from '../src/poll.ts';
import { stubs } from '../src/demo.ts';

test('offset timestamps normalize to the same instant; lifetime Wh is not session energy', () => {
  assert.equal(timestamp('2026-06-09T14:32:11+05:30'), '2026-06-09T09:02:11.000Z');
  assert.equal(timestamp('2026-06-09T23:14:58+02:00'), '2026-06-09T21:14:58.000Z');
  const raw = (stubs['1a'].events as unknown[])[1] as Record<string, unknown>;
  const event = normalize(raw);
  assert.equal(event.values.energy_register_wh, 4821553);
  const reordered = { ts: event.occurred_at, power_w: 41200, energy_register_wh: 4821553,
    connector_id: '1', charger_id: 'C-IN-0007-A', type: 'meter_value', delivery_id: 'different' };
  assert.equal(fingerprint(normalize(reordered)), fingerprint(event));
});

test('missing fields, null meters, impossible dates and missing offsets are rejected', () => {
  for (const raw of stubs['1e'].events as unknown[]) assert.throws(() => normalize(raw));
  for (const ts of ['2026-02-30T10:00:00Z','2026-06-09T10:00:00','2026-06-09T24:00:00Z']) {
    assert.throws(() => timestamp(ts));
  }
});

test('session energy uses register differences; a reset remains an exception', () => {
  const stopped = normalize((stubs['2a'].events as unknown[])[1]);
  assert.equal(stopped.values.energy_wh, 41420);
  const reset = normalize((stubs['2d'].events as unknown[])[0]);
  assert.equal(reset.values.energy_wh, null);
  assert.equal(reset.values.quality, 'meter_reset');
});

test('Retry-After supports seconds and HTTP dates, with a safe fallback', () => {
  const now = Date.parse('2026-06-10T00:00:00Z');
  assert.equal(retryDelay(new Response('', {headers:{'retry-after':'60'}}),{},now),60000);
  assert.equal(retryDelay(new Response('', {headers:{'retry-after':'Wed, 10 Jun 2026 00:02:00 GMT'}}),{},now),120000);
  assert.equal(retryDelay(new Response('', {headers:{'retry-after':'invalid'}}),{retry_after_seconds:45},now),45000);
});
