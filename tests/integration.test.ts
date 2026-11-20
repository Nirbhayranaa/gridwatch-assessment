import { before, beforeEach, after, test } from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { Pool } from 'pg';
import { initialize, ingest, summary, diagnostics } from '../src/store.ts';
import { connections, server, sign } from '../src/http.ts';
import { fixtureClock, fixtureTransport, pilot, replay, stubs } from '../src/demo.ts';
import { pollSite } from '../src/poll.ts';

const url = process.env.DATABASE_URL;
assert.ok(url && new URL(url).pathname.endsWith('_test'), 'Use a dedicated _test database; run docker compose --profile test run --rm tests');
const pool = new Pool({connectionString:url,max:10});
const app = server(pool);
let base: string;
const scope = connections.in;
const event = (status: string, ts = '2026-06-09T14:33:40+05:30') => ({
  type:'status',charger_id:'C-IN-0007-A',connector_id:'1',status,ts,
});
async function webhook(events: unknown[], connection = scope, delivery = 'test') {
  const body = JSON.stringify({delivery_id:delivery,events});
  return fetch(`${base}/webhooks/${connection.connection}`, {
    method:'POST',body,headers:{'x-csms-signature':sign(body,connection.secret)},
  });
}
async function get(path: string, token = scope.token) {
  return fetch(`${base}${path}`,{headers:{authorization:`Bearer ${token}`}});
}

before(async () => {
  await initialize(pool,pilot);
  app.listen(0,'127.0.0.1');
  await once(app,'listening');
  const addr = app.address();
  assert.ok(addr && typeof addr !== 'string');
  base = `http://127.0.0.1:${addr.port}`;
});
beforeEach(async () => {
  await pool.query('TRUNCATE events,rejections,poll_checkpoints,connectors,sites');
  await initialize(pool,pilot);
});
after(async () => {
  app.closeAllConnections();
  await new Promise<void>((resolve,reject) => app.close(e => e ? reject(e) : resolve()));
  await pool.end();
});

test('full fixture replay produces the correct aggregate; restarting it changes no facts', async () => {
  await replay(pool,base);
  const response = await get('/v1/site-summary');
  assert.equal(response.status,200);
  const report = await response.json();
  assert.equal(report.sites.length,2);
  const site = report.sites[0];
  assert.deepEqual([site.connectors,site.available,site.charging,site.unknown],[3,2,0,1]);
  assert.equal(site.fresh_available,0, 'dated fixture data must not look live');
  assert.equal(site.stale,2);
  assert.equal(site.location_status,'unresolved');
  assert.equal(site.latitude,null);
  assert.equal(site.longitude,null);
  await assert.rejects(pool.query("UPDATE sites SET latitude=91,longitude=0,location_status='verified' WHERE site_id='S-IN-0007'"), /check constraint/);
  await assert.rejects(pool.query("UPDATE sites SET latitude=0 WHERE site_id='S-IN-0007'"), /check constraint/);
  assert.ok(site.address.includes('Noida'));
  assert.equal((await diagnostics(pool,scope.operator)).stored_events,11);
  assert.equal((await diagnostics(pool,connections.de.operator)).stored_events,2);
  assert.equal((await diagnostics(pool,connections.de.operator)).rejected_events,2);
  assert.equal((await diagnostics(pool,scope.operator)).meter_resets,1);
  await replay(pool,base);
  assert.equal((await diagnostics(pool,scope.operator)).stored_events,11);
  assert.equal((await diagnostics(pool,connections.de.operator)).rejected_events,2);
});

test('429 and 500 preserve the request, obey spacing, and drain pagination before checkpointing', async () => {
  const clock = fixtureClock(Date.parse('2026-06-10T00:00:00Z'));
  const calls: URL[] = [];
  const times: number[] = [];
  const canned = fixtureTransport(['1c','1d','1a','1b'],calls);
  const result = await pollSite(pool,scope,'S-IN-0007',async url => {
    times.push(clock.now());
    return canned(url);
  },clock);
  assert.deepEqual(times.map(t => t-times[0]),[0,30000,60000,90000]);
  assert.equal(calls[0].href,calls[1].href);
  assert.equal(calls[1].href,calls[2].href);
  assert.equal(calls[3].searchParams.get('cursor'),stubs['1a'].next_cursor);
  assert.equal(calls[3].searchParams.get('since'),calls[0].searchParams.get('since'));
  assert.deepEqual(result,{accepted:5,duplicates:0,rejected:0});
  const data = await diagnostics(pool,scope.operator);
  assert.equal(data.poll_checkpoints[0].completed_at.toISOString(),'2026-06-09T09:02:25.000Z');
});

test('failure after page one leaves the scan boundary intact; replay recovers without duplicates', async () => {
  const clock = fixtureClock(Date.parse('2026-06-10T00:00:00Z'));
  await assert.rejects(pollSite(pool,scope,'S-IN-0007',fixtureTransport(['1a','1d','1d','1d']),clock));
  const before = await diagnostics(pool,scope.operator);
  assert.equal(before.stored_events,4);
  assert.equal(before.poll_checkpoints[0].completed_at.getTime(),0);
  const result = await pollSite(pool,scope,'S-IN-0007',fixtureTransport(['1a','1b']),clock);
  assert.deepEqual(result,{accepted:1,duplicates:4,rejected:0});
  assert.equal((await diagnostics(pool,scope.operator)).stored_events,5);
});

test('a second poll worker cannot take the same site while an owner is running', async () => {
  let release!: () => void;
  let entered!: () => void;
  const barrier = new Promise<void>(resolve => { release=resolve; });
  const start = new Promise<void>(resolve => { entered=resolve; });
  const clock = fixtureClock(Date.parse('2026-06-10T00:00:00Z'));
  const first = pollSite(pool,scope,'S-IN-0007',async () => {
    entered(); await barrier;
    return new Response(JSON.stringify(stubs['1b']));
  },clock);
  await start;
  try {
    await assert.rejects(pollSite(pool,scope,'S-IN-0007',fixtureTransport(['1b']),clock),/already has a poll owner/);
  } finally { release(); await first; }
});

test('invalid events are quarantined while valid siblings commit', async () => {
  const good = {...event('Available'),charger_id:'C-DE-0003-A'};
  const result = await ingest(pool,connections.de,[...(stubs['1e'].events as unknown[]),{...good,ts:'2099-01-01T00:00:00Z'},good]);
  assert.deepEqual(result,{accepted:1,duplicates:0,rejected:3});
  assert.equal((await summary(pool,connections.de.operator)).sites[0].available,1);
});

test('newest event wins; equal-time contradictions stay unknown until a newer observation', async () => {
  await ingest(pool,scope,[event('Available'),event('Charging','2026-06-09T14:31:50+05:30')]);
  let site = (await summary(pool,scope.operator)).sites[0];
  assert.equal(site.available,1);
  await ingest(pool,scope,[event('Charging')]);
  site = (await summary(pool,scope.operator)).sites[0];
  assert.equal(site.available,0);
  assert.equal(site.unknown,3);
  await ingest(pool,scope,[event('Available','2026-06-09T14:34:40+05:30')]);
  assert.equal((await summary(pool,scope.operator)).sites[0].available,1);
});

test('older meter readings never replace the latest meter or interfere with the status clock', async () => {
  await ingest(pool,scope,[...(stubs['1a'].events as unknown[])].reverse());
  const row = await pool.query(`SELECT energy_register_wh,power_w,meter_at,status FROM connectors WHERE charger_id='C-IN-0007-A' AND connector_id='1'`);
  assert.equal(row.rows[0].energy_register_wh,'4821667');
  assert.equal(row.rows[0].power_w,41050);
  assert.equal(row.rows[0].meter_at.toISOString(),'2026-06-09T09:02:21.000Z');
  assert.equal(row.rows[0].status,'charging');
});

test('a current snapshot confirms unchanged status without duplicating history or refreshing an older status', async () => {
  const raw = event('Available');
  await ingest(pool,scope,[raw]);
  const now = Date.now();
  const transport = async () => new Response(JSON.stringify({site_id:'S-IN-0007',server_time:new Date(now).toISOString(),events:[raw]}));
  const result = await pollSite(pool,scope,'S-IN-0007',transport,fixtureClock(now));
  assert.equal(result.duplicates,1);
  assert.equal((await summary(pool,scope.operator,new Date(now+1000))).sites[0].fresh_available,1);
  assert.equal((await diagnostics(pool,scope.operator)).stored_events,1);
  await ingest(pool,scope,[event('Charging','2026-06-09T14:34:40+05:30')]);
  await pollSite(pool,scope,'S-IN-0007',transport,fixtureClock(now));
  const row = await pool.query("SELECT status,status_observed_at FROM connectors WHERE charger_id='C-IN-0007-A' AND connector_id='1'");
  assert.equal(row.rows[0].status,'charging');
  assert.equal(row.rows[0].status_observed_at.toISOString(),'2026-06-09T09:04:40.000Z');
});

test('signature tampering and tenant substitution cannot change another operator', async () => {
  const raw = JSON.stringify({delivery_id:'bad',events:[event('Available')]});
  const bad = await fetch(`${base}/webhooks/in`,{method:'POST',body:raw+' ',headers:{'x-csms-signature':sign(raw,scope.secret)}});
  assert.equal(bad.status,401);
  assert.equal((await get('/v1/site-summary','wrong')).status,401);
  assert.equal((await get('/v1/site-summary?operator_id=stadtwerke-ev')).status,400);
  const alien = await webhook([event('Available')],connections.de);
  assert.equal(alien.status,200);
  assert.equal((await alien.json()).rejected,1);
  assert.equal((await diagnostics(pool,scope.operator)).stored_events,0);
  const germany = await get('/v1/site-summary',connections.de.token);
  assert.equal((await germany.json()).sites.length,1);
});

test('concurrent redelivery commits each business event once', async () => {
  const batch = stubs['2a'].events as unknown[];
  const responses = await Promise.all(Array.from({length:4},(_,i) => webhook(batch,connections.de,`delivery-${i}`)));
  assert.ok(responses.every(r => r.status===200));
  assert.equal((await diagnostics(pool,connections.de.operator)).stored_events,2);
});

test('a database failure rolls back the whole webhook and is not acknowledged as success', async () => {
  await pool.query(`CREATE OR REPLACE FUNCTION reject_test_event() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN IF NEW.charger_id='C-IN-0007-B' THEN RAISE EXCEPTION 'injected disk failure'; END IF; RETURN NEW; END $$;
    CREATE TRIGGER injected_failure BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION reject_test_event()`);
  const batch = [event('Available'),{...event('Available'),charger_id:'C-IN-0007-B'}];
  try {
    assert.equal((await webhook(batch)).status,503);
    assert.equal((await diagnostics(pool,scope.operator)).stored_events,0);
    assert.equal((await summary(pool,scope.operator)).sites[0].unknown,3);
  } finally {
    await pool.query('DROP TRIGGER injected_failure ON events; DROP FUNCTION reject_test_event()');
  }
  assert.equal((await webhook(batch)).status,200);
  assert.equal((await diagnostics(pool,scope.operator)).stored_events,2);
});

test('invalid page envelopes and pagination loops never advance the scan boundary', async () => {
  const clock = fixtureClock(Date.parse('2026-06-10T00:00:00Z'));
  await assert.rejects(pollSite(pool,scope,'S-IN-0007',async () => new Response(JSON.stringify({...stubs['1a'],site_id:'wrong'})),clock),/invalid poll page/);
  await assert.rejects(pollSite(pool,scope,'S-IN-0007',fixtureTransport(['1a','1a']),clock),/repeated pagination cursor/);
  assert.equal((await diagnostics(pool,scope.operator)).poll_checkpoints[0].completed_at.getTime(),0);
});
