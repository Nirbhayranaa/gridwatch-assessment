import { readFile } from 'node:fs/promises';
import { Pool } from 'pg';
import { fingerprint, InvalidEvent, normalize } from './normalize.ts';

export type Scope = { operator: string; connection: string };
export type Checkpoint = { site: string; completed: string };
export type Pilot = {
  sites: { operator_id: string; site_id: string; name: string; address: string; country: string }[];
  chargers: { charger_id: string; site_id: string; connectors: string[] }[];
  telemetry_samples: unknown[];
};

export async function initialize(pool: Pool, pilot: Pilot): Promise<void> {
  await pool.query(await readFile(new URL('./schema.sql', import.meta.url), 'utf8'));
  const db = await pool.connect();
  try {
    await db.query('BEGIN');
    for (const s of pilot.sites) {
      await db.query(`INSERT INTO sites(operator_id,site_id,name,address,country) VALUES($1,$2,$3,$4,$5)
        ON CONFLICT (operator_id,site_id) DO NOTHING`, [s.operator_id,s.site_id,s.name,s.address,s.country]);
    }
    for (const c of pilot.chargers) {
      const site = pilot.sites.find(s => s.site_id === c.site_id);
      if (!site) throw new Error('charger has no site');
      for (const connector of c.connectors) {
        await db.query(`INSERT INTO connectors(operator_id,site_id,charger_id,connector_id) VALUES($1,$2,$3,$4)
          ON CONFLICT DO NOTHING`, [site.operator_id,c.site_id,c.charger_id,connector]);
      }
    }
    await db.query('COMMIT');
  } catch (error) {
    await db.query('ROLLBACK');
    throw error;
  } finally { db.release(); }
}

export async function ingest(pool: Pool, scope: Scope, inputs: unknown[], site?: string, checkpoint?: Checkpoint, observedAt?: string) {
  const db = await pool.connect();
  const result = { accepted: 0, duplicates: 0, rejected: 0 };
  try {
    await db.query('BEGIN');
    for (const input of inputs) {
      let event;
      let reason;
      try {
        event = normalize(input);
        if (Date.parse(event.occurred_at) > Date.now() + 300_000) throw new InvalidEvent('event clock is too far ahead');
      }
      catch (error) {
        if (!(error instanceof InvalidEvent)) throw error;
        reason = error.message;
      }
      if (event) {
        const known = await db.query(`SELECT site_id FROM connectors
          WHERE operator_id=$1 AND charger_id=$2 AND connector_id=$3`, [scope.operator,event.charger_id,event.connector_id]);
        if (!known.rowCount || (site && known.rows[0].site_id !== site)) reason = 'connector is outside this connection or poll site';
      }
      if (reason || !event) {
        await db.query(`INSERT INTO rejections(operator_id,connection_id,fingerprint,reason,raw)
          VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,
        [scope.operator,scope.connection,fingerprint(input,reason),reason,JSON.stringify(input)]);
        result.rejected++;
        continue;
      }
      const hash = fingerprint(scope.operator,scope.connection,event);
      const inserted = await db.query(`INSERT INTO events(operator_id,connection_id,fingerprint,charger_id,connector_id,type,occurred_at,payload,raw)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING RETURNING fingerprint`,
      [scope.operator,scope.connection,hash,event.charger_id,event.connector_id,event.type,event.occurred_at,event.values,JSON.stringify(input)]);
      if (event.type === 'status' && observedAt) {
        await db.query(`UPDATE connectors SET status_observed_at=GREATEST(status_observed_at,$6::timestamptz)
          WHERE operator_id=$1 AND charger_id=$2 AND connector_id=$3 AND status_at=$4 AND status=$5 AND NOT status_conflict`,
        [scope.operator,event.charger_id,event.connector_id,event.occurred_at,event.values.status,observedAt]);
      }
      if (!inserted.rowCount) { result.duplicates++; continue; }
      if (event.type === 'status') {
        await db.query(`UPDATE connectors SET
          status=CASE WHEN status_at IS NULL OR $4::timestamptz > status_at THEN $5 ELSE 'unknown' END,
          status_conflict=CASE WHEN status_at IS NULL OR $4::timestamptz > status_at THEN false ELSE true END,
          status_at=$4, status_observed_at=GREATEST($4::timestamptz,$6::timestamptz)
          WHERE operator_id=$1 AND charger_id=$2 AND connector_id=$3 AND
          (status_at IS NULL OR $4::timestamptz > status_at OR
            ($4::timestamptz=status_at AND status<>$5 AND NOT status_conflict))`,
        [scope.operator,event.charger_id,event.connector_id,event.occurred_at,event.values.status,observedAt ?? event.occurred_at]);
      }
      if (event.type === 'meter_value') {
        await db.query(`UPDATE connectors SET
          energy_register_wh=CASE WHEN meter_at IS NULL OR $4::timestamptz > meter_at THEN $5::bigint ELSE NULL END,
          power_w=CASE WHEN meter_at IS NULL OR $4::timestamptz > meter_at THEN $6::double precision ELSE NULL END,
          meter_conflict=CASE WHEN meter_at IS NULL OR $4::timestamptz > meter_at THEN false ELSE true END,
          meter_at=$4
          WHERE operator_id=$1 AND charger_id=$2 AND connector_id=$3 AND
          (meter_at IS NULL OR $4::timestamptz > meter_at OR
            ($4::timestamptz=meter_at AND (energy_register_wh<>$5 OR power_w<>$6) AND NOT meter_conflict))`,
        [scope.operator,event.charger_id,event.connector_id,event.occurred_at,event.values.energy_register_wh,event.values.power_w]);
      }
      result.accepted++;
    }
    if (checkpoint) {
      await db.query(`UPDATE poll_checkpoints SET completed_at=GREATEST(completed_at,$4::timestamptz)
        WHERE operator_id=$1 AND connection_id=$2 AND site_id=$3`,
      [scope.operator,scope.connection,checkpoint.site,checkpoint.completed]);
    }
    await db.query('COMMIT');
    return result;
  } catch (error) {
    await db.query('ROLLBACK');
    throw error;
  } finally { db.release(); }
}

export async function summary(pool: Pool, operator: string, now = new Date()) {
  const result = await pool.query(`SELECT s.site_id,s.name,s.address,s.country,s.latitude,s.longitude,s.location_status,
    count(*)::int AS connectors,
    count(*) FILTER(WHERE c.status='available')::int AS available,
    count(*) FILTER(WHERE c.status='charging')::int AS charging,
    count(*) FILTER(WHERE c.status='faulted')::int AS faulted,
    count(*) FILTER(WHERE c.status NOT IN ('available','charging','faulted','unknown'))::int AS other,
    count(*) FILTER(WHERE c.status='unknown')::int AS unknown,
    count(*) FILTER(WHERE c.status_observed_at IS NOT NULL AND c.status_observed_at < $2::timestamptz - interval '90 seconds')::int AS stale,
    count(*) FILTER(WHERE c.status='available' AND NOT c.status_conflict AND c.status_observed_at >= $2::timestamptz - interval '90 seconds'
      AND c.status_observed_at <= $2::timestamptz)::int AS fresh_available,
    max(c.status_at) AS latest_status_at, max(c.status_observed_at) AS latest_observation_at
    FROM sites s JOIN connectors c USING(operator_id,site_id)
    WHERE s.operator_id=$1 GROUP BY s.operator_id,s.site_id ORDER BY s.site_id`, [operator,now]);
  return { operator_id: operator, as_of: now.toISOString(), freshness_seconds: 90, sites: result.rows };
}

export async function diagnostics(pool: Pool, operator: string) {
  const counts = await pool.query(`SELECT
    (SELECT count(*)::int FROM events WHERE operator_id=$1) AS stored_events,
    (SELECT count(*)::int FROM rejections WHERE operator_id=$1) AS rejected_events,
    (SELECT count(*)::int FROM events WHERE operator_id=$1 AND payload->>'quality'='meter_reset') AS meter_resets`, [operator]);
  const checkpoints = await pool.query(`SELECT site_id,completed_at,next_allowed_at FROM poll_checkpoints WHERE operator_id=$1 ORDER BY site_id`, [operator]);
  return { ...counts.rows[0], poll_checkpoints: checkpoints.rows };
}
