import { createHash } from 'node:crypto';

export class InvalidEvent extends Error {}
export type Json = Record<string, unknown>;
export type Event = {
  charger_id: string;
  connector_id: string;
  type: 'status' | 'meter_value' | 'session' | 'fault';
  occurred_at: string;
  values: Json;
};

export function object(value: unknown): Json {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new InvalidEvent('expected an object');
  return value as Json;
}

function text(value: unknown, field: string): string {
  if (typeof value !== 'string' || !value.trim() || value.length > 200) throw new InvalidEvent(`invalid ${field}`);
  return value;
}

function energy(value: unknown, field: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) throw new InvalidEvent(`invalid ${field}`);
  return value;
}

export function timestamp(value: unknown): string {
  const ts = text(value, 'timestamp');
  const m = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d{1,3})?(Z|[+-](\d{2}):(\d{2}))$/.exec(ts);
  if (!m || (m[4] && (+m[4] > 23 || +m[5] > 59))) throw new InvalidEvent('timestamp needs a valid UTC offset');
  const wall = new Date(`${m[1]}${m[2] ?? ''}Z`);
  if (!Number.isFinite(wall.getTime()) || wall.toISOString().slice(0, 19) !== m[1]) throw new InvalidEvent('invalid calendar time');
  const instant = new Date(ts);
  if (!Number.isFinite(instant.getTime())) throw new InvalidEvent('invalid timestamp');
  return instant.toISOString();
}

const statuses: Record<string, string> = {
  Available: 'available', Charging: 'charging', Faulted: 'faulted', Unavailable: 'unavailable',
  Preparing: 'preparing', Finishing: 'finishing', Reserved: 'reserved',
  SuspendedEV: 'suspended', SuspendedEVSE: 'suspended', Unknown: 'unknown',
};

export function normalize(input: unknown): Event {
  const raw = object(input);
  const charger_id = text(raw.charger_id, 'charger_id');
  const connector_id = text(raw.connector_id, 'connector_id');
  const occurred_at = timestamp(raw.ts);
  let values: Json;
  switch (raw.type) {
    case 'status': {
      const vendorStatus = text(raw.status, 'status');
      if (!Object.hasOwn(statuses, vendorStatus)) throw new InvalidEvent('unsupported status');
      values = { status: statuses[vendorStatus] };
      break;
    }
    case 'meter_value':
      if (typeof raw.power_w !== 'number' || !Number.isFinite(raw.power_w) || raw.power_w < 0) throw new InvalidEvent('invalid power_w');
      values = { energy_register_wh: energy(raw.energy_register_wh, 'energy_register_wh'), power_w: raw.power_w };
      break;
    case 'session': {
      if (raw.event !== 'session.start' && raw.event !== 'session.stop') throw new InvalidEvent('unsupported session event');
      const start = energy(raw.start_meter_wh, 'start_meter_wh');
      values = { event: raw.event, session_id: text(raw.session_id, 'session_id'), start_meter_wh: start };
      if (raw.event === 'session.stop') {
        const stop = energy(raw.stop_meter_wh, 'stop_meter_wh');
        values.stop_meter_wh = stop;
        values.energy_wh = stop >= start ? stop - start : null;
        values.quality = stop >= start ? 'valid' : 'meter_reset';
      }
      break;
    }
    case 'fault':
      values = { code: text(raw.code, 'code'), severity: text(raw.severity, 'severity') };
      break;
    default:
      throw new InvalidEvent('unsupported event type');
  }
  return { charger_id, connector_id, type: raw.type as Event['type'], occurred_at, values };
}

export function fingerprint(...parts: unknown[]): string {
  return createHash('sha256').update(JSON.stringify(parts)).digest('hex');
}
