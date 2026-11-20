CREATE TABLE IF NOT EXISTS sites (
  operator_id text NOT NULL,
  site_id text NOT NULL,
  name text NOT NULL,
  address text NOT NULL,
  country text NOT NULL CHECK (country IN ('IN', 'DE')),
  latitude double precision,
  longitude double precision,
  location_status text NOT NULL DEFAULT 'unresolved' CHECK (location_status IN ('unresolved', 'verified')),
  PRIMARY KEY (operator_id, site_id),
  CHECK ((latitude IS NULL AND longitude IS NULL AND location_status = 'unresolved') OR
         (latitude IS NOT NULL AND longitude IS NOT NULL AND latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180 AND location_status = 'verified'))
);
CREATE TABLE IF NOT EXISTS connectors (
  operator_id text NOT NULL,
  site_id text NOT NULL,
  charger_id text NOT NULL,
  connector_id text NOT NULL,
  status text NOT NULL DEFAULT 'unknown',
  status_at timestamptz,
  status_observed_at timestamptz,
  status_conflict boolean NOT NULL DEFAULT false,
  energy_register_wh bigint,
  power_w double precision,
  meter_at timestamptz,
  meter_conflict boolean NOT NULL DEFAULT false,
  PRIMARY KEY (operator_id, charger_id, connector_id),
  FOREIGN KEY (operator_id, site_id) REFERENCES sites
);
CREATE TABLE IF NOT EXISTS events (
  operator_id text NOT NULL,
  connection_id text NOT NULL,
  fingerprint text NOT NULL,
  charger_id text NOT NULL,
  connector_id text NOT NULL,
  type text NOT NULL,
  occurred_at timestamptz NOT NULL,
  received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  payload jsonb NOT NULL,
  raw jsonb NOT NULL,
  PRIMARY KEY (operator_id, connection_id, fingerprint),
  FOREIGN KEY (operator_id, charger_id, connector_id) REFERENCES connectors
);
CREATE TABLE IF NOT EXISTS rejections (
  operator_id text NOT NULL,
  connection_id text NOT NULL,
  fingerprint text NOT NULL,
  reason text NOT NULL,
  raw jsonb NOT NULL,
  received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (operator_id, connection_id, fingerprint)
);
CREATE TABLE IF NOT EXISTS poll_checkpoints (
  operator_id text NOT NULL,
  connection_id text NOT NULL,
  site_id text NOT NULL,
  completed_at timestamptz NOT NULL DEFAULT '1970-01-01T00:00:00Z',
  next_allowed_at timestamptz NOT NULL DEFAULT '1970-01-01T00:00:00Z',
  PRIMARY KEY (operator_id, connection_id, site_id),
  FOREIGN KEY (operator_id, site_id) REFERENCES sites
);
CREATE INDEX IF NOT EXISTS events_connector_time ON events (operator_id, charger_id, connector_id, occurred_at);
ALTER TABLE connectors ADD COLUMN IF NOT EXISTS status_observed_at timestamptz;
UPDATE connectors SET status_observed_at=status_at WHERE status_observed_at IS NULL AND status_at IS NOT NULL;
