CREATE TABLE events (
  event_id UUID NOT NULL, 
  ts TIMESTAMPTZ NOT NULL, 
  client_id TEXT NOT NULL,
  src_ip INET, 
  method TEXT, 
  domain TEXT, 
  url TEXT, 
  status INT,
  bytes_out BIGINT, 
  bytes_in BIGINT, 
  action TEXT, 
  category TEXT, 
  auth_result TEXT,
  PRIMARY KEY (event_id, ts)
);
SELECT create_hypertable('events', 'ts');
CREATE INDEX ON events (client_id, ts DESC);
CREATE INDEX ON events (domain, ts DESC);

CREATE TABLE alerts (
  id BIGSERIAL, 
  ts TIMESTAMPTZ NOT NULL, 
  type TEXT NOT NULL, 
  severity TEXT NOT NULL,
  client_id TEXT, 
  dedupe_key TEXT NOT NULL, 
  details JSONB,
  PRIMARY KEY (dedupe_key, ts)
);
