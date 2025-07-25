-- 1751058910_initial_settings.up.sql

CREATE TABLE IF NOT EXISTS settings (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

INSERT INTO settings (key, value) VALUES ('lock_ttl', '5 minutes') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('lock_ttl_dead', '1 hour') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('work_max_retries', '10') ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('work_fail_requeue_ttl', '1 minute') ON CONFLICT (key) DO NOTHING;
