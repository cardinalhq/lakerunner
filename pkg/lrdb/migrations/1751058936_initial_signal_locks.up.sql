-- 1751058936_initial_signal_locks.up.sql

CREATE TABLE IF NOT EXISTS signal_locks (
  id              BIGSERIAL      PRIMARY KEY,
  work_id         BIGINT,
  organization_id UUID           NOT NULL,
  instance_num    SMALLINT       NOT NULL,
  dateint         INTEGER        NOT NULL,
  frequency_ms    INTEGER        NOT NULL,
  signal          signal_enum    NOT NULL,
  ts_range        tstzrange      NOT NULL,
  claimed_by      BIGINT         NOT NULL DEFAULT -1,
  claimed_at      TIMESTAMPTZ,
  heartbeated_at  TIMESTAMPTZ    NOT NULL DEFAULT now(),

  UNIQUE (organization_id, instance_num, dateint, frequency_ms, signal),

  CONSTRAINT signal_locks_tsrange_not_empty CHECK (NOT isempty(ts_range)),

  CONSTRAINT signal_locks_conflict
    EXCLUDE USING GIST (
      organization_id  WITH =,
      instance_num     WITH =,
      frequency_ms     WITH =,
      signal           WITH =,
      ts_range         WITH &&
    )
);

CREATE OR REPLACE FUNCTION public.signal_lock_acquire(
    p_org_id     UUID,
    p_instance   SMALLINT,
    p_dateint    INTEGER,
    p_frequency  INTEGER,
    p_signal     signal_enum,
    p_ts_range   TSTZRANGE,
    p_claimed_by BIGINT
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_now         TIMESTAMPTZ := NOW();
    v_ttl         INTERVAL;
    v_inserted_id BIGINT;
    lower_ts      TIMESTAMPTZ := lower(p_ts_range);
    upper_ts      TIMESTAMPTZ := upper(p_ts_range);
BEGIN
    IF upper_ts <= lower_ts THEN
        RAISE EXCEPTION
            'signal_lock_acquire: ts_range upper (%) must be strictly greater than lower (%)',
            upper_ts, lower_ts;
    END IF;

    SELECT value::interval
      INTO v_ttl
      FROM public.settings
     WHERE key = 'lock_ttl';

    DELETE FROM public.signal_locks
     WHERE organization_id = p_org_id
       AND instance_num    = p_instance
       AND dateint         = p_dateint
       AND frequency_ms    = p_frequency
       AND signal          = p_signal
       AND heartbeated_at  < v_now - v_ttl;

    BEGIN
        INSERT INTO public.signal_locks (
            organization_id,
            instance_num,
            dateint,
            frequency_ms,
            signal,
            claimed_by,
            claimed_at,
            ts_range,
            heartbeated_at
        ) VALUES (
            p_org_id,
            p_instance,
            p_dateint,
            p_frequency,
            p_signal,
            p_claimed_by,
            v_now,
            p_ts_range,
            v_now
        )
        RETURNING id
        INTO v_inserted_id;

        RETURN v_inserted_id;
    EXCEPTION
        WHEN SQLSTATE '23P01' THEN
            RETURN -1;
    END;
END;
$$;

-- ===========================================================================
-- Function: signal_lock_release
--
-- Deletes (unlocks) a row from signal_locks *only* if the caller still owns it.
-- Inputs:
--   p_id          BIGINT – the primary key of the lock row
--   p_claimed_by  BIGINT – the worker ID that must match signal_locks.claimed_by
--
-- Returns:
--   BOOLEAN – TRUE if the row was found and deleted;
--             FALSE otherwise.
-- ===========================================================================

CREATE OR REPLACE FUNCTION signal_lock_release(
    p_id         BIGINT,
    p_claimed_by BIGINT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM signal_locks
     WHERE id = p_id
       AND claimed_by = p_claimed_by;

    IF FOUND THEN
        RETURN TRUE;
    ELSE
        RETURN FALSE;
    END IF;
END;
$$;

-- ===========================================================================
-- Function: signal_lock_heartbeat
--
-- Updates heartbeated_at = NOW() for active locks owned by the caller.
-- Inputs:
--   p_ids         BIGINT[] – array of primary key IDs of lock rows
--   p_claimed_by  BIGINT   – the worker ID that must match signal_locks.claimed_by
--
-- Returns:
--   INTEGER – number of rows updated
-- ===========================================================================

CREATE OR REPLACE FUNCTION public.signal_lock_heartbeat(
    p_ids        BIGINT[],
    p_claimed_by BIGINT
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := NOW();
    v_count INTEGER;
BEGIN
    UPDATE public.signal_locks
       SET heartbeated_at = v_now
     WHERE id = ANY(p_ids)
       AND claimed_by = p_claimed_by
       AND heartbeated_at >= v_now - (
           SELECT value::interval
             FROM public.settings
            WHERE key = 'lock_ttl'
       )
    RETURNING 1
    INTO v_count;

    RETURN COALESCE(v_count, 0);
END;
$$;

-- ===========================================================================
-- Function: signal_lock_cleanup
--
-- Deletes all locks whose heartbeated_at is older than NOW() minus the interval
-- stored under key 'lock_ttl_dead' in the settings table.
--
-- Returns:
--   INTEGER – the number of rows deleted.
-- ===========================================================================

CREATE OR REPLACE FUNCTION signal_lock_cleanup()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_now      TIMESTAMPTZ := NOW();
    v_dead_ttl INTERVAL;
    v_count    INTEGER;
BEGIN
    SELECT value::interval INTO v_dead_ttl
      FROM settings
     WHERE key = 'lock_ttl_dead';

    DELETE FROM signal_locks
     WHERE heartbeated_at < v_now - v_dead_ttl
    RETURNING 1
    INTO v_count;

    RETURN COALESCE(v_count, 0);
END;
$$;
