-- 1755306356_add_slot_id.down.sql

-- Drop new constraints
ALTER TABLE public.work_queue DROP CONSTRAINT work_queue_conflict;
ALTER TABLE public.signal_locks DROP CONSTRAINT signal_locks_conflict;

-- Remove slot_id columns
ALTER TABLE public.work_queue DROP COLUMN slot_id;
ALTER TABLE public.signal_locks DROP COLUMN slot_id;

-- Restore original constraints
ALTER TABLE public.work_queue ADD CONSTRAINT work_queue_conflict 
  EXCLUDE USING GIST (
    organization_id WITH =,
    instance_num    WITH =,
    frequency_ms    WITH =,
    signal          WITH =,
    action          WITH =,
    ts_range        WITH &&
  );

ALTER TABLE public.signal_locks ADD CONSTRAINT signal_locks_conflict
  EXCLUDE USING GIST (
    organization_id  WITH =,
    instance_num     WITH =,
    frequency_ms     WITH =,
    signal           WITH =,
    ts_range         WITH &&
  );

ALTER TABLE public.signal_locks ADD UNIQUE (organization_id, instance_num, dateint, frequency_ms, signal);

-- Restore original work_queue_add function (without slot_id parameter)
CREATE OR REPLACE FUNCTION public.work_queue_add(
    p_org_id      UUID,
    p_instance    SMALLINT,
    p_dateint     INTEGER,
    p_frequency   INTEGER,
    p_signal      signal_enum,
    p_action      action_enum,
    p_ts_range    TSTZRANGE,
    p_runnable_at TIMESTAMPTZ,
    p_priority    INTEGER
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    lower_ts TIMESTAMPTZ := lower(p_ts_range);
    upper_ts TIMESTAMPTZ := upper(p_ts_range);
BEGIN
    IF upper_ts <= lower_ts THEN
        RAISE EXCEPTION
          'work_queue_add: ts_range upper (%) must be > lower (%)',
          upper_ts, lower_ts;
    END IF;

    BEGIN
        INSERT INTO public.work_queue (
            organization_id,
            instance_num,
            dateint,
            frequency_ms,
            signal,
            action,
            tries,
            claimed_by,
            claimed_at,
            needs_run,
            ts_range,
            heartbeated_at,
            runnable_at,
            priority
        ) VALUES (
            p_org_id,
            p_instance,
            p_dateint,
            p_frequency,
            p_signal,
            p_action,
            0,
            -1,
            NULL,
            TRUE,
            p_ts_range,
            NOW(),
            p_runnable_at,
            p_priority
        );
        RETURN;
    EXCEPTION
        WHEN SQLSTATE '23P01' THEN
            UPDATE public.work_queue w
               SET needs_run   = TRUE,
                   runnable_at = CASE
                                   WHEN w.needs_run THEN w.runnable_at
                                   ELSE p_runnable_at
                                  END,
                   priority    = GREATEST(w.priority, p_priority)
             WHERE w.organization_id = p_org_id
               AND w.instance_num    = p_instance
               AND w.frequency_ms    = p_frequency
               AND w.signal          = p_signal
               AND w.action          = p_action
               AND w.ts_range && p_ts_range;
            RETURN;
    END;
END;
$$;

-- Restore original signal_lock_acquire function (without slot_id parameter)
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