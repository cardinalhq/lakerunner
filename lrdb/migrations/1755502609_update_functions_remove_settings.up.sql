-- 1755502609_update_functions_remove_settings.up.sql

-- Update signal_lock_acquire function to use hardcoded timeout instead of settings table
-- This removes the dependency on the settings table which is being dropped
CREATE OR REPLACE FUNCTION public.signal_lock_acquire(
    p_org_id     UUID,
    p_instance   SMALLINT,
    p_dateint    INTEGER,
    p_frequency  INTEGER,
    p_signal     signal_enum,
    p_ts_range   TSTZRANGE,
    p_claimed_by BIGINT,
    p_slot_id    INTEGER
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_now         TIMESTAMPTZ := NOW();
    v_ttl         INTERVAL := INTERVAL '5 minutes'; -- Hardcoded default lock_ttl
    v_inserted_id BIGINT;
    lower_ts      TIMESTAMPTZ := lower(p_ts_range);
    upper_ts      TIMESTAMPTZ := upper(p_ts_range);
BEGIN
    IF upper_ts <= lower_ts THEN
        RAISE EXCEPTION
            'signal_lock_acquire: ts_range upper (%) must be strictly greater than lower (%)',
            upper_ts, lower_ts;
    END IF;

    DELETE FROM public.signal_locks
     WHERE organization_id = p_org_id
       AND instance_num    = p_instance
       AND dateint         = p_dateint
       AND frequency_ms    = p_frequency
       AND signal          = p_signal
       AND slot_id         = p_slot_id
       AND heartbeated_at  < v_now - v_ttl;

    INSERT INTO public.signal_locks (
        organization_id,
        instance_num,
        dateint,
        frequency_ms,
        signal,
        ts_range,
        claimed_by,
        claimed_at,
        heartbeated_at,
        slot_id
    ) VALUES (
        p_org_id,
        p_instance,
        p_dateint,
        p_frequency,
        p_signal,
        p_ts_range,
        p_claimed_by,
        v_now,
        v_now,
        p_slot_id
    );

    SELECT currval('signal_locks_id_seq') INTO v_inserted_id;
    RETURN v_inserted_id;
END;
$$;
