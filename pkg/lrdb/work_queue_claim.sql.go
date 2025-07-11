// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.28.0
// source: work_queue_claim.sql

package lrdb

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

const workQueueClaimDirect = `-- name: WorkQueueClaimDirect :one
WITH params AS (
    SELECT
      NOW() AS v_now,
      (SELECT value::interval FROM public.settings WHERE key = 'lock_ttl') AS v_lock_ttl
  ),

  target_freqs AS (
    SELECT unnest($1::INTEGER[]) AS freq
  ),

  rollup_sources(parent_freq_ms, child_freq_ms) AS (
    VALUES
      (60000,    10000),
      (300000,   60000),
      (1200000,  300000),
      (3600000, 1200000)
  ),

  sl_small AS MATERIALIZED (
    SELECT id, work_id, organization_id, instance_num, dateint, frequency_ms, signal, ts_range, claimed_by, claimed_at, heartbeated_at
    FROM public.signal_locks sl
    WHERE
      sl.signal       = $2
      AND sl.frequency_ms = ANY (
        -- the “own” frequencies
        ARRAY(SELECT freq FROM target_freqs)
        -- plus, if rollup, the child freqs
        || COALESCE(
             (SELECT array_agg(child_freq_ms)
              FROM rollup_sources
              WHERE parent_freq_ms = ANY(SELECT freq FROM target_freqs)
                AND $3::action_enum = 'rollup'),
             '{}'
           )
      )
  ),

candidate AS (
  SELECT w.id, w.priority, w.runnable_at, w.organization_id, w.instance_num, w.dateint, w.frequency_ms, w.signal, w.action, w.needs_run, w.tries, w.ts_range, w.claimed_by, w.claimed_at, w.heartbeated_at
  FROM public.work_queue w
  LEFT JOIN sl_small sl
    ON sl.organization_id = w.organization_id
   AND sl.instance_num    = w.instance_num
   AND sl.signal          = w.signal
   AND sl.ts_range && w.ts_range
   AND sl.work_id <> w.id
  WHERE
    w.frequency_ms = ANY (SELECT freq FROM target_freqs)
    AND w.priority >= $4
    AND w.signal     = $2
    AND w.action     = $3
    AND w.runnable_at <= (SELECT v_now FROM params)
    AND sl.id IS NULL
    AND w.needs_run
  ORDER BY w.needs_run DESC, w.priority DESC, w.runnable_at, w.id
  LIMIT 1
  FOR UPDATE SKIP LOCKED
),

  lock_map AS (
    -- always insert a lock at the candidate’s own frequency
    SELECT c.frequency_ms AS lock_freq_ms
    FROM candidate c

    UNION ALL

    -- if this is a rollup action, also insert a lock at the child-frequency
    SELECT rs.child_freq_ms AS lock_freq_ms
    FROM candidate c
    JOIN rollup_sources rs
      ON c.frequency_ms = rs.parent_freq_ms
    WHERE $3 = 'rollup'
  ),

  cleanup_locks AS (
    DELETE FROM public.signal_locks sl
    USING candidate c
    WHERE sl.work_id = c.id
  ),

  new_locks AS (
    INSERT INTO public.signal_locks (
      organization_id, instance_num, dateint,
      frequency_ms,    signal,       claimed_by,
      claimed_at,      ts_range,     work_id
    )
    SELECT
      c.organization_id,
      c.instance_num,
      c.dateint,
      lm.lock_freq_ms,
      c.signal,
      $5,
      (SELECT v_now FROM params),
      c.ts_range,
      c.id
    FROM candidate c
    CROSS JOIN lock_map lm
    ORDER BY lm.lock_freq_ms
  ),

  updated AS (
    UPDATE public.work_queue w
    SET
      claimed_by     = $5,
      claimed_at     = (SELECT v_now FROM params),
      heartbeated_at = (SELECT v_now FROM params),
      needs_run      = FALSE,
      tries          = w.tries + 1
    FROM candidate c
    WHERE w.id = c.id
    RETURNING w.id, w.priority, w.runnable_at, w.organization_id, w.instance_num, w.dateint, w.frequency_ms, w.signal, w.action, w.needs_run, w.tries, w.ts_range, w.claimed_by, w.claimed_at, w.heartbeated_at
  )

SELECT id, priority, runnable_at, organization_id, instance_num, dateint, frequency_ms, signal, action, needs_run, tries, ts_range, claimed_by, claimed_at, heartbeated_at FROM updated
`

type WorkQueueClaimParams struct {
	TargetFreqs []int32    `json:"target_freqs"`
	Signal      SignalEnum `json:"signal"`
	Action      ActionEnum `json:"action"`
	MinPriority int32      `json:"min_priority"`
	WorkerID    int64      `json:"worker_id"`
}

type WorkQueueClaimRow struct {
	ID             int64                            `json:"id"`
	Priority       int32                            `json:"priority"`
	RunnableAt     time.Time                        `json:"runnable_at"`
	OrganizationID uuid.UUID                        `json:"organization_id"`
	InstanceNum    int16                            `json:"instance_num"`
	Dateint        int32                            `json:"dateint"`
	FrequencyMs    int32                            `json:"frequency_ms"`
	Signal         SignalEnum                       `json:"signal"`
	Action         ActionEnum                       `json:"action"`
	NeedsRun       bool                             `json:"needs_run"`
	Tries          int32                            `json:"tries"`
	TsRange        pgtype.Range[pgtype.Timestamptz] `json:"ts_range"`
	ClaimedBy      int64                            `json:"claimed_by"`
	ClaimedAt      *time.Time                       `json:"claimed_at"`
	HeartbeatedAt  time.Time                        `json:"heartbeated_at"`
}

func (q *Queries) WorkQueueClaimDirect(ctx context.Context, arg WorkQueueClaimParams) (WorkQueueClaimRow, error) {
	row := q.db.QueryRow(ctx, workQueueClaimDirect,
		arg.TargetFreqs,
		arg.Signal,
		arg.Action,
		arg.MinPriority,
		arg.WorkerID,
	)
	var i WorkQueueClaimRow
	err := row.Scan(
		&i.ID,
		&i.Priority,
		&i.RunnableAt,
		&i.OrganizationID,
		&i.InstanceNum,
		&i.Dateint,
		&i.FrequencyMs,
		&i.Signal,
		&i.Action,
		&i.NeedsRun,
		&i.Tries,
		&i.TsRange,
		&i.ClaimedBy,
		&i.ClaimedAt,
		&i.HeartbeatedAt,
	)
	return i, err
}
