-- Copyright (C) 2025 CardinalHQ, Inc
--
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU Affero General Public License as
-- published by the Free Software Foundation, version 3.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
-- GNU Affero General Public License for more details.
--
-- You should have received a copy of the GNU Affero General Public License
-- along with this program. If not, see <http://www.gnu.org/licenses/>.

-- Table to configure data retention policies per organization and signal type
-- A value of -1 means use system default, 0 means never expire, >0 means specific retention days
CREATE TABLE organization_signal_expiry (
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    signal_type TEXT NOT NULL CHECK (signal_type IN ('logs', 'metrics', 'traces')),
    max_age_days INTEGER NOT NULL CHECK (max_age_days >= -1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (organization_id, signal_type)
);

-- Table to track when expiry cleanup was last run
-- Separate from policy to avoid creating policy records just for tracking
CREATE TABLE expiry_run_tracking (
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    signal_type VARCHAR(50) NOT NULL CHECK (signal_type IN ('logs', 'metrics', 'traces')),
    last_run_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (organization_id, signal_type)
);

-- Indexes for efficient queries
CREATE INDEX idx_organization_signal_expiry_signal_type ON organization_signal_expiry(signal_type);
CREATE INDEX idx_expiry_run_tracking_last_run ON expiry_run_tracking (last_run_at);

-- Add comments to clarify table purposes
COMMENT ON TABLE organization_signal_expiry IS 'Stores data retention policies for each organization and signal type. A value of -1 means use system default, 0 means never expire, >0 means specific retention days.';
COMMENT ON TABLE expiry_run_tracking IS 'Tracks when expiry cleanup was last run for each organization and signal type. Separate from policy to avoid creating policy records just for tracking.';

-- Function to find the organization-level partition for a given table and org
CREATE OR REPLACE FUNCTION find_org_partition(
  root_table regclass,
  org_id     uuid
) RETURNS regclass
LANGUAGE plpgsql AS
$$
DECLARE
  cur     regclass;  -- current rel (starts at leaf)
  parent  regclass;
BEGIN
  -- 1) Find any leaf that has a row for this org
  EXECUTE format($sql$
    SELECT tableoid::regclass
    FROM   %s
    WHERE  organization_id = $1
    LIMIT  1
  $sql$, root_table)
  INTO cur
  USING org_id;

  IF cur IS NULL THEN
    RAISE EXCEPTION 'No rows for organization_id=% in %; cannot locate org partition.',
      org_id, root_table;
  END IF;

  -- 2) Walk up ancestors until the parent is the root
  LOOP
    SELECT inhparent::regclass
    INTO   parent
    FROM   pg_inherits
    WHERE  inhrelid = cur;

    IF parent IS NULL THEN
      RAISE EXCEPTION 'Partition % is not under % (no ancestor found matching root).', cur, root_table;
    END IF;

    IF parent = root_table THEN
      -- cur is the direct child of root => org-level partition
      RETURN cur;
    END IF;

    -- otherwise keep walking up
    cur := parent;
  END LOOP;
END;
$$;

-- Function to expire data by marking published=FALSE based on ingest_dateint cutoff
CREATE OR REPLACE FUNCTION expire_published_by_ingest_cutoff(
  target_table    regclass,   -- org-level partition (or root, if you want)
  org_id          uuid,       -- used only for logging; not filtered in WHERE
  cutoff_dateint  integer,
  batch_size      integer DEFAULT 10000
) RETURNS bigint
LANGUAGE plpgsql AS
$$
DECLARE
  parts_arr     regclass[];
  part          regclass;
  idx           int := 0;
  total         int := 0;
  rows_changed  bigint;
  total_changed bigint := 0;
  has_work      bool;
BEGIN
  -- cheaper commits for maintenance jobs
  PERFORM set_config('synchronous_commit', 'off', true);

  -- collect all leaf partitions (date-int leaves)
  SELECT array_agg(relid::regclass ORDER BY relid::regclass::text)
  INTO   parts_arr
  FROM (
    WITH tree AS (SELECT * FROM pg_partition_tree(target_table))
    SELECT relid
    FROM   tree
    WHERE  NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent = relid)
  ) s;

  IF parts_arr IS NULL OR array_length(parts_arr,1) IS NULL THEN
    RAISE NOTICE 'No leaf partitions found under %', target_table;
    RETURN 0;
  END IF;

  total := array_length(parts_arr,1);

  FOREACH part IN ARRAY parts_arr LOOP
    idx := idx + 1;
    RAISE NOTICE 'Scanning leaf (%/%): %', idx, total, part;

    -- probe for work in this leaf (published is NOT NULL -> just = TRUE)
    EXECUTE format($sql$
      SELECT TRUE
      FROM   ONLY %I
      WHERE  published = TRUE
        AND  ingest_dateint < $1
      LIMIT  1
    $sql$, part)
    USING cutoff_dateint
    INTO has_work;

    IF NOT COALESCE(has_work, FALSE) THEN
      RAISE NOTICE '  No rows to update in %', part;
      CONTINUE;
    END IF;

    RAISE NOTICE 'Processing % for org %, cutoff %', part, org_id, cutoff_dateint;

    -- chunked updates in index order
    LOOP
      EXECUTE format($sql$
        WITH to_update AS (
          SELECT ctid
          FROM   ONLY %I
          WHERE  published = TRUE
            AND  ingest_dateint < $1
          ORDER  BY ingest_dateint
          LIMIT  $2
          FOR UPDATE SKIP LOCKED
        )
        UPDATE ONLY %I t
        SET    published = FALSE
        FROM   to_update u
        WHERE  t.ctid = u.ctid
      $sql$, part, part)
      USING cutoff_dateint, batch_size;

      GET DIAGNOSTICS rows_changed = ROW_COUNT;
      IF rows_changed = 0 THEN
        EXIT;
      END IF;

      total_changed := total_changed + rows_changed;
      RAISE NOTICE '  Updated % rows in %', rows_changed, part;

      PERFORM pg_sleep(0.0);
    END LOOP;
  END LOOP;

  RAISE NOTICE 'Total rows updated for org % with cutoff %: %',
    org_id, cutoff_dateint, total_changed;

  RETURN total_changed;
END;
$$;
