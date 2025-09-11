-- Rollback: add slot_id and slot_count columns back

-- trace_seg: add slot_id back
ALTER TABLE trace_seg ADD COLUMN slot_id integer NOT NULL DEFAULT 0;

-- metric_seg: drop primary key, add columns, add constraints, recreate primary key
ALTER TABLE metric_seg DROP CONSTRAINT metric_seg_pkey;
ALTER TABLE metric_seg ADD COLUMN slot_id integer NOT NULL DEFAULT 0;
ALTER TABLE metric_seg ADD COLUMN slot_count integer NOT NULL DEFAULT 1;
ALTER TABLE metric_seg ADD CONSTRAINT metric_seg_slot_count_positive CHECK (slot_count > 0);
ALTER TABLE metric_seg ADD CONSTRAINT metric_seg_slot_id_range CHECK (slot_id >= 0 AND slot_id <= slot_count);
ALTER TABLE metric_seg ADD CONSTRAINT metric_seg_pkey PRIMARY KEY (organization_id, dateint, frequency_ms, segment_id, instance_num, slot_id, slot_count);

-- log_seg: drop primary key, add slot_id, recreate primary key
ALTER TABLE log_seg DROP CONSTRAINT log_seg_pkey;
ALTER TABLE log_seg ADD COLUMN slot_id integer NOT NULL DEFAULT 0;
ALTER TABLE log_seg ADD CONSTRAINT log_seg_pkey PRIMARY KEY (organization_id, dateint, segment_id, instance_num, slot_id);