-- Remove slot_id and slot_count columns from all tables

ALTER TABLE log_seg DROP CONSTRAINT log_seg_pkey;
ALTER TABLE log_seg DROP COLUMN slot_id;
ALTER TABLE log_seg ADD CONSTRAINT log_seg_pkey PRIMARY KEY (organization_id, dateint, segment_id, instance_num);

ALTER TABLE metric_seg DROP CONSTRAINT metric_seg_pkey;
ALTER TABLE metric_seg DROP CONSTRAINT metric_seg_slot_count_positive;
ALTER TABLE metric_seg DROP CONSTRAINT metric_seg_slot_id_range;
ALTER TABLE metric_seg DROP COLUMN slot_id;
ALTER TABLE metric_seg DROP COLUMN slot_count;
ALTER TABLE metric_seg ADD CONSTRAINT metric_seg_pkey PRIMARY KEY (organization_id, dateint, frequency_ms, segment_id, instance_num);

ALTER TABLE trace_seg DROP COLUMN slot_id;
