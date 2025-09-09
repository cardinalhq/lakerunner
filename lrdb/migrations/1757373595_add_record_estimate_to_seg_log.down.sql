-- Remove record_estimate field from seg_log table
ALTER TABLE seg_log 
DROP COLUMN record_estimate;