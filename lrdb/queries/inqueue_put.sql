-- name: PutInqueueWork :exec
INSERT INTO inqueue (organization_id, collector_name, instance_num, bucket, object_id, telemetry_type, priority, file_size)
VALUES (@organization_id, @collector_name, @instance_num, @bucket, @object_id, @telemetry_type, @priority, @file_size);