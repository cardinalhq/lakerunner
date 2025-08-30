-- name: PutInqueueWork :exec
INSERT INTO inqueue (organization_id, collector_name, instance_num, bucket, object_id, signal, priority, file_size)
VALUES (@organization_id, @collector_name, @instance_num, @bucket, @object_id, @signal, @priority, @file_size);