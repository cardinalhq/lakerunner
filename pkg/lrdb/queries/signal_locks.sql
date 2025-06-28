-- name: SignalLockCleanup :one
SELECT public.signal_lock_cleanup() as count;
