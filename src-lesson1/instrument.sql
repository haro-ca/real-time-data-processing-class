-- ============================================================
-- Instrumentation queries — run these in psql during demos
-- ============================================================

-- ─── Connections (Slide 5) ──────────────────────────────────
SELECT pid, state, wait_event_type, left(query, 50) AS query
FROM pg_stat_activity
WHERE backend_type = 'client backend'
ORDER BY state, pid;

-- ─── Buffer pool (Slide 9) ──────────────────────────────────
SELECT c.relname, count(*) AS buffers,
       pg_size_pretty(count(*) * 8192) AS size
FROM pg_buffercache b
JOIN pg_class c ON b.relfilenode = c.relfilenode
WHERE b.reldatabase = (SELECT oid FROM pg_database WHERE datname = 'bench')
GROUP BY c.relname ORDER BY 2 DESC LIMIT 10;

-- ─── Lock waits (Slide 13) ──────────────────────────────────
SELECT pid, wait_event_type, wait_event,
       state, left(query, 40) AS query
FROM pg_stat_activity
WHERE wait_event_type = 'Lock';

-- ─── Statement stats (Phase 3) ─────────────────────────────
SELECT query, calls, mean_exec_time::numeric(10,3) AS avg_ms,
       total_exec_time::numeric(12,1) AS total_ms
FROM pg_stat_statements
WHERE dbid = (SELECT oid FROM pg_database WHERE datname = 'bench')
ORDER BY total_exec_time DESC LIMIT 10;

-- ─── Background writer (Phase 3) ───────────────────────────
SELECT checkpoints_timed, checkpoints_req,
       buffers_checkpoint, buffers_backend, buffers_alloc
FROM pg_stat_bgwriter;

-- ─── Dead tuples (Experiment D) ─────────────────────────────
SELECT relname, n_live_tup, n_dead_tup,
       n_dead_tup::float / NULLIF(n_live_tup + n_dead_tup, 0) AS dead_ratio,
       last_vacuum, last_autovacuum,
       pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_stat_user_tables
WHERE relname = 'orders';

-- ─── Table bloat check ─────────────────────────────────────
SELECT pg_size_pretty(pg_relation_size('orders')) AS heap_size,
       pg_size_pretty(pg_total_relation_size('orders')) AS total_size,
       (SELECT count(*) FROM orders) AS live_rows;

-- ─── Disable autovacuum (Experiment D setup) ────────────────
-- ALTER TABLE orders SET (autovacuum_enabled = false);

-- ─── Run vacuum manually (Experiment D payoff) ──────────────
-- VACUUM VERBOSE orders;
