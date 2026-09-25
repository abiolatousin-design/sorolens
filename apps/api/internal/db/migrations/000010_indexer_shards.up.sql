-- ============================================================
-- 000010_indexer_shards
--
-- Coordinator state for the horizontally-scaled indexer (issue #272):
-- a sharded topology where several worker processes index disjoint sets
-- of contracts in parallel.
--
--   indexer_shards  : one row per shard, with the worker that currently
--                     owns it and the contract IDs mapped into it.
--   indexer_workers : one liveness row per worker, refreshed every
--                     heartbeat. A worker whose last_heartbeat is older
--                     than the coordinator's timeout is treated as failed
--                     and its shards are reassigned to a live worker.
--
-- Leader election does not need a table: the coordinator takes a
-- session-level Postgres advisory lock with
--
--   SELECT pg_try_advisory_lock(272272272);
--
-- Every coordinator contends for the same key (poller.LeaderLockKey), so
-- exactly one process leads at a time. Postgres releases the lock when the
-- leader's connection closes, so a crashed coordinator cannot wedge the
-- cluster.
-- ============================================================

CREATE TABLE IF NOT EXISTS indexer_shards (
    shard_id     INTEGER PRIMARY KEY,
    worker_id    TEXT,
    contract_ids TEXT[] NOT NULL DEFAULT '{}',
    assigned_at  TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The coordinator looks up a shard's owner and each worker resolves its own
-- shards on every pass.
CREATE INDEX IF NOT EXISTS idx_indexer_shards_worker ON indexer_shards (worker_id);

CREATE TABLE IF NOT EXISTS indexer_workers (
    worker_id      TEXT PRIMARY KEY,
    shard_ids      INTEGER[] NOT NULL DEFAULT '{}',
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Staleness checks scan by heartbeat age.
CREATE INDEX IF NOT EXISTS idx_indexer_workers_heartbeat ON indexer_workers (last_heartbeat);
