# Horizontally-scaled indexer (worker sharding)

This document covers the deployment topology introduced for
[#272](https://github.com/sorolens/sorolens/issues/272): several indexer
processes index disjoint sets of contracts in parallel, coordinated by a
single elected leader. A single-process deployment is unchanged and is still
the default (`--role standalone`).

## Topology

```
                    ┌───────────────────────┐
                    │  coordinator (1)      │
                    │  --role coordinator   │
                    │  Postgres advisory    │
                    │  lock leader election │
                    └───────────┬───────────┘
                                │ shard plan + heartbeats
                                ▼
   ┌───────────────────────┬───────────────┬───────────────────────┐
   │  worker A             │  worker B     │  worker C …           │
   │  --role worker        │  --role worker│  --role worker        │
   │  shards {0,3}         │  shards {1}   │  shards {2}           │
   └───────────┬───────────┴───────┬───────┴───────────┬───────────┘
               │                   │                   │
               ▼                   ▼                   ▼
   ┌───────────────────────────────────────────────────────────────┐
   │  Postgres: contracts, events, invocations, sync_state,        │
   │            indexer_cursors, indexer_shards, indexer_workers   │
   └───────────────────────────────────────────────────────────────┘
```

Every process runs the same binary. The role is selected by `--role`:

| Role | What it does |
|---|---|
| `standalone` (default) | Current behaviour: one process indexes every contract. `--mode once` runs a single pass (cron/GitHub Actions); `--mode continuous` loops. |
| `coordinator` | Elects itself leader via a Postgres advisory lock, then repeatedly partitions contracts into shards and assigns each shard to a live worker. It never indexes events itself. |
| `worker` | Registers a heartbeat, pulls its shard assignment, and indexes only the contracts in those shards until it is stopped. |

Run **one** coordinator and **N** workers. Worker identity defaults to the
hostname and can be overridden with `--worker-id` / `INDEXER_WORKER_ID`; it
must be unique per process.

## Sharding

Contracts are partitioned by a deterministic hash of their contract ID
(FNV-1a, `poller.ShardForContract`). Every process derives the same shard for
a contract without coordinating, so the assignment table only needs to record
*shard → worker*, never *contract → worker*.

The coordinator lists every `active`/`backfilling` contract, groups them into
`--shards` buckets, and assigns each bucket to a worker. A shard stays on its
current worker while that worker is healthy, so assignments are stable across
reconcile passes. When the coordinator is first elected it assigns each shard
to the least-loaded worker (ties broken on worker ID), then only moves shards
off failed workers.

## Leader election

The coordinator takes the session-level Postgres advisory lock

```sql
SELECT pg_try_advisory_lock(272272272);
```

`poller.LeaderLockKey` is the key. `pg_try_advisory_lock` is non-blocking: the
first coordinator wins and any other coordinator logs that another instance is
leading and exits. Postgres releases a session-level advisory lock when the
holding connection closes, so a crashed coordinator is replaced by the next
process that starts (`Kubernetes`/`systemd` restart, or simply the next
scheduled run). The lock is explicitly released on graceful shutdown.

## Heartbeats and failover

* A worker writes a heartbeat (`indexer_workers.last_heartbeat`) on start, then
  every `INDEXER_HEARTBEAT_INTERVAL` (default `10s`). While it is indexing it
  keeps heartbeating in the background, so a slow pass is not mistaken for a
  dead worker.
* The coordinator re-runs its assignment pass every `INDEXER_RECONCILE_INTERVAL`
  (default `5s`). A worker whose heartbeat is older than
  `INDEXER_WORKER_TIMEOUT` (default `30s`) is treated as failed; its shards are
  reassigned to the remaining live workers on that same pass.
* Reassignment therefore completes within `INDEXER_WORKER_TIMEOUT` +
  `INDEXER_RECONCILE_INTERVAL` (about 35s with the defaults).
* A worker that restarts re-registers with the same `--worker-id` and receives a
  fresh assignment. Shards are the unit of rebalancing, so no contract-level
  bookkeeping is needed.

## Schema

Migration `apps/api/internal/db/migrations/000010_indexer_shards.{up,down}.sql`
adds:

| Table | Purpose |
|---|---|
| `indexer_shards` | `shard_id` → owning `worker_id`, plus the `contract_ids` mapped into the shard and when it was assigned. |
| `indexer_workers` | `worker_id` → `last_heartbeat` and the shards the worker last reported. |

Both tables are runtime state; dropping and recreating them just forces the
coordinator to recompute the plan on its next pass.

## Environment variables

| Variable | Default | Used by |
|---|---|---|
| `INDEXER_SHARDS` | `1` | coordinator, worker |
| `INDEXER_WORKER_ID` | hostname | worker |
| `INDEXER_HEARTBEAT_INTERVAL` | `10s` | worker |
| `INDEXER_WORKER_TIMEOUT` | `30s` | coordinator |
| `INDEXER_RECONCILE_INTERVAL` | `5s` | coordinator |

`INDEXER_SHARDS` should match across the coordinator and every worker, since
all roles must agree on the partition function.

## Example deployment

```bash
# One coordinator (run it under a supervisor so it restarts on crash).
sorolens-indexer --role coordinator --shards 16

# Four workers, each on its own host/container.
sorolens-indexer --role worker --shards 16 --worker-id worker-1
sorolens-indexer --role worker --shards 16 --worker-id worker-2
sorolens-indexer --role worker --shards 16 --worker-id worker-3
sorolens-indexer --role worker --shards 16 --worker-id worker-4
```

A `docker-compose` override can scale the worker service with
`docker compose up --scale indexer-worker=4`, as long as each replica gets a
distinct `--worker-id` (for example from its container hostname). The existing
`standalone` cron deployment (GitHub Actions, `--mode once`) can keep running
until the sharded topology replaces it.

## Not sharded yet

The anomaly-detection and composite health-score jobs still iterate over every
contract in one process. A worker does not run them, so in the sharded topology
schedule them separately (a `standalone --mode once` job, or a dedicated
`coordinator`-side job) until they are made shard-aware. Event extraction and
the Wasm-upgrade check are fully sharded, because they run inside the per
contract path the owning worker executes.
