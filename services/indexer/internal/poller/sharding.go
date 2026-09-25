package poller

import (
	"context"
	"hash/fnv"
	"sort"
	"sync"
	"time"
)

// LeaderLockKey is the Postgres advisory-lock key the coordinator uses to
// elect exactly one leader across every process:
//
//	SELECT pg_try_advisory_lock(272272272);
//
// The key is a constant so all coordinators contend for the same lock.
// Postgres releases a session-level advisory lock automatically when the
// holding connection closes, so a crashed leader cannot wedge the cluster.
const LeaderLockKey int64 = 272272272

// ShardForContract returns the shard that owns contractID in a shardCount-way
// partition. The mapping is a pure function of the contract ID (FNV-1a), so
// every process derives the same owner without coordinating.
func ShardForContract(contractID string, shardCount int) int {
	if shardCount <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(contractID))
	return int(h.Sum32() % uint32(shardCount))
}

// planShards groups contracts into shardCount shards. Every index in
// [0, shardCount) is present, including empty shards, so a worker can be
// handed a shard that will pick up contracts later without a re-plan.
func planShards(contracts []Contract, shardCount int) [][]Contract {
	if shardCount <= 0 {
		shardCount = 1
	}
	shards := make([][]Contract, shardCount)
	for _, c := range contracts {
		s := ShardForContract(c.ID, shardCount)
		shards[s] = append(shards[s], c)
	}
	return shards
}

// WorkerHeartbeat is one worker's liveness record. A worker whose
// LastHeartbeat is older than the coordinator's timeout is treated as failed
// and has its shards reassigned.
type WorkerHeartbeat struct {
	WorkerID      string
	ShardIDs      []int
	LastHeartbeat time.Time
}

// ShardAssignment binds one shard to the worker that currently owns it.
type ShardAssignment struct {
	ShardID     int
	WorkerID    string
	ContractIDs []string
	AssignedAt  time.Time
}

// Assignment is the complete set of shards and contracts a single worker owns.
type Assignment struct {
	WorkerID    string
	ShardIDs    []int
	ContractIDs []string
}

// ShardStore persists coordinator state: leader election, shard ownership, and
// worker heartbeats.
//
// The production implementation is backed by Postgres (see
// docs/indexer-sharding.md): TryAcquireLeaderLock takes the session-level
// advisory lock named by LeaderLockKey with pg_try_advisory_lock, so exactly
// one coordinator leads at a time and the lock is dropped automatically if the
// leader's connection dies. The shard and worker tables are created by
// migration 000010_indexer_shards.
type ShardStore interface {
	// TryAcquireLeaderLock reports whether this process is now the leader.
	// It returns false (and no error) when another coordinator holds the lock.
	TryAcquireLeaderLock(ctx context.Context) (bool, error)
	// ReleaseLeaderLock releases the leader lock held by this process.
	ReleaseLeaderLock(ctx context.Context) error

	// SaveAssignments atomically replaces the shard ownership table.
	SaveAssignments(ctx context.Context, assignments []ShardAssignment) error
	// Assignments returns the current shard ownership table.
	Assignments(ctx context.Context) ([]ShardAssignment, error)

	// AssignmentForWorker returns the shards and contract IDs owned by one worker.
	AssignmentForWorker(ctx context.Context, workerID string) (Assignment, error)

	// Heartbeat upserts a worker's liveness record.
	Heartbeat(ctx context.Context, hb WorkerHeartbeat) error
	// Workers returns every known worker heartbeat.
	Workers(ctx context.Context) ([]WorkerHeartbeat, error)
	// RemoveWorker deletes a worker's heartbeat on graceful shutdown.
	RemoveWorker(ctx context.Context, workerID string) error
}

// MemoryShardStore is an in-process ShardStore. It keeps the sharded topology
// runnable when the indexer runs as a single process (local development and
// tests) and mirrors the semantics the Postgres store implements. It is safe
// for concurrent use.
type MemoryShardStore struct {
	mu          sync.Mutex
	leader      bool
	assignments map[int]ShardAssignment
	workers     map[string]WorkerHeartbeat
}

// NewMemoryShardStore returns an empty in-process ShardStore.
func NewMemoryShardStore() *MemoryShardStore {
	return &MemoryShardStore{
		assignments: make(map[int]ShardAssignment),
		workers:     make(map[string]WorkerHeartbeat),
	}
}

// TryAcquireLeaderLock implements ShardStore.
func (m *MemoryShardStore) TryAcquireLeaderLock(_ context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.leader {
		return false, nil
	}
	m.leader = true
	return true, nil
}

// ReleaseLeaderLock implements ShardStore.
func (m *MemoryShardStore) ReleaseLeaderLock(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leader = false
	return nil
}

// SaveAssignments implements ShardStore.
func (m *MemoryShardStore) SaveAssignments(_ context.Context, assignments []ShardAssignment) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := make(map[int]ShardAssignment, len(assignments))
	for _, a := range assignments {
		next[a.ShardID] = a
	}
	m.assignments = next
	return nil
}

// Assignments implements ShardStore.
func (m *MemoryShardStore) Assignments(_ context.Context) ([]ShardAssignment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]ShardAssignment, 0, len(m.assignments))
	for _, a := range m.assignments {
		a.ContractIDs = append([]string(nil), a.ContractIDs...)
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ShardID < out[j].ShardID })
	return out, nil
}

// AssignmentForWorker implements ShardStore.
func (m *MemoryShardStore) AssignmentForWorker(_ context.Context, workerID string) (Assignment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := Assignment{WorkerID: workerID}
	seen := make(map[string]struct{})
	for _, a := range m.assignments {
		if a.WorkerID != workerID {
			continue
		}
		out.ShardIDs = append(out.ShardIDs, a.ShardID)
		for _, id := range a.ContractIDs {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out.ContractIDs = append(out.ContractIDs, id)
		}
	}
	sort.Ints(out.ShardIDs)
	sort.Strings(out.ContractIDs)
	return out, nil
}

// Heartbeat implements ShardStore.
func (m *MemoryShardStore) Heartbeat(_ context.Context, hb WorkerHeartbeat) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	hb.ShardIDs = append([]int(nil), hb.ShardIDs...)
	sort.Ints(hb.ShardIDs)
	m.workers[hb.WorkerID] = hb
	return nil
}

// Workers implements ShardStore.
func (m *MemoryShardStore) Workers(_ context.Context) ([]WorkerHeartbeat, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]WorkerHeartbeat, 0, len(m.workers))
	for _, w := range m.workers {
		w.ShardIDs = append([]int(nil), w.ShardIDs...)
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].WorkerID < out[j].WorkerID })
	return out, nil
}

// RemoveWorker implements ShardStore.
func (m *MemoryShardStore) RemoveWorker(_ context.Context, workerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.workers, workerID)
	return nil
}
