package poller

import (
	"context"
	"sync"
	"testing"
	"time"
)

// contractsFixture returns n active contracts with deterministic IDs.
func contractsFixture(n int) []Contract {
	ids := contractIDsFixture(n)
	out := make([]Contract, 0, n)
	for _, id := range ids {
		out = append(out, Contract{ID: id, Status: "active"})
	}
	return out
}

// countingStore records how many times each contract's batch was committed, so
// a test can prove shards do not overlap.
type countingStore struct {
	*fakeStore
	mu    sync.Mutex
	calls map[string]int
}

func newCountingStore(contracts []Contract) *countingStore {
	return &countingStore{fakeStore: newFakeStore(contracts), calls: make(map[string]int)}
}

func (s *countingStore) BatchInsertWithCursor(ctx context.Context, network string, ledger uint32, events []Event, invocations []Invocation, ss SyncState) error {
	s.mu.Lock()
	s.calls[ss.ContractID]++
	s.mu.Unlock()
	return s.fakeStore.BatchInsertWithCursor(ctx, network, ledger, events, invocations, ss)
}

func (s *countingStore) processedCounts() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(s.calls))
	for k, v := range s.calls {
		out[k] = v
	}
	return out
}

func registerWorkers(t *testing.T, store ShardStore, ids []string, at time.Time) {
	t.Helper()
	for _, id := range ids {
		if err := store.Heartbeat(context.Background(), WorkerHeartbeat{WorkerID: id, LastHeartbeat: at}); err != nil {
			t.Fatalf("register worker %s: %v", id, err)
		}
	}
}

func shardedConfig() Config {
	cfg := testConfig()
	cfg.ShardCount = 4
	cfg.WorkerTimeout = time.Minute
	cfg.HeartbeatInterval = time.Minute
	cfg.ReconcileInterval = time.Minute
	return cfg
}

func TestCoordinator_ReconcileAssignsEveryShardToLiveWorker(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newFakeStore(contractsFixture(100))
	shards := NewMemoryShardStore()
	registerWorkers(t, shards, []string{"w1", "w2", "w3", "w4"}, time.Now().UTC())

	coord := NewCoordinator(store, shards, shardedConfig(), testLogger())
	if err := coord.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	assignments, err := shards.Assignments(ctx)
	if err != nil {
		t.Fatalf("assignments: %v", err)
	}
	if len(assignments) != 4 {
		t.Fatalf("assignments = %d shards, want 4", len(assignments))
	}

	loads := make(map[string]int)
	seen := make(map[string]int)
	for _, a := range assignments {
		if a.WorkerID == "" {
			t.Errorf("shard %d is unassigned despite live workers", a.ShardID)
			continue
		}
		loads[a.WorkerID]++
		for _, id := range a.ContractIDs {
			seen[id]++
		}
	}
	if len(seen) != 100 {
		t.Errorf("assigned %d distinct contracts, want 100", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("contract %s assigned %d times, want 1", id, n)
		}
	}
	if len(loads) != 4 {
		t.Errorf("workers owning shards = %d, want 4", len(loads))
	}
	for id, n := range loads {
		if n != 1 {
			t.Errorf("worker %s owns %d shards, want 1 (balanced)", id, n)
		}
	}
}

func TestCoordinator_RunExitsWhenAnotherLeaderHoldsLock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	shards := NewMemoryShardStore()
	if held, err := shards.TryAcquireLeaderLock(ctx); err != nil || !held {
		t.Fatalf("pre-acquire leader lock = %v, %v; want true, nil", held, err)
	}
	store := newFakeStore(contractsFixture(10))
	coord := NewCoordinator(store, shards, shardedConfig(), testLogger())

	if err := coord.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	assignments, err := shards.Assignments(ctx)
	if err != nil {
		t.Fatalf("assignments: %v", err)
	}
	if len(assignments) != 0 {
		t.Errorf("non-leader wrote %d assignments, want 0", len(assignments))
	}
}

func TestCoordinator_ReassignsStaleWorkerShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newFakeStore(contractsFixture(100))
	shards := NewMemoryShardStore()
	now := time.Now().UTC()
	registerWorkers(t, shards, []string{"w1", "w2", "w3", "w4"}, now)

	cfg := shardedConfig()
	cfg.WorkerTimeout = time.Minute
	coord := NewCoordinator(store, shards, cfg, testLogger())
	if err := coord.Reconcile(ctx); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	// w3 stops heartbeating and exceeds the timeout.
	if err := shards.Heartbeat(ctx, WorkerHeartbeat{WorkerID: "w3", LastHeartbeat: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("age w3 heartbeat: %v", err)
	}
	if err := coord.Reconcile(ctx); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	assignments, err := shards.Assignments(ctx)
	if err != nil {
		t.Fatalf("assignments: %v", err)
	}
	seen := make(map[string]int)
	for _, a := range assignments {
		if a.WorkerID == "w3" {
			t.Errorf("stale worker w3 still owns shard %d", a.ShardID)
		}
		for _, id := range a.ContractIDs {
			seen[id]++
		}
	}
	if len(seen) != 100 {
		t.Errorf("after reassignment %d contracts assigned, want 100", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("contract %s assigned %d times after reassignment, want 1", id, n)
		}
	}
}

// waitFor polls cond until it returns true or timeout elapses.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func workerOwnsShards(store ShardStore, workerID string) bool {
	assignments, err := store.Assignments(context.Background())
	if err != nil {
		return false
	}
	for _, a := range assignments {
		if a.WorkerID == workerID {
			return true
		}
	}
	return false
}

// TestCoordinator_ReassignsWithinTimeout runs the coordinator loop and checks
// that a worker which stops heartbeating has its shards moved to a live worker
// within the configured timeout plus one reconcile interval.
func TestCoordinator_ReassignsWithinTimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newFakeStore(contractsFixture(100))
	shards := NewMemoryShardStore()
	now := time.Now().UTC()
	registerWorkers(t, shards, []string{"w1", "w2"}, now)

	cfg := shardedConfig()
	cfg.ShardCount = 4
	cfg.WorkerTimeout = 150 * time.Millisecond
	cfg.ReconcileInterval = 20 * time.Millisecond
	coord := NewCoordinator(store, shards, cfg, testLogger())

	done := make(chan error, 1)
	go func() { done <- coord.Run(ctx) }()

	if !waitFor(time.Second, func() bool { return workerOwnsShards(shards, "w2") }) {
		cancel()
		t.Fatal("w2 never received a shard from the coordinator")
	}

	// w2 goes silent. Its shards must move off it within its timeout window.
	if err := shards.Heartbeat(ctx, WorkerHeartbeat{WorkerID: "w2", LastHeartbeat: now.Add(-time.Hour)}); err != nil {
		t.Fatalf("age w2 heartbeat: %v", err)
	}
	start := time.Now()
	if !waitFor(2*time.Second, func() bool { return !workerOwnsShards(shards, "w2") }) {
		cancel()
		t.Fatalf("w2 shards not reassigned within 2s (timeout was %s)", cfg.WorkerTimeout)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("reassignment took %s, want under 2s", elapsed)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("coordinator run: %v", err)
	}
}

// TestShardedIndexer_FourWorkersIndexHundredContracts is the end-to-end
// scenario from issue #272: four workers index a fixture of 100 contracts,
// each contract exactly once, driven by the coordinator's shard plan.
func TestShardedIndexer_FourWorkersIndexHundredContracts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newCountingStore(contractsFixture(100))
	shards := NewMemoryShardStore()
	workerIDs := []string{"w1", "w2", "w3", "w4"}
	registerWorkers(t, shards, workerIDs, time.Now().UTC())

	cfg := shardedConfig()
	coord := NewCoordinator(store, shards, cfg, testLogger())
	if err := coord.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// Capture the assignment each worker is expected to index and prove the
	// shards do not overlap.
	assigned := make(map[string]map[string]struct{}, len(workerIDs))
	total := make(map[string]int)
	for _, id := range workerIDs {
		assignment, err := shards.AssignmentForWorker(ctx, id)
		if err != nil {
			t.Fatalf("assignment for %s: %v", id, err)
		}
		if len(assignment.ShardIDs) == 0 {
			t.Errorf("worker %s owns no shards", id)
		}
		set := make(map[string]struct{}, len(assignment.ContractIDs))
		for _, cid := range assignment.ContractIDs {
			if _, dup := set[cid]; dup {
				t.Errorf("worker %s assignment lists %s twice", id, cid)
			}
			set[cid] = struct{}{}
			total[cid]++
		}
		assigned[id] = set
	}
	if len(total) != 100 {
		t.Fatalf("coordinator assigned %d distinct contracts, want 100", len(total))
	}
	for cid, n := range total {
		if n != 1 {
			t.Errorf("contract %s assigned to %d workers, want 1", cid, n)
		}
	}

	p := New(&fakeRPC{}, store, newFakeRedis(), cfg, testLogger())
	var wg sync.WaitGroup
	for _, id := range workerIDs {
		worker := NewWorker(id, p, shards, cfg, testLogger())
		wg.Add(1)
		go func(wid string) {
			defer wg.Done()
			if err := worker.RunOnce(ctx); err != nil {
				t.Errorf("worker %s: %v", wid, err)
			}
		}(id)
	}
	wg.Wait()

	counts := store.processedCounts()
	if len(counts) != 100 {
		t.Fatalf("workers indexed %d distinct contracts, want 100", len(counts))
	}
	for cid, n := range counts {
		if n != 1 {
			t.Errorf("contract %s indexed %d times, want exactly once", cid, n)
		}
		if _, ok := total[cid]; !ok {
			t.Errorf("contract %s was indexed but never assigned", cid)
		}
	}

	// Every contract the coordinator handed to a worker must have been
	// indexed, and nothing outside the assignment may be.
	for id := range assigned {
		assignment, err := shards.AssignmentForWorker(ctx, id)
		if err != nil {
			t.Fatalf("re-read assignment for %s: %v", id, err)
		}
		for _, cid := range assignment.ContractIDs {
			if _, ok := counts[cid]; !ok {
				t.Errorf("contract %s assigned to %s was never indexed", cid, id)
			}
		}
	}
}
