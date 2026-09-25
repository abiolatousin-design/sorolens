package poller

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// contractIDsFixture returns n deterministic 56-character contract IDs.
func contractIDsFixture(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("C%055d", i)
	}
	return ids
}

func TestShardForContract_DeterministicInRange(t *testing.T) {
	t.Parallel()

	const shards = 4
	seen := make(map[int]bool, shards)
	for _, id := range contractIDsFixture(100) {
		s := ShardForContract(id, shards)
		if s < 0 || s >= shards {
			t.Fatalf("shard %d out of range for %s", s, id)
		}
		if got := ShardForContract(id, shards); got != s {
			t.Fatalf("shard for %s is not deterministic: %d != %d", id, got, s)
		}
		seen[s] = true
	}
	if len(seen) != shards {
		t.Errorf("contracts spread over %d shards, want all %d", len(seen), shards)
	}
}

func TestShardForContract_NonPositiveCountIsShardZero(t *testing.T) {
	t.Parallel()

	if got := ShardForContract("CANY", 0); got != 0 {
		t.Errorf("shard with 0 shards = %d, want 0", got)
	}
	if got := ShardForContract("CANY", -1); got != 0 {
		t.Errorf("shard with negative shard count = %d, want 0", got)
	}
}

func TestPlanShards_PartitionsEveryContractOnce(t *testing.T) {
	t.Parallel()

	contracts := contractsFixture(100)
	shards := planShards(contracts, 4)
	if len(shards) != 4 {
		t.Fatalf("shard count = %d, want 4", len(shards))
	}

	seen := make(map[string]int, len(contracts))
	for _, shard := range shards {
		for _, c := range shard {
			seen[c.ID]++
		}
	}
	if len(seen) != len(contracts) {
		t.Fatalf("plan covered %d contracts, want %d", len(seen), len(contracts))
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("contract %s assigned %d times, want 1", id, n)
		}
	}
}

func TestPlanShards_NonPositiveCountIsSingleShard(t *testing.T) {
	t.Parallel()

	shards := planShards([]Contract{{ID: "C1"}, {ID: "C2"}}, 0)
	if len(shards) != 1 || len(shards[0]) != 2 {
		t.Fatalf("single-shard plan = %+v, want one shard holding both contracts", shards)
	}
}

func TestMemoryShardStore_LeaderElection(t *testing.T) {
	t.Parallel()

	store := NewMemoryShardStore()
	ctx := context.Background()

	first, err := store.TryAcquireLeaderLock(ctx)
	if err != nil || !first {
		t.Fatalf("first acquire = %v, %v; want true, nil", first, err)
	}
	second, err := store.TryAcquireLeaderLock(ctx)
	if err != nil || second {
		t.Fatalf("second acquire = %v, %v; want false, nil", second, err)
	}
	if err := store.ReleaseLeaderLock(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	third, err := store.TryAcquireLeaderLock(ctx)
	if err != nil || !third {
		t.Fatalf("acquire after release = %v, %v; want true, nil", third, err)
	}
}

func TestMemoryShardStore_AssignmentRoundTrip(t *testing.T) {
	t.Parallel()

	store := NewMemoryShardStore()
	ctx := context.Background()
	now := time.Now().UTC()
	if err := store.SaveAssignments(ctx, []ShardAssignment{
		{ShardID: 1, WorkerID: "w1", ContractIDs: []string{"C2"}, AssignedAt: now},
		{ShardID: 0, WorkerID: "w1", ContractIDs: []string{"C1"}, AssignedAt: now},
		{ShardID: 2, WorkerID: "w2", ContractIDs: []string{"C3"}, AssignedAt: now},
	}); err != nil {
		t.Fatalf("save assignments: %v", err)
	}

	all, err := store.Assignments(ctx)
	if err != nil || len(all) != 3 {
		t.Fatalf("assignments = %+v, %v; want 3", all, err)
	}
	if all[0].ShardID != 0 || all[1].ShardID != 1 || all[2].ShardID != 2 {
		t.Errorf("assignments not sorted by shard: %+v", all)
	}

	got, err := store.AssignmentForWorker(ctx, "w1")
	if err != nil {
		t.Fatalf("assignment for w1: %v", err)
	}
	if len(got.ShardIDs) != 2 || got.ShardIDs[0] != 0 || got.ShardIDs[1] != 1 {
		t.Errorf("w1 shards = %v, want [0 1]", got.ShardIDs)
	}
	if len(got.ContractIDs) != 2 || got.ContractIDs[0] != "C1" || got.ContractIDs[1] != "C2" {
		t.Errorf("w1 contracts = %v, want [C1 C2]", got.ContractIDs)
	}

	empty, err := store.AssignmentForWorker(ctx, "nobody")
	if err != nil || len(empty.ShardIDs) != 0 || len(empty.ContractIDs) != 0 {
		t.Errorf("unknown worker assignment = %+v, %v; want empty", empty, err)
	}
}

func TestMemoryShardStore_HeartbeatLifecycle(t *testing.T) {
	t.Parallel()

	store := NewMemoryShardStore()
	ctx := context.Background()
	now := time.Now().UTC()
	if err := store.Heartbeat(ctx, WorkerHeartbeat{WorkerID: "w2", ShardIDs: []int{3, 1}, LastHeartbeat: now}); err != nil {
		t.Fatalf("heartbeat w2: %v", err)
	}
	if err := store.Heartbeat(ctx, WorkerHeartbeat{WorkerID: "w1", LastHeartbeat: now.Add(-time.Second)}); err != nil {
		t.Fatalf("heartbeat w1: %v", err)
	}

	workers, err := store.Workers(ctx)
	if err != nil || len(workers) != 2 {
		t.Fatalf("workers = %+v, %v; want 2", workers, err)
	}
	if workers[0].WorkerID != "w1" || workers[1].WorkerID != "w2" {
		t.Errorf("workers not sorted by id: %+v", workers)
	}
	if len(workers[1].ShardIDs) != 2 || workers[1].ShardIDs[0] != 1 || workers[1].ShardIDs[1] != 3 {
		t.Errorf("w2 shards = %v, want [1 3]", workers[1].ShardIDs)
	}

	if err := store.RemoveWorker(ctx, "w1"); err != nil {
		t.Fatalf("remove w1: %v", err)
	}
	workers, err = store.Workers(ctx)
	if err != nil || len(workers) != 1 || workers[0].WorkerID != "w2" {
		t.Errorf("after remove = %+v, %v; want only w2", workers, err)
	}
}
