package poller

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/sorolens/sorolens/services/indexer/internal/partition"
)

// Defaults for the sharded topology. Config fields left at their zero value
// fall back to these.
const (
	defaultShardCount        = 1
	defaultHeartbeatInterval = 10 * time.Second
	defaultWorkerTimeout     = 30 * time.Second
	defaultReconcileInterval = 5 * time.Second
)

// ContractLister is the subset of Store the coordinator needs to discover the
// contracts it partitions. Store satisfies it.
type ContractLister interface {
	ListContracts(ctx context.Context, cursor string, limit int) ([]Contract, string, error)
}

// Coordinator owns shard assignment. Exactly one coordinator is active at a
// time, elected by the leader lock; the others exit immediately. The leader
// re-runs Reconcile on an interval, keeping a shard on its current worker
// while that worker is healthy and moving it off workers that stop
// heartbeating.
type Coordinator struct {
	contracts ContractLister
	store     ShardStore
	cfg       Config
	log       *slog.Logger
}

// NewCoordinator returns a Coordinator that partitions the contracts exposed
// by contracts and records the resulting ownership in store.
func NewCoordinator(contracts ContractLister, store ShardStore, cfg Config, log *slog.Logger) *Coordinator {
	return &Coordinator{contracts: contracts, store: store, cfg: cfg, log: log}
}

func (c *Coordinator) shardCount() int {
	if c.cfg.ShardCount > 0 {
		return c.cfg.ShardCount
	}
	return defaultShardCount
}

func (c *Coordinator) workerTimeout() time.Duration {
	if c.cfg.WorkerTimeout > 0 {
		return c.cfg.WorkerTimeout
	}
	return defaultWorkerTimeout
}

func (c *Coordinator) reconcileInterval() time.Duration {
	if c.cfg.ReconcileInterval > 0 {
		return c.cfg.ReconcileInterval
	}
	return defaultReconcileInterval
}

// Run elects the leader and reconciles shard assignments until ctx is done. A
// coordinator that does not win the leader lock returns nil immediately:
// another process is already doing the work.
func (c *Coordinator) Run(ctx context.Context) error {
	leader, err := c.store.TryAcquireLeaderLock(ctx)
	if err != nil {
		return fmt.Errorf("coordinator: acquire leader lock: %w", err)
	}
	if !leader {
		c.log.Info("coordinator: another instance holds the leader lock, exiting")
		return nil
	}
	c.log.Info("coordinator: elected leader", "shards", c.shardCount())
	defer func() {
		if err := c.store.ReleaseLeaderLock(context.WithoutCancel(ctx)); err != nil {
			c.log.Warn("coordinator: release leader lock", "err", err)
		}
	}()

	ticker := time.NewTicker(c.reconcileInterval())
	defer ticker.Stop()
	for {
		if err := c.Reconcile(ctx); err != nil {
			c.log.Error("coordinator: reconcile", "err", err)
		}
		select {
		case <-ctx.Done():
			c.log.Info("coordinator: shutting down")
			return nil
		case <-ticker.C:
		}
	}
}

// Reconcile performs one assignment pass: it partitions the trackable contracts
// into shards and assigns every shard to a live worker, keeping a shard on its
// current worker while that worker is healthy and moving it off workers that
// missed the heartbeat deadline. The new plan is written in a single
// SaveAssignments call, so workers never observe a half-applied plan.
func (c *Coordinator) Reconcile(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}

	contracts, err := c.listContracts(ctx)
	if err != nil {
		return fmt.Errorf("list contracts: %w", err)
	}
	shards := planShards(contracts, c.shardCount())

	workers, err := c.store.Workers(ctx)
	if err != nil {
		return fmt.Errorf("list workers: %w", err)
	}
	now := time.Now().UTC()
	live := make([]string, 0, len(workers))
	liveSet := make(map[string]struct{}, len(workers))
	for _, w := range workers {
		if now.Sub(w.LastHeartbeat) > c.workerTimeout() {
			c.log.Warn("coordinator: worker missed heartbeat deadline",
				"worker_id", w.WorkerID,
				"last_heartbeat", w.LastHeartbeat,
				"timeout", c.workerTimeout(),
			)
			continue
		}
		live = append(live, w.WorkerID)
		liveSet[w.WorkerID] = struct{}{}
	}
	sort.Strings(live)

	existing, err := c.store.Assignments(ctx)
	if err != nil {
		return fmt.Errorf("load assignments: %w", err)
	}
	// Start from the current plan so healthy workers keep their shards; only
	// shards whose owner is missing or stale are up for reassignment.
	owners := make(map[int]ShardAssignment, len(existing))
	load := make(map[string]int, len(live))
	for _, a := range existing {
		if _, ok := liveSet[a.WorkerID]; !ok {
			continue
		}
		if a.ShardID < 0 || a.ShardID >= len(shards) {
			continue
		}
		owners[a.ShardID] = a
		load[a.WorkerID]++
	}

	plan := make([]ShardAssignment, 0, len(shards))
	reassigned := 0
	for shardID, shardContracts := range shards {
		a, kept := owners[shardID]
		if !kept {
			if len(live) == 0 {
				// Nobody to own the shard yet; leave it unassigned and pick it
				// up on a later pass once a worker registers.
				continue
			}
			a = ShardAssignment{
				ShardID:    shardID,
				WorkerID:   leastLoaded(live, load),
				AssignedAt: now,
			}
			load[a.WorkerID]++
			reassigned++
		}
		if a.AssignedAt.IsZero() {
			a.AssignedAt = now
		}
		a.ContractIDs = contractIDs(shardContracts)
		plan = append(plan, a)
	}

	if err := c.store.SaveAssignments(ctx, plan); err != nil {
		return fmt.Errorf("save assignments: %w", err)
	}
	c.log.Info("coordinator: shards reconciled",
		"shards", len(plan),
		"workers", len(live),
		"reassigned", reassigned,
		"contracts", len(contracts),
	)
	return nil
}

// leastLoaded returns the live worker owning the fewest shards. live is sorted,
// so ties break deterministically on worker ID.
func leastLoaded(live []string, load map[string]int) string {
	best := live[0]
	for _, w := range live[1:] {
		if load[w] < load[best] {
			best = w
		}
	}
	return best
}

// contractIDs returns the sorted IDs of the given contracts.
func contractIDs(contracts []Contract) []string {
	ids := make([]string, 0, len(contracts))
	for _, c := range contracts {
		ids = append(ids, c.ID)
	}
	sort.Strings(ids)
	return ids
}

// listContracts pages through every trackable contract. Contracts that are not
// active or backfilling are excluded: they have nothing to index, so sharding
// them would only spread the assignment plan thin.
func (c *Coordinator) listContracts(ctx context.Context) ([]Contract, error) {
	var out []Contract
	var cursor string
	for {
		if ctx.Err() != nil {
			return out, nil
		}
		contracts, next, err := c.contracts.ListContracts(ctx, cursor, 50)
		if err != nil {
			return nil, err
		}
		for _, contract := range contracts {
			if contract.Status == "active" || contract.Status == "backfilling" {
				out = append(out, contract)
			}
		}
		if next == "" {
			return out, nil
		}
		cursor = next
	}
}

// Worker indexes only the contracts the coordinator assigned to it. It
// refreshes its heartbeat while it works; if it stops (crash, partition,
// kill -9), the coordinator stops seeing fresh heartbeats and reassigns its
// shards to the remaining workers.
type Worker struct {
	id     string
	poller *Poller
	store  ShardStore
	cfg    Config
	log    *slog.Logger
}

// NewWorker returns a sharded worker identified by id. An empty id is replaced
// with "worker" so heartbeats always carry a stable key.
func NewWorker(id string, p *Poller, store ShardStore, cfg Config, log *slog.Logger) *Worker {
	if id == "" {
		id = "worker"
	}
	return &Worker{id: id, poller: p, store: store, cfg: cfg, log: log}
}

func (w *Worker) heartbeatInterval() time.Duration {
	if w.cfg.HeartbeatInterval > 0 {
		return w.cfg.HeartbeatInterval
	}
	return defaultHeartbeatInterval
}

// Run registers the worker, then pulls and indexes its assignment until ctx is
// cancelled. Usage metrics and anomaly/health jobs are not sharded; see
// docs/indexer-sharding.md for where they run in the scaled topology.
func (w *Worker) Run(ctx context.Context) error {
	w.log.Info("worker: starting", "worker_id", w.id)
	defer func() {
		if err := w.store.RemoveWorker(context.WithoutCancel(ctx), w.id); err != nil {
			w.log.Warn("worker: deregister", "worker_id", w.id, "err", err)
		}
	}()

	// A worker may start before the coordinator has ever run, so make sure the
	// next month's partition exists before indexing into it.
	if err := partition.EnsureNextMonthPartition(ctx, w.poller.store); err != nil {
		w.log.Warn("worker: ensure next month partition", "err", err)
	}

	if err := w.store.Heartbeat(ctx, WorkerHeartbeat{
		WorkerID:      w.id,
		LastHeartbeat: time.Now().UTC(),
	}); err != nil {
		return fmt.Errorf("worker: initial heartbeat: %w", err)
	}

	for {
		if err := w.RunOnce(ctx); err != nil {
			w.log.Error("worker: pass failed", "worker_id", w.id, "err", err)
		}
		select {
		case <-ctx.Done():
			w.log.Info("worker: shutting down", "worker_id", w.id)
			return nil
		case <-time.After(w.heartbeatInterval()):
		}
	}
}

// RunOnce pulls this worker's assignment and indexes the assigned contracts one
// time. A background heartbeat keeps the worker alive in the coordinator's view
// for the whole pass, so a long pass is not mistaken for a dead worker.
func (w *Worker) RunOnce(ctx context.Context) error {
	assignment, err := w.store.AssignmentForWorker(ctx, w.id)
	if err != nil {
		return fmt.Errorf("load assignment: %w", err)
	}
	w.heartbeat(ctx, assignment.ShardIDs)
	stop := w.startHeartbeat(ctx, assignment.ShardIDs)
	defer stop()

	contracts, err := w.poller.contractsByID(ctx, assignment.ContractIDs)
	if err != nil {
		return fmt.Errorf("load assigned contracts: %w", err)
	}
	w.log.Info("worker: indexing assignment",
		"worker_id", w.id,
		"shards", assignment.ShardIDs,
		"contracts", len(contracts),
	)
	w.poller.ProcessContracts(ctx, contracts)
	return nil
}

// heartbeat records one liveness update, logging (but not failing the pass on)
// an error: a missed heartbeat is recoverable, a lost assignment is not.
func (w *Worker) heartbeat(ctx context.Context, shardIDs []int) {
	if err := w.store.Heartbeat(ctx, WorkerHeartbeat{
		WorkerID:      w.id,
		ShardIDs:      shardIDs,
		LastHeartbeat: time.Now().UTC(),
	}); err != nil {
		w.log.Warn("worker: heartbeat", "worker_id", w.id, "err", err)
	}
}

// startHeartbeat refreshes the worker's heartbeat on an interval until the
// returned stop function is called.
func (w *Worker) startHeartbeat(ctx context.Context, shardIDs []int) func() {
	hbCtx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(w.heartbeatInterval())
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				w.heartbeat(hbCtx, shardIDs)
			}
		}
	}()
	return cancel
}

// ProcessContracts indexes the given contracts, logging and continuing past a
// per-contract failure. It is the body a sharded worker runs for its
// assignment; standalone mode reaches the same code path through processAll.
//
// The context is detached for each contract so an in-flight batch finishes and
// commits its cursor even if ctx is cancelled mid-flight (see processAll).
func (p *Poller) ProcessContracts(ctx context.Context, contracts []Contract) {
	for _, c := range contracts {
		if ctx.Err() != nil {
			return
		}
		if c.Status != "active" && c.Status != "backfilling" {
			continue
		}
		if err := p.processContract(context.WithoutCancel(ctx), c); err != nil {
			p.log.Error("failed to index contract",
				"contract_id", c.ID,
				"err", err,
			)
		}
	}
}

// contractsByID loads the full Contract records for the given IDs, paging
// through the store so a large shard does not require one unbounded query.
func (p *Poller) contractsByID(ctx context.Context, ids []string) ([]Contract, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	want := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		want[id] = struct{}{}
	}
	var out []Contract
	var cursor string
	for {
		if ctx.Err() != nil {
			return out, nil
		}
		contracts, next, err := p.store.ListContracts(ctx, cursor, 50)
		if err != nil {
			return nil, err
		}
		for _, c := range contracts {
			if _, ok := want[c.ID]; ok {
				out = append(out, c)
			}
		}
		if next == "" {
			break
		}
		cursor = next
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
