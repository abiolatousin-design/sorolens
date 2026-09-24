package handler

import (
	"context"
	"log/slog"
	"time"

	"github.com/sorolens/sorolens/apps/api/internal/store"
)

// APIStore is the combined read/write interface required by the HTTP handlers.
type APIStore interface {
	store.Store
	store.QueryStore
	store.WatchdogStore
	store.ContractUpgradeStore
	store.HealthScoreStore
	store.APIKeyStore
	store.AlertSubscriptionStore
	store.WatchlistStore
	store.UserStore
	store.PerformanceStore
	store.ContractVerificationStore
}

// Pinger is implemented by both the postgres pool and the Redis client.
type Pinger interface {
	Ping(ctx context.Context) error
}

type RedisClient interface {
	Incr(ctx context.Context, key string) (int64, error)
	Expire(ctx context.Context, key string, expiration time.Duration) (bool, error)
}

// Handler holds shared dependencies for all HTTP handlers.
type Handler struct {
	Store       APIStore
	DB          Pinger
	Redis       Pinger
	RedisClient RedisClient
	Logger      *slog.Logger
	StreamHub   *StreamHub
	// Verifier rebuilds submitted contract source and compares the resulting
	// Wasm hash against the on-chain hash (issue #263). It is nil on
	// deployments without a build sandbox (for example the Vercel serverless
	// entrypoint), in which case POST /contracts/{id}/verify answers 503.
	Verifier ContractVerifier
}
