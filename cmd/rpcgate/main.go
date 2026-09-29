// Command rpcgate is a JSON-RPC gateway that spreads Ethereum traffic across
// providers and routes around unhealthy or lagging ones.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Amadeus-22/rpcgate/internal/adapter/httpapi"
	"github.com/Amadeus-22/rpcgate/internal/adapter/lrucache"
	"github.com/Amadeus-22/rpcgate/internal/adapter/upstreamhttp"
	"github.com/Amadeus-22/rpcgate/internal/app"
	"github.com/Amadeus-22/rpcgate/internal/domain/upstream"
	"github.com/Amadeus-22/rpcgate/internal/platform/config"
	"github.com/Amadeus-22/rpcgate/internal/platform/logging"
	"github.com/Amadeus-22/rpcgate/internal/platform/metrics"
)

const shutdownTimeout = 15 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "rpcgate:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}
	f := cfg.File
	log := logging.New(os.Stdout, cfg.LogLevel)
	rec := metrics.New()

	httpClient := upstreamhttp.NewHTTPClient()
	members := make([]app.Member, 0, len(f.Upstreams))
	for _, u := range f.Upstreams {
		client, err := upstreamhttp.New(u.Name, u.URL, f.Routing.UpstreamTimeout, httpClient)
		if err != nil {
			return err
		}
		members = append(members, app.Member{Name: u.Name, Weight: u.Weight, MaxRPS: u.MaxRPS, Client: client})
	}
	pool := app.NewPool(app.PoolConfig{
		MaxLagBlocks: f.Health.MaxLagBlocks,
		State: upstream.Settings{
			BreakerThreshold: f.Breaker.FailureThreshold,
			BreakerCooldown:  f.Breaker.Cooldown,
			LatencyAlpha:     0.3,
			ErrorRateAlpha:   0.1,
		},
	}, members)

	var cache app.Cache
	if f.Cache.MaxBytes > 0 {
		cache = lrucache.New(f.Cache.MaxBytes)
	}
	router := app.NewRouter(pool, cache, rec, log, app.RouterConfig{
		MaxRetries:       *f.Routing.MaxRetries,
		RetryBackoff:     25 * time.Millisecond,
		FinalityDepth:    f.Cache.FinalityDepth,
		BatchConcurrency: 16,
	})
	health := app.NewHealthChecker(pool, app.HealthConfig{
		Interval: f.Health.Interval,
		Timeout:  f.Health.Timeout,
		ChainID:  f.ChainID,
	}, rec, log)

	keys := make([]app.ClientLimit, 0, len(f.Clients.Keys))
	for _, k := range f.Clients.Keys {
		keys = append(keys, app.ClientLimit{Key: k.Key, RPS: k.RPS, Burst: k.Burst})
	}
	var anonymous *app.ClientLimit
	if a := f.Clients.Anonymous; a != nil {
		anonymous = &app.ClientLimit{RPS: a.RPS, Burst: a.Burst}
	}
	limiter := app.NewRateLimiter(keys, anonymous)

	api := httpapi.New(router, limiter, pool, rec.Handler(), log, httpapi.Config{
		RequestTimeout: f.Routing.RequestTimeout,
		MaxBatchSize:   f.Routing.MaxBatchSize,
		MaxBodyBytes:   f.Routing.MaxBodyBytes,
	})
	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      f.Routing.RequestTimeout + 5*time.Second,
		IdleTimeout:       90 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Probe once before accepting traffic so the first requests have a head.
	health.CheckOnce(ctx)
	if !pool.Ready() {
		log.Warn("no upstream reachable at startup; serving 503 until one answers")
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return health.Run(gctx) })
	g.Go(func() error {
		log.Info("listening", slog.String("addr", cfg.ListenAddr), slog.Int("upstreams", len(members)))
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve http: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		log.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown http: %w", err)
		}
		return nil
	})
	return g.Wait()
}
