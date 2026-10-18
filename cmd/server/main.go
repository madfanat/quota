package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/madfanat/quota/internal/carrierhttp"
	"github.com/madfanat/quota/internal/httpapi"
	"github.com/madfanat/quota/internal/quote"
	"github.com/madfanat/quota/internal/redis"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	maxCalls := flag.Int("max-provider-calls", 8, "maximum provider calls across all requests")
	maxRequests := flag.Int("max-requests", 32, "maximum admitted quote requests")
	budget := flag.Duration("request-timeout", 600*time.Millisecond, "total quote request budget")
	providerTimeout := flag.Duration("provider-timeout", 250*time.Millisecond, "HTTP provider call timeout")
	sequential := flag.Bool("sequential", false, "run the sequential Day 1 baseline")
	redisAddr := flag.String("redis-addr", "127.0.0.1:6379", "Redis cache address")
	cacheTTL := flag.Duration("cache-ttl", 30*time.Second, "complete quote cache lifetime")
	cacheTimeout := flag.Duration("cache-timeout", 30*time.Millisecond, "budget for each Redis operation")
	flag.Parse()
	if _, _, err := net.SplitHostPort(*redisAddr); err != nil {
		slog.Error("redis-addr must be a host:port address", "error", err)
		os.Exit(1)
	}
	if *maxCalls < 1 || *maxRequests < 1 || *budget <= 0 || *providerTimeout <= 0 || *cacheTTL < time.Millisecond || *cacheTimeout <= 0 {
		slog.Error("limits and timeouts must be positive")
		os.Exit(1)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = *maxCalls
	client := &http.Client{
		Transport:     transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	defer transport.CloseIdleConnections()
	service, err := quote.NewService([]quote.Provider{
		carrierhttp.New("swift", "http://127.0.0.1:8081/quote", client),
		carrierhttp.New("steady", "http://127.0.0.1:8082/quote", client),
	}, *maxCalls, *providerTimeout)
	if err != nil {
		slog.Error("configure service", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cache := redis.New(*redisAddr, *cacheTimeout, *maxRequests)
	defer cache.Close()
	cached, err := quote.NewCachedService(ctx, service, cache, quote.CacheConfig{
		TTL: *cacheTTL, Timeout: *cacheTimeout, WorkTimeout: *budget, MaxRequests: *maxRequests,
	})
	if err != nil {
		slog.Error("configure cache", "error", err)
		os.Exit(1)
	}
	defer func() {
		stats := cached.Stats()
		slog.Info("cache statistics", "hits", stats.Hits, "misses", stats.Misses, "errors", stats.Errors)
	}()
	server := &http.Server{
		Addr: *addr, Handler: httpapi.New(cached, *maxRequests, *budget, *sequential),
		ReadHeaderTimeout: 2 * time.Second, WriteTimeout: *budget + time.Second,
		IdleTimeout: 30 * time.Second,
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("shutdown", "error", err)
			server.Close()
		}
	}()
	slog.Info("quota listening", "address", *addr, "sequential", *sequential, "max_provider_calls", *maxCalls)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("serve", "error", err)
		os.Exit(1)
	}
	<-shutdownDone
}
