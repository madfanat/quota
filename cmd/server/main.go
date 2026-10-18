package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/madfanat/quota/internal/carrierhttp"
	quotav1 "github.com/madfanat/quota/internal/gen/quota/v1"
	"github.com/madfanat/quota/internal/grpcapi"
	"github.com/madfanat/quota/internal/httpapi"
	"github.com/madfanat/quota/internal/quota"
	"github.com/madfanat/quota/internal/redis"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:9090", "gRPC listen address")
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
		return fmt.Errorf("redis-addr must be a host:port address: %w", err)
	}
	if *maxCalls < 1 || *maxRequests < 1 || *budget <= 0 || *providerTimeout <= 0 || *cacheTTL < time.Millisecond || *cacheTimeout <= 0 {
		return errors.New("limits and timeouts must be positive; cache TTL must be at least 1ms")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = *maxCalls
	client := &http.Client{
		Transport:     transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	defer transport.CloseIdleConnections()
	service, err := quota.NewService([]quota.Provider{
		carrierhttp.New("swift", "http://127.0.0.1:8081/quote", client),
		carrierhttp.New("steady", "http://127.0.0.1:8082/quote", client),
	}, *maxCalls, *providerTimeout)
	if err != nil {
		return fmt.Errorf("configure service: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cache := redis.New(*redisAddr, *cacheTimeout, *maxRequests)
	defer cache.Close()
	cached, err := quota.NewCachedService(ctx, service, cache, quota.CacheConfig{
		TTL: *cacheTTL, Timeout: *cacheTimeout, WorkTimeout: *budget, MaxRequests: *maxRequests,
	})
	if err != nil {
		return fmt.Errorf("configure cache: %w", err)
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
	httpListener, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listen HTTP: %w", err)
	}
	defer httpListener.Close()
	grpcListener, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		return fmt.Errorf("listen gRPC: %w", err)
	}
	defer grpcListener.Close()
	rpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(64 << 10))
	quotav1.RegisterQuotaServiceServer(rpcServer, grpcapi.New(cached, *maxRequests, *budget, *sequential))
	serveErrors := make(chan error, 2)
	go func() { serveErrors <- server.Serve(httpListener) }()
	go func() { serveErrors <- rpcServer.Serve(grpcListener) }()
	slog.Info("quota listening", "http", *addr, "grpc", *grpcAddr, "sequential", *sequential, "max_provider_calls", *maxCalls)
	var serveErr error
	select {
	case serveErr = <-serveErrors:
	case <-ctx.Done():
	}
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	rpcDone := make(chan struct{})
	go func() { rpcServer.GracefulStop(); close(rpcDone) }()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP shutdown", "error", err)
		server.Close()
	}
	select {
	case <-rpcDone:
	case <-shutdownCtx.Done():
		rpcServer.Stop()
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) && !errors.Is(serveErr, grpc.ErrServerStopped) {
		return serveErr
	}
	return nil
}
