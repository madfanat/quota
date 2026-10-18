//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/madfanat/quota/internal/carrierhttp"
	"github.com/madfanat/quota/internal/quote"
	"github.com/madfanat/quota/internal/redis"
	"github.com/madfanat/quota/internal/simulators"
	goredis "github.com/redis/go-redis/v9"
)

var redisWeight atomic.Int64

type redisFixture struct {
	ctx     context.Context
	raw     *goredis.Client
	service *quote.CachedService
	query   quote.Query
	sims    []*simulators.Simulator
	started chan struct{}
}

func newRedisFixture(t *testing.T, ttl time.Duration, failures int, gate <-chan struct{}, cacheAddr string) *redisFixture {
	t.Helper()
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Fatal("set REDIS_TEST_ADDR to a disposable Redis instance; see README.md")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	f := &redisFixture{
		ctx: ctx, query: quote.Query{Origin: "MOW", Destination: "LED", WeightGrams: 18000 + int(redisWeight.Add(1))},
		started: make(chan struct{}, 64),
	}
	f.raw = goredis.NewClient(&goredis.Options{Addr: addr, Protocol: 2, MaxRetries: -1, ContextTimeoutEnabled: true})
	t.Cleanup(func() { f.raw.Close() })
	if err := f.raw.Ping(ctx).Err(); err != nil {
		cancel()
		t.Fatalf("Redis is not ready: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		f.raw.Del(ctx, f.key())
	})
	var providers []quote.Provider
	for i, name := range []string{"swift", "steady"} {
		sim := simulators.New(name, 0, i < failures)
		f.sims = append(f.sims, sim)
		handler := sim.Handler()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			f.started <- struct{}{}
			if gate != nil {
				select {
				case <-gate:
				case <-r.Context().Done():
					return
				}
			}
			handler.ServeHTTP(w, r)
		}))
		t.Cleanup(server.Close)
		providers = append(providers, carrierhttp.New(name, server.URL+"/quote", server.Client()))
	}
	service, err := quote.NewService(providers, 2, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if cacheAddr == "" {
		cacheAddr = addr
	}
	cache := redis.New(cacheAddr, 100*time.Millisecond, 32)
	t.Cleanup(func() { cache.Close() })
	f.service, err = quote.NewCachedService(ctx, service, cache, quote.CacheConfig{
		TTL: ttl, Timeout: 100 * time.Millisecond, WorkTimeout: 3 * time.Second, MaxRequests: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cancel)
	return f
}

func (f *redisFixture) key() string {
	return fmt.Sprintf("quota:quotes:v1:%s:%s:%d", f.query.Origin, f.query.Destination, f.query.WeightGrams)
}

func (f *redisFixture) get(t *testing.T) quote.Result {
	t.Helper()
	result, err := f.service.GetQuotes(f.ctx, f.query)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func (f *redisFixture) assertCalls(t *testing.T, count int) {
	t.Helper()
	for _, sim := range f.sims {
		if stats := sim.Stats(); stats.Calls != count {
			t.Fatalf("provider counters = %+v; want %d calls", stats, count)
		}
	}
}

func TestRedisHitMissAndExpiry(t *testing.T) {
	f := newRedisFixture(t, time.Second, 0, nil, "")
	first := f.get(t)
	q := f.query
	q.Origin, q.Destination = " mow ", "led"
	second, err := f.service.GetQuotes(f.ctx, q)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("normalized cache hit: result = %+v, error = %v", second, err)
	}
	f.assertCalls(t, 1)
	if stats := f.service.Stats(); stats != (quote.CacheStats{Hits: 1, Misses: 1}) {
		t.Fatalf("stats = %+v", stats)
	}
	ttl, err := f.raw.PTTL(f.ctx, f.key()).Result()
	if err != nil || ttl <= 0 || ttl > time.Second {
		t.Fatalf("TTL = %s, error = %v", ttl, err)
	}
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		exists, err := f.raw.Exists(f.ctx, f.key()).Result()
		if err != nil {
			t.Fatal(err)
		}
		if exists == 0 {
			break
		}
		select {
		case <-tick.C:
		case <-f.ctx.Done():
			t.Fatal("cached quote did not expire")
		}
	}
	third := f.get(t)
	if !third.GeneratedAt.After(first.GeneratedAt) {
		t.Fatal("expiry did not generate a fresh quote")
	}
	f.assertCalls(t, 2)
}

func TestRedisCorruptDataIsRefetched(t *testing.T) {
	for _, data := range []string{`{`, `{"complete":true,"quotes":[],"generated_at":"2026-10-18T00:00:00Z"}`} {
		f := newRedisFixture(t, time.Second, 0, nil, "")
		if err := f.raw.Set(f.ctx, f.key(), data, time.Second).Err(); err != nil {
			t.Fatal(err)
		}
		result := f.get(t)
		f.assertCalls(t, 1)
		if !result.Complete || f.service.Stats().Errors != 1 {
			t.Fatalf("result = %+v, stats = %+v", result, f.service.Stats())
		}
		if cached := f.get(t); !reflect.DeepEqual(result, cached) {
			t.Fatal("corrupt entry was not replaced")
		}
		f.assertCalls(t, 1)
	}
}

func TestRedisSkipsPartialAndErrorResults(t *testing.T) {
	for _, failures := range []int{1, 2} {
		f := newRedisFixture(t, time.Second, failures, nil, "")
		for range 2 {
			result, err := f.service.GetQuotes(f.ctx, f.query)
			if failures == 1 && (err != nil || result.Complete || len(result.Quotes) != 1) {
				t.Fatalf("partial result = %+v, error = %v", result, err)
			}
			if failures == 2 && !errors.Is(err, quote.ErrUnavailable) {
				t.Fatalf("all failed: %v", err)
			}
		}
		f.assertCalls(t, 2)
		if count, err := f.raw.Exists(f.ctx, f.key()).Result(); err != nil || count != 0 {
			t.Fatalf("partial/error result was cached: count = %d, error = %v", count, err)
		}
	}
}

type registeredContext struct {
	context.Context
	registered chan struct{}
	once       sync.Once
}

func (c *registeredContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.registered) })
	return c.Context.Done()
}

func TestRedisConcurrentMissesShareFetch(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	f := newRedisFixture(t, time.Second, 0, release, "")
	results := make(chan quote.Result, 20)
	for range 20 {
		ctx := &registeredContext{Context: f.ctx, registered: make(chan struct{})}
		go func() {
			result, err := f.service.GetQuotes(ctx, f.query)
			if err != nil {
				t.Error(err)
			}
			results <- result
		}()
		select {
		case <-ctx.registered:
		case <-f.ctx.Done():
			t.Fatal("caller failed to join shared work")
		}
	}
	unblock()
	var first quote.Result
	for i := range 20 {
		result := <-results
		if !result.Complete {
			t.Fatal("incomplete shared result")
		}
		if i == 0 {
			first = result
		} else if !reflect.DeepEqual(first, result) {
			t.Fatal("shared callers received different results")
		}
	}
	f.assertCalls(t, 1)
	if stats := f.service.Stats(); stats != (quote.CacheStats{Misses: 1}) {
		t.Fatalf("shared miss stats = %+v", stats)
	}
}

func TestRedisOutageFallsBackWithProviderLimit(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	f := newRedisFixture(t, time.Second, 0, release, addr)
	first := make(chan quote.Result, 1)
	go func() {
		result, err := f.service.GetQuotes(f.ctx, f.query)
		if err != nil {
			t.Error(err)
		}
		first <- result
	}()
	for range 2 {
		select {
		case <-f.started:
		case <-f.ctx.Done():
			t.Fatal("Redis outage did not fall back to both providers")
		}
	}
	var callers sync.WaitGroup
	for i := range 12 {
		callers.Go(func() {
			query := f.query
			query.WeightGrams += 100 + i
			if _, err := f.service.GetQuotes(f.ctx, query); !errors.Is(err, quote.ErrBusy) {
				t.Errorf("provider limit during Redis outage: %v", err)
			}
		})
	}
	callers.Wait()
	select {
	case <-f.started:
		t.Fatal("Redis outage bypassed the global provider limit")
	default:
	}
	unblock()
	result := <-first
	if !result.Complete {
		t.Fatal("Redis outage prevented provider fallback")
	}
	f.assertCalls(t, 1)
	if f.service.Stats().Errors != 14 {
		t.Fatalf("expected 13 GET failures and one SET failure, stats = %+v", f.service.Stats())
	}
}
