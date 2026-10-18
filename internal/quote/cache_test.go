package quote

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type cacheProvider struct {
	fetch func(context.Context, Query) (Quote, error)
}

func (p cacheProvider) Name() string { return "swift" }
func (p cacheProvider) GetQuote(ctx context.Context, q Query) (Quote, error) {
	return p.fetch(ctx, q)
}

type stubCache struct {
	get func(context.Context, Query) (Result, error)
	set func(context.Context, Query, Result, time.Duration) error
}

func (c stubCache) Get(ctx context.Context, q Query) (Result, error) {
	if c.get != nil {
		return c.get(ctx, q)
	}
	return Result{}, ErrCacheMiss
}

func (c stubCache) Set(ctx context.Context, q Query, r Result, ttl time.Duration) error {
	if c.set != nil {
		return c.set(ctx, q, r, ttl)
	}
	return nil
}

func cacheQuery() Query { return Query{Origin: "MOW", Destination: "LED", WeightGrams: 1000} }
func cacheQuote() Quote {
	return Quote{Provider: "swift", PriceMinor: 100, Currency: "RUB", EstimateDays: 1}
}

func testCached(t *testing.T, root context.Context, p Provider, cache Cache, config CacheConfig) *CachedService {
	t.Helper()
	service, err := NewService([]Provider{p}, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	cached, err := NewCachedService(root, service, cache, config)
	if err != nil {
		t.Fatal(err)
	}
	return cached
}

func cacheConfig() CacheConfig {
	return CacheConfig{TTL: 30 * time.Second, Timeout: 20 * time.Millisecond, WorkTimeout: time.Second, MaxRequests: 8}
}

// Done is evaluated by GetQuotes after its waiter has joined singleflight.
type waitingContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *waitingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for work")
	}
}

func TestSharedWorkSurvivesCallerCancellation(t *testing.T) {
	root, stop := context.WithCancel(context.Background())
	defer stop()
	release := make(chan struct{})
	var calls atomic.Int64
	p := cacheProvider{func(ctx context.Context, q Query) (Quote, error) {
		calls.Add(1)
		select {
		case <-release:
			return cacheQuote(), nil
		case <-ctx.Done():
			return Quote{}, ctx.Err()
		}
	}}
	s := testCached(t, root, p, stubCache{}, cacheConfig())
	ctx, cancel := context.WithCancel(root)
	first := &waitingContext{Context: ctx, waiting: make(chan struct{})}
	firstDone := make(chan error, 1)
	go func() { _, err := s.GetQuotes(first, cacheQuery()); firstDone <- err }()
	await(t, first.waiting)
	results := make(chan Result, 2)
	for range 2 {
		waiter := &waitingContext{Context: root, waiting: make(chan struct{})}
		go func() {
			result, err := s.GetQuotes(waiter, cacheQuery())
			if err != nil {
				t.Error(err)
			}
			results <- result
		}()
		await(t, waiter.waiting)
	}
	cancel()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first caller: %v", err)
	}
	close(release)
	a, b := <-results, <-results
	if !a.Complete || !b.Complete || calls.Load() != 1 {
		t.Fatalf("results = %+v, %+v; provider calls = %d", a, b, calls.Load())
	}
	a.Quotes[0].PriceMinor = 999
	if b.Quotes[0].PriceMinor != 100 {
		t.Fatal("callers share a mutable result slice")
	}
}

func TestAbandonedWaitersRemainBounded(t *testing.T) {
	root, stop := context.WithCancel(context.Background())
	defer stop()
	started := make(chan struct{})
	p := cacheProvider{func(ctx context.Context, q Query) (Quote, error) {
		close(started)
		<-ctx.Done()
		return Quote{}, ctx.Err()
	}}
	config := cacheConfig()
	config.MaxRequests = 1
	s := testCached(t, root, p, stubCache{}, config)
	ctx, cancel := context.WithCancel(root)
	done := make(chan error, 1)
	go func() { _, err := s.GetQuotes(ctx, cacheQuery()); done <- err }()
	await(t, started)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.GetQuotes(root, cacheQuery()); !errors.Is(err, ErrBusy) {
		t.Fatalf("abandoned work must retain its admission slot: %v", err)
	}
	stop()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for len(s.callers) != 0 {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("shutdown did not release the abandoned slot")
		}
	}
}

func TestSharedWorkTimeout(t *testing.T) {
	p := cacheProvider{func(ctx context.Context, q Query) (Quote, error) {
		<-ctx.Done()
		return Quote{}, ctx.Err()
	}}
	config := cacheConfig()
	config.WorkTimeout = 20 * time.Millisecond
	s := testCached(t, context.Background(), p, stubCache{}, config)
	if _, err := s.GetQuotes(context.Background(), cacheQuery()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shared work deadline: %v", err)
	}
	if len(s.callers) != 0 || len(s.service.permits) != 0 {
		t.Fatal("timeout leaked a permit")
	}
}

func TestCacheTimeoutsFallBack(t *testing.T) {
	p := cacheProvider{func(context.Context, Query) (Quote, error) { return cacheQuote(), nil }}
	cache := stubCache{
		get: func(ctx context.Context, q Query) (Result, error) { <-ctx.Done(); return Result{}, ctx.Err() },
		set: func(ctx context.Context, q Query, r Result, ttl time.Duration) error { <-ctx.Done(); return ctx.Err() },
	}
	s := testCached(t, context.Background(), p, cache, cacheConfig())
	result, err := s.GetQuotes(context.Background(), cacheQuery())
	if err != nil || !result.Complete || s.Stats() != (CacheStats{Errors: 2}) {
		t.Fatalf("result = %+v, error = %v, stats = %+v", result, err, s.Stats())
	}
}

func TestCacheHitAndCorruption(t *testing.T) {
	valid := Result{Quotes: []Quote{cacheQuote()}, Complete: true, GeneratedAt: time.Now().UTC()}
	for _, tc := range []struct {
		name  string
		value Result
		hit   bool
	}{
		{"complete", valid, true},
		{"partial", Result{Quotes: valid.Quotes, GeneratedAt: valid.GeneratedAt}, false},
		{"missing providers", Result{Complete: true, GeneratedAt: valid.GeneratedAt}, false},
		{"missing timestamp", Result{Complete: true, Quotes: valid.Quotes}, false},
		{"invalid quote", Result{Complete: true, Quotes: []Quote{{Provider: "swift"}}, GeneratedAt: valid.GeneratedAt}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls, writes := 0, 0
			p := cacheProvider{func(context.Context, Query) (Quote, error) { calls++; return cacheQuote(), nil }}
			cache := stubCache{
				get: func(context.Context, Query) (Result, error) { return tc.value, nil },
				set: func(ctx context.Context, q Query, r Result, ttl time.Duration) error {
					writes++
					if ttl != 30*time.Second || !r.Complete {
						t.Errorf("invalid cache write: TTL = %s, result = %+v", ttl, r)
					}
					return nil
				},
			}
			s := testCached(t, context.Background(), p, cache, cacheConfig())
			result, err := s.GetQuotes(context.Background(), cacheQuery())
			if err != nil || !result.Complete {
				t.Fatalf("result = %+v, error = %v", result, err)
			}
			if tc.hit {
				if calls != 0 || writes != 0 || !reflect.DeepEqual(result, valid) || s.Stats().Hits != 1 {
					t.Fatalf("cache hit: calls = %d, writes = %d, result = %+v", calls, writes, result)
				}
			} else if calls != 1 || writes != 1 || s.Stats().Errors != 1 {
				t.Fatalf("corruption: calls = %d, writes = %d, stats = %+v", calls, writes, s.Stats())
			}
		})
	}
}
