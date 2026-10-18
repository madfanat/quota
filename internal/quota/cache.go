package quota

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

var ErrCacheMiss = errors.New("quote not cached")

type Cache interface {
	Get(context.Context, Query) (Result, error)
	Set(context.Context, Query, Result, time.Duration) error
}

type CacheConfig struct {
	TTL         time.Duration
	Timeout     time.Duration
	WorkTimeout time.Duration
	MaxRequests int
}

type CacheStats struct {
	Hits   uint64
	Misses uint64
	Errors uint64
}

type CachedService struct {
	root    context.Context
	service *Service
	cache   Cache
	config  CacheConfig
	callers chan struct{}
	group   singleflight.Group
	hits    atomic.Uint64
	misses  atomic.Uint64
	errors  atomic.Uint64
}

func NewCachedService(root context.Context, service *Service, cache Cache, config CacheConfig) (*CachedService, error) {
	if root == nil || service == nil || cache == nil || config.TTL < time.Millisecond || config.Timeout <= 0 || config.WorkTimeout <= 0 || config.MaxRequests < 1 {
		return nil, errors.New("cache requires a root context, service, cache backend, TTL of at least 1ms, positive timeouts and request limit")
	}
	return &CachedService{
		root: root, service: service, cache: cache, config: config,
		callers: make(chan struct{}, config.MaxRequests),
	}, nil
}

func (s *CachedService) GetQuotes(ctx context.Context, query Query) (Result, error) {
	query, err := NewQuery(query.Origin, query.Destination, query.WeightGrams)
	if err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if err := s.root.Err(); err != nil {
		return Result{}, err
	}
	select {
	case s.callers <- struct{}{}:
	default:
		return Result{}, ErrBusy
	}
	key := fmt.Sprintf("%s:%s:%d", query.Origin, query.Destination, query.WeightGrams)
	done := s.group.DoChan(key, func() (any, error) {
		workCtx, cancel := context.WithTimeout(s.root, s.config.WorkTimeout)
		defer cancel()
		return s.load(workCtx, query)
	})
	select {
	case <-ctx.Done():

		go func() { <-done; <-s.callers }()
		return Result{}, ctx.Err()
	case shared := <-done:
		<-s.callers
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		if shared.Err != nil {
			return Result{}, shared.Err
		}
		result := shared.Val.(Result)
		result.Quotes = slices.Clone(result.Quotes)
		result.FailedProviders = slices.Clone(result.FailedProviders)
		return result, nil
	}
}

func (s *CachedService) GetQuotesSequential(ctx context.Context, query Query) (Result, error) {
	return s.service.GetQuotesSequential(ctx, query)
}

func (s *CachedService) Stats() CacheStats {
	return CacheStats{Hits: s.hits.Load(), Misses: s.misses.Load(), Errors: s.errors.Load()}
}

func (s *CachedService) AdmittedRequests() int { return len(s.callers) }

func (s *CachedService) load(ctx context.Context, query Query) (Result, error) {
	cacheCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	result, err := s.cache.Get(cacheCtx, query)
	cancel()
	switch {
	case err == nil && s.valid(result):
		s.hits.Add(1)
		return result, nil
	case errors.Is(err, ErrCacheMiss):
		s.misses.Add(1)
	default:
		s.errors.Add(1)
	}
	result, err = s.service.GetQuotes(ctx, query)
	if err != nil {
		return Result{}, err
	}
	if result.Complete {
		cacheCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
		err := s.cache.Set(cacheCtx, query, result, s.config.TTL)
		cancel()
		if err != nil {
			s.errors.Add(1)
		}
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	return result, nil
}

func (s *CachedService) valid(result Result) bool {
	if !result.Complete || len(result.FailedProviders) != 0 || result.GeneratedAt.IsZero() || len(result.Quotes) != len(s.service.providers) {
		return false
	}
	providers := make(map[string]bool, len(s.service.providers))
	for _, provider := range s.service.providers {
		providers[provider.Name()] = true
	}
	for _, item := range result.Quotes {
		if !providers[item.Provider] || item.PriceMinor <= 0 || item.Currency != "RUB" || item.EstimateDays <= 0 {
			return false
		}
		delete(providers, item.Provider)
	}
	return slices.IsSortedFunc(result.Quotes, compareQuotes)
}
