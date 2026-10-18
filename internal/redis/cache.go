package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/madfanat/quota/internal/quote"
	goredis "github.com/redis/go-redis/v9"
)

type Cache struct {
	client *goredis.Client
}

func New(addr string, timeout time.Duration, poolSize int) *Cache {
	return &Cache{client: goredis.NewClient(&goredis.Options{
		Addr: addr, Protocol: 2, MaxRetries: -1, DialerRetries: 1,
		DialTimeout: timeout, ReadTimeout: timeout, WriteTimeout: timeout,
		PoolTimeout: timeout, PoolSize: poolSize, MaxActiveConns: poolSize,
		ContextTimeoutEnabled: true, DisableIdentity: true,
	})}
}

func (c *Cache) Get(ctx context.Context, query quote.Query) (quote.Result, error) {
	data, err := c.client.Get(ctx, key(query)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return quote.Result{}, quote.ErrCacheMiss
	}
	if err != nil {
		return quote.Result{}, err
	}
	var result quote.Result
	if err := json.Unmarshal(data, &result); err != nil {
		return quote.Result{}, fmt.Errorf("corrupt cached quote: %w", err)
	}
	return result, nil
}

func (c *Cache) Set(ctx context.Context, query quote.Query, result quote.Result, ttl time.Duration) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, key(query), data, ttl).Err()
}

func (c *Cache) Close() error { return c.client.Close() }

func key(query quote.Query) string {
	return fmt.Sprintf("quota:quotes:v1:%s:%s:%d", query.Origin, query.Destination, query.WeightGrams)
}
