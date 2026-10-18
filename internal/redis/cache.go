package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/madfanat/quota/internal/quota"
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

func (c *Cache) Get(ctx context.Context, query quota.Query) (quota.Result, error) {
	data, err := c.client.Get(ctx, key(query)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return quota.Result{}, quota.ErrCacheMiss
	}
	if err != nil {
		return quota.Result{}, err
	}
	var result quota.Result
	if err := json.Unmarshal(data, &result); err != nil {
		return quota.Result{}, fmt.Errorf("corrupt cached quote: %w", err)
	}
	return result, nil
}

func (c *Cache) Set(ctx context.Context, query quota.Query, result quota.Result, ttl time.Duration) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, key(query), data, ttl).Err()
}

func (c *Cache) Close() error { return c.client.Close() }

func key(query quota.Query) string {
	return fmt.Sprintf("quota:quotes:v1:%s:%s:%d", query.Origin, query.Destination, query.WeightGrams)
}
