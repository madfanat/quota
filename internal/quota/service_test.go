package quota_test

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/madfanat/quota/internal/quota"
)

type provider struct {
	name  string
	fetch func(context.Context, quota.Query) (quota.Quote, error)
}

func (p provider) Name() string { return p.name }
func (p provider) GetQuote(ctx context.Context, q quota.Query) (quota.Quote, error) {
	return p.fetch(ctx, q)
}

func newService(t *testing.T, providers []quota.Provider, limit int) *quota.Service {
	t.Helper()
	s, err := quota.NewService(providers, limit, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func testQuery() quota.Query {
	return quota.Query{Origin: "MOW", Destination: "LED", WeightGrams: 1000}
}

func TestNewQuery(t *testing.T) {
	q, err := quota.NewQuery(" mow ", "led", 1000)
	if err != nil || q != testQuery() {
		t.Fatalf("query = %+v, error = %v", q, err)
	}
	for _, q := range []quota.Query{
		{Origin: "XXX", Destination: "LED", WeightGrams: 1000},
		{Origin: "MOW", Destination: "", WeightGrams: 1000},
		{Origin: "MOW", Destination: "LED", WeightGrams: 0},
		{Origin: "MOW", Destination: "LED", WeightGrams: 30001},
	} {
		if _, err := quota.NewQuery(q.Origin, q.Destination, q.WeightGrams); !errors.Is(err, quota.ErrInvalidQuery) {
			t.Fatalf("query %+v: error = %v", q, err)
		}
	}
}

func TestProvidersStartConcurrently(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := make(chan string, 2)
	release := make(chan struct{})
	makeProvider := func(name string) provider {
		return provider{name, func(ctx context.Context, q quota.Query) (quota.Quote, error) {
			started <- name
			select {
			case <-release:
				return quota.Quote{Provider: name, PriceMinor: 100, Currency: "RUB", EstimateDays: 1}, nil
			case <-ctx.Done():
				return quota.Quote{}, ctx.Err()
			}
		}}
	}
	s := newService(t, []quota.Provider{makeProvider("swift"), makeProvider("steady")}, 2)
	done := make(chan error, 1)
	go func() {
		result, err := s.GetQuotes(ctx, testQuery())
		if err == nil && (!result.Complete || len(result.Quotes) != 2) {
			err = errors.New("expected two complete quotes")
		}
		done <- err
	}()
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("both providers must start before either is released")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestResultAndFailures(t *testing.T) {
	for _, mode := range []string{"concurrent", "sequential"} {
		for _, failures := range []int{0, 1, 2} {
			t.Run(mode+"/failures="+strconv.Itoa(failures), func(t *testing.T) {
				providers := make([]quota.Provider, 2)
				for i, name := range []string{"swift", "steady"} {
					providers[i] = provider{name, func(ctx context.Context, q quota.Query) (quota.Quote, error) {
						if i < failures {
							return quota.Quote{}, errors.New("offline")
						}
						return quota.Quote{Provider: name, PriceMinor: 100, Currency: "RUB", EstimateDays: 1}, nil
					}}
				}
				s := newService(t, providers, 2)
				fetch := s.GetQuotes
				if mode == "sequential" {
					fetch = s.GetQuotesSequential
				}
				result, err := fetch(context.Background(), testQuery())
				if failures == 2 {
					if !errors.Is(err, quota.ErrUnavailable) {
						t.Fatalf("error = %v", err)
					}
					return
				}
				if err != nil || len(result.Quotes) != 2-failures || result.Complete != (failures == 0) || len(result.FailedProviders) != failures || result.GeneratedAt.IsZero() {
					t.Fatalf("result = %+v, error = %v", result, err)
				}
				if failures == 0 && result.Quotes[0].Provider != "steady" {
					t.Fatal("equal prices must be ordered by provider")
				}
			})
		}
	}
}

func TestCancellationReturnsPermit(t *testing.T) {
	started := make(chan struct{}, 1)
	p := provider{"swift", func(ctx context.Context, q quota.Query) (quota.Quote, error) {
		started <- struct{}{}
		<-ctx.Done()
		return quota.Quote{}, ctx.Err()
	}}
	s := newService(t, []quota.Provider{p}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := s.GetQuotes(ctx, testQuery()); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("provider did not start")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go func() { _, err := s.GetQuotes(ctx2, testQuery()); done <- err }()
	select {
	case <-started:
		cancel2()
	case err := <-done:
		t.Fatalf("permit not returned: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("second provider call did not start")
	}
	<-done
}

func TestGlobalLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	p := provider{"swift", func(ctx context.Context, q quota.Query) (quota.Quote, error) {
		started <- struct{}{}
		select {
		case <-release:
			return quota.Quote{Provider: "swift", PriceMinor: 100}, nil
		case <-ctx.Done():
			return quota.Quote{}, ctx.Err()
		}
	}}
	s := newService(t, []quota.Provider{p}, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { s.GetQuotes(ctx, testQuery()) })
	}
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("provider did not start")
		}
	}
	for range 10 {
		if _, err := s.GetQuotes(ctx, testQuery()); !errors.Is(err, quota.ErrBusy) {
			t.Fatalf("saturated call: error = %v", err)
		}
	}
	close(release)
	wg.Wait()
}

func TestProviderTimeout(t *testing.T) {
	for _, mode := range []string{"concurrent", "sequential"} {
		t.Run(mode, func(t *testing.T) {
			slow := provider{"slow", func(ctx context.Context, q quota.Query) (quota.Quote, error) {
				if _, ok := ctx.Deadline(); !ok {
					t.Error("provider has no deadline")
				}
				<-ctx.Done()
				return quota.Quote{}, ctx.Err()
			}}
			healthy := provider{"healthy", func(ctx context.Context, q quota.Query) (quota.Quote, error) {
				return quota.Quote{Provider: "healthy", PriceMinor: 100}, ctx.Err()
			}}
			s, err := quota.NewService([]quota.Provider{slow, healthy}, 2, 50*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			fetch := s.GetQuotes
			if mode == "sequential" {
				fetch = s.GetQuotesSequential
			}
			result, err := fetch(ctx, testQuery())
			if err != nil || result.Complete || len(result.Quotes) != 1 || !reflect.DeepEqual(result.FailedProviders, []string{"slow"}) {
				t.Fatalf("result = %+v, error = %v", result, err)
			}
			if ctx.Err() != nil {
				t.Fatal("provider timeout cancelled its caller")
			}
		})
	}
}

func TestEarlierCallerDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	wantDeadline, _ := ctx.Deadline()
	p := provider{"swift", func(ctx context.Context, q quota.Query) (quota.Quote, error) {
		deadline, ok := ctx.Deadline()
		if !ok || !deadline.Equal(wantDeadline) {
			t.Errorf("provider deadline = %v, want caller deadline %v", deadline, wantDeadline)
		}
		<-ctx.Done()
		return quota.Quote{}, ctx.Err()
	}}
	s := newService(t, []quota.Provider{p}, 1)
	if _, err := s.GetQuotes(ctx, testQuery()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired caller: error = %v", err)
	}
}

func TestCancelledCallerAtCapacity(t *testing.T) {
	started := make(chan struct{}, 1)
	p := provider{"swift", func(ctx context.Context, q quota.Query) (quota.Quote, error) {
		started <- struct{}{}
		<-ctx.Done()
		return quota.Quote{}, ctx.Err()
	}}
	s := newService(t, []quota.Provider{p}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := s.GetQuotes(ctx, testQuery()); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("provider did not start")
	}
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := s.GetQuotes(cancelled, testQuery()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled request at capacity: error = %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}

func TestConcurrentResultOrdering(t *testing.T) {
	want := []quota.Quote{
		{Provider: "steady", PriceMinor: 100},
		{Provider: "swift", PriceMinor: 100},
		{Provider: "budget", PriceMinor: 200},
	}
	var providers []quota.Provider
	for _, item := range []quota.Quote{want[2], want[1], want[0]} {
		providers = append(providers, provider{item.Provider, func(context.Context, quota.Query) (quota.Quote, error) {
			return item, nil
		}})
	}
	s := newService(t, providers, 48)
	var callers sync.WaitGroup
	for range 16 {
		callers.Go(func() {
			result, err := s.GetQuotes(context.Background(), testQuery())
			if err != nil || !result.Complete || !reflect.DeepEqual(result.Quotes, want) {
				t.Errorf("result = %+v, error = %v", result, err)
			}
		})
	}
	callers.Wait()
}
