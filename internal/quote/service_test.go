package quote_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/madfanat/quota/internal/quote"
)

type provider struct {
	name  string
	fetch func(context.Context, quote.Query) (quote.Quote, error)
}

func (p provider) Name() string { return p.name }
func (p provider) GetQuote(ctx context.Context, q quote.Query) (quote.Quote, error) {
	return p.fetch(ctx, q)
}

func newService(t *testing.T, providers []quote.Provider, limit int) *quote.Service {
	t.Helper()
	s, err := quote.NewService(providers, limit)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func testQuery() quote.Query {
	return quote.Query{Origin: "MOW", Destination: "LED", WeightGrams: 1000}
}

func TestNewQuery(t *testing.T) {
	q, err := quote.NewQuery(" mow ", "led", 1000)
	if err != nil || q != testQuery() {
		t.Fatalf("query = %+v, error = %v", q, err)
	}
	for _, q := range []quote.Query{
		{Origin: "XXX", Destination: "LED", WeightGrams: 1000},
		{Origin: "MOW", Destination: "", WeightGrams: 1000},
		{Origin: "MOW", Destination: "LED", WeightGrams: 0},
		{Origin: "MOW", Destination: "LED", WeightGrams: 30001},
	} {
		if _, err := quote.NewQuery(q.Origin, q.Destination, q.WeightGrams); !errors.Is(err, quote.ErrInvalidQuery) {
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
		return provider{name, func(ctx context.Context, q quote.Query) (quote.Quote, error) {
			started <- name
			select {
			case <-release:
				return quote.Quote{Provider: name, PriceMinor: 100, Currency: "RUB", EstimateDays: 1}, nil
			case <-ctx.Done():
				return quote.Quote{}, ctx.Err()
			}
		}}
	}
	s := newService(t, []quote.Provider{makeProvider("swift"), makeProvider("steady")}, 2)
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
				providers := make([]quote.Provider, 2)
				for i, name := range []string{"swift", "steady"} {
					providers[i] = provider{name, func(ctx context.Context, q quote.Query) (quote.Quote, error) {
						if i < failures {
							return quote.Quote{}, errors.New("offline")
						}
						return quote.Quote{Provider: name, PriceMinor: 100, Currency: "RUB", EstimateDays: 1}, nil
					}}
				}
				s := newService(t, providers, 2)
				fetch := s.GetQuotes
				if mode == "sequential" {
					fetch = s.GetQuotesSequential
				}
				result, err := fetch(context.Background(), testQuery())
				if failures == 2 {
					if !errors.Is(err, quote.ErrUnavailable) {
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
	p := provider{"swift", func(ctx context.Context, q quote.Query) (quote.Quote, error) {
		started <- struct{}{}
		<-ctx.Done()
		return quote.Quote{}, ctx.Err()
	}}
	s := newService(t, []quote.Provider{p}, 1)
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
	// The completed call must have returned its only permit.
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
	p := provider{"swift", func(ctx context.Context, q quote.Query) (quote.Quote, error) {
		started <- struct{}{}
		select {
		case <-release:
			return quote.Quote{Provider: "swift", PriceMinor: 100}, nil
		case <-ctx.Done():
			return quote.Quote{}, ctx.Err()
		}
	}}
	s := newService(t, []quote.Provider{p}, 2)
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
		if _, err := s.GetQuotes(ctx, testQuery()); !errors.Is(err, quote.ErrBusy) {
			t.Fatalf("saturated call: error = %v", err)
		}
	}
	close(release)
	wg.Wait()
}
