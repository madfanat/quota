package quote

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"sync"
	"time"
)

type Provider interface {
	Name() string
	GetQuote(context.Context, Query) (Quote, error)
}

type Service struct {
	providers []Provider
	permits   chan struct{}
}

func NewService(providers []Provider, maxCalls int) (*Service, error) {
	if len(providers) == 0 || maxCalls < 1 {
		return nil, errors.New("at least one provider and a positive provider limit are required")
	}
	return &Service{
		providers: slices.Clone(providers),
		permits:   make(chan struct{}, maxCalls),
	}, nil
}

func (s *Service) GetQuotes(ctx context.Context, query Query) (Result, error) {
	return s.getQuotes(ctx, query, true)
}

func (s *Service) GetQuotesSequential(ctx context.Context, query Query) (Result, error) {
	return s.getQuotes(ctx, query, false)
}

func (s *Service) getQuotes(ctx context.Context, query Query, concurrent bool) (Result, error) {
	query, err := NewQuery(query.Origin, query.Destination, query.WeightGrams)
	if err != nil {
		return Result{}, err
	}
	type outcome struct {
		quote Quote
		err   error
	}
	outcomes := make([]outcome, len(s.providers))
	var wg sync.WaitGroup
	for i, provider := range s.providers {
		fetch := func() {
			outcomes[i].quote, outcomes[i].err = s.fetch(ctx, provider, query)
		}
		if concurrent {
			wg.Go(fetch)
		} else {
			fetch()
		}
	}

	wg.Wait()
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	result := Result{
		Quotes:          make([]Quote, 0, len(s.providers)),
		FailedProviders: make([]string, 0, len(s.providers)),
		GeneratedAt:     time.Now().UTC(),
	}
	busy := false
	for i, outcome := range outcomes {
		if outcome.err != nil {
			result.FailedProviders = append(result.FailedProviders, s.providers[i].Name())
			busy = busy || errors.Is(outcome.err, ErrBusy)
			continue
		}
		result.Quotes = append(result.Quotes, outcome.quote)
	}
	if len(result.Quotes) == 0 {
		if busy {
			return Result{}, ErrBusy
		}
		return Result{}, ErrUnavailable
	}
	slices.SortFunc(result.Quotes, func(a, b Quote) int {
		if price := cmp.Compare(a.PriceMinor, b.PriceMinor); price != 0 {
			return price
		}
		return cmp.Compare(a.Provider, b.Provider)
	})
	result.Complete = len(result.FailedProviders) == 0
	return result, nil
}

func (s *Service) fetch(ctx context.Context, provider Provider, query Query) (Quote, error) {
	if err := ctx.Err(); err != nil {
		return Quote{}, err
	}
	select {
	case s.permits <- struct{}{}:
		defer func() { <-s.permits }()
	default:
		return Quote{}, ErrBusy
	}
	return provider.GetQuote(ctx, query)
}
