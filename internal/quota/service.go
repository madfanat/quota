package quota

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"
)

type Provider interface {
	Name() string
	GetQuote(context.Context, Query) (Quote, error)
}

type Service struct {
	providers       []Provider
	permits         chan struct{}
	providerTimeout time.Duration
}

func NewService(providers []Provider, maxCalls int, providerTimeout time.Duration) (*Service, error) {
	if len(providers) == 0 || maxCalls < 1 || providerTimeout <= 0 {
		return nil, errors.New("at least one provider, a positive provider limit and timeout are required")
	}
	return &Service{
		providers:       slices.Clone(providers),
		permits:         make(chan struct{}, maxCalls),
		providerTimeout: providerTimeout,
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
	group, groupCtx := errgroup.WithContext(ctx)
	for i, provider := range s.providers {
		fetch := func() error {
			outcomes[i].quote, outcomes[i].err = s.fetch(groupCtx, provider, query)
			return ctx.Err()
		}
		if concurrent {
			group.Go(fetch)
		} else {
			if err := fetch(); err != nil {
				return Result{}, err
			}
		}
	}

	if err := group.Wait(); err != nil {
		return Result{}, err
	}
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
	slices.SortFunc(result.Quotes, compareQuotes)
	result.Complete = len(result.FailedProviders) == 0
	return result, nil
}

func compareQuotes(a, b Quote) int {
	if price := cmp.Compare(a.PriceMinor, b.PriceMinor); price != 0 {
		return price
	}
	return cmp.Compare(a.Provider, b.Provider)
}

func (s *Service) fetch(ctx context.Context, provider Provider, query Query) (Quote, error) {
	if err := ctx.Err(); err != nil {
		return Quote{}, err
	}
	select {
	case <-ctx.Done():
		return Quote{}, ctx.Err()
	case s.permits <- struct{}{}:
		defer func() { <-s.permits }()
	default:
		return Quote{}, ErrBusy
	}
	if err := ctx.Err(); err != nil {
		return Quote{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.providerTimeout)
	defer cancel()
	return provider.GetQuote(ctx, query)
}
