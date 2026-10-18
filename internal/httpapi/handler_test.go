package httpapi_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/madfanat/quota/internal/httpapi"
	"github.com/madfanat/quota/internal/quote"
)

const queryPath = "/v1/quotes?origin=MOW&destination=LED&weight_grams=1000"

type provider struct {
	fetch func(context.Context, quote.Query) (quote.Quote, error)
}

func (p provider) Name() string { return "swift" }
func (p provider) GetQuote(ctx context.Context, q quote.Query) (quote.Quote, error) {
	return p.fetch(ctx, q)
}

func TestAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	s, err := quote.NewService([]quote.Provider{provider{func(ctx context.Context, q quote.Query) (quote.Quote, error) {
		started <- struct{}{}
		select {
		case <-release:
			return quote.Quote{Provider: "swift", PriceMinor: 100, Currency: "RUB", EstimateDays: 1}, nil
		case <-ctx.Done():
			return quote.Quote{}, ctx.Err()
		}
	}}}, 2)
	if err != nil {
		t.Fatal(err)
	}
	handler := httpapi.New(s, 1, 3*time.Second, false)
	first := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(first, httptest.NewRequest("GET", queryPath, nil).WithContext(ctx))
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first request did not start")
	}
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest("GET", queryPath, nil).WithContext(ctx))
	if second.Code != http.StatusServiceUnavailable {
		t.Fatalf("saturated status = %d", second.Code)
	}
	close(release)
	<-done
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d", first.Code)
	}
}

func TestFailureStatuses(t *testing.T) {
	s, err := quote.NewService([]quote.Provider{provider{func(context.Context, quote.Query) (quote.Quote, error) {
		return quote.Quote{}, errors.New("offline")
	}}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	handler := httpapi.New(s, 1, time.Second, false)
	for _, expired := range []bool{false, true} {
		ctx := context.Background()
		status := http.StatusServiceUnavailable
		if expired {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
			defer cancel()
			status = http.StatusGatewayTimeout
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("GET", queryPath, nil).WithContext(ctx))
		if w.Code != status {
			t.Fatalf("expired = %v: status = %d, want %d", expired, w.Code, status)
		}
	}
}
