package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/madfanat/quota/internal/carrierhttp"
	"github.com/madfanat/quota/internal/httpapi"
	"github.com/madfanat/quota/internal/quote"
	"github.com/madfanat/quota/internal/simulators"
)

func TestHTTPQuotes(t *testing.T) {
	swift := httptest.NewServer(simulators.New("swift", 0, false).Handler())
	defer swift.Close()
	steady := httptest.NewServer(simulators.New("steady", 0, false).Handler())
	defer steady.Close()
	client := &http.Client{Timeout: time.Second}
	service, err := quote.NewService([]quote.Provider{
		carrierhttp.New("swift", swift.URL+"/quote", client),
		carrierhttp.New("steady", steady.URL+"/quote", client),
	}, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	handler := httpapi.New(service, 4, time.Second, false)
	for _, tc := range []struct {
		method string
		path   string
		status int
	}{
		{"GET", "/v1/quotes?origin=%20mow%20&destination=led&weight_grams=1000", 200},
		{"GET", "/v1/quotes?origin=MOW&destination=LED&weight_grams=0", 400},
		{"GET", "/v1/quotes?origin=XXX&destination=LED&weight_grams=1000", 400},
		{"GET", "/v1/quotes?origin=MOW&destination=LED&weight_grams=abc", 400},
		{"POST", "/v1/quotes", 405},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
			if w.Code != tc.status {
				t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
			}
			if tc.status != 200 {
				return
			}
			var result quote.Result
			if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if !result.Complete || len(result.Quotes) != 2 || result.Quotes[0].Provider != "steady" || result.Quotes[0].PriceMinor != 35000 || result.Quotes[1].PriceMinor != 52000 {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}

func TestSimulatorsObserveGlobalLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	started := make(chan struct{}, 26)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var mu sync.Mutex
	active, maximum, calls := 0, 0, 0
	newProvider := func(name string) *carrierhttp.Provider {
		sim := simulators.New(name, 0, false)
		handler := sim.Handler()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			active++
			calls++
			maximum = max(maximum, active)
			mu.Unlock()
			defer func() { mu.Lock(); active--; mu.Unlock() }()
			started <- struct{}{}
			select {
			case <-release:
				handler.ServeHTTP(w, r)
			case <-r.Context().Done():
			}
		}))
		t.Cleanup(server.Close)
		return carrierhttp.New(name, server.URL+"/quote", &http.Client{Timeout: 4 * time.Second})
	}
	service, err := quote.NewService([]quote.Provider{newProvider("swift"), newProvider("steady")}, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	query := quote.Query{Origin: "MOW", Destination: "LED", WeightGrams: 1000}
	go func() { _, err := service.GetQuotes(ctx, query); done <- err }()
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("both HTTP providers did not start")
		}
	}
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			_, err := service.GetQuotes(ctx, query)
			if err != quote.ErrBusy {
				t.Errorf("saturated request: error = %v", err)
			}
		})
	}
	wg.Wait()
	mu.Lock()
	if active != 2 || maximum != 2 || calls != 2 {
		t.Errorf("active = %d, maximum = %d, calls = %d; want 2 each", active, maximum, calls)
	}
	mu.Unlock()
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDelayedProviders(t *testing.T) {
	var sims []*simulators.Simulator
	var providers []quote.Provider

	for _, name := range []string{"swift", "steady"} {
		sim := simulators.New(name, 100*time.Millisecond, false)
		server := httptest.NewServer(sim.Handler())
		defer server.Close()
		sims = append(sims, sim)
		providers = append(providers, carrierhttp.New(name, server.URL+"/quote", &http.Client{Timeout: time.Second}))
	}
	service, err := quote.NewService(providers, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	query := quote.Query{Origin: "MOW", Destination: "LED", WeightGrams: 1000}
	start := time.Now()
	if _, err := service.GetQuotesSequential(context.Background(), query); err != nil {
		t.Fatal(err)
	}
	sequential := time.Since(start)
	start = time.Now()
	if _, err := service.GetQuotes(context.Background(), query); err != nil {
		t.Fatal(err)
	}
	concurrent := time.Since(start)
	t.Logf("two 100ms simulators: sequential=%s concurrent=%s", sequential, concurrent)
	for _, sim := range sims {
		stats := sim.Stats()
		if stats.Active != 0 || stats.MaxConcurrent != 1 || stats.Calls != 2 {
			t.Fatalf("simulator counters = %+v", stats)
		}
	}
}

func TestHTTPProviderFailures(t *testing.T) {
	for _, tc := range []struct {
		name            string
		broken          string
		bothBroken      bool
		requestTimeout  time.Duration
		providerTimeout time.Duration
		status          int
	}{
		{"failing", "status", false, time.Second, time.Second, 200},
		{"malformed", "json", false, time.Second, time.Second, 200},
		{"slow", "slow", false, time.Second, 50 * time.Millisecond, 200},
		{"all failing", "status", true, time.Second, time.Second, 503},
		{"all malformed", "json", true, time.Second, time.Second, 503},
		{"all provider deadlines", "slow", true, time.Second, 50 * time.Millisecond, 503},
		{"request deadline", "slow", true, 50 * time.Millisecond, time.Second, 504},
	} {
		t.Run(tc.name, func(t *testing.T) {
			newProvider := func(name string, broken bool) quote.Provider {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if broken {
						switch tc.broken {
						case "status":
							http.Error(w, "offline", http.StatusServiceUnavailable)
						case "json":
							w.Write([]byte(`{`))
						case "slow":
							<-r.Context().Done()
						}
						return
					}
					simulators.New(name, 10*time.Millisecond, false).Handler().ServeHTTP(w, r)
				}))
				t.Cleanup(server.Close)
				return carrierhttp.New(name, server.URL+"/quote", server.Client())
			}
			service, err := quote.NewService([]quote.Provider{
				newProvider("swift", true), newProvider("steady", tc.bothBroken),
			}, 2, tc.providerTimeout)
			if err != nil {
				t.Fatal(err)
			}
			handler := httpapi.New(service, 4, tc.requestTimeout, false)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest("GET", "/v1/quotes?origin=MOW&destination=LED&weight_grams=1000", nil))
			if w.Code != tc.status {
				t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
			}
			if tc.status == 200 {
				var result quote.Result
				if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				if result.Complete || len(result.Quotes) != 1 || result.Quotes[0].Provider != "steady" || !reflect.DeepEqual(result.FailedProviders, []string{"swift"}) {
					t.Fatalf("result = %+v", result)
				}
			}
		})
	}
}

func TestRepeatedHTTPCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan struct{}, 2)
	stopped := make(chan struct{}, 2)
	var providers []quote.Provider
	for _, name := range []string{"swift", "steady"} {
		sim := simulators.New(name, 0, false).Handler()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("weight_grams") == "2000" {
				sim.ServeHTTP(w, r)
				return
			}
			started <- struct{}{}
			<-r.Context().Done()
			stopped <- struct{}{}
		}))
		t.Cleanup(server.Close)
		providers = append(providers, carrierhttp.New(name, server.URL+"/quote", server.Client()))
	}
	service, err := quote.NewService(providers, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	handler := httpapi.New(service, 1, time.Second, false)
	for range 20 {
		requestCtx, stop := context.WithCancel(ctx)
		req := httptest.NewRequest("GET", "/v1/quotes?origin=MOW&destination=LED&weight_grams=1000", nil).WithContext(requestCtx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			handler.ServeHTTP(httptest.NewRecorder(), req)
		}()
		for range 2 {
			select {
			case <-started:
			case <-ctx.Done():
				stop()
				t.Fatal("provider did not start; a permit may have leaked")
			}
		}
		stop()
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("cancelled handler did not stop")
		}
		for range 2 {
			select {
			case <-stopped:
			case <-ctx.Done():
				t.Fatal("provider HTTP handler did not stop")
			}
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("GET", "/v1/quotes?origin=MOW&destination=LED&weight_grams=2000", nil).WithContext(ctx))
		var result quote.Result
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if w.Code != 200 || !result.Complete || len(result.Quotes) != 2 {
			t.Fatalf("after cancellation: status = %d, result = %+v", w.Code, result)
		}
	}
	if err := ctx.Err(); err != nil {
		t.Fatal(err)
	}
}
