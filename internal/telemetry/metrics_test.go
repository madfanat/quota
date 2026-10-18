package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/madfanat/quota/internal/httpapi"
	"github.com/madfanat/quota/internal/quota"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testProvider struct{}

func (testProvider) Name() string { return "swift" }
func (testProvider) GetQuote(ctx context.Context, q quota.Query) (quota.Quote, error) {
	<-ctx.Done()
	return quota.Quote{}, ctx.Err()
}

type testCache struct{}

func (testCache) Get(context.Context, quota.Query) (quota.Result, error) {
	return quota.Result{}, quota.ErrCacheMiss
}
func (testCache) Set(context.Context, quota.Query, quota.Result, time.Duration) error { return nil }

func TestHTTPMetricsKeepLabelsBounded(t *testing.T) {
	m := New()
	handler := m.HTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("unavailable"))
	}))
	for _, path := range []string{"/v1/quotes?origin=secret-input", "/untrusted-one", "/untrusted-two"} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 503 || w.Body.String() != "unavailable" || w.Header().Get("Content-Type") != "text/plain" {
			t.Fatal("metrics middleware changed the response")
		}
	}
	if got := testutil.ToFloat64(m.requests.WithLabelValues("http", "unmatched", "503")); got != 2 {
		t.Fatalf("unknown routes must share one label: %v", got)
	}
	if got := testutil.ToFloat64(m.inFlight.WithLabelValues("http")); got != 0 {
		t.Fatalf("completed requests retained in-flight gauge: %v", got)
	}
	w := httptest.NewRecorder()
	m.Admin().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != 200 || strings.Contains(w.Body.String(), "secret-input") || strings.Contains(w.Body.String(), "untrusted-one") {
		t.Fatal("raw request values appeared in metric labels")
	}
	if got := testutil.ToFloat64(m.errors.WithLabelValues("http", "503")); got != 3 {
		t.Fatalf("request errors = %v", got)
	}
}

func TestGRPCMetrics(t *testing.T) {
	m := New()
	_, err := m.Unary()(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/untrusted/method"},
		func(context.Context, any) (any, error) { return nil, status.Error(codes.ResourceExhausted, "busy") })
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(m.requests.WithLabelValues("grpc", "unmatched", "ResourceExhausted")); got != 1 {
		t.Fatalf("RPC counter = %v", got)
	}
	if got := testutil.ToFloat64(m.inFlight.WithLabelValues("grpc")); got != 0 {
		t.Fatalf("RPC gauge = %v", got)
	}
}

func TestProviderCancellationMetrics(t *testing.T) {
	m := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := m.Provider(testProvider{}).GetQuote(ctx, quota.Query{})
	if err != context.Canceled {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(m.providerInFlight.WithLabelValues("swift")); got != 0 {
		t.Fatalf("provider retained in-flight gauge: %v", got)
	}
	if got := testutil.ToFloat64(m.providerFailures.WithLabelValues("swift", "canceled")); got != 1 {
		t.Fatalf("provider failures = %v", got)
	}
}

func TestCacheCountersAndAdminIsolation(t *testing.T) {
	m := New()
	service, err := quota.NewService([]quota.Provider{m.Provider(testProvider{})}, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	cached, err := quota.NewCachedService(context.Background(), service, m.Cache(testCache{}), quota.CacheConfig{
		TTL: time.Second, Timeout: time.Second, WorkTimeout: time.Millisecond, MaxRequests: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	m.TrackCache(cached)
	cached.GetQuotes(context.Background(), quota.Query{Origin: "MOW", Destination: "LED", WeightGrams: 1000})
	w := httptest.NewRecorder()
	m.Admin().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(w.Body.String(), "quota_cache_misses_total 1") || !strings.Contains(w.Body.String(), "quota_admission_slots 0") {
		t.Fatal("cache statistics were not exported")
	}
	api := m.HTTP(httpapi.New(cached, 1, time.Second, false))
	for _, path := range []string{"/metrics", "/debug/pprof/", "/debug/pprof/heap"} {
		w := httptest.NewRecorder()
		api.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("diagnostics exposed on public API: %s = %d", path, w.Code)
		}
	}
}
