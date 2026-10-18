package telemetry

import (
	"context"
	"errors"
	"net/http"
	"net/http/pprof"
	"strconv"
	"time"

	"github.com/madfanat/quota/internal/quota"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type Metrics struct {
	registry         *prometheus.Registry
	requests         *prometheus.CounterVec
	errors           *prometheus.CounterVec
	duration         *prometheus.HistogramVec
	inFlight         *prometheus.GaugeVec
	providerCalls    *prometheus.CounterVec
	providerFailures *prometheus.CounterVec
	providerDuration *prometheus.HistogramVec
	providerInFlight *prometheus.GaugeVec
	cacheDuration    *prometheus.HistogramVec
}

func New() *Metrics {
	buckets := []float64{.001, .005, .01, .025, .05, .1, .15, .25, .5, 1, 2, 5}
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "quota_requests_total", Help: "Completed requests by transport, fixed route and status.",
		}, []string{"transport", "route", "status"}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "quota_request_errors_total", Help: "Failed or cancelled requests.",
		}, []string{"transport", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "quota_request_duration_seconds", Help: "Request latency, including rejected requests.", Buckets: buckets,
		}, []string{"transport", "route", "status"}),
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "quota_requests_in_flight", Help: "Active transport handlers.",
		}, []string{"transport"}),
		providerCalls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "quota_provider_calls_total", Help: "Executed provider calls, excluding rejected permit acquisition.",
		}, []string{"provider", "outcome"}),
		providerFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "quota_provider_failures_total", Help: "Failed executed provider calls.",
		}, []string{"provider", "reason"}),
		providerDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "quota_provider_duration_seconds", Help: "Executed provider call latency.", Buckets: buckets,
		}, []string{"provider"}),
		providerInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "quota_provider_calls_in_flight", Help: "Provider calls currently holding permits.",
		}, []string{"provider"}),
		cacheDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "quota_cache_duration_seconds", Help: "Redis operation latency, including errors.", Buckets: buckets,
		}, []string{"operation"}),
	}
	m.registry.MustRegister(m.requests, m.errors, m.duration, m.inFlight, m.providerCalls,
		m.providerFailures, m.providerDuration, m.providerInFlight, m.cacheDuration,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

func (m *Metrics) TrackCache(service *quota.CachedService) {
	for _, item := range []struct {
		name string
		help string
		read func() float64
	}{
		{"quota_cache_hits_total", "Valid shared cache hits.", func() float64 { return float64(service.Stats().Hits) }},
		{"quota_cache_misses_total", "Shared cache misses.", func() float64 { return float64(service.Stats().Misses) }},
		{"quota_cache_errors_total", "Cache read, corruption and write errors.", func() float64 { return float64(service.Stats().Errors) }},
	} {
		m.registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: item.name, Help: item.help}, item.read))
	}
	m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "quota_admission_slots", Help: "Occupied shared-work slots, including abandoned waiters until work ends.",
	}, func() float64 { return float64(service.AdmittedRequests()) }))
}

func (m *Metrics) Admin() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	return mux
}

func (m *Metrics) observe(transport, route, code string, started time.Time, failed bool) {
	m.inFlight.WithLabelValues(transport).Dec()
	m.requests.WithLabelValues(transport, route, code).Inc()
	m.duration.WithLabelValues(transport, route, code).Observe(time.Since(started).Seconds())
	if failed {
		m.errors.WithLabelValues(transport, code).Inc()
	}
}

func (m *Metrics) HTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route := "unmatched"
		if r.URL.Path == "/v1/quotes" || r.URL.Path == "/healthz" {
			route = r.URL.Path
		}
		started := time.Now()
		m.inFlight.WithLabelValues("http").Inc()
		recorder := &responseWriter{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(recorder, r)
		code := strconv.Itoa(recorder.code)
		failed := recorder.code >= 400
		if r.Context().Err() != nil {
			code, failed = "canceled", true
		}
		m.observe("http", route, code, started, failed)
	})
}

type responseWriter struct {
	http.ResponseWriter
	code  int
	wrote bool
}

func (w *responseWriter) WriteHeader(code int) {
	if !w.wrote {
		w.code, w.wrote = code, true
		w.ResponseWriter.WriteHeader(code)
	}
}

func (w *responseWriter) Write(data []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}

func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (m *Metrics) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		route := "unmatched"
		if info.FullMethod == "/quota.v1.QuotaService/GetQuotes" {
			route = info.FullMethod
		}
		started := time.Now()
		m.inFlight.WithLabelValues("grpc").Inc()
		response, err := handler(ctx, req)
		m.observe("grpc", route, status.Code(err).String(), started, err != nil)
		return response, err
	}
}

func (m *Metrics) Provider(next quota.Provider) quota.Provider {
	return &provider{Provider: next, metrics: m}
}

type provider struct {
	quota.Provider
	metrics *Metrics
}

func (p *provider) GetQuote(ctx context.Context, query quota.Query) (quota.Quote, error) {
	started, name := time.Now(), p.Name()
	p.metrics.providerInFlight.WithLabelValues(name).Inc()
	defer p.metrics.providerInFlight.WithLabelValues(name).Dec()
	result, err := p.Provider.GetQuote(ctx, query)
	outcome := "success"
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		outcome = "deadline"
	case errors.Is(err, context.Canceled):
		outcome = "canceled"
	case err != nil:
		outcome = "error"
	}
	p.metrics.providerCalls.WithLabelValues(name, outcome).Inc()
	p.metrics.providerDuration.WithLabelValues(name).Observe(time.Since(started).Seconds())
	if err != nil {
		p.metrics.providerFailures.WithLabelValues(name, outcome).Inc()
	}
	return result, err
}

func (m *Metrics) Cache(next quota.Cache) quota.Cache { return &cache{Cache: next, metrics: m} }

type cache struct {
	quota.Cache
	metrics *Metrics
}

func (c *cache) Get(ctx context.Context, query quota.Query) (quota.Result, error) {
	started := time.Now()
	defer func() { c.metrics.cacheDuration.WithLabelValues("get").Observe(time.Since(started).Seconds()) }()
	return c.Cache.Get(ctx, query)
}

func (c *cache) Set(ctx context.Context, query quota.Query, result quota.Result, ttl time.Duration) error {
	started := time.Now()
	defer func() { c.metrics.cacheDuration.WithLabelValues("set").Observe(time.Since(started).Seconds()) }()
	return c.Cache.Set(ctx, query, result, ttl)
}
