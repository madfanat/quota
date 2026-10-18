package integration_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/madfanat/quota/internal/carrierhttp"
	quotav1 "github.com/madfanat/quota/internal/gen/quota/v1"
	"github.com/madfanat/quota/internal/grpcapi"
	"github.com/madfanat/quota/internal/httpapi"
	"github.com/madfanat/quota/internal/quota"
	"github.com/madfanat/quota/internal/simulators"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func grpcClient(t *testing.T, service grpcapi.Service, budget time.Duration) quotav1.QuotaServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	quotav1.RegisterQuotaServiceServer(server, grpcapi.New(service, 32, budget, false))
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("serve test gRPC: %v", err)
		}
	}()
	conn, err := grpc.NewClient("passthrough:///quota-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(); server.Stop(); listener.Close() })
	return quotav1.NewQuotaServiceClient(conn)
}

func rpcRequest() *quotav1.GetQuotesRequest {
	return &quotav1.GetQuotesRequest{Origin: " mow ", Destination: "led", WeightGrams: 1000}
}

func rpcResult(response *quotav1.GetQuotesResponse) quota.Result {
	result := quota.Result{
		Quotes: make([]quota.Quote, 0, len(response.Quotes)), Complete: response.Complete,
		FailedProviders: append([]string{}, response.FailedProviders...), GeneratedAt: response.GeneratedAt.AsTime(),
	}
	for _, item := range response.Quotes {
		result.Quotes = append(result.Quotes, quota.Quote{
			Provider: item.Provider, PriceMinor: item.PriceMinor, Currency: item.Currency, EstimateDays: int(item.EstimateDays),
		})
	}
	return result
}

func TestHTTPAndGRPCEquivalentResults(t *testing.T) {
	for _, failures := range []int{0, 1, 2} {
		t.Run(strconv.Itoa(failures)+" failures", func(t *testing.T) {
			var providers []quota.Provider
			for i, name := range []string{"swift", "steady"} {
				server := httptest.NewServer(simulators.New(name, 0, i < failures).Handler())
				t.Cleanup(server.Close)
				providers = append(providers, carrierhttp.New(name, server.URL+"/quote", server.Client()))
			}
			service, err := quota.NewService(providers, 2, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			client := grpcClient(t, service, time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			response, rpcErr := client.GetQuotes(ctx, rpcRequest())
			w := httptest.NewRecorder()
			httpapi.New(service, 32, time.Second, false).ServeHTTP(w,
				httptest.NewRequest("GET", "/v1/quotes?origin=%20mow%20&destination=led&weight_grams=1000", nil))
			if failures == 2 {
				if w.Code != 503 || status.Code(rpcErr) != codes.Unavailable {
					t.Fatalf("HTTP = %d, gRPC = %v", w.Code, rpcErr)
				}
				return
			}
			if rpcErr != nil || w.Code != 200 {
				t.Fatalf("HTTP = %d, gRPC = %v", w.Code, rpcErr)
			}
			var rest quota.Result
			if err := json.Unmarshal(w.Body.Bytes(), &rest); err != nil {
				t.Fatal(err)
			}
			mapped := rpcResult(response)
			if mapped.GeneratedAt.IsZero() || response.GeneratedAt.CheckValid() != nil {
				t.Fatal("invalid RPC timestamp")
			}

			mapped.GeneratedAt = rest.GeneratedAt
			if !reflect.DeepEqual(mapped, rest) {
				t.Fatalf("REST = %+v, gRPC = %+v", rest, mapped)
			}
		})
	}
}

func TestGRPCClientDeadlineCancelsProviderHTTP(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("weight_grams") == "2000" {
			simulators.New("swift", 0, false).Handler().ServeHTTP(w, r)
			return
		}
		close(started)
		<-r.Context().Done()
		close(stopped)
	}))
	defer server.Close()
	service, err := quota.NewService([]quota.Provider{carrierhttp.New("swift", server.URL+"/quote", server.Client())}, 1, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client := grpcClient(t, service, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := client.GetQuotes(ctx, rpcRequest()); done <- err }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("provider did not start")
	}
	if err := <-done; status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("short client deadline: %v", err)
	}
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("RPC cancellation did not reach the provider")
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	req := rpcRequest()
	req.WeightGrams = 2000
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		response, err := client.GetQuotes(ctx2, req)
		if status.Code(err) != codes.ResourceExhausted {
			if err != nil || !response.GetComplete() {
				t.Fatalf("provider permit was not returned: result = %v, error = %v", response, err)
			}
			break
		}
		select {
		case <-tick.C:
		case <-ctx2.Done():
			t.Fatal("provider permit was not returned")
		}
	}
}

type missCache struct{}

func (missCache) Get(context.Context, quota.Query) (quota.Result, error) {
	return quota.Result{}, quota.ErrCacheMiss
}
func (missCache) Set(context.Context, quota.Query, quota.Result, time.Duration) error { return nil }

type joinedContext struct {
	context.Context
	joined chan<- struct{}
	once   sync.Once
}

func (c *joinedContext) Done() <-chan struct{} {
	c.once.Do(func() { c.joined <- struct{}{} })
	return c.Context.Done()
}

type observedCachedService struct {
	*quota.CachedService
	joined chan struct{}
}

func (s observedCachedService) GetQuotes(ctx context.Context, q quota.Query) (quota.Result, error) {
	return s.CachedService.GetQuotes(&joinedContext{Context: ctx, joined: s.joined}, q)
}

func TestGRPCDeadlineLeavesSharedWorkForHTTPCaller(t *testing.T) {
	root, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	release := make(chan struct{})
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		select {
		case <-release:
			simulators.New("swift", 0, false).Handler().ServeHTTP(w, r)
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	service, err := quota.NewService([]quota.Provider{carrierhttp.New("swift", server.URL+"/quote", server.Client())}, 1, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	cached, err := quota.NewCachedService(root, service, missCache{}, quota.CacheConfig{
		TTL: time.Second, Timeout: 20 * time.Millisecond, WorkTimeout: 2 * time.Second, MaxRequests: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	observed := observedCachedService{CachedService: cached, joined: make(chan struct{}, 2)}
	client := grpcClient(t, observed, 2*time.Second)
	short, cancel := context.WithTimeout(root, 200*time.Millisecond)
	defer cancel()
	rpcDone := make(chan error, 1)
	go func() { _, err := client.GetQuotes(short, rpcRequest()); rpcDone <- err }()
	select {
	case <-observed.joined:
	case <-root.Done():
		t.Fatal("RPC caller did not join shared work")
	}
	response := httptest.NewRecorder()
	httpDone := make(chan struct{})
	go func() {
		defer close(httpDone)
		httpapi.New(observed, 2, 2*time.Second, false).ServeHTTP(response,
			httptest.NewRequest("GET", "/v1/quotes?origin=MOW&destination=LED&weight_grams=1000", nil).WithContext(root))
	}()
	select {
	case <-observed.joined:
	case <-root.Done():
		t.Fatal("HTTP caller did not join shared work")
	}
	if err := <-rpcDone; status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("short RPC caller: %v", err)
	}
	close(release)
	select {
	case <-httpDone:
	case <-root.Done():
		t.Fatal("HTTP caller did not complete")
	}
	var result quota.Result
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if response.Code != 200 || !result.Complete || calls.Load() != 1 {
		t.Fatalf("HTTP = %d, result = %+v, shared provider calls = %d", response.Code, result, calls.Load())
	}
}
