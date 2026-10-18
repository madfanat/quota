package carrierhttp_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"testing"
	"time"

	"github.com/madfanat/quota/internal/carrierhttp"
	"github.com/madfanat/quota/internal/quota"
)

const validQuote = `{"price_minor":35000,"currency":"RUB","estimate_days":3}`

var query = quota.Query{Origin: "MOW", Destination: "LED", WeightGrams: 1000}

func TestGetQuote(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		valid  bool
	}{
		{"healthy", 200, validQuote, true},
		{"failing", 503, "unavailable", false},
		{"malformed", 200, `{`, false},
		{"trailing JSON", 200, validQuote + `{}`, false},
		{"missing fields", 200, `{}`, false},
		{"wrong currency", 200, `{"price_minor":35000,"currency":"USD","estimate_days":3}`, false},
		{"oversized", 200, strings.Repeat(" ", 64<<10) + validQuote, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Query().Get("origin") != "MOW" || r.URL.Query().Get("destination") != "LED" || r.URL.Query().Get("weight_grams") != "1000" {
					t.Errorf("unexpected provider request: %s %s", r.Method, r.URL)
				}
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			}))
			defer server.Close()
			p := carrierhttp.New("steady", server.URL, server.Client())
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			result, err := p.GetQuote(ctx, query)
			if !tc.valid {
				if err == nil {
					t.Fatal("expected a provider error")
				}
				return
			}
			want := quota.Quote{Provider: "steady", PriceMinor: 35000, Currency: "RUB", EstimateDays: 3}
			if err != nil || result != want {
				t.Fatalf("quote = %+v, error = %v", result, err)
			}
		})
	}
}

func TestCancellationStopsHTTP(t *testing.T) {
	for _, bodyStarted := range []bool{false, true} {
		t.Run(map[bool]string{false: "waiting for headers", true: "reading body"}[bodyStarted], func(t *testing.T) {
			started := make(chan struct{})
			stopped := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(stopped)
				if bodyStarted {
					w.Write([]byte(`{"price_minor":`))
					w.(http.Flusher).Flush()
				}
				close(started)
				<-r.Context().Done()
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			p := carrierhttp.New("steady", server.URL, server.Client())
			done := make(chan error, 1)
			go func() { _, err := p.GetQuote(ctx, query); done <- err }()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("HTTP fixture did not start")
			}
			cancel()
			guard := time.NewTimer(3 * time.Second)
			defer guard.Stop()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("error = %v", err)
				}
			case <-guard.C:
				t.Fatal("provider did not stop")
			}
			select {
			case <-stopped:
			case <-guard.C:
				t.Fatal("cancellation did not reach HTTP fixture")
			}
		})
	}
}

func TestConnectionReuse(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				w.Write([]byte(validQuote))
			}))
			defer server.Close()
			client := server.Client()
			defer client.CloseIdleConnections()
			p := carrierhttp.New("steady", server.URL, client)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if _, err := p.GetQuote(ctx, query); (err == nil) != (status == http.StatusOK) {
				t.Fatalf("first call: error = %v", err)
			}
			reused := make(chan bool, 1)
			ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
				GotConn: func(info httptrace.GotConnInfo) { reused <- info.Reused },
			})
			if _, err := p.GetQuote(ctx, query); (err == nil) != (status == http.StatusOK) {
				t.Fatalf("second call: error = %v", err)
			}
			select {
			case ok := <-reused:
				if !ok {
					t.Fatal("the second request did not reuse the HTTP connection")
				}
			default:
				t.Fatal("no connection was observed")
			}
		})
	}
}
