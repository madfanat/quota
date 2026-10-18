package simulators

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/madfanat/quota/internal/quote"
)

type Stats struct {
	Active        int `json:"active"`
	MaxConcurrent int `json:"max_concurrent"`
	Calls         int `json:"calls"`
}

type Simulator struct {
	name  string
	delay time.Duration
	fail  bool
	mu    sync.Mutex
	stats Stats
}

func New(name string, delay time.Duration, fail bool) *Simulator {
	return &Simulator{name: name, delay: delay, fail: fail}
}

func (s *Simulator) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /quote", s.quote)
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.Stats())
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

func (s *Simulator) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (s *Simulator) quote(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	weight, err := strconv.Atoi(params.Get("weight_grams"))
	if err != nil {
		http.Error(w, quote.ErrInvalidQuery.Error(), http.StatusBadRequest)
		return
	}
	query, err := quote.NewQuery(params.Get("origin"), params.Get("destination"), weight)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.stats.Active++
	s.stats.Calls++
	s.stats.MaxConcurrent = max(s.stats.MaxConcurrent, s.stats.Active)
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.stats.Active--
		s.mu.Unlock()
	}()
	timer := time.NewTimer(s.delay)
	defer timer.Stop()
	select {
	case <-r.Context().Done():
		return
	case <-timer.C:
	}
	if s.fail {
		http.Error(w, "simulated provider failure", http.StatusServiceUnavailable)
		return
	}
	price, days := int64(30000+query.WeightGrams*5), 3
	if s.name == "swift" {
		price, days = int64(45000+query.WeightGrams*7), 1
	}
	if query.Origin == query.Destination {
		price /= 2
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct {
		PriceMinor   int64  `json:"price_minor"`
		Currency     string `json:"currency"`
		EstimateDays int    `json:"estimate_days"`
	}{price, "RUB", days})
}
