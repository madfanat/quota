package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/madfanat/quota/internal/quota"
)

type Service interface {
	GetQuotes(context.Context, quota.Query) (quota.Result, error)
	GetQuotesSequential(context.Context, quota.Query) (quota.Result, error)
}

type Handler struct {
	service    Service
	admission  chan struct{}
	budget     time.Duration
	sequential bool
}

func New(service Service, maxRequests int, budget time.Duration, sequential bool) http.Handler {
	h := &Handler{service: service, admission: make(chan struct{}, maxRequests), budget: budget, sequential: sequential}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/quotes", h.quotes)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

func (h *Handler) quotes(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.budget)
	defer cancel()
	params := r.URL.Query()
	weight, err := strconv.Atoi(params.Get("weight_grams"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": quota.ErrInvalidQuery.Error()})
		return
	}
	query, err := quota.NewQuery(params.Get("origin"), params.Get("destination"), weight)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := ctx.Err(); err != nil {
		writeError(w, err)
		return
	}
	select {
	case h.admission <- struct{}{}:
		defer func() { <-h.admission }()
	default:
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "request capacity exhausted"})
		return
	}
	var result quota.Result
	if h.sequential {
		result, err = h.service.GetQuotesSequential(ctx, query)
	} else {
		result, err = h.service.GetQuotes(ctx, query)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func writeError(w http.ResponseWriter, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	status := http.StatusServiceUnavailable
	if errors.Is(err, context.DeadlineExceeded) {
		status = http.StatusGatewayTimeout
	}
	slog.Warn("quote request failed", "error", err)
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Warn("write response", "error", err)
	}
}
