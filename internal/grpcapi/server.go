package grpcapi

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	quotav1 "github.com/madfanat/quota/internal/gen/quota/v1"
	"github.com/madfanat/quota/internal/quota"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Service interface {
	GetQuotes(context.Context, quota.Query) (quota.Result, error)
	GetQuotesSequential(context.Context, quota.Query) (quota.Result, error)
}

type Server struct {
	quotav1.UnimplementedQuotaServiceServer
	service    Service
	admission  chan struct{}
	budget     time.Duration
	sequential bool
}

func New(service Service, maxRequests int, budget time.Duration, sequential bool) *Server {
	return &Server{service: service, admission: make(chan struct{}, maxRequests), budget: budget, sequential: sequential}
}

func (s *Server) GetQuotes(ctx context.Context, req *quotav1.GetQuotesRequest) (*quotav1.GetQuotesResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.budget)
	defer cancel()
	query, err := quota.NewQuery(req.GetOrigin(), req.GetDestination(), int(req.GetWeightGrams()))
	if err != nil {
		return nil, rpcError(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, rpcError(err)
	}
	select {
	case s.admission <- struct{}{}:
		defer func() { <-s.admission }()
	default:
		return nil, status.Error(codes.ResourceExhausted, "request capacity exhausted")
	}
	var result quota.Result
	if s.sequential {
		result, err = s.service.GetQuotesSequential(ctx, query)
	} else {
		result, err = s.service.GetQuotes(ctx, query)
	}
	if err != nil {
		return nil, rpcError(err)
	}
	response := &quotav1.GetQuotesResponse{
		Quotes:          make([]*quotav1.Quote, 0, len(result.Quotes)),
		Complete:        result.Complete,
		FailedProviders: slices.Clone(result.FailedProviders),
		GeneratedAt:     timestamppb.New(result.GeneratedAt),
	}
	for _, item := range result.Quotes {
		response.Quotes = append(response.Quotes, &quotav1.Quote{
			Provider: item.Provider, PriceMinor: item.PriceMinor, Currency: item.Currency,
			EstimateDays: int64(item.EstimateDays),
		})
	}
	return response, nil
}

func rpcError(err error) error {
	code := codes.Internal
	switch {
	case errors.Is(err, quota.ErrInvalidQuery):
		code = codes.InvalidArgument
	case errors.Is(err, context.Canceled):
		code = codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		code = codes.DeadlineExceeded
	case errors.Is(err, quota.ErrBusy):
		code = codes.ResourceExhausted
	case errors.Is(err, quota.ErrUnavailable):
		code = codes.Unavailable
	}
	if code == codes.Internal {
		slog.Error("gRPC quote request failed", "error", err)
		return status.Error(code, "internal server error")
	}
	return status.Error(code, err.Error())
}
