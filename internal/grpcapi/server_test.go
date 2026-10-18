package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	quotav1 "github.com/madfanat/quota/internal/gen/quota/v1"
	"github.com/madfanat/quota/internal/quota"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type stubService func(context.Context, quota.Query) (quota.Result, error)

func (s stubService) GetQuotes(ctx context.Context, q quota.Query) (quota.Result, error) {
	return s(ctx, q)
}
func (s stubService) GetQuotesSequential(ctx context.Context, q quota.Query) (quota.Result, error) {
	return s(ctx, q)
}

func TestErrorMappings(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code codes.Code
	}{
		{quota.ErrInvalidQuery, codes.InvalidArgument},
		{quota.ErrBusy, codes.ResourceExhausted},
		{quota.ErrUnavailable, codes.Unavailable},
		{context.Canceled, codes.Canceled},
		{context.DeadlineExceeded, codes.DeadlineExceeded},
		{errors.New("private failure details"), codes.Internal},
	} {
		t.Run(tc.code.String(), func(t *testing.T) {
			service := stubService(func(context.Context, quota.Query) (quota.Result, error) {
				return quota.Result{}, fmt.Errorf("wrapped: %w", tc.err)
			})
			_, err := New(service, 1, time.Second, false).GetQuotes(context.Background(), &quotav1.GetQuotesRequest{
				Origin: "MOW", Destination: "LED", WeightGrams: 1000,
			})
			if status.Code(err) != tc.code {
				t.Fatalf("error = %v, want %s", err, tc.code)
			}
			if tc.code == codes.Internal && status.Convert(err).Message() != "internal server error" {
				t.Fatal("internal error details exposed to client")
			}
		})
	}
}

func TestValidation(t *testing.T) {
	service := stubService(func(context.Context, quota.Query) (quota.Result, error) {
		t.Error("invalid request reached the service")
		return quota.Result{}, nil
	})
	server := New(service, 1, time.Second, false)
	for _, req := range []*quotav1.GetQuotesRequest{
		nil,
		{Origin: "XXX", Destination: "LED", WeightGrams: 1000},
		{Origin: "MOW", Destination: "LED", WeightGrams: 0},
		{Origin: "MOW", Destination: "LED", WeightGrams: 30001},
	} {
		if _, err := server.GetQuotes(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("request = %v, error = %v", req, err)
		}
	}
}

func TestAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := make(chan struct{})
	service := stubService(func(ctx context.Context, q quota.Query) (quota.Result, error) {
		close(started)
		<-ctx.Done()
		return quota.Result{}, ctx.Err()
	})
	server := New(service, 1, 3*time.Second, false)
	req := &quotav1.GetQuotesRequest{Origin: "MOW", Destination: "LED", WeightGrams: 1000}
	done := make(chan error, 1)
	go func() { _, err := server.GetQuotes(ctx, req); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first request did not start")
	}
	if _, err := server.GetQuotes(ctx, req); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("saturated request: %v", err)
	}
	cancel()
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatalf("cancelled request: %v", err)
	}
	if len(server.admission) != 0 {
		t.Fatal("cancelled request retained its admission permit")
	}
}
