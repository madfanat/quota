package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	quotav1 "github.com/madfanat/quota/internal/gen/quota/v1"
	"github.com/madfanat/quota/internal/quota"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", "127.0.0.1:9090", "gRPC server address")
	origin := flag.String("origin", "MOW", "origin region code")
	destination := flag.String("destination", "LED", "destination region code")
	weight := flag.Int("weight-grams", 1000, "parcel weight in grams")
	timeout := flag.Duration("timeout", time.Second, "RPC deadline, including connecting")
	flag.Parse()
	if *timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	query, err := quota.NewQuery(*origin, *destination, *weight)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	response, err := quotav1.NewQuotaServiceClient(conn).GetQuotes(ctx, &quotav1.GetQuotesRequest{
		Origin: query.Origin, Destination: query.Destination, WeightGrams: int32(query.WeightGrams),
	})
	if err != nil {
		return err
	}
	data, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true, Indent: "  "}).Marshal(response)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(os.Stdout, string(data))
	return err
}
