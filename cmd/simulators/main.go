package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/madfanat/quota/internal/simulators"
)

func main() {
	name := flag.String("provider", "swift", "provider name: swift or steady")
	addr := flag.String("addr", "127.0.0.1:8081", "HTTP listen address")
	delay := flag.Duration("delay", 80*time.Millisecond, "delay before responding")
	fail := flag.Bool("fail", false, "return HTTP 503 for quotes")
	flag.Parse()
	if (*name != "swift" && *name != "steady") || *delay < 0 {
		slog.Error("provider must be swift or steady, and delay must not be negative")
		os.Exit(1)
	}
	server := &http.Server{
		Addr: *addr, Handler: simulators.New(*name, *delay, *fail).Handler(),
		ReadHeaderTimeout: 2 * time.Second, IdleTimeout: 30 * time.Second,
	}
	slog.Info("provider simulator listening", "provider", *name, "address", *addr, "delay", *delay, "fail", *fail)
	if err := server.ListenAndServe(); err != nil {
		slog.Error("serve", "error", err)
		os.Exit(1)
	}
}
