.PHONY: test integration check build demo vuln tools generate up down

.DEFAULT_GOAL := build

PROTOC ?= protoc
PROTOC_VERSION := 36.2
PROTOC_GEN_GO_VERSION := v1.36.12
PROTOC_GEN_GO_GRPC_VERSION := v1.6.2

tools:
	GOBIN="$(CURDIR)/bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOBIN="$(CURDIR)/bin" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

generate: tools
	test "$$($(PROTOC) --version)" = "libprotoc $(PROTOC_VERSION)"
	PATH="$(CURDIR)/bin:$$PATH" $(PROTOC) -I . \
		--go_out=. --go_opt=module=github.com/madfanat/quota \
		--go-grpc_out=. --go-grpc_opt=module=github.com/madfanat/quota \
		api/quota/v1/quota.proto

test:
	go test ./...

integration:
	go test -race -count=1 -tags=integration -v ./tests/integration

check:
	go vet ./...
	go test -race ./...
	go build ./...

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...

build:
	go build -o bin/server ./cmd/server
	go build -o bin/simulators ./cmd/simulators
	go build -o bin/quotacli ./cmd/quotacli

up:
	docker compose up --build --wait --wait-timeout 60

down:
	docker compose down

demo:
	curl --fail-with-body --silent --show-error 'http://127.0.0.1:8080/v1/quotes?origin=MOW&destination=LED&weight_grams=1000'
