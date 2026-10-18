.PHONY: test integration check build demo vuln

test:
	go test ./...

integration:
	go test -v ./tests/integration

check:
	go vet ./...
	go test -race ./...
	go build ./...

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...

build:
	go build -o bin/server ./cmd/server
	go build -o bin/simulators ./cmd/simulators

demo:
	curl --fail-with-body --silent --show-error 'http://127.0.0.1:8080/v1/quotes?origin=MOW&destination=LED&weight_grams=1000'
