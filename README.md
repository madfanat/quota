# quota

__quota__ is a concurrent Go service for comparing delivery quotes.

## Usage

Start the API, simulators, Redis and Prometheus:

```sh
make up
make demo
docker compose exec -T api quotacli -origin MOW -destination LED -weight-grams 1000
```

### HTTP API

```sh
curl 'http://127.0.0.1:8080/v1/quotes?origin=MOW&destination=LED&weight_grams=1000'
```

| Parameter      | Description                                       |
| -------------- | ------------------------------------------------- |
| `origin`       | Origin region: `MOW`, `LED`, or `KZN` (required). |
| `destination`  | Destination region (required).                    |
| `weight_grams` | Parcel weight from 1 to 30000 grams (required).   |

Example:

```json
{
  "quotes": [
    {"provider": "steady", "price_minor": 35000, "currency": "RUB", "estimate_days": 3},
    {"provider": "swift", "price_minor": 52000, "currency": "RUB", "estimate_days": 1}
  ],
  "complete": true,
  "failed_providers": [],
  "generated_at": "2026-11-07T12:00:00Z"
}
```

| Outcome                       | HTTP | gRPC              |
| ----------------------------- | ---- | ----------------- |
| Complete or partial success   | 200  | OK                |
| Invalid input                 | 400  | InvalidArgument   |
| All providers unavailable     | 503  | Unavailable       |
| Admission capacity exhausted  | 503  | ResourceExhausted |
| Request deadline exceeded     | 504  | DeadlineExceeded  |

`GET /healthz` checks whether the HTTP server is running, independently of Redis and providers.

### Server options

| Option                | Default                       | Description                                |
| --------------------- | ----------------------------- | ------------------------------------------ |
| `-addr`               | `127.0.0.1:8080`              | HTTP listen address.                       |
| `-grpc-addr`          | `127.0.0.1:9090`              | gRPC listen address.                       |
| `-admin-addr`         | `127.0.0.1:6060`              | Metrics and pprof; requires a loopback IP. |
| `-swift-url`          | `http://127.0.0.1:8081/quote` | Swift provider endpoint.                   |
| `-steady-url`         | `http://127.0.0.1:8082/quote` | Steady provider endpoint.                  |
| `-redis-addr`         | `127.0.0.1:6379`              | Redis cache address.                       |
| `-max-provider-calls` | `16`                          | Provider calls.                            |
| `-max-requests`       | `32`                          | Admitted requests                          |
| `-request-timeout`    | `600ms`                       | Request timeout.                           |
| `-provider-timeout`   | `250ms`                       | Provider timeout.                          |
| `-cache-ttl`          | `30s`                         | Complete response lifetime.                |
| `-cache-timeout`      | `30ms`                        | Redis operation timout.                    |
| `-sequential`         | `false`                       | Sequential mode.                           |

### Specifications

- Both providers are called concurrently.
- Only complete successful responses are cached.
- Concurrent same requests share one fetch per process.

## Building and testing

Requirements:

- Go 1.27.2 or later.
- Make.
- Docker Compose.

Build the project:

```sh
make build
```

Run locally:

```sh
./bin/simulators -provider swift -addr 127.0.0.1:8081 -delay 80ms
./bin/simulators -provider steady -addr 127.0.0.1:8082 -delay 120ms
./bin/server
```

Run the tests:

```sh
make test
make check
make vuln
```

## License

The application source code is licensed under the MIT License.
