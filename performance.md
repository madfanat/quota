# Local performance experiment

Measured on 2026-10-18 with k6 v2.3.0 and Go 1.27.2 on an Apple M4
(10 logical CPUs, 24 GiB RAM, macOS 27.0.1). The API, simulators, and generator
used `GOMAXPROCS=2 GOMEMLIMIT=256MiB` (scheduler and soft memory limits).
Redis 8.2.2 ran in Docker with 1 CPU and 128 MiB RAM.
These synthetic local measurements do not establish production capacity.

## Workload

Each case has a 15-second warmup followed by 60 seconds of constant arrivals.
Cold and slow cases use unique region/weight combinations; warm cases reuse
16 keys. Healthy provider delays are 80/120 ms. Slow steady exceeds its 250 ms
deadline. Request admission stays at 32, total budget at 600 ms, Redis timeout
at 30 ms, and experimental cache TTL at 5 minutes (application default: 30 s).

Throughput is measurement completions divided by 60 seconds. Errors are non-200
or invalid success responses. HTTP 200 partial results are counted separately.
Latency includes rejections and excludes warmup. All 18 cases passed response
contract checks and reported no dropped iterations. Each configuration was run
once on the same host; small latency differences may be noise.

## Eight provider slots

| Case / requests per second | Responses/s | Complete/s | Partial % | Error % | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| cold / 10 | 10.02 | 10.02 | 0.00 | 0.00 | 125.10 | 126.39 | 127.06 |
| cold / 40 | 40.02 | 30.00 | 24.91 | 0.12 | 123.69 | 124.74 | 125.25 |
| cold / 80 | 80.00 | 0.77 | 78.35 | 20.69 | 122.02 | 122.76 | 123.10 |
| warm / 10 | 10.02 | 10.02 | 0.00 | 0.00 | 2.00 | 2.27 | 2.97 |
| warm / 40 | 40.02 | 40.02 | 0.00 | 0.00 | 1.68 | 2.08 | 2.82 |
| warm / 80 | 80.02 | 80.02 | 0.00 | 0.00 | 1.09 | 1.53 | 1.76 |
| slow / 10 | 10.02 | 0.00 | 100.00 | 0.00 | 253.14 | 253.47 | 253.88 |
| slow / 40 | 40.02 | 0.00 | 20.07 | 79.93 | 251.88 | 252.54 | 253.13 |
| slow / 80 | 80.02 | 0.00 | 2.87 | 97.13 | 1.35 | 251.66 | 251.88 |

## Sixteen provider slots

| Case / requests per second | Responses/s | Complete/s | Partial % | Error % | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| cold / 10 | 10.02 | 10.02 | 0.00 | 0.00 | 124.66 | 125.30 | 126.86 |
| cold / 40 | 40.02 | 40.02 | 0.00 | 0.00 | 123.75 | 124.37 | 124.73 |
| cold / 80 | 80.02 | 68.52 | 14.35 | 0.02 | 123.01 | 123.92 | 124.48 |
| warm / 10 | 10.00 | 10.00 | 0.00 | 0.00 | 1.91 | 2.24 | 3.00 |
| warm / 40 | 40.00 | 40.00 | 0.00 | 0.00 | 1.81 | 2.12 | 2.71 |
| warm / 80 | 80.02 | 80.02 | 0.00 | 0.00 | 1.04 | 1.49 | 1.74 |
| slow / 10 | 10.00 | 0.00 | 100.00 | 0.00 | 253.09 | 253.47 | 254.28 |
| slow / 40 | 40.02 | 0.00 | 100.00 | 0.00 | 251.97 | 252.31 | 252.90 |
| slow / 80 | 80.00 | 0.00 | 27.77 | 72.23 | 251.10 | 251.65 | 252.02 |

## Findings

At cold/80, baseline CPU samples totaled 480 ms over 10.03 seconds (4.8% of one
CPU). Goroutine profiles showed eight provider calls and eight aggregations
waiting; metrics reached the eight-call cap. Redis operations averaged 0.61 ms.
Warm/80 issued no provider calls, averaging 0.75 ms per Redis operation. The
provider admission limit and dependency waits explain the lost complete
results; no CPU, Redis, network, or generator ceiling was demonstrated.

Doubling provider slots was the only configuration change between matrices.
At cold/80, complete throughput rose from 0.77 to 68.52/sec. Candidate CPU use
remained low: 710 ms over 10.12 seconds. Slow/40 returned partial results for
every request, while slow/80 still had 72.23% errors. Keep the default at 16
for this learning workload; real provider limits need a separate experiment.
Permits are acquired independently per provider, so overload can yield partial
results even when some calls succeed.

At stable cold/10, Little's law gives `L = λW = 10.02 × 0.1253 ≈ 1.26` active
requests, close to the sampled mean of 1.20. Use consistent boundaries and a
stable interval; a growing queue invalidates this steady-state estimate.
Averages conceal tails and response quality: baseline slow/80 has a 1.35 ms
median from fast rejections, but a 251.88 ms p99 and 97.13% errors.
