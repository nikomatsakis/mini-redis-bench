# redis-bench

A minimal port of Redis's `redis-benchmark` tool from C to Rust, built for
the workshop debugging exercise.

The tool uses raw TCP sockets with the Redis RESP protocol and a
single-threaded mio event loop, matching the architecture of the original
`redis-benchmark`. It runs SET and GET workloads with configurable pipelining
and reports per-command latency percentiles.

## Usage

```bash
cargo run --release -p redis-bench
```

## Flags

| Flag           | Description                                         | Default   |
|----------------|-----------------------------------------------------|-----------|
| `-c`           | Number of parallel connections                      |         5 |
| `-n`           | Total requests per command type (SET and GET)       |    10,000 |
| `-P`           | Pipeline depth (commands in flight per connection)  |        50 |
| `--value-size` | Value payload size in bytes                         |        64 |
| `--host`       | Server hostname                                     | 127.0.0.1 |
| `--port`       | Server port                                         |      6379 |

## Examples

```bash
# Default settings (5 connections, pipeline depth 50)
cargo run --release -p redis-bench

# Heavy pipelining
cargo run --release -p redis-bench -- -c 10 -n 100000 -P 500

# Minimal pipelining
cargo run --release -p redis-bench -- -c 2 -P 1
```
