//! Benchmark tool for mini-redis.
//!
//! Single-threaded mio event loop managing all connections with raw RESP
//! protocol encoding/decoding. Each connection pipelines up to a configurable
//! depth. Latency is measured per-response from the start of the pipeline
//! batch, matching redis-benchmark's behavior.

use clap::Parser;
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};
use std::io::{self, Read, Write};
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(
    name = "redis-bench",
    about = "Benchmark mini-redis GET/SET performance"
)]
struct Cli {
    /// Number of parallel connections.
    #[arg(short, long, default_value_t = 5)]
    connections: usize,

    /// Total number of requests per command type (SET and GET each).
    #[arg(short, long, default_value_t = 10_000)]
    num_requests: usize,

    /// Pipeline depth: max commands in flight per connection.
    #[arg(short = 'P', long, default_value_t = 50)]
    pipeline: usize,

    /// Size of the value payload in bytes.
    #[arg(long, default_value_t = 64)]
    value_size: usize,

    /// Server hostname.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Server port.
    #[arg(long, default_value_t = 6379)]
    port: u16,
}

/// State of a single connection in the event loop.
struct Conn {
    stream: TcpStream,

    // Pre-built command buffer: the full pipeline batch, written once.
    obuf: Vec<u8>,
    written: usize,

    // Receive buffer.
    recv_buf: Vec<u8>,
    recv_len: usize,

    // Pipeline tracking.
    pending: usize,    // responses remaining for this batch
    batch_size: usize, // commands per batch
    pipeline: usize,
    batch_start: Instant,
    // Latency for this batch, computed once on first read event, then
    // recorded for every response in the batch (matches redis-benchmark).
    batch_latency: Option<Duration>,

    // Total progress.
    sent: usize,   // total batches sent
    total: usize,  // total requests to send
    conn_id: usize,

    // Whether this connection has been established (first writable event).
    connected: bool,
}

fn main() {
    let cli = Cli::parse();
    let addr: std::net::SocketAddr = format!("{}:{}", cli.host, cli.port).parse().unwrap();
    let requests_per_conn = cli.num_requests / cli.connections;

    println!("Benchmarking {}", addr);
    println!(
        "  {} connections, {} requests each, pipeline depth {}",
        cli.connections, requests_per_conn, cli.pipeline,
    );
    println!("  Value size: {} bytes", cli.value_size);

    let value = vec![b'x'; cli.value_size];

    let (mut set_latencies, set_elapsed) = run_phase(
        &addr,
        cli.connections,
        requests_per_conn,
        cli.pipeline,
        &value,
        Phase::Set,
    );

    let (mut get_latencies, get_elapsed) = run_phase(
        &addr,
        cli.connections,
        requests_per_conn,
        cli.pipeline,
        &value,
        Phase::Get,
    );

    println!();
    print_stats("SET", &mut set_latencies, set_elapsed);
    println!();
    print_stats("GET", &mut get_latencies, get_elapsed);
}

#[derive(Clone, Copy)]
enum Phase {
    Set,
    Get,
}

fn run_phase(
    addr: &std::net::SocketAddr,
    num_conns: usize,
    requests_per_conn: usize,
    pipeline: usize,
    value: &[u8],
    phase: Phase,
) -> (Vec<Duration>, Duration) {
    let start = Instant::now();
    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);

    // Pre-build the pipeline command buffer for each connection. Each
    // connection sends the same sequence of commands repeatedly.
    let mut conns: Vec<Conn> = Vec::with_capacity(num_conns);
    for i in 0..num_conns {
        let token = Token(i);
        let mut stream = TcpStream::connect(*addr).unwrap();
        poll.registry()
            .register(&mut stream, token, Interest::READABLE | Interest::WRITABLE)
            .unwrap();

        // Build the command buffer once per connection. The same buffer is
        // reused for every pipeline batch (same keys each time), matching
        // redis-benchmark which reuses its obuf across batches.
        let batch_size = pipeline.min(requests_per_conn);
        let obuf = build_batch(phase, i, batch_size, value);

        conns.push(Conn {
            stream,
            obuf,
            written: 0,
            recv_buf: vec![0u8; 64 * 1024],
            recv_len: 0,
            pending: batch_size,
            batch_size,
            pipeline,
            batch_start: Instant::now(),
            batch_latency: None,
            sent: batch_size,
            total: requests_per_conn,
            conn_id: i,
            connected: false,
        });
    }

    let mut latencies = Vec::with_capacity(num_conns * requests_per_conn);
    let mut done_count = 0;

    while done_count < num_conns {
        poll.poll(&mut events, None).unwrap();

        for event in events.iter() {
            let idx = event.token().0;
            let conn = &mut conns[idx];

            if conn.sent >= conn.total && conn.pending == 0 {
                continue; // already done
            }

            // --- Writable: flush the command buffer ---
            if event.is_writable() {
                if !conn.connected {
                    conn.connected = true;
                }

                // Record the start time when we begin writing a new batch,
                // matching redis-benchmark's behavior of timestamping inside
                // the write handler when written == 0.
                if conn.written == 0 {
                    conn.batch_start = Instant::now();
                }

                while conn.written < conn.obuf.len() {
                    match conn.stream.write(&conn.obuf[conn.written..]) {
                        Ok(n) => conn.written += n,
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                        Err(e) => panic!("write error on conn {}: {}", idx, e),
                    }
                }

                // If fully flushed, switch to read-only until all responses
                // arrive. Skip the readable handler this iteration so latency
                // is measured on the *next* poll (actual round-trip), not
                // within the same event dispatch.
                if conn.written == conn.obuf.len() {
                    poll.registry()
                        .reregister(&mut conn.stream, Token(idx), Interest::READABLE)
                        .unwrap();
                    continue;
                }
            }

            // --- Readable: parse responses, record latencies ---
            if event.is_readable() {
                // Compute latency on the first read event for this batch.
                // The server has already sent (at least part of) the reply,
                // so we measure up to this point. This matches redis-benchmark:
                // one latency measurement per pipeline batch, recorded for
                // every response in the batch.
                if conn.batch_latency.is_none() {
                    conn.batch_latency = Some(conn.batch_start.elapsed());
                }

                loop {
                    if conn.recv_len == conn.recv_buf.len() {
                        conn.recv_buf.resize(conn.recv_buf.len() * 2, 0);
                    }
                    match conn.stream.read(&mut conn.recv_buf[conn.recv_len..]) {
                        Ok(0) => panic!("connection {} closed unexpectedly", idx),
                        Ok(n) => conn.recv_len += n,
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                        Err(e) => panic!("read error on conn {}: {}", idx, e),
                    }
                }

                let batch_lat = conn.batch_latency.unwrap();

                // Parse responses.
                let mut parse_pos = 0;
                while parse_pos < conn.recv_len && conn.pending > 0 {
                    match try_parse_response(&conn.recv_buf[parse_pos..conn.recv_len]) {
                        Some(consumed) => {
                            parse_pos += consumed;
                            latencies.push(batch_lat);
                            conn.pending -= 1;
                        }
                        None => break,
                    }
                }

                // Compact the receive buffer.
                if parse_pos > 0 {
                    conn.recv_buf.copy_within(parse_pos..conn.recv_len, 0);
                    conn.recv_len -= parse_pos;
                }

                // If the batch is complete, start the next one.
                if conn.pending == 0 {
                    if conn.sent >= conn.total {
                        // All done for this connection.
                        poll.registry().deregister(&mut conn.stream).unwrap();
                        done_count += 1;
                    } else {
                        // Reset for the next batch. Reuse the same obuf.
                        conn.written = 0;
                        conn.pending = conn.batch_size;
                        conn.sent += conn.batch_size;
                        conn.batch_latency = None;

                        // Re-register for writable to flush the new batch.
                        poll.registry()
                            .reregister(
                                &mut conn.stream,
                                Token(idx),
                                Interest::READABLE | Interest::WRITABLE,
                            )
                            .unwrap();
                    }
                }
            }
        }
    }

    (latencies, start.elapsed())
}

/// Build the RESP-encoded command buffer for a pipeline batch. The same
/// buffer is reused across batches (keys repeat), matching redis-benchmark.
fn build_batch(phase: Phase, conn_id: usize, count: usize, value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(count * 128);
    for i in 0..count {
        match phase {
            Phase::Set => {
                let key = format!("k:{}:{}", conn_id, i);
                write!(
                    buf,
                    "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n",
                    key.len(),
                    key,
                    value.len()
                )
                .unwrap();
                buf.extend_from_slice(value);
                buf.extend_from_slice(b"\r\n");
            }
            Phase::Get => {
                let key = format!("k:{}:{}", conn_id, i);
                write!(buf, "*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key).unwrap();
            }
        }
    }
    buf
}

/// Try to parse one RESP response. Returns bytes consumed or None if incomplete.
fn try_parse_response(buf: &[u8]) -> Option<usize> {
    if buf.is_empty() {
        return None;
    }

    match buf[0] {
        b'+' | b'-' | b':' => {
            let crlf = find_crlf(buf)?;
            Some(crlf + 2)
        }
        b'$' => {
            let crlf = find_crlf(buf)?;
            let len: i64 = std::str::from_utf8(&buf[1..crlf]).ok()?.parse().ok()?;
            if len < 0 {
                Some(crlf + 2)
            } else {
                let total = crlf + 2 + len as usize + 2;
                if buf.len() >= total {
                    Some(total)
                } else {
                    None
                }
            }
        }
        b'*' => {
            let crlf = find_crlf(buf)?;
            let count: i64 = std::str::from_utf8(&buf[1..crlf]).ok()?.parse().ok()?;
            if count < 0 {
                return Some(crlf + 2);
            }
            let mut pos = crlf + 2;
            for _ in 0..count {
                let consumed = try_parse_response(&buf[pos..])?;
                pos += consumed;
            }
            Some(pos)
        }
        _ => panic!("unexpected RESP type byte: 0x{:02x}", buf[0]),
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    for i in 0..buf.len().saturating_sub(1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
    }
    None
}

fn print_stats(label: &str, latencies: &mut [Duration], elapsed: Duration) {
    if latencies.is_empty() {
        println!("{}: no samples", label);
        return;
    }

    latencies.sort();

    let len = latencies.len();
    let sum: Duration = latencies.iter().sum();
    let avg = sum / len as u32;
    let rps = len as f64 / elapsed.as_secs_f64();

    println!(
        "{}: {:.2} requests per second, {:.2}s total",
        label, rps, elapsed.as_secs_f64(),
    );
    println!();
    println!("Latency by percentile distribution:");
    // Print at doubling percentile resolution, similar to redis-benchmark.
    let percentiles = [
        0.0, 50.0, 75.0, 87.5, 93.75, 96.875, 98.4375, 99.21875, 99.609375,
        99.8046875, 99.90234375, 99.951171875, 100.0,
    ];
    for &pct in &percentiles {
        let idx = ((pct / 100.0) * (len - 1) as f64) as usize;
        let idx = idx.min(len - 1);
        println!(
            "{:>9.3}% <= {:.3} milliseconds (cumulative count {})",
            pct,
            latencies[idx].as_secs_f64() * 1000.0,
            idx + 1,
        );
        if idx == len - 1 {
            break;
        }
    }

    println!();
    println!("Summary:");
    println!(
        "  throughput: {:.2} requests per second",
        rps,
    );
    println!(
        "  latency (msec): avg {:.3}, min {:.3}, p50 {:.3}, p95 {:.3}, p99 {:.3}, max {:.3}",
        avg.as_secs_f64() * 1000.0,
        latencies[0].as_secs_f64() * 1000.0,
        latencies[len * 50 / 100].as_secs_f64() * 1000.0,
        latencies[len * 95 / 100].as_secs_f64() * 1000.0,
        latencies[len * 99 / 100].as_secs_f64() * 1000.0,
        latencies[len - 1].as_secs_f64() * 1000.0,
    );
}
