use std::env;
use std::io::{self, BufRead};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(|s| s.as_str()) {
        Some("bench") => {
            let rest = &args[1..];
            let rest = rest.strip_prefix(&["--".to_string()][..]).unwrap_or(rest);
            bench(rest);
        }
        Some(other) => {
            eprintln!("unknown xtask: {other}");
            std::process::exit(1);
        }
        None => {
            eprintln!("usage: cargo xtask bench [-- <bench-args>...]");
            std::process::exit(1);
        }
    }
}

fn bench(args: &[String]) {
    let workspace_root = workspace_root();
    let port: u16 = 16379;

    // Build both binaries in release mode.
    let status = Command::new("cargo")
        .args([
            "build", "--release", "--bin", "mini-redis", "-p", "mini-redis",
            "--bin", "bench", "-p", "redis-bench",
        ])
        .current_dir(&workspace_root)
        .status()
        .expect("failed to run cargo build");
    if !status.success() {
        eprintln!("cargo build failed");
        std::process::exit(1);
    }

    let target_dir = workspace_root.join("target").join("release");
    let server_bin = target_dir.join("mini-redis");
    let bench_bin = target_dir.join("bench");

    // Start the server.
    let mut server = Command::new(&server_bin)
        .args(["--port", &port.to_string()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start mini-redis server");

    // Drain server stderr in background so it doesn't block.
    let stderr = server.stderr.take().unwrap();
    thread::spawn(move || {
        let reader = io::BufReader::new(stderr);
        for line in reader.lines() {
            if let Ok(line) = line {
                eprintln!("[server] {line}");
            }
        }
    });

    // Wait for the server to accept connections.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if Instant::now() > deadline {
            server.kill().ok();
            eprintln!("timed out waiting for server to start on port {port}");
            std::process::exit(1);
        }
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    // Run the benchmark. Forward any extra args the user passed after `bench`.
    let mut bench_cmd = Command::new(&bench_bin);
    bench_cmd.args(["--port", &port.to_string()]);
    bench_cmd.args(args);
    bench_cmd.current_dir(&workspace_root);

    let status = bench_cmd.status().expect("failed to run bench");

    // Shut down the server.
    server.kill().ok();
    server.wait().ok();

    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }
}

fn workspace_root() -> PathBuf {
    let dir = env!("CARGO_MANIFEST_DIR");
    Path::new(dir).parent().unwrap().to_path_buf()
}
