//! mini-redis server.
//!
//! This file is the entry point for the server implemented in the library. It
//! performs command line parsing and passes the arguments on to
//! `mini_redis::server`.
//!
//! The `clap` crate is used for parsing arguments.

use mini_redis::{metrics, server, DEFAULT_PORT};

use clap::Parser;
use tokio::net::TcpListener;
use tokio::signal;

#[cfg(feature = "otel")]
// To be able to set the XrayPropagator
use opentelemetry::global;
#[cfg(feature = "otel")]
// To configure certain options such as sampling rate
use opentelemetry::sdk::trace as sdktrace;
#[cfg(feature = "otel")]
// For passing along the same XrayId across services
use opentelemetry_aws::trace::XrayPropagator;
#[cfg(feature = "otel")]
// The `Ext` traits are to allow the Registry to accept the
// OpenTelemetry-specific types (such as `OpenTelemetryLayer`)
use tracing_subscriber::{
    fmt, layer::SubscriberExt, util::SubscriberInitExt, util::TryInitError, EnvFilter,
};

fn main() -> mini_redis::Result<()> {
    set_up_logging()?;

    let cli = Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);

    let rt = metrics::build_runtime();

    let task_monitor = metrics::task_monitor();

    rt.block_on(async {
        let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;

        server::run(listener, signal::ctrl_c(), task_monitor.clone()).await;

        metrics::print_metrics(&task_monitor);

        Ok(())
    })
}

#[derive(Parser, Debug)]
#[command(name = "mini-redis-server", version, author, about = "A Redis server")]
struct Cli {
    #[arg(long)]
    port: Option<u16>,
}

#[cfg(not(feature = "otel"))]
fn set_up_logging() -> mini_redis::Result<()> {
    // See https://docs.rs/tracing for more info
    tracing_subscriber::fmt::try_init()
}

#[cfg(feature = "otel")]
fn set_up_logging() -> Result<(), TryInitError> {
    global::set_text_map_propagator(XrayPropagator::default());

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_trace_config(
            sdktrace::config()
                .with_sampler(sdktrace::Sampler::AlwaysOn)
                .with_id_generator(sdktrace::XrayIdGenerator::default()),
        )
        .install_simple()
        .expect("Unable to initialize OtlpPipeline");

    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let filter = EnvFilter::from_default_env();

    tracing_subscriber::registry()
        .with(opentelemetry)
        .with(filter)
        .with(fmt::Layer::default())
        .try_init()
}
