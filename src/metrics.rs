use std::time::Duration;

/// Build a Tokio runtime with poll time histogram enabled.
pub fn build_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .enable_metrics_poll_time_histogram()
        .metrics_poll_time_histogram_configuration(
            tokio::runtime::HistogramConfiguration::log(
                tokio::runtime::LogHistogram::default(),
            ),
        )
        .build()
        .unwrap()
}

/// Task monitor for connection handler tasks. Shared across all connections
/// so they are tracked as a single group.
pub fn task_monitor() -> tokio_metrics::TaskMonitor {
    tokio_metrics::TaskMonitor::new()
}

/// Print all collected metrics to stdout.
pub fn print_metrics(task_monitor: &tokio_metrics::TaskMonitor) {
    print_poll_time_histogram();
    print_task_metrics(task_monitor);
}

/// Print the poll time histogram from the runtime metrics.
fn print_poll_time_histogram() {
    let metrics = tokio::runtime::Handle::current().metrics();
    let num_buckets = metrics.poll_time_histogram_num_buckets();
    let num_workers = metrics.num_workers();

    if num_buckets == 0 {
        println!("Poll time histogram not available.");
        return;
    }

    let mut buckets: Vec<(std::ops::Range<Duration>, u64)> = Vec::new();
    let mut total: u64 = 0;

    for b in 0..num_buckets {
        let range = metrics.poll_time_histogram_bucket_range(b);
        let mut count: u64 = 0;
        for w in 0..num_workers {
            count += metrics.poll_time_histogram_bucket_count(w, b);
        }
        total += count;
        buckets.push((range, count));
    }

    if total == 0 {
        println!("No polls recorded.");
        return;
    }

    let max_count = buckets.iter().map(|(_, c)| *c).max().unwrap_or(1);
    let bar_width = 40;

    println!();
    println!("=== Poll time histogram ({} total polls) ===", total);
    println!();

    let mut cumulative: u64 = 0;

    for (range, count) in &buckets {
        if *count == 0 {
            continue;
        }

        cumulative += count;
        let pct = (*count as f64 / total as f64) * 100.0;
        let cum_pct = (cumulative as f64 / total as f64) * 100.0;
        let bar_len = (*count as f64 / max_count as f64 * bar_width as f64) as usize;
        let bar: String = "#".repeat(bar_len);

        println!(
            "  {:>10.2?} .. {:>10.2?} : {:>8} ({:>5.1}%, cum {:>5.1}%) |{}",
            range.start, range.end, count, pct, cum_pct, bar,
        );
    }
    println!();
}

/// Print task-level metrics from the TaskMonitor.
fn print_task_metrics(monitor: &tokio_metrics::TaskMonitor) {
    let metrics = monitor.cumulative();

    println!("=== Connection handler task metrics ===");
    println!();
    println!("  Instrumented tasks:     {}", metrics.instrumented_count);
    println!("  Dropped tasks:          {}", metrics.dropped_count);
    println!("  Total polls:            {}", metrics.total_poll_count);
    println!("  Total poll duration:    {:.2?}", metrics.total_poll_duration);
    println!(
        "  Mean poll duration:     {:.2?}",
        metrics.mean_poll_duration()
    );
    println!();
    println!(
        "  Total scheduled dur:    {:.2?}",
        metrics.total_scheduled_duration
    );
    println!(
        "  Mean scheduled delay:   {:.2?}",
        metrics.mean_scheduled_duration()
    );
    println!();
}
