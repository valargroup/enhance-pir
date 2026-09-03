use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, ensure, Context};
use enhance_pir::client::{ClientError, EnhancePirClient, QueryTiming};
use hdrhistogram::Histogram;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use tokio::sync::mpsc;

use crate::Args;

const QUERY_POOL_SIZE: usize = 1_024;

struct Sample {
    total: Duration,
    timing: Option<QueryTiming>,
    error_class: Option<&'static str>,
}

#[derive(Debug, Serialize)]
struct LoadSummary {
    server: String,
    generation: u64,
    anchor_height: u64,
    ironwood_tree_size: u64,
    duration_s: f64,
    parallelism: usize,
    completed: u64,
    succeeded: u64,
    errors: u64,
    error_rate: f64,
    requests_per_second: f64,
    stages: Vec<StageSummary>,
    error_classes: Vec<ErrorClassCount>,
}

#[derive(Debug, Serialize)]
struct StageSummary {
    name: &'static str,
    samples: u64,
    p50_ms: f64,
    p90_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    p999_ms: f64,
    max_ms: f64,
}

#[derive(Debug, Serialize)]
struct ErrorClassCount {
    class: &'static str,
    count: u64,
}

struct StatsCollector {
    end_to_end: Histogram<u64>,
    prepare: Histogram<u64>,
    http: Histogram<u64>,
    decode: Histogram<u64>,
    succeeded: u64,
    errors: u64,
    error_counts: HashMap<&'static str, u64>,
}

impl StatsCollector {
    fn new() -> Self {
        Self {
            end_to_end: Histogram::new(3).expect("valid histogram precision"),
            prepare: Histogram::new(3).expect("valid histogram precision"),
            http: Histogram::new(3).expect("valid histogram precision"),
            decode: Histogram::new(3).expect("valid histogram precision"),
            succeeded: 0,
            errors: 0,
            error_counts: HashMap::new(),
        }
    }

    fn record(&mut self, sample: Sample) {
        record_duration(&mut self.end_to_end, sample.total);
        if let Some(timing) = sample.timing {
            self.succeeded += 1;
            record_duration(&mut self.prepare, timing.prepare);
            record_duration(&mut self.http, timing.http);
            record_duration(&mut self.decode, timing.decode);
        } else {
            self.errors += 1;
            if let Some(class) = sample.error_class {
                *self.error_counts.entry(class).or_default() += 1;
            }
        }
    }

    fn into_summary(
        self,
        args: &Args,
        generation: u64,
        anchor_height: u64,
        ironwood_tree_size: u64,
    ) -> LoadSummary {
        let completed = self.succeeded + self.errors;
        let duration_s = args.duration.as_secs_f64();
        let mut error_classes: Vec<_> = self
            .error_counts
            .into_iter()
            .map(|(class, count)| ErrorClassCount { class, count })
            .collect();
        error_classes.sort_by_key(|entry| std::cmp::Reverse(entry.count));

        LoadSummary {
            server: args.server.clone(),
            generation,
            anchor_height,
            ironwood_tree_size,
            duration_s,
            parallelism: args.parallelism,
            completed,
            succeeded: self.succeeded,
            errors: self.errors,
            error_rate: if completed == 0 {
                0.0
            } else {
                self.errors as f64 / completed as f64
            },
            requests_per_second: completed as f64 / duration_s,
            stages: vec![
                stage_summary("end-to-end", &self.end_to_end),
                stage_summary("prepare", &self.prepare),
                stage_summary("http", &self.http),
                stage_summary("decode", &self.decode),
            ],
            error_classes,
        }
    }
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    validate(&args)?;

    eprintln!("=== Enhance PIR load test ===");
    eprintln!("server:      {}", args.server);
    eprintln!("parallelism: {}", args.parallelism);
    eprintln!("duration:    {}", humantime::format_duration(args.duration));
    eprintln!("warmup:      {}", humantime::format_duration(args.warmup));
    if let Some(seed) = args.seed {
        eprintln!("seed:        {seed}");
    }

    eprintln!("\nConnecting and validating public parameters...");
    let client = Arc::new(EnhancePirClient::connect(&args.server).await?);
    let generation = client.generation();
    let generation_id = generation.generation;
    let anchor_height = generation.anchor_height;
    let ironwood_tree_size = generation.ironwood_tree_size;
    ensure!(
        ironwood_tree_size > 0,
        "server advertises an empty Ironwood tree"
    );
    eprintln!(
        "generation={generation_id} anchor_height={anchor_height} positions={ironwood_tree_size}"
    );

    let pool = Arc::new(build_query_pool(
        ironwood_tree_size,
        QUERY_POOL_SIZE,
        args.seed,
    ));

    eprintln!("Running preflight query...");
    client
        .query_position_with_timing(pool[0])
        .await
        .context("preflight query failed")?;
    eprintln!("Preflight query succeeded.");

    if !args.warmup.is_zero() {
        eprintln!(
            "\nWarming up for {}...",
            humantime::format_duration(args.warmup)
        );
        run_phase(
            Arc::clone(&client),
            Arc::clone(&pool),
            args.parallelism,
            args.warmup,
            false,
        )
        .await?;
    }

    eprintln!(
        "\nApplying measured load for {}...",
        humantime::format_duration(args.duration)
    );
    let collector = run_phase(client, pool, args.parallelism, args.duration, true).await?;
    let summary = collector.into_summary(&args, generation_id, anchor_height, ironwood_tree_size);
    print_summary(&summary);

    if let Some(path) = &args.json_out {
        let json = serde_json::to_string_pretty(&summary)?;
        std::fs::write(path, format!("{json}\n"))
            .with_context(|| format!("writing load-test report to {}", path.display()))?;
        eprintln!("\nJSON report: {}", path.display());
    }

    let failures = threshold_failures(&summary, &args);
    if !failures.is_empty() {
        for failure in &failures {
            eprintln!("FAIL: {failure}");
        }
        bail!("load-test thresholds failed");
    }

    Ok(())
}

fn validate(args: &Args) -> anyhow::Result<()> {
    ensure!(
        args.parallelism > 0,
        "--parallelism must be greater than zero"
    );
    ensure!(
        !args.duration.is_zero(),
        "--duration must be greater than zero"
    );
    ensure!(
        args.max_error_rate.is_finite() && (0.0..=1.0).contains(&args.max_error_rate),
        "--max-error-rate must be between 0 and 1"
    );
    if let Some(p99) = args.slo_p99_ms {
        ensure!(
            p99.is_finite() && p99 > 0.0,
            "--slo-p99-ms must be greater than zero"
        );
    }
    Ok(())
}

async fn run_phase(
    client: Arc<EnhancePirClient>,
    pool: Arc<Vec<u64>>,
    parallelism: usize,
    duration: Duration,
    collect: bool,
) -> anyhow::Result<StatsCollector> {
    let deadline = Instant::now() + duration;
    let request_index = Arc::new(AtomicU64::new(0));
    let in_flight = Arc::new(AtomicU64::new(0));
    let (sender, mut receiver) = mpsc::unbounded_channel();

    let progress_index = Arc::clone(&request_index);
    let progress_in_flight = Arc::clone(&in_flight);
    let progress = tokio::spawn(async move {
        let started = Instant::now();
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.tick().await;
        loop {
            interval.tick().await;
            if Instant::now() >= deadline {
                break;
            }
            eprintln!(
                "elapsed={:.0}s requests={} in_flight={}",
                started.elapsed().as_secs_f64(),
                progress_index.load(Ordering::Relaxed),
                progress_in_flight.load(Ordering::Relaxed)
            );
        }
    });

    let mut workers = Vec::with_capacity(parallelism);
    for _ in 0..parallelism {
        let client = Arc::clone(&client);
        let pool = Arc::clone(&pool);
        let request_index = Arc::clone(&request_index);
        let in_flight = Arc::clone(&in_flight);
        let sender = sender.clone();
        workers.push(tokio::spawn(async move {
            while Instant::now() < deadline {
                let index = request_index.fetch_add(1, Ordering::Relaxed);
                let position = pool[index as usize % pool.len()];
                in_flight.fetch_add(1, Ordering::Relaxed);
                let sample = do_request(&client, position).await;
                in_flight.fetch_sub(1, Ordering::Relaxed);
                if sender.send(sample).is_err() {
                    break;
                }
            }
        }));
    }
    drop(sender);

    for worker in workers {
        worker.await.context("load-test worker panicked")?;
    }
    progress.abort();
    let _ = progress.await;

    let mut collector = StatsCollector::new();
    while let Some(sample) = receiver.recv().await {
        if collect {
            collector.record(sample);
        }
    }
    Ok(collector)
}

async fn do_request(client: &EnhancePirClient, position: u64) -> Sample {
    let started = Instant::now();
    match client.query_position_with_timing(position).await {
        Ok((_record, timing)) => Sample {
            total: timing.total,
            timing: Some(timing),
            error_class: None,
        },
        Err(error) => Sample {
            total: started.elapsed(),
            timing: None,
            error_class: Some(classify_error(&error)),
        },
    }
}

fn classify_error(error: &ClientError) -> &'static str {
    match error {
        ClientError::Http(error) if error.is_timeout() => "timeout",
        ClientError::HttpStatus(429) => "http_429",
        ClientError::HttpStatus(503) => "http_503",
        ClientError::HttpStatus(_) => "http_status",
        ClientError::Response(_) => "response",
        ClientError::Pir(_) => "pir",
        ClientError::Http(_) => "http",
        ClientError::Json(_)
        | ClientError::PublicParamsBase64(_)
        | ClientError::Generation(_)
        | ClientError::OutsideCoverage(_) => "client",
    }
}

fn build_query_pool(tree_size: u64, size: usize, seed: Option<u64>) -> Vec<u64> {
    let mut rng = match seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::from_entropy(),
    };
    (0..size).map(|_| rng.gen_range(0..tree_size)).collect()
}

fn record_duration(histogram: &mut Histogram<u64>, duration: Duration) {
    let micros = duration.as_micros().max(1).min(u64::MAX as u128) as u64;
    histogram
        .record(micros)
        .expect("u64 duration fits auto-resizing histogram");
}

fn stage_summary(name: &'static str, histogram: &Histogram<u64>) -> StageSummary {
    let millis = |micros: u64| micros as f64 / 1_000.0;
    StageSummary {
        name,
        samples: histogram.len(),
        p50_ms: millis(histogram.value_at_quantile(0.50)),
        p90_ms: millis(histogram.value_at_quantile(0.90)),
        p95_ms: millis(histogram.value_at_quantile(0.95)),
        p99_ms: millis(histogram.value_at_quantile(0.99)),
        p999_ms: millis(histogram.value_at_quantile(0.999)),
        max_ms: millis(histogram.max()),
    }
}

fn threshold_failures(summary: &LoadSummary, args: &Args) -> Vec<String> {
    let mut failures = Vec::new();
    if summary.completed == 0 {
        failures.push("no requests completed".to_string());
    }
    if summary.error_rate > args.max_error_rate {
        failures.push(format!(
            "error rate {:.2}% exceeds {:.2}%",
            summary.error_rate * 100.0,
            args.max_error_rate * 100.0
        ));
    }
    if let Some(limit) = args.slo_p99_ms {
        if summary.stages[0].p99_ms > limit {
            failures.push(format!(
                "end-to-end p99 {:.1}ms exceeds {:.1}ms",
                summary.stages[0].p99_ms, limit
            ));
        }
    }
    failures
}

fn print_summary(summary: &LoadSummary) {
    eprintln!("\n=== Load-test report ===");
    eprintln!(
        "server={} generation={} anchor_height={} positions={}",
        summary.server, summary.generation, summary.anchor_height, summary.ironwood_tree_size
    );
    eprintln!(
        "duration={:.1}s parallelism={} completed={} succeeded={} errors={} ({:.2}%) throughput={:.2} req/s",
        summary.duration_s,
        summary.parallelism,
        summary.completed,
        summary.succeeded,
        summary.errors,
        summary.error_rate * 100.0,
        summary.requests_per_second
    );
    eprintln!(
        "{:>14} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>8}",
        "stage", "p50", "p90", "p95", "p99", "p99.9", "max", "n"
    );
    for stage in &summary.stages {
        eprintln!(
            "{:>14} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>8}",
            stage.name,
            format_ms(stage.p50_ms),
            format_ms(stage.p90_ms),
            format_ms(stage.p95_ms),
            format_ms(stage.p99_ms),
            format_ms(stage.p999_ms),
            format_ms(stage.max_ms),
            stage.samples
        );
    }
    if !summary.error_classes.is_empty() {
        let counts: Vec<_> = summary
            .error_classes
            .iter()
            .map(|entry| format!("{}={}", entry.class, entry.count))
            .collect();
        eprintln!("errors by class: {}", counts.join(" "));
    }
}

fn format_ms(ms: f64) -> String {
    if ms >= 1_000.0 {
        format!("{:.2}s", ms / 1_000.0)
    } else {
        format!("{ms:.1}ms")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn args() -> Args {
        Args {
            server: "https://example.invalid".to_string(),
            parallelism: 2,
            duration: Duration::from_secs(10),
            warmup: Duration::ZERO,
            seed: Some(42),
            json_out: Some(PathBuf::from("summary.json")),
            max_error_rate: 0.1,
            slo_p99_ms: None,
        }
    }

    #[test]
    fn seeded_query_pool_is_repeatable_and_bounded() {
        let first = build_query_pool(17, 100, Some(7));
        let second = build_query_pool(17, 100, Some(7));
        assert_eq!(first, second);
        assert!(first.iter().all(|position| *position < 17));
        assert!(first.iter().any(|position| *position != first[0]));
    }

    #[test]
    fn collector_reports_latency_throughput_and_errors() {
        let mut collector = StatsCollector::new();
        collector.record(Sample {
            total: Duration::from_millis(100),
            timing: Some(QueryTiming {
                prepare: Duration::from_millis(10),
                http: Duration::from_millis(70),
                decode: Duration::from_millis(20),
                total: Duration::from_millis(100),
            }),
            error_class: None,
        });
        collector.record(Sample {
            total: Duration::from_millis(200),
            timing: None,
            error_class: Some("http_503"),
        });

        let summary = collector.into_summary(&args(), 3, 100, 17);
        assert_eq!(summary.completed, 2);
        assert_eq!(summary.succeeded, 1);
        assert_eq!(summary.errors, 1);
        assert_eq!(summary.error_rate, 0.5);
        assert_eq!(summary.requests_per_second, 0.2);
        assert_eq!(summary.stages[0].samples, 2);
        assert_eq!(summary.stages[1].samples, 1);
        assert_eq!(summary.error_classes[0].class, "http_503");
    }

    #[test]
    fn errors_are_classified_for_overload_and_decode_failures() {
        assert_eq!(classify_error(&ClientError::HttpStatus(429)), "http_429");
        assert_eq!(classify_error(&ClientError::HttpStatus(503)), "http_503");
        assert_eq!(
            classify_error(&ClientError::Response("bad body".to_string())),
            "response"
        );
    }

    #[test]
    fn thresholds_check_error_rate_and_p99() {
        let mut test_args = args();
        test_args.max_error_rate = 0.01;
        test_args.slo_p99_ms = Some(50.0);
        let summary = LoadSummary {
            server: test_args.server.clone(),
            generation: 1,
            anchor_height: 2,
            ironwood_tree_size: 3,
            duration_s: 10.0,
            parallelism: 2,
            completed: 10,
            succeeded: 9,
            errors: 1,
            error_rate: 0.1,
            requests_per_second: 1.0,
            stages: vec![StageSummary {
                name: "end-to-end",
                samples: 10,
                p50_ms: 10.0,
                p90_ms: 20.0,
                p95_ms: 40.0,
                p99_ms: 60.0,
                p999_ms: 60.0,
                max_ms: 60.0,
            }],
            error_classes: Vec::new(),
        };

        let failures = threshold_failures(&summary, &test_args);
        assert_eq!(failures.len(), 2);
    }
}
