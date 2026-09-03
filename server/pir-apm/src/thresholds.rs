pub const SCRAPE_FAILURE_TICKS: u32 = 2;
pub const READY_FAILURE_SECONDS: u64 = 5 * 60;
pub const HTTP_5XX_RATIO: f64 = 0.05;
pub const HTTP_5XX_MIN_REQUESTS: f64 = 10.0;
/// p99 budget for any endpoint without a `PIR_APM_LATENCY_P99_OVERRIDES` entry.
pub const DEFAULT_LATENCY_P99_SECONDS: f64 = 1.0;
pub const LATENCY_MIN_REQUESTS: f64 = 20.0;
pub const DISK_USED_RATIO: f64 = 0.90;
pub const MEMORY_AVAILABLE_BYTES: u64 = 512 * 1024 * 1024;
