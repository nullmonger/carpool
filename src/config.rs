use std::num::NonZeroUsize;
use std::time::Duration;

/// Tuning for a `BatchLoader`: collection window, batch size, and the limits
/// that bound concurrency and waiting.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BatchLoaderConfig {
    /// Time a batch stays open collecting calls before it is flushed. Closes
    /// early once [`max_batch_size`](Self::max_batch_size) is reached.
    /// Default: 30ms.
    pub window: Duration,

    /// Upper bound on inputs per batch; reaching it closes the window
    /// immediately. Default: 1024.
    pub max_batch_size: NonZeroUsize,

    /// Deadline for one `BatchCollector::load`, measured from dispatch.
    /// Default: 30s.
    pub timeout: Duration,

    /// Max batches in flight at once. `None` means unbounded. Default: `None`.
    pub concurrency_limit: Option<NonZeroUsize>,

    /// Max time a batch waits for a slot before its callers fail. Only
    /// meaningful with [`concurrency_limit`](Self::concurrency_limit) set.
    /// Default: `None`.
    pub max_waiting: Option<Duration>,
}

impl Default for BatchLoaderConfig {
    fn default() -> Self {
        Self {
            window: Duration::from_millis(30),
            max_batch_size: NonZeroUsize::new(1024).expect("1024 is non-zero"),
            timeout: Duration::from_secs(30),
            concurrency_limit: None,
            max_waiting: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_matches_documented_values() {
        let cfg = BatchLoaderConfig::default();

        assert_eq!(cfg.window, Duration::from_millis(30));
        assert_eq!(cfg.max_batch_size.get(), 1024);
        assert_eq!(cfg.timeout, Duration::from_secs(30));
        assert_eq!(cfg.concurrency_limit, None);
        assert_eq!(cfg.max_waiting, None);
    }
}
