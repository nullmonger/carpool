use std::num::NonZeroUsize;
use std::time::Duration;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BatchConfig {
    pub window: Duration,
    pub max_batch_size: NonZeroUsize,
    pub timeout: Duration,
    pub concurrency_limit: Option<NonZeroUsize>,
    // only consulted when concurrency_limit is set
    pub max_waiting: Option<Duration>,
}

impl Default for BatchConfig {
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
        let cfg = BatchConfig::default();

        assert_eq!(cfg.window, Duration::from_millis(30));
        assert_eq!(cfg.max_batch_size.get(), 1024);
        assert_eq!(cfg.timeout, Duration::from_secs(30));
        assert_eq!(cfg.concurrency_limit, None);
        assert_eq!(cfg.max_waiting, None);
    }
}
