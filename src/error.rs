use std::fmt;

/// Failure surfaced to a caller of `BatchLoader::load`.
///
/// Generic over the collector's own error type `E`, which rides unchanged in
/// [`Collector`](Self::Collector).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Error<E> {
    /// The collector's `load` returned an error; carries it unchanged.
    Collector(E),

    /// The collector returned the wrong number of outputs, breaking the
    /// position-aligned contract.
    ContractViolation {
        /// Outputs expected: the deduplicated input count.
        expected: usize,
        /// Outputs the collector actually returned.
        got: usize,
    },

    /// A batch exceeded the configured per-batch `timeout`.
    Timeout,

    /// A batch waited longer than `max_waiting` for a concurrency slot.
    WaitingTimeout,
}

impl<E: fmt::Display> fmt::Display for Error<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Collector(e) => write!(f, "collector failed: {e}"),
            Error::ContractViolation { expected, got } => write!(
                f,
                "collector broke the position-aligned contract: expected {expected} outputs, got {got}"
            ),
            Error::Timeout => f.write_str("batch timed out"),
            Error::WaitingTimeout => f.write_str("timed out waiting for a concurrency slot"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for Error<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Collector(e) => Some(e),
            Error::ContractViolation { .. } | Error::Timeout | Error::WaitingTimeout => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    struct DownstreamError(&'static str);

    impl std::fmt::Display for DownstreamError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "downstream: {}", self.0)
        }
    }

    impl std::error::Error for DownstreamError {}

    #[test]
    fn collector_variant_exposes_downstream_as_source() {
        let err: Error<DownstreamError> = Error::Collector(DownstreamError("boom"));
        let source = std::error::Error::source(&err).expect("Collector has a source");

        assert_eq!(source.to_string(), "downstream: boom");
    }
}
