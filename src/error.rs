use std::fmt;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Error<E> {
    Collector(E),
    // implementor bug: the response carries keys that were not in the batch;
    // not attributable to any single caller, so the whole batch fails
    // TODO: consider #[non_exhaustive] on this variant before 1.0 -
    // enum-level #[non_exhaustive] does not stop exhaustive field destructuring
    ContractViolation { unknown_keys: usize },
    // implementor bug: no output under a requested key; not a domain "not found" -
    // absence semantics belong to the implementor's Output type
    MissingOutput,
    Timeout,
    WaitingTimeout,
    // the loader's background pipeline has shut down, so the request cannot be
    // served (for example a downstream panic tore the dispatcher down)
    Closed,
}

impl<E: fmt::Display> fmt::Display for Error<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Collector(e) => write!(f, "collector failed: {e}"),
            Error::ContractViolation { unknown_keys } => write!(
                f,
                "collector broke the key-addressed contract: {unknown_keys} unknown key(s) in the response"
            ),
            Error::MissingOutput => f.write_str("collector returned no output for a requested key"),
            Error::Timeout => f.write_str("batch timed out"),
            Error::WaitingTimeout => f.write_str("timed out waiting for a concurrency slot"),
            Error::Closed => f.write_str("the batch loader has shut down"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for Error<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Collector(e) => Some(e),
            Error::ContractViolation { .. }
            | Error::MissingOutput
            | Error::Timeout
            | Error::WaitingTimeout
            | Error::Closed => None,
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

    // Contract-bug variants wrap nothing downstream:
    // no source, and their Display stands on its own without touching E.
    #[test]
    fn contract_bug_variants_have_no_source_and_render() {
        let missing: Error<DownstreamError> = Error::MissingOutput;
        let violation: Error<DownstreamError> = Error::ContractViolation { unknown_keys: 2 };

        assert!(std::error::Error::source(&missing).is_none());
        assert!(std::error::Error::source(&violation).is_none());
        assert_eq!(
            missing.to_string(),
            "collector returned no output for a requested key"
        );
        assert_eq!(
            violation.to_string(),
            "collector broke the key-addressed contract: 2 unknown key(s) in the response"
        );
    }
}
