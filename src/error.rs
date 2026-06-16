use thiserror::Error;

#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum Error<E> {
    // #[source], not #[from]: #[from] would generate From<E> and clash for a generic E
    #[error("collector failed: {0}")]
    Collector(#[source] E),
    // implementor bug: response key not in the batch - fails the whole batch
    #[error(
        "collector broke the key-addressed contract: {unknown_keys} unknown key(s) in the response"
    )]
    ContractViolation { unknown_keys: usize },
    // implementor bug: no output for a requested key - not a domain "not found"
    #[error("collector returned no output for a requested key")]
    MissingOutput,
    #[error("batch timed out")]
    Timeout,
    #[error("timed out waiting for a concurrency slot")]
    WaitingTimeout,
    // background pipeline shut down (for example a downstream panic tore the dispatcher down)
    #[error("the batch loader has shut down")]
    Closed,
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
