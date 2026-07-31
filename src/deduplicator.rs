use crate::fetcher::Fetcher;

#[derive(Clone)]
pub struct Deduplicator<F: Fetcher> {
    fetcher: F,
}

impl<F: Fetcher> Deduplicator<F> {
    pub fn new(fetcher: F) -> Self {
        Self { fetcher }
    }

    pub async fn call(&self, input: F::Input) -> Result<F::Output, F::Error> {
        self.fetcher.load(input).await
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::fmt;

    use crate::{Deduplicator, Fetcher};

    #[derive(Clone)]
    struct Squaring;

    impl Fetcher for Squaring {
        type Input = u64;
        type Output = u64;
        type Error = Infallible;

        async fn load(&self, input: u64) -> Result<u64, Infallible> {
            Ok(input * input)
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    struct Boom;

    impl fmt::Display for Boom {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("boom")
        }
    }

    impl std::error::Error for Boom {}

    #[derive(Clone)]
    struct Failing;

    impl Fetcher for Failing {
        type Input = u64;
        type Output = u64;
        type Error = Boom;

        async fn load(&self, _input: u64) -> Result<u64, Boom> {
            Err(Boom)
        }
    }

    #[tokio::test]
    async fn a_call_delivers_the_fetched_value() {
        let d = Deduplicator::new(Squaring);
        assert_eq!(d.call(7).await, Ok(49));
    }

    #[tokio::test]
    async fn a_failed_fetch_reaches_the_caller() {
        let d = Deduplicator::new(Failing);
        assert_eq!(d.call(1).await, Err(Boom));
    }
}
