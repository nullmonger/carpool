use std::future::Future;
use std::hash::Hash;

use thiserror::Error;

pub trait Fetcher: Clone + Send + Sync + 'static {
    type Input: Hash + Eq + Clone + Send + 'static;
    type Output: Send + Clone + 'static;
    type Error: std::error::Error + Send + Sync + Clone + 'static;

    fn load(
        &self,
        input: Self::Input,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send;
}

#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum DedupError<E> {
    #[error("fetch failed: {0}")]
    Load(#[source] E),
    #[error("the fetcher panicked while loading")]
    Panic,
}
