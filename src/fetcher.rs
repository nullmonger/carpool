use std::future::Future;
use std::hash::Hash;

pub trait Fetcher: Clone + Send + Sync + 'static {
    type Input: Hash + Eq + Clone + Send + 'static;
    type Output: Send + Clone + 'static;
    type Error: std::error::Error + Send + Sync + Clone + 'static;

    fn load(
        &self,
        input: Self::Input,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send;
}
