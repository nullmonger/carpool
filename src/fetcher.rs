use std::future::Future;
use std::hash::Hash;

pub trait Fetcher: Clone + Send + Sync + 'static {
    type Input: Hash + Eq + Clone + Send + 'static;
    type Output: Clone + Send + Sync + 'static;
    type Error: std::error::Error + Clone + Send + Sync + 'static;

    fn load(
        &self,
        input: Self::Input,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send;
}
