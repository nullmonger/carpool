use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;

pub trait BatchCollector: Send + Sync + 'static {
    type Input: Send + 'static;
    type Output: Send + Clone + 'static;
    type Key: Hash + Eq + Send + Clone + 'static;
    type Error: std::error::Error + Send + Sync + Clone + 'static;

    fn key(&self, input: &Self::Input) -> Self::Key;

    // batch arrives deduplicated: inputs sharing a key are interchangeable,
    // only one representative reaches load.
    // strict contract: every requested key must get a value;
    // a missing key is Error::MissingOutput for its waiters,
    // an unknown key in the response is Error::ContractViolation
    // for the whole batch (it takes precedence when both occur).
    // absence semantics live in the implementor's Output type.
    fn load(
        &self,
        batch: HashMap<Self::Key, Self::Input>,
    ) -> impl Future<Output = Result<HashMap<Self::Key, Self::Output>, Self::Error>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct SquareLoader;

    impl BatchCollector for SquareLoader {
        type Input = u64;
        type Output = u64;
        type Key = u64;
        type Error = std::convert::Infallible;

        fn key(&self, input: &u64) -> u64 {
            *input
        }

        async fn load(&self, batch: HashMap<u64, u64>) -> Result<HashMap<u64, u64>, Self::Error> {
            Ok(batch.into_iter().map(|(k, x)| (k, x * x)).collect())
        }
    }

    // Spawning the load future proves it is `Send` (the whole point of the
    // RPITIT + Send form) and that a plain `async fn` satisfies the bound.
    #[tokio::test]
    async fn load_future_is_send_and_key_addressed() {
        let loader = SquareLoader;
        let batch = HashMap::from([(2, 2), (3, 3), (4, 4)]);
        let out = tokio::spawn(async move { loader.load(batch).await })
            .await
            .expect("task joins")
            .expect("load succeeds");

        assert_eq!(out, HashMap::from([(2, 4), (3, 9), (4, 16)]));
    }
}
