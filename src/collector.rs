use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;

pub trait BatchCollector: Clone + Send + Sync + 'static {
    type Input: Hash + Eq + Clone + Send + 'static;
    type Output: Send + Clone + 'static;
    type Error: std::error::Error + Send + Sync + Clone + 'static;

    // Strict contract: every requested input gets a value.
    // A missing input yields Error::MissingOutput to its waiters;
    // an unknown input yields Error::ContractViolation for the whole batch
    // (it wins if both occur). Absence semantics belong in the Output type.
    fn load(
        &self,
        batch: HashSet<Self::Input>,
    ) -> impl Future<Output = Result<HashMap<Self::Input, Self::Output>, Self::Error>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct SquareLoader;

    impl BatchCollector for SquareLoader {
        type Input = u64;
        type Output = u64;
        type Error = std::convert::Infallible;

        async fn load(&self, batch: HashSet<u64>) -> Result<HashMap<u64, u64>, Self::Error> {
            Ok(batch.into_iter().map(|x| (x, x * x)).collect())
        }
    }

    // Spawning the load future proves it is `Send` (the whole point of the
    // RPITIT + Send form) and that a plain `async fn` satisfies the bound.
    #[tokio::test]
    async fn load_future_is_send_and_input_addressed() {
        let loader = SquareLoader;
        let batch = HashSet::from([2, 3, 4]);
        let out = tokio::spawn(async move { loader.load(batch).await })
            .await
            .expect("task joins")
            .expect("load succeeds");

        assert_eq!(out, HashMap::from([(2, 4), (3, 9), (4, 16)]));
    }
}
