use std::future::Future;
use std::hash::Hash;

pub trait BatchCollector: Send + Sync + 'static {
    type Input: Send + 'static;
    type Output: Send + Clone + 'static;
    type Key: Hash + Eq + Send + Clone + 'static;
    type Error: std::error::Error + Send + Sync + Clone + 'static;

    fn key(&self, input: &Self::Input) -> Self::Key;

    // inputs are deduplicated by key; output must be position-aligned to them
    fn load(
        &self,
        inputs: Vec<Self::Input>,
    ) -> impl Future<Output = Result<Vec<Self::Output>, Self::Error>> + Send;
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

        async fn load(&self, inputs: Vec<u64>) -> Result<Vec<u64>, Self::Error> {
            Ok(inputs.iter().map(|x| x * x).collect())
        }
    }

    // Spawning the load future proves it is `Send` (the whole point of the
    // RPITIT + Send form) and that a plain `async fn` satisfies the bound.
    #[tokio::test]
    async fn load_future_is_send_and_position_aligned() {
        let loader = SquareLoader;
        let out = tokio::spawn(async move { loader.load(vec![2, 3, 4]).await })
            .await
            .expect("task joins")
            .expect("load succeeds");

        assert_eq!(out, vec![4, 9, 16]);
    }
}
