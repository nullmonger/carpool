use std::future::Future;
use std::hash::Hash;

/// A downstream source that a `BatchLoader` batches and deduplicates calls
/// against.
///
/// Implement this to define how an input maps to a dedup key and how a
/// deduplicated batch is fetched.
///
/// # Contract
///
/// `load` receives inputs already deduplicated by [`Key`](Self::Key) and must
/// return a `Vec<Output>` of the same length and order: the output at index
/// `i` answers the input at index `i`. Violating it surfaces to callers as
/// `Error::ContractViolation`.
pub trait BatchCollector: Send + Sync + 'static {
    /// A single request handed to the loader.
    type Input: Send + 'static;

    /// The result produced for one input; cloned to every caller that shared
    /// its key.
    type Output: Send + Clone + 'static;

    /// Dedup key: inputs with the same key share one downstream slot.
    type Key: Hash + Eq + Send + Clone + 'static;

    /// Failure returned by [`load`](Self::load); cloned to every caller waiting
    /// on the batch. Must implement [`std::error::Error`].
    type Error: std::error::Error + Send + Sync + Clone + 'static;

    /// Extracts the dedup key for an input.
    fn key(&self, input: &Self::Input) -> Self::Key;

    /// Fetches one deduplicated batch. See the trait [contract](Self#contract)
    /// for the length and ordering requirements on the returned vector.
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
