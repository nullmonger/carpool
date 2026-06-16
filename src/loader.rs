use tokio::sync::{mpsc, oneshot};

use crate::collector::BatchCollector;
use crate::config::BatchLoaderConfig;
use crate::dispatch::{Request, dispatch_window};
use crate::error::Error;
use crate::window;

// Public runtime: every `load` drops its request into the collection window and
// awaits a one-shot reply. A background dispatcher turns each closed window into
// one deduplicated downstream call and fans the results back out by key.
// Cloning is cheap (a collector clone and a sender), so the loader is shared across tasks.
#[derive(Clone)]
pub struct BatchLoader<C: BatchCollector> {
    collector: C,
    inbound: mpsc::Sender<Request<C>>,
}

impl<C: BatchCollector> BatchLoader<C> {
    // spawn, not new: two background tasks start eagerly; a lazy ctor can come later.
    pub fn spawn(collector: C, config: BatchLoaderConfig) -> Self {
        // inbound buffers a burst up to one window's worth; outbound holds at most
        // one closed window awaiting dispatch - deeper buffering would only hide
        // backpressure behind memory.
        let (inbound, requests) = mpsc::channel::<Request<C>>(config.max_batch_size.get());
        let (windows_tx, windows_rx) = mpsc::channel::<Vec<Request<C>>>(1);

        // Both tasks are detached: they end on channel close once every loader
        // clone is dropped, so there is nothing to join.
        tokio::spawn(window::collect(
            requests,
            windows_tx,
            config.window,
            config.max_batch_size,
        ));
        tokio::spawn(run_dispatcher(collector.clone(), windows_rx));

        Self { collector, inbound }
    }

    // The key is derived through the collector so dedup and dispatch address the
    // same key the implementor sees.
    pub async fn load(&self, input: C::Input) -> Result<C::Output, Error<C::Error>> {
        let key = self.collector.key(&input);
        let (respond, reply) = oneshot::channel();
        if self
            .inbound
            .send(Request {
                key,
                input,
                respond,
            })
            .await
            .is_err()
        {
            return Err(Error::Closed);
        }
        match reply.await {
            Ok(result) => result,
            // The dispatcher dropped our responder without answering
            // (a downstream panic can tear it down), so the loader cannot serve.
            Err(_) => Err(Error::Closed),
        }
    }
}

async fn run_dispatcher<C: BatchCollector>(
    collector: C,
    mut windows: mpsc::Receiver<Vec<Request<C>>>,
) {
    // One closed window at a time, one downstream call each. Ends when the window
    // task drops its sender (all loaders gone), winding the runtime down.
    while let Some(batch) = windows.recv().await {
        dispatch_window(&collector, batch).await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    #[derive(Clone)]
    struct Squares {
        calls: Arc<AtomicUsize>,
    }

    impl BatchCollector for Squares {
        type Input = u64;
        type Output = u64;
        type Key = u64;
        type Error = std::convert::Infallible;

        fn key(&self, input: &u64) -> u64 {
            *input
        }

        async fn load(&self, batch: HashMap<u64, u64>) -> Result<HashMap<u64, u64>, Self::Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(batch.into_iter().map(|(k, v)| (k, v * v)).collect())
        }
    }

    // A loader whose window never closes on the timer (virtual time stays frozen),
    // so windows close deterministically at `max` items - no scheduler race.
    fn loader(max: usize, calls: Arc<AtomicUsize>) -> BatchLoader<Squares> {
        let config = BatchLoaderConfig {
            window: Duration::from_secs(3600),
            max_batch_size: NonZeroUsize::new(max).expect("max is non-zero"),
            ..BatchLoaderConfig::default()
        };
        BatchLoader::spawn(Squares { calls }, config)
    }

    fn spawn_load(
        loader: &BatchLoader<Squares>,
        input: u64,
    ) -> tokio::task::JoinHandle<Result<u64, Error<std::convert::Infallible>>> {
        let loader = loader.clone();
        tokio::spawn(async move { loader.load(input).await })
    }

    async fn collect_results(
        handles: Vec<tokio::task::JoinHandle<Result<u64, Error<std::convert::Infallible>>>>,
    ) -> Vec<u64> {
        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.expect("task joins").expect("load succeeds"));
        }
        results
    }

    // Dedup proven by the call count: five concurrent loads over two keys collapse
    // to a single downstream call, and every waiter still gets its key's value.
    #[tokio::test(start_paused = true)]
    async fn duplicate_keys_collapse_to_one_downstream_call() {
        let calls = Arc::new(AtomicUsize::new(0));
        let loader = loader(5, calls.clone());

        let handles = [1u64, 1, 1, 2, 2]
            .into_iter()
            .map(|k| spawn_load(&loader, k))
            .collect();
        let results = collect_results(handles).await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "dedup -> single downstream call"
        );
        assert_eq!(results, vec![1, 1, 1, 4, 4]);
    }

    // Distinct keys in one window each get their own result from the one call.
    #[tokio::test(start_paused = true)]
    async fn distinct_keys_each_get_their_own_result() {
        let calls = Arc::new(AtomicUsize::new(0));
        let loader = loader(3, calls.clone());

        let handles = [2u64, 3, 4]
            .into_iter()
            .map(|k| spawn_load(&loader, k))
            .collect();
        let results = collect_results(handles).await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(results, vec![4, 9, 16]);
    }

    // A load arriving after the window closed opens a fresh one, so it is a new
    // downstream call - the count goes from one to two.
    #[tokio::test(start_paused = true)]
    async fn a_load_after_the_window_closes_starts_a_new_call() {
        let calls = Arc::new(AtomicUsize::new(0));
        let loader = loader(1, calls.clone());

        let first = spawn_load(&loader, 5)
            .await
            .expect("task joins")
            .expect("load succeeds");
        assert_eq!(first, 25);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let second = spawn_load(&loader, 6)
            .await
            .expect("task joins")
            .expect("load succeeds");
        assert_eq!(second, 36);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "the later load opens a fresh window"
        );
    }

    // Regression guard: load must stay Send so a cloned loader can spawn it.
    #[tokio::test(start_paused = true)]
    async fn load_future_is_send_after_clone() {
        let calls = Arc::new(AtomicUsize::new(0));
        let loader = loader(1, calls);

        let value = spawn_load(&loader, 7)
            .await
            .expect("task joins")
            .expect("load succeeds");

        assert_eq!(value, 49);
    }
}
