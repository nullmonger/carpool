use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::watch;

use crate::error::DedupError;
use crate::fetcher::Fetcher;

type Outcome<F> = Result<<F as Fetcher>::Output, DedupError<<F as Fetcher>::Error>>;
type Entry<F> = Arc<watch::Sender<Option<Outcome<F>>>>;
type Registry<F> = Arc<Mutex<HashMap<<F as Fetcher>::Input, Entry<F>>>>;

// A panic under the lock must not brick the registry: recover the poisoned mutex.
fn locked<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Clone)]
pub struct Deduplicator<F: Fetcher> {
    fetcher: F,
    registry: Registry<F>,
}

impl<F: Fetcher> Deduplicator<F> {
    pub fn new(fetcher: F) -> Self {
        Self {
            fetcher,
            registry: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn call(&self, input: F::Input) -> Result<F::Output, DedupError<F::Error>> {
        let mut waiter = self.join_or_open(input);
        loop {
            // `subscribe` marks the current value as seen, so read before awaiting a change.
            if let Some(outcome) = waiter.borrow_and_update().clone() {
                return outcome;
            }
            if waiter.changed().await.is_err() {
                return Err(DedupError::Lost);
            }
        }
    }

    fn join_or_open(&self, input: F::Input) -> watch::Receiver<Option<Outcome<F>>> {
        // The clone stays out of the critical section,
        // and declaring it before the guard keeps a joining call's drop out too.
        let for_flight = input.clone();
        let mut registry = locked(&self.registry);
        if let Some(entry) = registry.get(&input) {
            return entry.subscribe();
        }

        let (sender, waiter) = watch::channel(None);
        let entry = Arc::new(sender);
        registry.insert(input, entry.clone());
        drop(registry);

        tokio::spawn(fly(
            self.fetcher.clone(),
            self.registry.clone(),
            for_flight,
            entry,
        ));
        waiter
    }
}

async fn fly<F: Fetcher>(fetcher: F, registry: Registry<F>, input: F::Input, entry: Entry<F>) {
    let outcome = fetcher.load(input.clone()).await.map_err(DedupError::Load);

    // Checking out before sending frees the input for the next call;
    // nothing may yield in between, or a waiter is left without an outcome.
    // The named pair outlives the temporary guard, so consumer-owned values drop unlocked.
    let checked_out = locked(&registry).remove_entry(&input);
    entry.send_replace(Some(outcome));
    drop(checked_out);
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::error::Error;
    use std::fmt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::sync::{Barrier, Notify};
    use tokio::time::timeout;

    use crate::{DedupError, Deduplicator, Fetcher};

    // Wall-clock only guards against a hang, never times an interleaving.
    const GUARD: Duration = Duration::from_secs(5);

    #[derive(Clone)]
    struct Counting {
        fetches: Arc<AtomicUsize>,
    }

    impl Fetcher for Counting {
        type Input = u64;
        type Output = u64;
        type Error = Infallible;

        async fn load(&self, input: u64) -> Result<u64, Infallible> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            Ok(input * input)
        }
    }

    fn counting() -> (Counting, Arc<AtomicUsize>) {
        let fetches = Arc::new(AtomicUsize::new(0));
        (
            Counting {
                fetches: fetches.clone(),
            },
            fetches,
        )
    }

    #[derive(Debug, Clone)]
    struct Boom;

    impl fmt::Display for Boom {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("boom")
        }
    }

    impl Error for Boom {}

    #[derive(Clone)]
    struct Failing {
        fetches: Arc<AtomicUsize>,
        failures: usize,
    }

    impl Fetcher for Failing {
        type Input = u64;
        type Output = u64;
        type Error = Boom;

        async fn load(&self, input: u64) -> Result<u64, Boom> {
            if self.fetches.fetch_add(1, Ordering::SeqCst) < self.failures {
                return Err(Boom);
            }
            Ok(input * input)
        }
    }

    fn failing(failures: usize) -> (Failing, Arc<AtomicUsize>) {
        let fetches = Arc::new(AtomicUsize::new(0));
        (
            Failing {
                fetches: fetches.clone(),
                failures,
            },
            fetches,
        )
    }

    // Seats one flight and holds it, so other calls can join.
    #[derive(Clone)]
    struct Holding {
        fetches: Arc<AtomicUsize>,
        entered: Arc<Notify>,
        gate: Arc<Notify>,
    }

    impl Fetcher for Holding {
        type Input = u64;
        type Output = u64;
        type Error = Infallible;

        async fn load(&self, input: u64) -> Result<u64, Infallible> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.gate.notified().await;
            Ok(input * input)
        }
    }

    #[tokio::test]
    async fn a_call_delivers_the_fetched_value() {
        let (fetcher, _) = counting();
        let d = Deduplicator::new(fetcher);
        assert_eq!(d.call(7).await.unwrap(), 49);
    }

    #[tokio::test]
    async fn a_failed_fetch_reaches_the_caller_with_its_cause() {
        let (fetcher, _) = failing(usize::MAX);
        let d = Deduplicator::new(fetcher);

        let err = d.call(1).await.unwrap_err();

        assert!(matches!(err, DedupError::Load(Boom)));
        assert!(err.source().is_some_and(|cause| cause.is::<Boom>()));
    }

    #[tokio::test]
    async fn a_failed_fetch_is_shared_by_the_collapsed_calls() {
        let (fetcher, fetches) = failing(usize::MAX);
        let d = Deduplicator::new(fetcher);

        let (o1, o2, o3) = tokio::join!(d.call(7), d.call(7), d.call(7));

        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        for outcome in [o1, o2, o3] {
            assert!(matches!(outcome, Err(DedupError::Load(Boom))));
        }
    }

    #[tokio::test]
    async fn the_input_is_free_after_a_failed_fetch() {
        let (fetcher, fetches) = failing(1);
        let d = Deduplicator::new(fetcher);

        assert!(matches!(d.call(3).await, Err(DedupError::Load(Boom))));
        assert_eq!(d.call(3).await.unwrap(), 9);
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }

    // The flight cannot start until this task yields,
    // and every call subscribes before the first yield, so the count proves the collapse.
    #[tokio::test]
    async fn concurrent_calls_of_one_input_collapse_to_one_flight() {
        let (fetcher, fetches) = counting();
        let d = Deduplicator::new(fetcher);

        let (o1, o2, o3, o4, o5) =
            tokio::join!(d.call(7), d.call(7), d.call(7), d.call(7), d.call(7));

        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        assert_eq!(
            [
                o1.unwrap(),
                o2.unwrap(),
                o3.unwrap(),
                o4.unwrap(),
                o5.unwrap()
            ],
            [49; 5]
        );
    }

    #[tokio::test]
    async fn distinct_inputs_each_open_their_own_flight() {
        let (fetcher, fetches) = counting();
        let d = Deduplicator::new(fetcher);

        let (a, b, c) = tokio::join!(d.call(2), d.call(3), d.call(4));

        assert_eq!((a.unwrap(), b.unwrap(), c.unwrap()), (4, 9, 16));
        assert_eq!(fetches.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn a_call_after_the_flight_opens_a_new_one() {
        let (fetcher, fetches) = counting();
        let d = Deduplicator::new(fetcher);

        assert_eq!(d.call(5).await.unwrap(), 25);
        assert_eq!(d.call(5).await.unwrap(), 25);
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn clones_share_one_flight() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(Notify::new());
        let gate = Arc::new(Notify::new());
        let d = Deduplicator::new(Holding {
            fetches: fetches.clone(),
            entered: entered.clone(),
            gate: gate.clone(),
        });

        let leader = tokio::spawn({
            let d = d.clone();
            async move { d.call(7).await }
        });
        timeout(GUARD, entered.notified())
            .await
            .expect("the leader must reach the fetcher");

        // The follower subscribes in the first branch;
        // the flight is released only by the second, so it cannot settle before the join.
        let (follower, ()) = timeout(GUARD, async {
            tokio::join!(d.call(7), async { gate.notify_one() })
        })
        .await
        .expect("the follower must join the seated flight");

        assert_eq!(follower.unwrap(), 49);
        let leader = timeout(GUARD, leader)
            .await
            .expect("the leader must resolve too")
            .unwrap();
        assert_eq!(leader.unwrap(), 49);
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn race_concurrent_calls_of_one_input_leave_it_free() {
        const CALLERS: usize = 8;

        for _ in 0..50 {
            let (fetcher, fetches) = counting();
            let d = Deduplicator::new(fetcher);
            let start = Arc::new(Barrier::new(CALLERS));

            let callers: Vec<_> = (0..CALLERS)
                .map(|_| {
                    let d = d.clone();
                    let start = start.clone();
                    tokio::spawn(async move {
                        start.wait().await;
                        d.call(7).await
                    })
                })
                .collect();

            for caller in callers {
                let outcome = timeout(GUARD, caller)
                    .await
                    .expect("no caller may hang")
                    .unwrap();
                assert_eq!(outcome.unwrap(), 49);
            }

            // However many fetches the interleaving cost, the input is free once the racers finish.
            let before = fetches.load(Ordering::SeqCst);
            let after = timeout(GUARD, d.call(7))
                .await
                .expect("the input must be free again");
            assert_eq!(after.unwrap(), 49);
            assert_eq!(fetches.load(Ordering::SeqCst), before + 1);
        }
    }
}
