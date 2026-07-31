use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::watch;

use crate::fetcher::Fetcher;

type Outcome<F> = Result<<F as Fetcher>::Output, <F as Fetcher>::Error>;
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
            registry: Registry::<F>::default(),
        }
    }

    pub async fn call(&self, input: F::Input) -> Result<F::Output, F::Error> {
        let mut waiter = self.join_or_open(input);
        loop {
            // `subscribe` marks the current value as seen, so read before awaiting a change.
            if let Some(outcome) = waiter.borrow_and_update().clone() {
                return outcome;
            }
            waiter
                .changed()
                .await
                .expect("the entry outlives its flight");
        }
    }

    fn join_or_open(&self, input: F::Input) -> watch::Receiver<Option<Outcome<F>>> {
        // Cloned before the lock and declared before the guard,
        // so a joining call drops its key outside the critical section.
        let key = input.clone();
        let mut registry = locked(&self.registry);
        if let Some(entry) = registry.get(&key) {
            return entry.subscribe();
        }

        let (sender, waiter) = watch::channel(None);
        let entry = Entry::<F>::new(sender);
        registry.insert(key, entry.clone());
        drop(registry);

        tokio::spawn(fly(
            self.fetcher.clone(),
            self.registry.clone(),
            input,
            entry,
        ));
        waiter
    }
}

async fn fly<F: Fetcher>(fetcher: F, registry: Registry<F>, input: F::Input, entry: Entry<F>) {
    let outcome = fetcher.load(input.clone()).await;

    // Checking out before sending keeps the input free for the next call;
    // the removed pair leaves the critical section, so no consumer destructor runs
    // under the lock. Nothing may yield between the two: a waiter would be stranded.
    let checked_out = locked(&registry).remove_entry(&input);
    entry.send_replace(Some(outcome));
    drop(checked_out);
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::fmt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::sync::{Barrier, Notify};
    use tokio::time::timeout;

    use crate::{Deduplicator, Fetcher};

    // Wall-clock only guards against a hang, never times an interleaving.
    const GUARD: Duration = Duration::from_secs(5);

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

    #[derive(Clone)]
    struct Counting {
        calls: Arc<AtomicUsize>,
    }

    impl Fetcher for Counting {
        type Input = u64;
        type Output = u64;
        type Error = Infallible;

        async fn load(&self, input: u64) -> Result<u64, Infallible> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(input * input)
        }
    }

    fn counting() -> (Counting, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Counting {
                calls: calls.clone(),
            },
            calls,
        )
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

    // Signals on entry and holds until released,
    // so a test can seat one flight and let others join it.
    #[derive(Clone)]
    struct Holding {
        calls: Arc<AtomicUsize>,
        entered: Arc<Notify>,
        gate: Arc<Notify>,
    }

    impl Fetcher for Holding {
        type Input = u64;
        type Output = u64;
        type Error = Infallible;

        async fn load(&self, input: u64) -> Result<u64, Infallible> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.gate.notified().await;
            Ok(input * input)
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

    // Every call subscribes before the first yield,
    // so the flight cannot settle in between and the count proves the collapse.
    #[tokio::test]
    async fn concurrent_calls_of_one_input_collapse_to_one_flight() {
        let (fetcher, calls) = counting();
        let d = Deduplicator::new(fetcher);

        let outcomes = tokio::join!(d.call(7), d.call(7), d.call(7), d.call(7), d.call(7));

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(outcomes, (Ok(49), Ok(49), Ok(49), Ok(49), Ok(49)));
    }

    #[tokio::test]
    async fn distinct_inputs_each_open_their_own_flight() {
        let (fetcher, calls) = counting();
        let d = Deduplicator::new(fetcher);

        let (a, b, c) = tokio::join!(d.call(2), d.call(3), d.call(4));

        assert_eq!((a, b, c), (Ok(4), Ok(9), Ok(16)));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn a_call_after_the_flight_opens_a_new_one() {
        let (fetcher, calls) = counting();
        let d = Deduplicator::new(fetcher);

        assert_eq!(d.call(5).await, Ok(25));
        assert_eq!(d.call(5).await, Ok(25));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn clones_share_one_flight() {
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(Notify::new());
        let gate = Arc::new(Notify::new());
        let d = Deduplicator::new(Holding {
            calls: calls.clone(),
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

        assert_eq!(follower, Ok(49));
        assert_eq!(leader.await.unwrap(), Ok(49));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn race_concurrent_calls_of_one_input() {
        const CALLERS: usize = 8;

        for _ in 0..50 {
            let (fetcher, calls) = counting();
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
                assert_eq!(outcome, Ok(49));
            }

            // Whatever the interleaving cost in extra flights, the registry is empty
            // once the racers are done: the next call opens exactly one more.
            let flights = calls.load(Ordering::SeqCst);
            assert_eq!(d.call(7).await, Ok(49));
            assert_eq!(calls.load(Ordering::SeqCst), flights + 1);
        }
    }
}
