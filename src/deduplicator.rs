use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex};

use thiserror::Error;
use tokio::sync::oneshot;

use crate::fetcher::Fetcher;

#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum DedupError<E> {
    #[error("fetch failed: {0}")]
    Load(#[source] E),
    #[error("the fetcher panicked while loading")]
    Panic,
}

// Aliased to keep the nested generics under clippy's type-complexity lint.
type Responder<F> =
    oneshot::Sender<Result<<F as Fetcher>::Output, DedupError<<F as Fetcher>::Error>>>;

type InFlight<F> = Arc<Mutex<HashMap<<F as Fetcher>::Input, Vec<Responder<F>>>>>;

#[derive(Clone)]
pub struct Deduplicator<F: Fetcher> {
    fetcher: F,
    in_flight: InFlight<F>,
}

impl<F: Fetcher> Deduplicator<F> {
    pub fn new(fetcher: F) -> Self {
        Self {
            fetcher,
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn call(&self, input: F::Input) -> Result<F::Output, DedupError<F::Error>> {
        let (respond, reply) = oneshot::channel();

        // First caller of an input leads and starts the fetch; the rest wait on the entry.
        let leader = {
            let mut in_flight = self
                .in_flight
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match in_flight.entry(input.clone()) {
                Entry::Occupied(mut e) => {
                    e.get_mut().push(respond);
                    false
                }
                Entry::Vacant(e) => {
                    e.insert(vec![respond]);
                    true
                }
            }
        };

        if leader {
            // Detached so the fetch outlives its callers (cancelled leader, or all waiters gone).
            tokio::spawn(run_fetch(
                self.fetcher.clone(),
                self.in_flight.clone(),
                input,
            ));
        }

        match reply.await {
            Ok(result) => result,
            // Sender gone without a reply: run_fetch died abnormally, report instead of hanging.
            Err(_) => Err(DedupError::Panic),
        }
    }
}

// A load panic becomes a JoinError (never a hung waiter).
// The Fetcher and Input trait impls (Clone, Hash, Eq) run outside that isolation:
// they must not panic, or the entry orphans and this input's later callers hang.
async fn run_fetch<F: Fetcher>(fetcher: F, in_flight: InFlight<F>, input: F::Input) {
    let key = input.clone();
    let outcome = tokio::spawn(async move { fetcher.load(input).await }).await;

    // Clear the entry on every path (success, error, panic) so a later call refetches.
    let waiters = in_flight
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&key)
        .unwrap_or_default();

    match outcome {
        Ok(Ok(value)) => fan_out::<F>(waiters, Ok(value)),
        Ok(Err(e)) => fan_out::<F>(waiters, Err(DedupError::Load(e))),
        Err(panicked) => {
            warn_load_panicked(&panicked);
            fan_out::<F>(waiters, Err(DedupError::Panic));
        }
    }
}

fn fan_out<F: Fetcher>(
    waiters: Vec<Responder<F>>,
    result: Result<F::Output, DedupError<F::Error>>,
) {
    for respond in waiters {
        let delivered = respond.send(result.clone()).is_ok();
        warn_if_dropped(delivered);
    }
}

#[cfg(feature = "tracing")]
fn warn_load_panicked(err: &tokio::task::JoinError) {
    tracing::warn!("carpool: the fetcher panicked while loading: {err}");
}

#[cfg(not(feature = "tracing"))]
fn warn_load_panicked(_err: &tokio::task::JoinError) {}

#[cfg(feature = "tracing")]
fn warn_if_dropped(delivered: bool) {
    if !delivered {
        tracing::warn!("carpool: dropped a fetch result because its caller went away");
    }
}

#[cfg(not(feature = "tracing"))]
fn warn_if_dropped(_delivered: bool) {}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::sync::Notify;

    use super::*;

    // Counts fetches so dedup is observable, not just equal results.
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

    // join! on the current_thread runtime: all subscribe before the leader's fetch
    // runs, so the count deterministically proves the collapse.
    #[tokio::test]
    async fn concurrent_calls_of_one_input_collapse_to_one_fetch() {
        let (fetcher, calls) = counting();
        let d = Deduplicator::new(fetcher);

        let (a, b, c, e, f) = tokio::join!(d.call(7), d.call(7), d.call(7), d.call(7), d.call(7));

        assert_eq!(calls.load(Ordering::SeqCst), 1, "five calls, one fetch");
        for r in [a, b, c, e, f] {
            assert_eq!(r.unwrap(), 49, "every caller gets the shared value");
        }
    }

    #[tokio::test]
    async fn distinct_inputs_each_get_their_own_fetch() {
        let (fetcher, calls) = counting();
        let d = Deduplicator::new(fetcher);

        let (a, b, c) = tokio::join!(d.call(2), d.call(3), d.call(4));

        assert_eq!(a.unwrap(), 4);
        assert_eq!(b.unwrap(), 9);
        assert_eq!(c.unwrap(), 16);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "one fetch per distinct input"
        );
    }

    // A call after completion finds the entry cleared and refetches (1 -> 2).
    #[tokio::test]
    async fn a_call_after_completion_starts_a_new_fetch() {
        let (fetcher, calls) = counting();
        let d = Deduplicator::new(fetcher);

        assert_eq!(d.call(5).await.unwrap(), 25);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        assert_eq!(d.call(5).await.unwrap(), 25);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "a later call reopens a fetch"
        );
    }

    #[derive(Debug, Clone)]
    struct Boom;

    impl std::fmt::Display for Boom {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

    #[tokio::test]
    async fn a_fetch_error_reaches_every_coalesced_caller() {
        let d = Deduplicator::new(Failing);

        let (a, b) = tokio::join!(d.call(1), d.call(1));

        assert!(matches!(a, Err(DedupError::Load(_))));
        assert!(matches!(b, Err(DedupError::Load(_))));
    }

    // Panics on its first fetch, succeeds after.
    #[derive(Clone)]
    struct PanicOnce {
        calls: Arc<AtomicUsize>,
    }

    impl Fetcher for PanicOnce {
        type Input = u64;
        type Output = u64;
        type Error = Infallible;

        async fn load(&self, input: u64) -> Result<u64, Infallible> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("fetch blew up");
            }
            Ok(input)
        }
    }

    // A fetch panic reaches all callers as DedupError::Panic and frees the input,
    // so the next call refetches. Looped on virtual time for the cleanup race;
    // the printed panic is expected.
    #[tokio::test(start_paused = true)]
    async fn a_fetch_panic_reaches_callers_and_frees_the_input() {
        for _ in 0..50 {
            let calls = Arc::new(AtomicUsize::new(0));
            let d = Deduplicator::new(PanicOnce {
                calls: calls.clone(),
            });

            let (a, b) = tokio::join!(d.call(1), d.call(1));
            assert!(matches!(a, Err(DedupError::Panic)));
            assert!(matches!(b, Err(DedupError::Panic)));

            let next = d.call(1).await;
            assert_eq!(next.unwrap(), 1, "the freed input opens a fresh fetch");
            assert_eq!(calls.load(Ordering::SeqCst), 2);
        }
    }

    // Signals on fetch entry, holds for `hold`, signals on completion - lets a test
    // seat one fetch in flight before touching its waiters.
    #[derive(Clone)]
    struct Slow {
        calls: Arc<AtomicUsize>,
        completed: Arc<AtomicUsize>,
        entered: Arc<Notify>,
        finished: Arc<Notify>,
        hold: Duration,
    }

    impl Fetcher for Slow {
        type Input = u64;
        type Output = u64;
        type Error = Infallible;

        async fn load(&self, input: u64) -> Result<u64, Infallible> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            tokio::time::sleep(self.hold).await;
            self.completed.fetch_add(1, Ordering::SeqCst);
            self.finished.notify_one();
            Ok(input)
        }
    }

    fn slow(
        hold: Duration,
    ) -> (
        Slow,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Arc<Notify>,
        Arc<Notify>,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(Notify::new());
        let finished = Arc::new(Notify::new());
        let fetcher = Slow {
            calls: calls.clone(),
            completed: completed.clone(),
            entered: entered.clone(),
            finished: finished.clone(),
            hold,
        };
        (fetcher, calls, completed, entered, finished)
    }

    // The initiator's wait times out, but the started fetch is not cancelled:
    // the follower still gets the value and the fetch ran once. Auto-advance drives the clock.
    #[tokio::test(start_paused = true)]
    async fn abandoning_the_initiator_does_not_cancel_the_fetch() {
        const HOLD: Duration = Duration::from_secs(10);
        const SHORT: Duration = Duration::from_secs(1);
        for _ in 0..50 {
            let (fetcher, calls, _completed, _entered, _finished) = slow(HOLD);
            let d = Deduplicator::new(fetcher);

            let leader = tokio::time::timeout(SHORT, d.call(1));
            let follower = d.call(1);
            let (l, f) = tokio::join!(leader, follower);

            assert!(l.is_err(), "the initiator timed out and abandoned its wait");
            assert_eq!(f.unwrap(), 1, "the follower still gets the fetched value");
            assert_eq!(
                calls.load(Ordering::SeqCst),
                1,
                "the fetch ran once, uncancelled"
            );
        }
    }

    // All waiters leave after the fetch started: it still runs to completion,
    // and delivery into the closed receiver does not panic.
    #[tokio::test(start_paused = true)]
    async fn all_waiters_leaving_does_not_cancel_the_started_fetch() {
        const HOLD: Duration = Duration::from_secs(10);
        for _ in 0..50 {
            let (fetcher, calls, completed, entered, finished) = slow(HOLD);
            let d = Deduplicator::new(fetcher);

            let caller = {
                let d = d.clone();
                tokio::spawn(async move { d.call(1).await })
            };
            entered.notified().await;

            caller.abort();
            assert!(caller.await.unwrap_err().is_cancelled());

            tokio::time::advance(HOLD).await;
            finished.notified().await;

            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                completed.load(Ordering::SeqCst),
                1,
                "the fetch finished with no waiter left, without panicking"
            );
        }
    }

    // One of several waiters for an input departs; the rest still get the value
    // from the single shared fetch, and delivery into the departed receiver does
    // not panic. Auto-advance drives the virtual clock.
    #[tokio::test(start_paused = true)]
    async fn a_departed_waiter_does_not_starve_the_others() {
        const HOLD: Duration = Duration::from_secs(10);
        const SHORT: Duration = Duration::from_secs(1);
        for _ in 0..50 {
            let (fetcher, calls, _completed, _entered, _finished) = slow(HOLD);
            let d = Deduplicator::new(fetcher);

            let departing = tokio::time::timeout(SHORT, d.call(1));
            let survivor_a = d.call(1);
            let survivor_b = d.call(1);
            let (gone, a, b) = tokio::join!(departing, survivor_a, survivor_b);

            assert!(gone.is_err(), "one waiter timed out and departed");
            assert_eq!(a.unwrap(), 1, "a surviving waiter still gets the value");
            assert_eq!(b.unwrap(), 1, "the other survivor too");
            assert_eq!(calls.load(Ordering::SeqCst), 1, "one shared fetch for all");
        }
    }
}
