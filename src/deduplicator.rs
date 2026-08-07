use std::collections::HashMap;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::Poll;

use tokio::sync::watch;

use crate::error::DedupError;
use crate::fetcher::Fetcher;

type Outcome<F> = Result<<F as Fetcher>::Output, DedupError<<F as Fetcher>::Error>>;
type Entry<F> = Arc<watch::Sender<Option<Outcome<F>>>>;
type Entries<F> = HashMap<<F as Fetcher>::Input, Entry<F>>;
type Registry<F> = Arc<Mutex<Entries<F>>>;

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

        // Armed before anything fallible runs: a panic here still has to clear the entry.
        let checkout = Checkout::new(self.registry.clone(), for_flight);
        tokio::spawn(fly(self.fetcher.clone(), checkout, entry));
        waiter
    }
}

struct Checkout<F: Fetcher> {
    registry: Registry<F>,
    input: F::Input,
    armed: bool,
}

impl<F: Fetcher> Checkout<F> {
    fn new(registry: Registry<F>, input: F::Input) -> Self {
        Self {
            registry,
            input,
            armed: true,
        }
    }

    // Idempotent: a second check-out would take whichever flight owns the input by then.
    #[must_use = "drop the checked-out entry after releasing the lock"]
    fn check_out_in(&mut self, entries: &mut Entries<F>) -> Option<(F::Input, Entry<F>)> {
        if !self.armed {
            return None;
        }
        self.armed = false;
        entries.remove_entry(&self.input)
    }

    // The re-check and the removal share one critical section: the lock is not reentrant,
    // and letting go in between lets a waiter subscribe to an entry that is about to go.
    fn check_out_if_empty(&mut self, entry: &Entry<F>) -> bool {
        let registry = self.registry.clone();
        let mut entries = locked(&registry);
        if entry.receiver_count() > 0 {
            return false;
        }
        let checked_out = self.check_out_in(&mut entries);
        drop(entries);
        drop(checked_out);
        true
    }

    fn check_out(&mut self) {
        // Nothing to take, so the lock stays untouched.
        if !self.armed {
            return;
        }
        // The guard has to borrow the Arc, not `self`.
        let registry = self.registry.clone();
        // Named, so the pair outlives the guard.
        let checked_out = self.check_out_in(&mut locked(&registry));
        drop(checked_out);
    }
}

impl<F: Fetcher> Drop for Checkout<F> {
    fn drop(&mut self) {
        self.check_out();
    }
}

async fn fly<F: Fetcher>(fetcher: F, mut checkout: Checkout<F>, entry: Entry<F>) {
    let mut fetch = pin!(fetcher.load(checkout.input.clone()));
    let mut emptied = pin!(entry.closed());

    let outcome = loop {
        // The fetch wins a tie: an outcome already in hand
        // beats an emptiness that arrived with it.
        let fetched = poll_fn(|cx| match fetch.as_mut().poll(cx) {
            Poll::Ready(outcome) => Poll::Ready(Some(outcome)),
            Poll::Pending => emptied.as_mut().poll(cx).map(|()| None),
        })
        .await;

        match fetched {
            Some(outcome) => break outcome.map_err(DedupError::Load),
            // Nobody is left to pay for the fetch: it dies with the task.
            None if checkout.check_out_if_empty(&entry) => return,
            // Someone joined instead;
            // a fresh wait re-reads the count rather than trusting the spent signal.
            None => emptied.set(entry.closed()),
        }
    };

    // Checking out before sending frees the input for the next call;
    // nothing may yield in between, or a waiter is left without an outcome.
    checkout.check_out();
    entry.send_replace(Some(outcome));
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::error::Error;
    use std::fmt;
    use std::future::Future;
    use std::pin::{Pin, pin};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Waker};
    use std::time::Duration;

    use tokio::sync::{Barrier, Notify};
    use tokio::time::timeout;

    use super::locked;
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

    #[derive(Clone)]
    struct Panicking {
        fetches: Arc<AtomicUsize>,
    }

    impl Fetcher for Panicking {
        type Input = u64;
        type Output = u64;
        type Error = Infallible;

        async fn load(&self, _input: u64) -> Result<u64, Infallible> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            panic!("the fetch blows up")
        }
    }

    fn panicking() -> (Panicking, Arc<AtomicUsize>) {
        let fetches = Arc::new(AtomicUsize::new(0));
        (
            Panicking {
                fetches: fetches.clone(),
            },
            fetches,
        )
    }

    // Holds one flight inside the fetch, so other calls can join it,
    // and tells apart a fetch that ran to the end from one dropped midway.
    #[derive(Clone)]
    struct Holding {
        fetches: Arc<AtomicUsize>,
        entered: Arc<Notify>,
        gate: Arc<Notify>,
        completed: Arc<AtomicBool>,
        cancelled: Arc<Notify>,
    }

    impl Holding {
        fn new() -> Self {
            Self {
                fetches: Arc::new(AtomicUsize::new(0)),
                entered: Arc::new(Notify::new()),
                gate: Arc::new(Notify::new()),
                completed: Arc::new(AtomicBool::new(false)),
                cancelled: Arc::new(Notify::new()),
            }
        }
    }

    impl Fetcher for Holding {
        type Input = u64;
        type Output = u64;
        type Error = Infallible;

        async fn load(&self, input: u64) -> Result<u64, Infallible> {
            let mut fetch = Interrupted::new(self.cancelled.clone());
            self.fetches.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.gate.notified().await;
            self.completed.store(true, Ordering::SeqCst);
            fetch.complete();
            Ok(input * input)
        }
    }

    // Signals on drop unless what it watches ran to the end.
    struct Interrupted {
        signal: Arc<Notify>,
        done: bool,
    }

    impl Interrupted {
        fn new(signal: Arc<Notify>) -> Self {
            Self {
                signal,
                done: false,
            }
        }

        fn complete(&mut self) {
            self.done = true;
        }
    }

    impl Drop for Interrupted {
        fn drop(&mut self) {
            if !self.done {
                self.signal.notify_one();
            }
        }
    }

    // A fetcher that spawns its own task, so folding the flight cannot cut the fetch short.
    #[derive(Clone)]
    struct Spawning {
        inner: Holding,
        finished: Arc<Notify>,
        folded: Arc<Notify>,
    }

    impl Fetcher for Spawning {
        type Input = u64;
        type Output = u64;
        type Error = Infallible;

        async fn load(&self, input: u64) -> Result<u64, Infallible> {
            let inner = self.inner.clone();
            let finished = self.finished.clone();
            let fetch = tokio::spawn(async move {
                let outcome = inner.load(input).await;
                finished.notify_one();
                outcome
            });

            let mut flight = Interrupted::new(self.folded.clone());
            let outcome = fetch.await.expect("the spawned fetch must not panic");
            flight.complete();
            outcome
        }
    }

    // One poll is all a call needs to subscribe.
    fn poll_once<T>(call: Pin<&mut impl Future<Output = T>>) {
        let mut cx = Context::from_waker(Waker::noop());
        assert!(
            call.poll(&mut cx).is_pending(),
            "a call cannot be ready before the flight settles"
        );
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
        assert!(locked(&d.registry).is_empty());
    }

    #[tokio::test]
    async fn a_lost_flight_reaches_the_waiter_and_frees_the_input() {
        let (fetcher, fetches) = panicking();
        let d = Deduplicator::new(fetcher);

        let first = timeout(GUARD, d.call(7))
            .await
            .expect("a lost flight must not hang its waiter");
        let second = timeout(GUARD, d.call(7))
            .await
            .expect("the input must be free again");

        assert!(matches!(first, Err(DedupError::Lost)));
        assert!(matches!(second, Err(DedupError::Lost)));
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
        assert!(locked(&d.registry).is_empty());
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
        assert!(locked(&d.registry).is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn clones_share_one_flight() {
        let fetcher = Holding::new();
        let d = Deduplicator::new(fetcher.clone());

        let leader = tokio::spawn({
            let d = d.clone();
            async move { d.call(7).await }
        });
        timeout(GUARD, fetcher.entered.notified())
            .await
            .expect("the leader must reach the fetcher");

        // The follower subscribes in the first branch;
        // the flight is released only by the second, so it cannot settle before the join.
        let (follower, ()) = timeout(GUARD, async {
            tokio::join!(d.call(7), async { fetcher.gate.notify_one() })
        })
        .await
        .expect("the follower must join the open flight");

        assert_eq!(follower.unwrap(), 49);
        let leader = timeout(GUARD, leader)
            .await
            .expect("the leader must resolve too")
            .unwrap();
        assert_eq!(leader.unwrap(), 49);
        assert_eq!(fetcher.fetches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_leaving_waiter_does_not_fold_the_flight_for_the_rest() {
        let fetcher = Holding::new();
        let d = Deduplicator::new(fetcher.clone());

        let stays = tokio::spawn({
            let d = d.clone();
            async move { d.call(7).await }
        });
        timeout(GUARD, fetcher.entered.notified())
            .await
            .expect("the flight must reach the fetcher");

        {
            let mut leaves = pin!(d.call(7));
            poll_once(leaves.as_mut());
        }

        fetcher.gate.notify_one();
        let stayed = timeout(GUARD, stays)
            .await
            .expect("the remaining caller must not hang")
            .unwrap();

        assert_eq!(stayed.unwrap(), 49);
        assert!(fetcher.completed.load(Ordering::SeqCst));
        assert_eq!(fetcher.fetches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_flight_folds_when_the_last_waiter_leaves() {
        let fetcher = Holding::new();
        let d = Deduplicator::new(fetcher.clone());

        {
            let mut only = pin!(d.call(7));
            poll_once(only.as_mut());
            timeout(GUARD, fetcher.entered.notified())
                .await
                .expect("the flight must reach the fetcher");
        }

        timeout(GUARD, fetcher.cancelled.notified())
            .await
            .expect("the fetch must be dropped where it stands");

        assert!(!fetcher.completed.load(Ordering::SeqCst));
        assert!(locked(&d.registry).is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_input_is_free_after_a_folded_flight() {
        let fetcher = Holding::new();
        let d = Deduplicator::new(fetcher.clone());

        {
            let mut only = pin!(d.call(7));
            poll_once(only.as_mut());
            timeout(GUARD, fetcher.entered.notified())
                .await
                .expect("the flight must reach the fetcher");
        }
        timeout(GUARD, fetcher.cancelled.notified())
            .await
            .expect("the flight must fold");

        fetcher.gate.notify_one();
        let value = timeout(GUARD, d.call(7))
            .await
            .expect("the input must be free again");

        assert_eq!(value.unwrap(), 49);
        assert_eq!(fetcher.fetches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_fetch_of_its_own_task_outlives_the_fold() {
        let inner = Holding::new();
        let finished = Arc::new(Notify::new());
        let folded = Arc::new(Notify::new());
        let d = Deduplicator::new(Spawning {
            inner: inner.clone(),
            finished: finished.clone(),
            folded: folded.clone(),
        });

        {
            let mut only = pin!(d.call(7));
            poll_once(only.as_mut());
            timeout(GUARD, inner.entered.notified())
                .await
                .expect("the flight must reach the fetcher");
        }
        timeout(GUARD, folded.notified())
            .await
            .expect("the flight must fold");

        inner.gate.notify_one();
        timeout(GUARD, finished.notified())
            .await
            .expect("a fetch of its own task must run to the end");

        assert!(inner.completed.load(Ordering::SeqCst));
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
