use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use crate::collector::BatchCollector;
use crate::config::BatchLoaderConfig;
use crate::dispatch::Request;
use crate::error::Error;
use crate::limiter::Slots;
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
        let slots = Slots::from_config(&config);

        // Both tasks are detached: they end on channel close once every loader
        // clone is dropped, so there is nothing to join.
        tokio::spawn(window::collect(
            requests,
            windows_tx,
            config.window,
            config.max_batch_size,
        ));
        tokio::spawn(run_dispatcher(
            collector.clone(),
            windows_rx,
            slots,
            config.timeout,
        ));

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
            // Responder dropped without an answer: the dispatcher is gone
            // (every loader clone dropped, channels closed), so we cannot serve.
            Err(_) => Err(Error::Closed),
        }
    }
}

async fn run_dispatcher<C: BatchCollector>(
    collector: C,
    mut windows: mpsc::Receiver<Vec<Request<C>>>,
    slots: Slots,
    timeout: Duration,
) {
    // One task per window for parallel batches, capped by Slots. The set drains
    // in-flight batches when the source closes; report_batch is a panic backstop
    // (downstream panics are handled in dispatch).
    let mut batches = JoinSet::new();
    loop {
        tokio::select! {
            window = windows.recv() => match window {
                Some(batch) => {
                    let collector = collector.clone();
                    let slots = slots.clone();
                    batches.spawn(async move { slots.run(collector, batch, timeout).await });
                }
                None => break,
            },
            Some(outcome) = batches.join_next() => report_batch(outcome),
        }
    }
    while let Some(outcome) = batches.join_next().await {
        report_batch(outcome);
    }
}

// Results go home over the oneshot; only an abnormal end (a panic) needs surfacing.
fn report_batch(outcome: Result<(), tokio::task::JoinError>) {
    if let Err(panicked) = outcome {
        warn_batch_panicked(&panicked);
    }
}

#[cfg(feature = "tracing")]
fn warn_batch_panicked(err: &tokio::task::JoinError) {
    tracing::warn!("carpool: a batch task ended abnormally: {err}");
}

#[cfg(not(feature = "tracing"))]
fn warn_batch_panicked(_err: &tokio::task::JoinError) {}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::sync::Notify;

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
            .map(|k| spawn_load_on(&loader, k))
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
            .map(|k| spawn_load_on(&loader, k))
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

        let first = spawn_load_on(&loader, 5)
            .await
            .expect("task joins")
            .expect("load succeeds");
        assert_eq!(first, 25);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let second = spawn_load_on(&loader, 6)
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

        let value = spawn_load_on(&loader, 7)
            .await
            .expect("task joins")
            .expect("load succeeds");

        assert_eq!(value, 49);
    }

    fn spawn_load_on<C: BatchCollector>(
        loader: &BatchLoader<C>,
        input: C::Input,
    ) -> tokio::task::JoinHandle<Result<C::Output, Error<C::Error>>> {
        let loader = loader.clone();
        tokio::spawn(async move { loader.load(input).await })
    }

    // Records the concurrent-load peak and holds each load for `hold` of virtual
    // time, so a test can read off how many batches ran downstream at once.
    #[derive(Clone)]
    struct Tracked {
        calls: Arc<AtomicUsize>,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
        hold: Duration,
    }

    impl BatchCollector for Tracked {
        type Input = u64;
        type Output = u64;
        type Key = u64;
        type Error = std::convert::Infallible;

        fn key(&self, input: &u64) -> u64 {
            *input
        }

        async fn load(&self, batch: HashMap<u64, u64>) -> Result<HashMap<u64, u64>, Self::Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(self.hold).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(batch)
        }
    }

    // limit slots, one item per window (each load is its own batch, so they
    // contend for slots), timer long enough never to fire under virtual time.
    fn limited_loader(
        limit: usize,
        hold: Duration,
    ) -> (BatchLoader<Tracked>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let collector = Tracked {
            calls: calls.clone(),
            in_flight: Arc::new(AtomicUsize::new(0)),
            max_in_flight: max_in_flight.clone(),
            hold,
        };
        let config = BatchLoaderConfig {
            window: Duration::from_secs(3600),
            max_batch_size: NonZeroUsize::new(1).expect("1 is non-zero"),
            concurrency_limit: NonZeroUsize::new(limit),
            ..BatchLoaderConfig::default()
        };
        (BatchLoader::spawn(collector, config), calls, max_in_flight)
    }

    // Limit honored under contention and the queue fully drains. With limit 2 and
    // six contending batches (a four-deep wait queue at the peak) the in-flight
    // count reaches exactly 2 - both slots used, never a third - and every load
    // still completes, so no batch is left waiting while a slot is free. Looped to
    // surface any queue race the virtual clock would otherwise hide.
    #[tokio::test(start_paused = true)]
    async fn concurrency_limit_caps_in_flight_and_queue_drains() {
        const LIMIT: usize = 2;
        const BATCHES: u64 = 6;
        for _ in 0..50 {
            let (loader, calls, max_in_flight) = limited_loader(LIMIT, Duration::from_secs(10));

            let handles: Vec<_> = (1..=BATCHES).map(|k| spawn_load_on(&loader, k)).collect();
            for handle in handles {
                handle.await.expect("task joins").expect("load succeeds");
            }

            assert_eq!(
                calls.load(Ordering::SeqCst),
                BATCHES as usize,
                "every batch reached downstream"
            );
            assert_eq!(
                max_in_flight.load(Ordering::SeqCst),
                LIMIT,
                "in-flight peak equals the limit, never exceeds it"
            );
        }
    }

    // Panics on its first downstream call, succeeds on every later one.
    #[derive(Clone)]
    struct PanicOnce {
        calls: Arc<AtomicUsize>,
    }

    impl BatchCollector for PanicOnce {
        type Input = u64;
        type Output = u64;
        type Key = u64;
        type Error = std::convert::Infallible;

        fn key(&self, input: &u64) -> u64 {
            *input
        }

        async fn load(&self, batch: HashMap<u64, u64>) -> Result<HashMap<u64, u64>, Self::Error> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("downstream blew up");
            }
            Ok(batch)
        }
    }

    // A downstream panic reaches its waiter as CollectorPanic and frees the slot, so
    // the next batch runs. Looped on virtual time to surface a race; the printed
    // panic message is expected.
    #[tokio::test(start_paused = true)]
    async fn a_panicking_batch_reports_panic_and_frees_its_slot() {
        for _ in 0..50 {
            let calls = Arc::new(AtomicUsize::new(0));
            let config = BatchLoaderConfig {
                window: Duration::from_secs(3600),
                max_batch_size: NonZeroUsize::new(1).expect("1 is non-zero"),
                concurrency_limit: NonZeroUsize::new(1),
                ..BatchLoaderConfig::default()
            };
            let loader = BatchLoader::spawn(
                PanicOnce {
                    calls: calls.clone(),
                },
                config,
            );

            let first = spawn_load_on(&loader, 1).await.expect("task joins");
            assert!(
                matches!(first, Err(Error::CollectorPanic)),
                "the panicking batch reports a panic to its waiter"
            );

            let second = spawn_load_on(&loader, 2)
                .await
                .expect("task joins")
                .expect("load succeeds");
            assert_eq!(second, 2, "the next batch got the freed slot");
            assert_eq!(
                calls.load(Ordering::SeqCst),
                2,
                "both batches reached downstream"
            );
        }
    }

    // Signals when a load enters downstream, then holds the slot for `hold` of
    // virtual time - lets a test seat one batch in the slot before queuing another.
    #[derive(Clone)]
    struct Holder {
        calls: Arc<AtomicUsize>,
        entered: Arc<Notify>,
        hold: Duration,
    }

    impl BatchCollector for Holder {
        type Input = u64;
        type Output = u64;
        type Key = u64;
        type Error = std::convert::Infallible;

        fn key(&self, input: &u64) -> u64 {
            *input
        }

        async fn load(&self, batch: HashMap<u64, u64>) -> Result<HashMap<u64, u64>, Self::Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            tokio::time::sleep(self.hold).await;
            Ok(batch)
        }
    }

    // A batch that cannot get a slot within max_waiting is dropped with a wait
    // error and never reaches downstream. With a single slot held past the limit,
    // the queued batch times out; the call count proves downstream ran once (the
    // holder), not for the timed-out batch. Looped to surface any queue race.
    #[tokio::test(start_paused = true)]
    async fn a_batch_that_waits_past_max_waiting_fails_without_downstream() {
        for _ in 0..50 {
            let calls = Arc::new(AtomicUsize::new(0));
            let entered = Arc::new(Notify::new());
            let collector = Holder {
                calls: calls.clone(),
                entered: entered.clone(),
                hold: Duration::from_secs(3600),
            };
            let config = BatchLoaderConfig {
                window: Duration::from_secs(3600),
                max_batch_size: NonZeroUsize::new(1).expect("1 is non-zero"),
                concurrency_limit: NonZeroUsize::new(1),
                max_waiting: Some(Duration::from_secs(5)),
                ..BatchLoaderConfig::default()
            };
            let loader = BatchLoader::spawn(collector, config);

            // First batch takes the only slot and holds it well past the limit.
            let holder = spawn_load_on(&loader, 1);
            entered.notified().await;

            // Second batch queues for the slot and never gets it within max_waiting.
            let waited = spawn_load_on(&loader, 2).await.expect("task joins");
            assert!(
                matches!(waited, Err(Error::WaitingTimeout)),
                "the queued batch times out waiting for a slot"
            );
            assert_eq!(
                calls.load(Ordering::SeqCst),
                1,
                "downstream ran for the holder only, not the timed-out batch"
            );

            holder.abort();
        }
    }

    // max_waiting without a concurrency limit is a soft no-op: loads still run and
    // succeed, no panic.
    #[tokio::test(start_paused = true)]
    async fn max_waiting_without_a_limit_is_a_soft_no_op() {
        let calls = Arc::new(AtomicUsize::new(0));
        let config = BatchLoaderConfig {
            window: Duration::from_secs(3600),
            max_batch_size: NonZeroUsize::new(1).expect("1 is non-zero"),
            concurrency_limit: None,
            max_waiting: Some(Duration::from_secs(5)),
            ..BatchLoaderConfig::default()
        };
        let loader = BatchLoader::spawn(
            Squares {
                calls: calls.clone(),
            },
            config,
        );

        let value = spawn_load_on(&loader, 6)
            .await
            .expect("task joins")
            .expect("load succeeds");

        assert_eq!(value, 36);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    // Sleeps past the deadline on its first downstream call, returns at once after.
    #[derive(Clone)]
    struct SlowOnce {
        calls: Arc<AtomicUsize>,
        slow: Duration,
    }

    impl BatchCollector for SlowOnce {
        type Input = u64;
        type Output = u64;
        type Key = u64;
        type Error = std::convert::Infallible;

        fn key(&self, input: &u64) -> u64 {
            *input
        }

        async fn load(&self, batch: HashMap<u64, u64>) -> Result<HashMap<u64, u64>, Self::Error> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                tokio::time::sleep(self.slow).await;
            }
            Ok(batch)
        }
    }

    // Looped on virtual time to surface a cleanup race: a timed-out batch must not
    // strand in-flight for the next.
    #[tokio::test(start_paused = true)]
    async fn a_batch_slower_than_the_timeout_fails_and_the_next_passes() {
        for _ in 0..50 {
            let calls = Arc::new(AtomicUsize::new(0));
            let config = BatchLoaderConfig {
                window: Duration::from_secs(3600),
                max_batch_size: NonZeroUsize::new(1).expect("1 is non-zero"),
                timeout: Duration::from_secs(5),
                ..BatchLoaderConfig::default()
            };
            let loader = BatchLoader::spawn(
                SlowOnce {
                    calls: calls.clone(),
                    slow: Duration::from_secs(3600),
                },
                config,
            );

            let timed_out = spawn_load_on(&loader, 1).await.expect("task joins");
            assert!(
                matches!(timed_out, Err(Error::Timeout)),
                "the slow batch times out"
            );

            let next = spawn_load_on(&loader, 2)
                .await
                .expect("task joins")
                .expect("load succeeds");
            assert_eq!(next, 2, "the next batch runs after the timeout");
            assert_eq!(
                calls.load(Ordering::SeqCst),
                2,
                "downstream was entered for both batches"
            );
        }
    }

    // Cancelling one waiter mid-flight must not cancel downstream or strand the slot:
    // the abandoned batch still runs, its result is discarded without a panic,
    // and the freed slot serves the next batch. Looped on virtual time for races.
    #[tokio::test(start_paused = true)]
    async fn cancelling_one_load_leaves_the_others_and_the_slot_clean() {
        const HOLD: Duration = Duration::from_secs(10);
        for _ in 0..50 {
            let calls = Arc::new(AtomicUsize::new(0));
            let entered = Arc::new(Notify::new());
            let config = BatchLoaderConfig {
                window: Duration::from_secs(3600),
                max_batch_size: NonZeroUsize::new(1).expect("1 is non-zero"),
                concurrency_limit: NonZeroUsize::new(1),
                ..BatchLoaderConfig::default()
            };
            let loader = BatchLoader::spawn(
                Holder {
                    calls: calls.clone(),
                    entered: entered.clone(),
                    hold: HOLD,
                },
                config,
            );

            // First batch enters downstream and holds the only slot.
            let h1 = spawn_load_on(&loader, 1);
            entered.notified().await;

            // Cancel its waiter: downstream is not aborted, the result is discarded.
            h1.abort();
            assert!(h1.await.unwrap_err().is_cancelled());

            // Finishing the abandoned batch must not panic on its dropped receiver;
            // the freed slot then serves the next batch.
            tokio::time::advance(HOLD).await;
            let h2 = spawn_load_on(&loader, 2);
            entered.notified().await;
            tokio::time::advance(HOLD).await;
            let value = h2.await.expect("task joins").expect("load succeeds");

            assert_eq!(value, 2, "the next batch got the freed slot");
            assert_eq!(
                calls.load(Ordering::SeqCst),
                2,
                "downstream ran for both - the cancelled batch was not aborted"
            );
        }
    }

    #[derive(Debug, Clone)]
    struct Boom;

    impl std::fmt::Display for Boom {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("boom")
        }
    }

    impl std::error::Error for Boom {}

    // How MisbehaveOnce breaks its first call:
    // a downstream error or a contract violation (an unknown key in the response).
    #[derive(Clone, Copy)]
    enum Misbehavior {
        Error,
        Contract,
    }

    // Breaks its first downstream call the chosen way, then returns identity.
    #[derive(Clone)]
    struct MisbehaveOnce {
        calls: Arc<AtomicUsize>,
        how: Misbehavior,
    }

    impl BatchCollector for MisbehaveOnce {
        type Input = u64;
        type Output = u64;
        type Key = u64;
        type Error = Boom;

        fn key(&self, input: &u64) -> u64 {
            *input
        }

        async fn load(&self, batch: HashMap<u64, u64>) -> Result<HashMap<u64, u64>, Boom> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                match self.how {
                    Misbehavior::Error => return Err(Boom),
                    Misbehavior::Contract => {
                        let mut out = batch;
                        out.insert(u64::MAX, 0); // key never requested -> ContractViolation
                        return Ok(out);
                    }
                }
            }
            Ok(batch)
        }
    }

    // The error and contract paths free the slot like every other return:
    // after either failure the next batch reaches downstream and succeeds.
    // Contract has its own early return, so it is exercised explicitly.
    #[tokio::test(start_paused = true)]
    async fn failure_paths_free_the_slot_for_the_next() {
        for how in [Misbehavior::Error, Misbehavior::Contract] {
            for _ in 0..50 {
                let calls = Arc::new(AtomicUsize::new(0));
                let config = BatchLoaderConfig {
                    window: Duration::from_secs(3600),
                    max_batch_size: NonZeroUsize::new(1).expect("1 is non-zero"),
                    concurrency_limit: NonZeroUsize::new(1),
                    ..BatchLoaderConfig::default()
                };
                let loader = BatchLoader::spawn(
                    MisbehaveOnce {
                        calls: calls.clone(),
                        how,
                    },
                    config,
                );

                let failed = spawn_load_on(&loader, 1).await.expect("task joins");
                match how {
                    Misbehavior::Error => {
                        assert!(matches!(failed, Err(Error::Collector(_))));
                    }
                    Misbehavior::Contract => {
                        assert!(matches!(failed, Err(Error::ContractViolation { .. })));
                    }
                }

                let next = spawn_load_on(&loader, 2)
                    .await
                    .expect("task joins")
                    .expect("load succeeds");
                assert_eq!(next, 2, "the next batch got the freed slot");
                assert_eq!(calls.load(Ordering::SeqCst), 2);
            }
        }
    }
}
