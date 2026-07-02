use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tokio::sync::oneshot;

use crate::collector::BatchCollector;
use crate::error::Error;

// The one-shot reply a waiting caller holds. Factored into an alias because the
// nested generics otherwise trip clippy's type-complexity lint at each use site.
type Responder<C> =
    oneshot::Sender<Result<<C as BatchCollector>::Output, Error<<C as BatchCollector>::Error>>>;

// One queued load: the input to fold into the batch plus its one-shot reply channel.
// Internal - the oneshot never appears in the public `load` signature.
pub(crate) struct Request<C: BatchCollector> {
    pub(crate) input: C::Input,
    pub(crate) respond: Responder<C>,
}

// Collapse a closed window into one downstream call, bound by `timeout`;
// hand each result back to every caller that asked for that input.
pub(crate) async fn dispatch_window<C: BatchCollector>(
    collector: C,
    batch: Vec<Request<C>>,
    timeout: Duration,
) {
    // Group waiters by input: equal inputs collapse to one downstream entry
    // and share the result. A dropped waiter is skipped,
    // so an input with no live waiter never reaches downstream.
    let mut waiters: HashMap<C::Input, Vec<Responder<C>>> = HashMap::new();
    for Request { input, respond } in batch {
        if respond.is_closed() {
            continue;
        }
        waiters.entry(input).or_default().push(respond);
    }

    // Whole window abandoned before it closed: nothing to serve, skip downstream.
    if waiters.is_empty() {
        return;
    }

    let inputs: HashSet<C::Input> = waiters.keys().cloned().collect();

    // Load on its own task so a downstream panic surfaces as a JoinError here
    // instead of unwinding the dispatcher; abort it on the deadline (a dropped
    // JoinHandle only detaches, leaving the work running).
    let load = tokio::spawn(async move { collector.load(inputs).await });
    let aborter = load.abort_handle();
    match tokio::time::timeout(timeout, load).await {
        // Deadline hit: abort the call (cancels at its next await point) and time out.
        Err(_elapsed) => {
            aborter.abort();
            deliver_to_all::<C>(waiters, Error::Timeout);
        }
        // Downstream panicked: turn the JoinError into CollectorPanic for its waiters.
        Ok(Err(panicked)) => {
            warn_collector_panicked(&panicked);
            deliver_to_all::<C>(waiters, Error::CollectorPanic);
        }
        // Downstream failed: the whole batch shares the same error.
        Ok(Ok(Err(e))) => deliver_to_all::<C>(waiters, Error::Collector(e)),
        Ok(Ok(Ok(mut response))) => {
            // An unknown input in the response taints the whole batch (implementor bug)
            // and takes precedence over any missing input.
            let unknown = response.keys().filter(|k| !waiters.contains_key(k)).count();
            if unknown > 0 {
                deliver_to_all::<C>(
                    waiters,
                    Error::ContractViolation {
                        unknown_inputs: unknown,
                    },
                );
                return;
            }

            for (input, senders) in waiters {
                match response.remove(&input) {
                    // Fan-out: each waiter gets its own clone of the shared value.
                    Some(value) => {
                        for respond in senders {
                            deliver::<C>(respond, Ok(value.clone()));
                        }
                    }
                    // Requested input absent: only its waiters get the addressed error.
                    None => {
                        for respond in senders {
                            deliver::<C>(respond, Err(Error::MissingOutput));
                        }
                    }
                }
            }
        }
    }
}

fn deliver<C: BatchCollector>(respond: Responder<C>, result: Result<C::Output, Error<C::Error>>) {
    let delivered = respond.send(result).is_ok();
    warn_if_dropped(delivered);
}

// Hand the same error to every waiter of a batch.
fn deliver_to_all<C: BatchCollector>(
    waiters: HashMap<C::Input, Vec<Responder<C>>>,
    error: Error<C::Error>,
) {
    for senders in waiters.into_values() {
        for respond in senders {
            deliver::<C>(respond, Err(error.clone()));
        }
    }
}

// Give every waiter the same error without calling downstream (batch dropped pre-dispatch).
pub(crate) fn fail_batch<C: BatchCollector>(batch: Vec<Request<C>>, error: Error<C::Error>) {
    for Request { respond, .. } in batch {
        deliver::<C>(respond, Err(error.clone()));
    }
}

// A dropped waiter is benign - the caller cancelled - but it is never silently
// ignored: when `tracing` is compiled in we surface it at warn level, otherwise
// there is no log facade to write to.
#[cfg(feature = "tracing")]
fn warn_if_dropped(delivered: bool) {
    if !delivered {
        tracing::warn!("carpool: dropped a load result because its caller went away");
    }
}

#[cfg(not(feature = "tracing"))]
fn warn_if_dropped(_delivered: bool) {}

#[cfg(feature = "tracing")]
fn warn_collector_panicked(err: &tokio::task::JoinError) {
    tracing::warn!("carpool: the collector panicked while loading a batch: {err}");
}

#[cfg(not(feature = "tracing"))]
fn warn_collector_panicked(_err: &tokio::task::JoinError) {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    // These tests use immediate collectors; the timeout never fires.
    const NO_TIMEOUT: Duration = Duration::from_secs(3600);

    #[derive(Debug, Clone)]
    struct TestError;

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("test downstream error")
        }
    }

    impl std::error::Error for TestError {}

    // What the collector does with a batch, so each contract branch can be driven.
    #[derive(Clone)]
    enum Behavior {
        Square,
        OmitInput(u64),
        InjectUnknown(u64),
        Fail,
    }

    #[derive(Clone)]
    struct TestCollector {
        calls: Arc<AtomicUsize>,
        batch_len: Arc<AtomicUsize>,
        behavior: Behavior,
    }

    impl BatchCollector for TestCollector {
        type Input = u64;
        type Output = u64;
        type Error = TestError;

        async fn load(&self, batch: HashSet<u64>) -> Result<HashMap<u64, u64>, TestError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.batch_len.store(batch.len(), Ordering::SeqCst);
            let squared = || batch.iter().map(|&x| (x, x * x));
            match self.behavior {
                Behavior::Square => Ok(squared().collect()),
                Behavior::OmitInput(drop) => Ok(squared().filter(|(x, _)| *x != drop).collect()),
                Behavior::InjectUnknown(extra) => {
                    let mut out: HashMap<u64, u64> = squared().collect();
                    out.insert(extra, 0);
                    Ok(out)
                }
                Behavior::Fail => Err(TestError),
            }
        }
    }

    type Reply = oneshot::Receiver<Result<u64, Error<TestError>>>;

    fn req(input: u64) -> (Request<TestCollector>, Reply) {
        let (tx, rx) = oneshot::channel();
        (Request { input, respond: tx }, rx)
    }

    fn collector(behavior: Behavior) -> (TestCollector, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let batch_len = Arc::new(AtomicUsize::new(0));
        let collector = TestCollector {
            calls: calls.clone(),
            batch_len: batch_len.clone(),
            behavior,
        };
        (collector, calls, batch_len)
    }

    #[tokio::test]
    async fn duplicate_inputs_collapse_to_one_downstream_entry() {
        let (collector, calls, batch_len) = collector(Behavior::Square);
        let (a, rx_a) = req(1);
        let (b, rx_b) = req(1);
        let (c, rx_c) = req(2);

        dispatch_window(collector, vec![a, b, c], NO_TIMEOUT).await;

        assert_eq!(calls.load(Ordering::SeqCst), 1, "one downstream call");
        assert_eq!(
            batch_len.load(Ordering::SeqCst),
            2,
            "three requests, two unique inputs"
        );
        assert_eq!(rx_a.await.unwrap().unwrap(), 1);
        assert_eq!(rx_b.await.unwrap().unwrap(), 1);
        assert_eq!(rx_c.await.unwrap().unwrap(), 4);
    }

    #[tokio::test]
    async fn missing_input_is_addressed_only_to_its_waiter() {
        let (collector, _calls, _len) = collector(Behavior::OmitInput(2));
        let (a, rx_a) = req(1);
        let (b, rx_b) = req(2);

        dispatch_window(collector, vec![a, b], NO_TIMEOUT).await;

        assert_eq!(rx_a.await.unwrap().unwrap(), 1);
        assert!(matches!(rx_b.await.unwrap(), Err(Error::MissingOutput)));
    }

    // The unknown input taints the whole batch, so rx_a fails despite a correct answer.
    #[tokio::test]
    async fn unknown_input_fails_the_whole_batch() {
        let (collector, _calls, _len) = collector(Behavior::InjectUnknown(99));
        let (a, rx_a) = req(1);
        let (b, rx_b) = req(2);

        dispatch_window(collector, vec![a, b], NO_TIMEOUT).await;

        assert!(matches!(
            rx_a.await.unwrap(),
            Err(Error::ContractViolation { unknown_inputs: 1 })
        ));
        assert!(matches!(
            rx_b.await.unwrap(),
            Err(Error::ContractViolation { unknown_inputs: 1 })
        ));
    }

    #[tokio::test]
    async fn collector_error_reaches_every_waiter() {
        let (collector, _calls, _len) = collector(Behavior::Fail);
        let (a, rx_a) = req(1);
        let (b, rx_b) = req(2);

        dispatch_window(collector, vec![a, b], NO_TIMEOUT).await;

        assert!(matches!(rx_a.await.unwrap(), Err(Error::Collector(_))));
        assert!(matches!(rx_b.await.unwrap(), Err(Error::Collector(_))));
    }

    #[tokio::test]
    async fn abandoned_input_is_not_sent_downstream() {
        let (collector, calls, batch_len) = collector(Behavior::Square);
        let (a, rx_a) = req(1);
        let (b, rx_b) = req(2);
        drop(rx_a); // input 1's caller cancelled

        dispatch_window(collector, vec![a, b], NO_TIMEOUT).await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            batch_len.load(Ordering::SeqCst),
            1,
            "only the live input reaches downstream"
        );
        assert_eq!(rx_b.await.unwrap().unwrap(), 4);
    }

    #[tokio::test]
    async fn all_waiters_gone_skips_downstream() {
        let (collector, calls, _len) = collector(Behavior::Square);
        let (a, rx_a) = req(1);
        let (b, rx_b) = req(2);
        drop(rx_a);
        drop(rx_b);

        dispatch_window(collector, vec![a, b], NO_TIMEOUT).await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "no live waiter -> downstream skipped"
        );
    }

    #[tokio::test]
    async fn a_dropped_waiter_does_not_starve_its_inputs_survivor() {
        let (collector, calls, _len) = collector(Behavior::Square);
        let (a, rx_a) = req(1);
        let (b, rx_b) = req(1); // same input
        drop(rx_a);

        dispatch_window(collector, vec![a, b], NO_TIMEOUT).await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            rx_b.await.unwrap().unwrap(),
            1,
            "the surviving waiter still gets its value"
        );
    }
}
