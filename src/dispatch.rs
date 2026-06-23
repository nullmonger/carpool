use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::oneshot;

use crate::collector::BatchCollector;
use crate::error::Error;

// The one-shot reply a waiting caller holds. Factored into an alias because the
// nested generics otherwise trip clippy's type-complexity lint at each use site.
type Responder<C> =
    oneshot::Sender<Result<<C as BatchCollector>::Output, Error<<C as BatchCollector>::Error>>>;

// One queued load: its key, the input to fold into the batch, and the one-shot
// channel that carries the result back to the waiting caller. Stays internal -
// the oneshot never appears in the public `load` signature.
pub(crate) struct Request<C: BatchCollector> {
    pub(crate) key: C::Key,
    pub(crate) input: C::Input,
    pub(crate) respond: Responder<C>,
}

// Collapse a closed window into one downstream call and hand each result back to
// every caller that asked for that key. The call is bound by `timeout`.
pub(crate) async fn dispatch_window<C: BatchCollector>(
    collector: C,
    batch: Vec<Request<C>>,
    timeout: Duration,
) {
    // Dedup: one representative input per key for downstream, and every waiter
    // grouped under its key so the result reaches all of them. Addressing is by
    // key, never by position - inputs sharing a key are interchangeable.
    let mut inputs: HashMap<C::Key, C::Input> = HashMap::new();
    let mut waiters: HashMap<C::Key, Vec<Responder<C>>> = HashMap::new();
    for Request {
        key,
        input,
        respond,
    } in batch
    {
        waiters.entry(key.clone()).or_default().push(respond);
        inputs.entry(key).or_insert(input);
    }

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
            // An unknown key in the response is an implementor bug that taints the
            // whole batch and takes precedence over any missing key.
            let unknown = response.keys().filter(|k| !waiters.contains_key(k)).count();
            if unknown > 0 {
                deliver_to_all::<C>(
                    waiters,
                    Error::ContractViolation {
                        unknown_keys: unknown,
                    },
                );
                return;
            }

            for (key, senders) in waiters {
                match response.remove(&key) {
                    // Fan-out: each waiter gets its own clone of the shared value.
                    Some(value) => {
                        for respond in senders {
                            deliver::<C>(respond, Ok(value.clone()));
                        }
                    }
                    // Requested key absent: only its waiters get the addressed error.
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
    waiters: HashMap<C::Key, Vec<Responder<C>>>,
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
        OmitKey(u64),
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
        type Key = u64;
        type Error = TestError;

        fn key(&self, input: &u64) -> u64 {
            *input
        }

        async fn load(&self, batch: HashMap<u64, u64>) -> Result<HashMap<u64, u64>, TestError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.batch_len.store(batch.len(), Ordering::SeqCst);
            let squared = || batch.iter().map(|(k, v)| (*k, v * v));
            match self.behavior {
                Behavior::Square => Ok(squared().collect()),
                Behavior::OmitKey(drop) => Ok(squared().filter(|(k, _)| *k != drop).collect()),
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

    fn req(key: u64, input: u64) -> (Request<TestCollector>, Reply) {
        let (tx, rx) = oneshot::channel();
        (
            Request {
                key,
                input,
                respond: tx,
            },
            rx,
        )
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

    // Dedup: duplicate keys collapse to one downstream input and one call; both
    // waiters of a key receive the shared value.
    #[tokio::test]
    async fn duplicate_keys_collapse_to_one_input_per_key() {
        let (collector, calls, batch_len) = collector(Behavior::Square);
        let (a, rx_a) = req(1, 1);
        let (b, rx_b) = req(1, 1);
        let (c, rx_c) = req(2, 2);

        dispatch_window(collector, vec![a, b, c], NO_TIMEOUT).await;

        assert_eq!(calls.load(Ordering::SeqCst), 1, "one downstream call");
        assert_eq!(
            batch_len.load(Ordering::SeqCst),
            2,
            "three requests, two unique keys"
        );
        assert_eq!(rx_a.await.unwrap().unwrap(), 1);
        assert_eq!(rx_b.await.unwrap().unwrap(), 1);
        assert_eq!(rx_c.await.unwrap().unwrap(), 4);
    }

    // Absent requested key: only its waiter gets the addressed MissingOutput,
    // the others still get their values.
    #[tokio::test]
    async fn missing_key_is_addressed_only_to_its_waiter() {
        let (collector, _calls, _len) = collector(Behavior::OmitKey(2));
        let (a, rx_a) = req(1, 1);
        let (b, rx_b) = req(2, 2);

        dispatch_window(collector, vec![a, b], NO_TIMEOUT).await;

        assert_eq!(rx_a.await.unwrap().unwrap(), 1);
        assert!(matches!(rx_b.await.unwrap(), Err(Error::MissingOutput)));
    }

    // Unknown key in the response fails the whole batch, even the waiters whose
    // keys were answered correctly.
    #[tokio::test]
    async fn unknown_key_fails_the_whole_batch() {
        let (collector, _calls, _len) = collector(Behavior::InjectUnknown(99));
        let (a, rx_a) = req(1, 1);
        let (b, rx_b) = req(2, 2);

        dispatch_window(collector, vec![a, b], NO_TIMEOUT).await;

        assert!(matches!(
            rx_a.await.unwrap(),
            Err(Error::ContractViolation { unknown_keys: 1 })
        ));
        assert!(matches!(
            rx_b.await.unwrap(),
            Err(Error::ContractViolation { unknown_keys: 1 })
        ));
    }

    // Downstream error reaches every waiter in the batch.
    #[tokio::test]
    async fn collector_error_reaches_every_waiter() {
        let (collector, _calls, _len) = collector(Behavior::Fail);
        let (a, rx_a) = req(1, 1);
        let (b, rx_b) = req(2, 2);

        dispatch_window(collector, vec![a, b], NO_TIMEOUT).await;

        assert!(matches!(rx_a.await.unwrap(), Err(Error::Collector(_))));
        assert!(matches!(rx_b.await.unwrap(), Err(Error::Collector(_))));
    }
}
