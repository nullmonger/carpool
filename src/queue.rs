use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard};

use tokio::sync::{Notify, oneshot};

// Mutex guard that recovers a poisoned lock via into_inner.
// A panic under the lock must not brick the queue.
fn locked<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

// A pending request: the input and the channel its caller awaits.
pub type Pending<I, O> = (I, oneshot::Sender<O>);

pub struct Queue<I, O> {
    items: Mutex<VecDeque<Pending<I, O>>>,
    arrivals: Notify,
}

impl<I, O> Default for Queue<I, O> {
    fn default() -> Self {
        Queue {
            items: Mutex::new(VecDeque::new()),
            arrivals: Notify::new(),
        }
    }
}

impl<I, O> Queue<I, O> {
    pub fn push(&self, input: I, tx: oneshot::Sender<O>) {
        locked(&self.items).push_back((input, tx));
        self.arrivals.notify_waiters();
    }

    // Raw count: entries whose receiver is gone still count until a scan buries them.
    pub fn len(&self) -> usize {
        locked(&self.items).len()
    }

    pub fn is_empty(&self) -> bool {
        locked(&self.items).is_empty()
    }

    // Up to n still-awaited entries in arrival order.
    // Dead entries met on the way are dropped and do not count against n.
    pub fn take(&self, n: usize) -> Vec<Pending<I, O>> {
        let mut items = locked(&self.items);
        let mut live = Vec::with_capacity(n.min(items.len()));
        while live.len() < n {
            let Some((input, tx)) = items.pop_front() else {
                break;
            };
            if !tx.is_closed() {
                live.push((input, tx));
            }
        }
        live
    }

    // Exactly n live entries or None; the raw-len gate keeps sub-threshold calls O(1).
    pub fn take_if(&self, n: usize) -> Option<Vec<Pending<I, O>>> {
        let mut items = locked(&self.items);
        if items.len() < n {
            return None;
        }
        items.retain(|(_, tx)| !tx.is_closed());
        if items.len() < n {
            return None;
        }
        Some(items.drain(..n).collect())
    }

    // Resolve once the raw len reaches n.
    // Enable before the check so a push in the gap is not lost.
    pub async fn reached(&self, n: usize) {
        loop {
            let mut signal = std::pin::pin!(self.arrivals.notified());
            signal.as_mut().enable();
            if self.len() >= n {
                return;
            }
            signal.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;

    fn enqueue(q: &Queue<u32, u32>, input: u32) -> oneshot::Receiver<u32> {
        let (tx, rx) = oneshot::channel();
        q.push(input, tx);
        rx
    }

    fn inputs(batch: &[Pending<u32, u32>]) -> Vec<u32> {
        batch.iter().map(|(input, _)| *input).collect()
    }

    #[test]
    fn len_is_raw_until_a_scan_buries_the_dead() {
        let q: Queue<u32, u32> = Queue::default();
        let rxs: Vec<_> = (0..3).map(|i| enqueue(&q, i)).collect();
        assert_eq!(q.len(), 3);
        drop(rxs); // every caller cancels, len does not notice
        assert_eq!(q.len(), 3);
        assert!(q.take(usize::MAX).is_empty()); // the scan buries all three
        assert_eq!(q.len(), 0);
    }

    #[test]
    fn take_skips_dead_and_keeps_arrival_order() {
        let q: Queue<u32, u32> = Queue::default();
        let mut rxs: Vec<_> = (0..5).map(|i| enqueue(&q, i)).collect();
        drop(rxs.remove(3));
        drop(rxs.remove(1)); // callers 1 and 3 cancel
        let taken = q.take(usize::MAX);
        assert_eq!(inputs(&taken), vec![0, 2, 4]);
        assert_eq!(q.len(), 0);
    }

    #[test]
    fn take_counts_live_entries_against_the_limit() {
        let q: Queue<u32, u32> = Queue::default();
        let mut rxs: Vec<_> = (0..5).map(|i| enqueue(&q, i)).collect();
        drop(rxs.remove(0)); // a dead head must not eat the limit
        assert!(q.take(0).is_empty());
        assert_eq!(q.len(), 5);
        let taken = q.take(2);
        assert_eq!(inputs(&taken), vec![1, 2]);
        assert_eq!(q.len(), 2); // 3 and 4 stay
    }

    #[test]
    fn push_works_after_a_full_take() {
        let q: Queue<u32, u32> = Queue::default();
        let _r1 = enqueue(&q, 1);
        assert_eq!(q.take(usize::MAX).len(), 1);
        let _r2 = enqueue(&q, 2); // no terminal states: the queue keeps serving
        assert_eq!(inputs(&q.take(usize::MAX)), vec![2]);
    }

    #[test]
    fn take_if_refuses_below_the_raw_gate() {
        let q: Queue<u32, u32> = Queue::default();
        let _rxs: Vec<_> = (0..3).map(|i| enqueue(&q, i)).collect();
        assert!(q.take_if(5).is_none());
        assert_eq!(q.len(), 3); // the gate fails before any scan
    }

    #[test]
    fn take_if_prunes_then_refuses_when_live_is_short() {
        let q: Queue<u32, u32> = Queue::default();
        let mut rxs: Vec<_> = (0..5).map(|i| enqueue(&q, i)).collect();
        drop(rxs.split_off(2)); // callers 2, 3 and 4 cancel
        assert!(q.take_if(4).is_none());
        assert_eq!(q.len(), 2); // the failed attempt still buried the dead
    }

    #[test]
    fn take_if_delivers_exactly_n_in_order() {
        let q: Queue<u32, u32> = Queue::default();
        let _rxs: Vec<_> = (0..5).map(|i| enqueue(&q, i)).collect();
        let batch = q.take_if(3).expect("5 live entries cover a batch of 3");
        assert_eq!(inputs(&batch), vec![0, 1, 2]);
        assert_eq!(q.len(), 2);
    }

    #[tokio::test]
    async fn a_taken_entry_delivers_to_its_receiver() {
        let q: Queue<u32, u32> = Queue::default();
        let rx = enqueue(&q, 7);
        for (input, tx) in q.take(usize::MAX) {
            tx.send(input * 10).expect("receiver is alive");
        }
        assert_eq!(rx.await, Ok(70));
    }

    #[tokio::test]
    async fn reached_resolves_once_len_covers_n() {
        let q: Queue<u32, u32> = Queue::default();
        let _rxs: Vec<_> = (0..2).map(|i| enqueue(&q, i)).collect();
        q.reached(2).await;
    }

    // Race catalog: no internal timers, so each race is real cross-thread
    // concurrency, repeated to shake out interleavings.

    #[test]
    fn race_concurrent_pushes_reach_one_take() {
        for _ in 0..50 {
            let q: Queue<u32, u32> = Queue::default();
            let n = 8u32;
            let start = Barrier::new(n as usize + 1);
            let rxs = Mutex::new(Vec::new());
            thread::scope(|s| {
                for i in 0..n {
                    let q = &q;
                    let rxs = &rxs;
                    let start = &start;
                    s.spawn(move || {
                        let rx = enqueue(q, i);
                        locked(rxs).push(rx);
                        start.wait();
                    });
                }
                start.wait();
                let mut delivered = inputs(&q.take(usize::MAX));
                delivered.sort_unstable();
                assert_eq!(delivered, (0..n).collect::<Vec<_>>());
            });
        }
    }

    #[test]
    fn race_take_against_receiver_drop() {
        for _ in 0..100 {
            let q: Queue<u32, u32> = Queue::default();
            let mut rxs: Vec<_> = (0..6).map(|i| enqueue(&q, i)).collect();
            let victim = rxs.remove(0); // head of the take(3) prefix
            let start = Barrier::new(2);
            let taken = thread::scope(|s| {
                let taker = {
                    let q = &q;
                    let start = &start;
                    s.spawn(move || {
                        start.wait();
                        q.take(3)
                    })
                };
                start.wait();
                drop(victim); // cancel while the take may be scanning
                taker.join().unwrap()
            });
            // the victim is either buried or rides along dead, never duplicated
            assert_eq!(taken.len(), 3);
            assert!(matches!(q.len(), 2 | 3));
        }
    }

    #[test]
    fn race_take_if_against_receiver_drop() {
        for _ in 0..100 {
            let q: Queue<u32, u32> = Queue::default();
            let mut rxs: Vec<_> = (0..5).map(|i| enqueue(&q, i)).collect();
            let victim = rxs.remove(0);
            let start = Barrier::new(2);
            let batch = thread::scope(|s| {
                let taker = {
                    let q = &q;
                    let start = &start;
                    s.spawn(move || {
                        start.wait();
                        q.take_if(5)
                    })
                };
                start.wait();
                drop(victim);
                taker.join().unwrap()
            });
            match batch {
                // the cancel lost the race: the batch left whole, dead rider included
                Some(batch) => {
                    assert_eq!(batch.len(), 5);
                    assert_eq!(q.len(), 0);
                }
                // the cancel won: the scan buried it and the threshold failed
                None => assert_eq!(q.len(), 4),
            }
        }
    }

    #[test]
    fn race_pushes_wake_a_reached_waiter() {
        for _ in 0..50 {
            let q: Queue<u32, u32> = Queue::default();
            let start = Barrier::new(2);
            thread::scope(|s| {
                let waiter = {
                    let q = &q;
                    let start = &start;
                    s.spawn(move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_time()
                            .build()
                            .unwrap();
                        start.wait();
                        rt.block_on(async {
                            tokio::time::timeout(Duration::from_secs(2), q.reached(3))
                                .await
                                .expect("the third push must wake the waiter");
                        });
                    })
                };
                start.wait();
                let _rxs: Vec<_> = (0..3).map(|i| enqueue(&q, i)).collect();
                waiter.join().unwrap();
            });
        }
    }
}
