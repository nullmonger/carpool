use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use tokio::sync::oneshot;

use carpool::queue::Queue;

type Tx = oneshot::Sender<u64>;
type Rx = oneshot::Receiver<u64>;

const FILL: usize = 1024;
const BATCH: usize = 128;

// Callers' channel halves, prebuilt so enqueue benches measure the queue alone.
fn channels(n: usize) -> (Vec<(u64, Tx)>, Vec<Rx>) {
    (0..n as u64)
        .map(|i| {
            let (tx, rx) = oneshot::channel();
            ((i, tx), rx)
        })
        .unzip()
}

fn filled_queue() -> (Queue<u64, u64>, Vec<Rx>) {
    let (pairs, rxs) = channels(FILL);
    let queue = Queue::default();
    for (input, tx) in pairs {
        queue.push(input, tx);
    }
    (queue, rxs)
}

fn fill(c: &mut Criterion) {
    c.bench_function("fill_1024", |b| {
        b.iter_batched(
            || channels(FILL),
            |(pairs, rxs)| {
                let queue: Queue<u64, u64> = Queue::default();
                for (input, tx) in pairs {
                    queue.push(input, tx);
                }
                (queue, rxs)
            },
            BatchSize::SmallInput,
        )
    });
}

// Every caller cancels; one full take scan buries the dead.
fn cancel_all(c: &mut Criterion) {
    c.bench_function("cancel_all_1024", |b| {
        b.iter_batched(
            filled_queue,
            |(queue, rxs)| {
                drop(rxs);
                black_box(queue.take(usize::MAX));
                queue
            },
            BatchSize::SmallInput,
        )
    });
}

fn slice(c: &mut Criterion) {
    c.bench_function("slice_take_128_of_1024", |b| {
        b.iter_batched(
            filled_queue,
            |(queue, rxs)| {
                let batch = queue.take(BATCH);
                (queue, batch, rxs)
            },
            BatchSize::SmallInput,
        )
    });
    c.bench_function("slice_take_if_128_of_1024", |b| {
        b.iter_batched(
            filled_queue,
            |(queue, rxs)| {
                let batch = queue.take_if(BATCH).expect("all entries are live");
                (queue, batch, rxs)
            },
            BatchSize::SmallInput,
        )
    });
}

// One consumer window end to end: fill, a quarter cancels, slice a batch, deliver it.
// The leftover tail survives into the next window, so its teardown is not timed.
fn window_cycle(c: &mut Criterion) {
    const N: usize = 512;
    c.bench_function("window_cycle_512", |b| {
        b.iter_with_large_drop(|| {
            let queue: Queue<u64, u64> = Queue::default();
            let mut rxs = Vec::with_capacity(N);
            for i in 0..N as u64 {
                let (tx, rx) = oneshot::channel();
                queue.push(i, tx);
                rxs.push(Some(rx));
            }
            for i in (0..N).step_by(4) {
                rxs[i] = None;
            }
            let batch = queue
                .take_if(BATCH)
                .expect("384 live entries cover the batch");
            for (input, tx) in batch {
                tx.send(input * 2).expect("receiver is alive");
            }
            (queue, rxs)
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = fill, cancel_all, slice, window_cycle
}
criterion_main!(benches);
