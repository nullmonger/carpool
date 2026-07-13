use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use tokio::sync::oneshot;

use carpool::queue::Queue;
use carpool::transit::{Pass, Ride};

type Tx = oneshot::Sender<u64>;
type Rx = oneshot::Receiver<u64>;
type Cargo = (u64, Tx);

const FILL: usize = 1024;
const BATCH: usize = 128;

// Callers' channel halves, prebuilt so enqueue benches measure the instrument alone.
fn channels(n: usize) -> (Vec<Cargo>, Vec<Rx>) {
    (0..n as u64)
        .map(|i| {
            let (tx, rx) = oneshot::channel();
            ((i, tx), rx)
        })
        .unzip()
}

fn filled_ride() -> (Ride<Cargo>, Vec<Pass>, Vec<Rx>) {
    let (cargos, rxs) = channels(FILL);
    let ride = Ride::new();
    let passes = cargos.into_iter().map(|cargo| ride.board(cargo)).collect();
    (ride, passes, rxs)
}

fn filled_queue() -> (Queue<u64, u64>, Vec<Rx>) {
    let (cargos, rxs) = channels(FILL);
    let queue = Queue::default();
    for (input, tx) in cargos {
        queue.push(input, tx);
    }
    (queue, rxs)
}

fn tuned(g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(3));
}

fn fill(c: &mut Criterion) {
    let mut g = c.benchmark_group("fill_1024");
    tuned(&mut g);
    g.bench_function("transit", |b| {
        b.iter_batched(
            || channels(FILL),
            |(cargos, rxs)| {
                let ride: Ride<Cargo> = Ride::new();
                let passes: Vec<Pass> = cargos.into_iter().map(|cargo| ride.board(cargo)).collect();
                (ride, passes, rxs)
            },
            BatchSize::SmallInput,
        )
    });
    g.bench_function("queue", |b| {
        b.iter_batched(
            || channels(FILL),
            |(cargos, rxs)| {
                let queue: Queue<u64, u64> = Queue::default();
                for (input, tx) in cargos {
                    queue.push(input, tx);
                }
                (queue, rxs)
            },
            BatchSize::SmallInput,
        )
    });
    g.finish();
}

// Every caller cancels and the instrument ends up clean:
// transit pays eagerly in each guard drop, the queue lazily in one prune.
fn cancel_all(c: &mut Criterion) {
    let mut g = c.benchmark_group("cancel_all_1024");
    tuned(&mut g);
    g.bench_function("transit", |b| {
        b.iter_batched(
            filled_ride,
            |(ride, passes, rxs)| {
                drop(passes);
                drop(rxs);
                ride
            },
            BatchSize::SmallInput,
        )
    });
    g.bench_function("queue", |b| {
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
    g.finish();
}

fn slice(c: &mut Criterion) {
    let mut g = c.benchmark_group("slice_128_of_1024");
    tuned(&mut g);
    g.bench_function("transit_take", |b| {
        b.iter_batched(
            filled_ride,
            |(ride, passes, rxs)| {
                let train = ride.take(BATCH);
                (ride, train, passes, rxs)
            },
            BatchSize::SmallInput,
        )
    });
    g.bench_function("queue_take", |b| {
        b.iter_batched(
            filled_queue,
            |(queue, rxs)| {
                let batch = queue.take(BATCH);
                (queue, batch, rxs)
            },
            BatchSize::SmallInput,
        )
    });
    g.bench_function("queue_take_if", |b| {
        b.iter_batched(
            filled_queue,
            |(queue, rxs)| {
                let batch = queue.take_if(BATCH).expect("all entries are live");
                (queue, batch, rxs)
            },
            BatchSize::SmallInput,
        )
    });
    g.finish();
}

// One consumer window end to end: fill, a quarter cancels, slice a batch, deliver it.
// The leftover tail survives into the next window, so its teardown is not timed.
fn window_cycle(c: &mut Criterion) {
    const N: usize = 512;
    let mut g = c.benchmark_group("window_cycle_512");
    tuned(&mut g);
    g.bench_function("transit", |b| {
        b.iter_with_large_drop(|| {
            let ride: Ride<Cargo> = Ride::new();
            let mut passes = Vec::with_capacity(N);
            let mut rxs = Vec::with_capacity(N);
            for i in 0..N as u64 {
                let (tx, rx) = oneshot::channel();
                passes.push(Some(ride.board((i, tx))));
                rxs.push(Some(rx));
            }
            for i in (0..N).step_by(4) {
                passes[i] = None; // a cancel drops the whole ticket: pass and rx
                rxs[i] = None;
            }
            let train = ride.take(BATCH);
            train.depart();
            for (input, tx) in train {
                tx.send(input * 2).expect("receiver is alive");
            }
            (ride, passes, rxs)
        })
    });
    g.bench_function("queue", |b| {
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
    g.finish();
}

criterion_group!(benches, fill, cancel_all, slice, window_cycle);
criterion_main!(benches);
