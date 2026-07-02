// Smoke demo: implement BatchCollector, read the default config, and load a batch
// where equal inputs are one entry - the input is its own dedup identity.
// Run: cargo run --example quickstart
// Expected output:
//   config: window=30ms max_batch=1024 timeout=30s concurrency=None max_waiting=None
//   batch of three 7s -> 1 entry: 7*7 = 49

use std::collections::{HashMap, HashSet};

use carpool::{BatchCollector, BatchConfig};

#[derive(Clone)]
struct SquareLoader;

impl BatchCollector for SquareLoader {
    type Input = u64;
    type Output = u64;
    type Error = std::convert::Infallible;

    async fn load(&self, batch: HashSet<u64>) -> Result<HashMap<u64, u64>, Self::Error> {
        Ok(batch.into_iter().map(|n| (n, n * n)).collect())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cfg = BatchConfig::default();
    println!(
        "config: window={:?} max_batch={} timeout={:?} concurrency={:?} max_waiting={:?}",
        cfg.window,
        cfg.max_batch_size.get(),
        cfg.timeout,
        cfg.concurrency_limit,
        cfg.max_waiting,
    );

    let batch = HashSet::from([7u64, 7, 7]);
    let out = SquareLoader.load(batch).await.expect("load succeeds");
    println!(
        "batch of three 7s -> {} entry: 7*7 = {}",
        out.len(),
        out[&7]
    );
}
