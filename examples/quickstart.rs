// Smoke demo: implement BatchCollector, read the default config,
// see that equal inputs map to the same key.
// Run: cargo run --example quickstart
// Expected output:
//   config: window=30ms max_batch=1024 timeout=30s concurrency=None max_waiting=None
//   key(7) = 7, key(7) = 7 -> equal inputs share one batch entry

use std::collections::HashMap;

use carpool::{BatchCollector, BatchLoaderConfig};

#[derive(Clone)]
struct SquareLoader;

impl BatchCollector for SquareLoader {
    type Input = u64;
    type Output = u64;
    type Key = u64;
    type Error = std::convert::Infallible;

    fn key(&self, input: &u64) -> u64 {
        *input
    }

    async fn load(&self, batch: HashMap<u64, u64>) -> Result<HashMap<u64, u64>, Self::Error> {
        Ok(batch.into_iter().map(|(k, n)| (k, n * n)).collect())
    }
}

fn main() {
    let cfg = BatchLoaderConfig::default();
    println!(
        "config: window={:?} max_batch={} timeout={:?} concurrency={:?} max_waiting={:?}",
        cfg.window,
        cfg.max_batch_size.get(),
        cfg.timeout,
        cfg.concurrency_limit,
        cfg.max_waiting,
    );

    let loader = SquareLoader;
    let (a, b) = (loader.key(&7), loader.key(&7));
    println!("key(7) = {a}, key(7) = {b} -> equal inputs share one batch entry");
}
