//! Smoke demo of the carpool API surface: implement `BatchCollector`, read the
//! default config, and see that equal inputs hash to one key. There is no
//! runtime yet - the real `load` dispatch lands in a later release.
//!
//! Run with `cargo run --example quickstart`. Expected output:
//!
//! ```text
//! config: window=30ms max_batch=1024 timeout=30s concurrency=None max_waiting=None
//! key(7) = 7, key(7) = 7 -> one key, one shared ride
//! ```

use carpool::{BatchCollector, BatchLoaderConfig};

struct SquareLoader;

impl BatchCollector for SquareLoader {
    type Input = u64;
    type Output = u64;
    type Key = u64;
    type Error = std::convert::Infallible;

    fn key(&self, input: &u64) -> u64 {
        *input
    }

    async fn load(&self, inputs: Vec<u64>) -> Result<Vec<u64>, Self::Error> {
        Ok(inputs.iter().map(|n| n * n).collect())
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
    println!("key(7) = {a}, key(7) = {b} -> one key, one shared ride");
}
