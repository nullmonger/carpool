use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use carpool::{BatchCollector, BatchLoader, BatchLoaderConfig};

// Records how many times it is actually called, so dedup shows up as an
// observable fact, not just as equal results. Output depends on the key alone,
// so it does not matter which duplicate became the batch representative.
#[derive(Clone)]
struct CountingSquares {
    calls: Arc<AtomicUsize>,
}

impl BatchCollector for CountingSquares {
    type Input = u64;
    type Output = u64;
    type Key = u64;
    type Error = Infallible;

    // Fold many inputs onto four keys so duplicates dominate the window.
    fn key(&self, input: &u64) -> u64 {
        *input % 4
    }

    async fn load(&self, batch: HashMap<u64, u64>) -> Result<HashMap<u64, u64>, Infallible> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(batch.into_keys().map(|k| (k, k * k)).collect())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let calls = Arc::new(AtomicUsize::new(0));
    let loader = BatchLoader::new(
        CountingSquares {
            calls: calls.clone(),
        },
        BatchLoaderConfig::default(),
    );

    // A hundred concurrent loads, but only four distinct keys.
    let handles: Vec<_> = (0..100u64).map(|i| tokio::spawn(loader.load(i))).collect();

    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.expect("task joins").expect("load succeeds"));
    }

    let downstream_calls = calls.load(Ordering::SeqCst);
    println!("loads:            {}", results.len());
    println!("distinct keys:    4");
    println!("downstream calls: {downstream_calls}");

    // The invariant, not the line order: one call served the whole window, and
    // every load got the square of its key.
    assert_eq!(results.len(), 100);
    assert_eq!(
        downstream_calls, 1,
        "100 loads over 4 keys collapse to one downstream call"
    );
    for (i, result) in results.iter().enumerate() {
        let key = i as u64 % 4;
        assert_eq!(*result, key * key, "each load gets the square of its key");
    }

    println!("dedup confirmed: 100 loads served by {downstream_calls} downstream call");
}
