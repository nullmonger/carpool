use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use carpool::{BatchCollector, BatchConfig, Batcher};

// Counts downstream calls, so dedup is visible in the number of calls,
// not only in equal results.
#[derive(Clone)]
struct CountingSquares {
    calls: Arc<AtomicUsize>,
}

impl BatchCollector for CountingSquares {
    type Input = u64;
    type Output = u64;
    type Error = Infallible;

    async fn load(&self, batch: HashSet<u64>) -> Result<HashMap<u64, u64>, Infallible> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(batch.into_iter().map(|n| (n, n * n)).collect())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let calls = Arc::new(AtomicUsize::new(0));
    let batcher = Batcher::spawn(
        CountingSquares {
            calls: calls.clone(),
        },
        BatchConfig::default(),
    );

    // A hundred concurrent loads over only four distinct inputs (i % 4).
    let handles: Vec<_> = (0..100u64)
        .map(|i| {
            let batcher = batcher.clone();
            tokio::spawn(async move { batcher.load(i % 4).await })
        })
        .collect();

    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.expect("task joins").expect("load succeeds"));
    }

    let downstream_calls = calls.load(Ordering::SeqCst);
    println!("loads:            {}", results.len());
    println!("distinct inputs:  4");
    println!("downstream calls: {downstream_calls}");

    // The invariant, not the line order: one call served the whole window, and
    // every load got the square of its input.
    assert_eq!(results.len(), 100);
    assert_eq!(
        downstream_calls, 1,
        "100 loads over 4 distinct inputs collapse to one downstream call"
    );
    for (i, result) in results.iter().enumerate() {
        let input = i as u64 % 4;
        assert_eq!(
            *result,
            input * input,
            "each load gets the square of its input"
        );
    }

    println!("dedup confirmed: 100 loads served by {downstream_calls} downstream call");
}
