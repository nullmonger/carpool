use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use carpool::{Deduplicator, Fetcher};

// Counts fetches so dedup is an observable fact, not just equal results.
#[derive(Clone)]
struct CountingFetch {
    calls: Arc<AtomicUsize>,
}

impl Fetcher for CountingFetch {
    type Input = u64;
    type Output = u64;
    type Error = Infallible;

    async fn load(&self, input: u64) -> Result<u64, Infallible> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(input * input)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let calls = Arc::new(AtomicUsize::new(0));
    let d = Deduplicator::new(CountingFetch {
        calls: calls.clone(),
    });

    // Concurrent calls for one input in a single task, so they all subscribe
    // before the leader's fetch runs and collapse to one downstream hit.
    let (a, b, c, e, f) = tokio::join!(d.call(9), d.call(9), d.call(9), d.call(9), d.call(9));
    let results = [a, b, c, e, f];

    let downstream = calls.load(Ordering::SeqCst);
    println!("concurrent calls: {}", results.len());
    println!("downstream hits:  {downstream}");

    for r in &results {
        assert_eq!(
            *r.as_ref().unwrap(),
            81,
            "every caller gets the shared value"
        );
    }
    assert_eq!(
        downstream, 1,
        "five concurrent calls of one input collapse to one fetch"
    );

    println!(
        "dedup confirmed: {} calls served by {downstream} fetch",
        results.len()
    );
}
