// Walk the queue through its observable behavior:
// filling, a leaving caller, both slicing paths, delivery.

use std::time::Duration;

use carpool::queue::{Pending, Queue};
use tokio::sync::oneshot::{self, Receiver};
use tokio::time::timeout;

fn deliver(batch: Vec<Pending<&str, String>>) {
    for (input, tx) in batch {
        let _ = tx.send(input.to_uppercase());
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let queue: Queue<&str, String> = Queue::default();

    // Filling: three callers enqueue and keep their receivers.
    let mut receivers: Vec<Receiver<String>> = Vec::new();
    for input in ["alpha", "beta", "gamma"] {
        let (tx, rx) = oneshot::channel();
        queue.push(input, tx);
        receivers.push(rx);
    }
    println!("pushed 3 requests: len = {}", queue.len());

    // One caller leaves; the raw len does not notice.
    drop(receivers.remove(1));
    println!("\"beta\" left: len = {}", queue.len());

    // Threshold slice: the scan buries the dead entry, and two live are not three.
    match queue.take_if(3) {
        Some(batch) => deliver(batch),
        None => println!("take_if(3) refused: len = {}", queue.len()),
    }

    // Two live entries meet a threshold of two; delivery stays with us.
    let batch = queue.take_if(2).expect("two live entries");
    println!("take_if(2) sliced a batch of {}", batch.len());
    deliver(batch);
    for rx in receivers.drain(..) {
        println!("caller got {:?}", rx.await);
    }

    // Timer slice: the threshold is out of reach, the deadline fires instead.
    let (tx, rx) = oneshot::channel();
    queue.push("delta", tx);
    if timeout(Duration::from_millis(50), queue.reached(3))
        .await
        .is_err()
    {
        println!("reached(3) timed out with len = {}", queue.len());
    }
    deliver(queue.take(usize::MAX));
    println!("timer batch delivered: {:?}", rx.await);
}
