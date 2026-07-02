use std::collections::HashSet;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::{self, Instant};

// Collection window: the first item opens a window and arms a `window`-long timer.
// It closes on whichever comes first - `max_batch_size` distinct keys or the timer -
// and the next item opens a fresh one. Duplicate keys join the open window
// without advancing the count, so `max_batch_size` bounds the distinct keys downstream,
// not the raw item count.
//
// The boundary rule lives in one place: the select! is `biased` with the timer
// first, so when an item and the timer are ready together the timer wins - the
// window closes and the item stays in the channel for the next one, never lost
// and never duplicated.
//
// Spawns no task: both channel-close paths are normal teardown, not errors (inbound
// closed - source gone, flush and stop; outbound closed - consumer gone, stop).
pub(crate) async fn collect<T, K, F>(
    mut inbound: mpsc::Receiver<T>,
    outbound: mpsc::Sender<Vec<T>>,
    window: Duration,
    max_batch_size: NonZeroUsize,
    key: F,
) where
    K: Hash + Eq,
    F: Fn(&T) -> K,
{
    let max = max_batch_size.get();

    while let Some(first) = inbound.recv().await {
        let mut seen = HashSet::with_capacity(max);
        seen.insert(key(&first));
        let mut buffer = Vec::with_capacity(max);
        buffer.push(first);

        let mut timer = pin!(time::sleep_until(Instant::now() + window));
        let mut inbound_open = true;

        // Fill toward max_batch_size distinct keys or until the timer fires. The
        // buffer is owned across select!, so a cancelled branch never drops an
        // accepted item - the cancel-safety the later tasks rely on.
        while seen.len() < max {
            tokio::select! {
                biased;
                () = timer.as_mut() => break,
                maybe = inbound.recv() => match maybe {
                    Some(item) => {
                        seen.insert(key(&item));
                        buffer.push(item);
                    }
                    None => {
                        inbound_open = false;
                        break;
                    }
                },
            }
        }

        // Single hand-off point for the closed window. A closed outbound or a
        // closed inbound both end collection - nothing left to do.
        if outbound.send(buffer).await.is_err() || !inbound_open {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW: Duration = Duration::from_millis(30);

    // Test items are their own dedup key, so the projection is identity.
    fn spawn_window<T: Copy + Hash + Eq + Send + 'static>(
        max: usize,
    ) -> (mpsc::Sender<T>, mpsc::Receiver<Vec<T>>) {
        let (in_tx, in_rx) = mpsc::channel::<T>(64);
        let (out_tx, out_rx) = mpsc::channel::<Vec<T>>(64);
        let max = NonZeroUsize::new(max).expect("max is non-zero");
        tokio::spawn(collect(in_rx, out_tx, WINDOW, max, |x| *x));
        (in_tx, out_rx)
    }

    // Close-by-size at the boundary: max = 3, so item 4 must not ride in the
    // first batch - it opens a new window.
    #[tokio::test(start_paused = true)]
    async fn size_closes_at_max_and_spills_to_next_window() {
        let (tx, mut batches) = spawn_window::<u32>(3);
        for i in 1..=4 {
            tx.send(i).await.expect("send");
        }

        assert_eq!(batches.recv().await.expect("first batch"), vec![1, 2, 3]);

        time::advance(WINDOW).await;
        assert_eq!(batches.recv().await.expect("second batch"), vec![4]);
    }

    // Distinct-key count, not raw count: three copies of one input stay at one
    // distinct key, so max = 2 never closes by size - the timer carries all three.
    #[tokio::test(start_paused = true)]
    async fn duplicate_inputs_do_not_close_the_window_by_size() {
        let (tx, mut batches) = spawn_window::<u32>(2);
        for _ in 0..3 {
            tx.send(7).await.expect("send");
        }

        time::advance(WINDOW).await;
        assert_eq!(batches.recv().await.expect("batch"), vec![7, 7, 7]);
    }

    // Close-by-timer with a single underfilled item: the window must close on
    // `window` and not hang waiting for max_batch_size.
    #[tokio::test(start_paused = true)]
    async fn single_input_closes_on_timer() {
        let (tx, mut batches) = spawn_window::<u32>(16);
        tx.send(7).await.expect("send");

        time::advance(WINDOW).await;
        assert_eq!(batches.recv().await.expect("batch"), vec![7]);
    }

    // Close-by-timer with several underfilled items: all of them ride the one
    // batch the timer closes.
    #[tokio::test(start_paused = true)]
    async fn partial_window_closes_on_timer_with_all_items() {
        let (tx, mut batches) = spawn_window::<u32>(16);
        tx.send(1).await.expect("send 1");
        tx.send(2).await.expect("send 2");

        time::advance(WINDOW).await;
        assert_eq!(batches.recv().await.expect("batch"), vec![1, 2]);
    }

    // Boundary: item 2 is waiting in the channel when the timer fires. The
    // biased select gives the timer priority, so the window closes with only
    // [1] and item 2 falls into the next window - present exactly once across
    // both batches, never lost and never duplicated.
    #[tokio::test(start_paused = true)]
    async fn timer_wins_boundary_against_waiting_input() {
        let (tx, mut batches) = spawn_window::<u32>(16);
        tx.send(1).await.expect("send 1");
        // Let the window open on item 1 and park in the select before the
        // deadline.
        tokio::task::yield_now().await;
        // Reach the deadline first, then deliver item 2 at that instant: the
        // timer is already ready, so the biased select closes the window before
        // it ever looks at recv. Item 2 lands at exactly now == deadline.
        time::advance(WINDOW).await;
        tx.try_send(2).expect("channel has capacity");

        assert_eq!(batches.recv().await.expect("first batch"), vec![1]);

        time::advance(WINDOW).await;
        assert_eq!(batches.recv().await.expect("second batch"), vec![2]);
    }
}
