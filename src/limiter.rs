use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::collector::BatchCollector;
use crate::config::BatchLoaderConfig;
use crate::dispatch::{Request, dispatch_window, fail_batch};
use crate::error::Error;

// Slot gate for parallel batches; Limited caps concurrent downstream calls.
#[derive(Clone)]
pub(crate) enum Slots {
    Unlimited,
    Limited {
        permits: Arc<Semaphore>,
        max_waiting: Option<Duration>,
    },
}

impl Slots {
    pub(crate) fn from_config(config: &BatchLoaderConfig) -> Self {
        match config.concurrency_limit {
            // clamp so a huge limit degrades instead of panicking in Semaphore::new
            Some(limit) => Slots::Limited {
                permits: Arc::new(Semaphore::new(limit.get().min(Semaphore::MAX_PERMITS))),
                max_waiting: config.max_waiting,
            },
            None => {
                // max_waiting gates slot acquisition only; with no limit it has no effect
                if config.max_waiting.is_some() {
                    warn_max_waiting_ignored();
                }
                Slots::Unlimited
            }
        }
    }

    // Hold a slot while limited; the permit frees on drop, so a panic never strands it.
    // `timeout` bounds the downstream call and applies regardless of the slot gate.
    pub(crate) async fn run<C: BatchCollector>(
        &self,
        collector: C,
        batch: Vec<Request<C>>,
        timeout: Duration,
    ) {
        match self {
            Slots::Unlimited => dispatch_window(&collector, batch, timeout).await,
            Slots::Limited {
                permits,
                max_waiting,
            } => {
                let _permit = match max_waiting {
                    None => permits.acquire().await.expect("semaphore is never closed"),
                    Some(wait) => match tokio::time::timeout(*wait, permits.acquire()).await {
                        Ok(permit) => permit.expect("semaphore is never closed"),
                        // no slot in time: fail the waiters, never call downstream
                        Err(_) => {
                            fail_batch(batch, Error::WaitingTimeout);
                            return;
                        }
                    },
                };
                dispatch_window(&collector, batch, timeout).await;
            }
        }
    }
}

#[cfg(feature = "tracing")]
fn warn_max_waiting_ignored() {
    tracing::warn!("carpool: max_waiting is set without concurrency_limit and has no effect");
}

#[cfg(not(feature = "tracing"))]
fn warn_max_waiting_ignored() {}
