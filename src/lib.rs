#![forbid(unsafe_code)]

mod batcher;
mod collector;
mod config;
mod deduplicator;
mod dispatch;
mod error;
mod fetcher;
mod limiter;
mod window;

pub use batcher::Batcher;
pub use collector::BatchCollector;
pub use config::BatchConfig;
pub use deduplicator::{DedupError, Deduplicator};
pub use error::Error;
pub use fetcher::Fetcher;
