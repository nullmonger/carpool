//! Deduplicate and batch concurrent async requests.
//!
//! Concurrent requests within a collection window are merged into a single downstream batch,
//! and duplicate inputs share one result.
//! No cache; built on `tokio`.

#![forbid(unsafe_code)]

mod deduplicator;
mod fetcher;
pub mod queue;

pub use deduplicator::Deduplicator;
pub use fetcher::Fetcher;
