//! Deduplicate and batch concurrent async requests.
//!
//! Many concurrent `load(input)` calls are merged within a collection window
//! into a single downstream batch, and duplicate keys share one result. There
//! is no cache; the API is trait-based and built on `tokio`.
//!
//! Implement [`BatchCollector`] to describe the downstream, then drive it with
//! a `BatchLoader` (runtime arrives in a later release). [`BatchLoaderConfig`]
//! tunes the window and the concurrency limits; [`Error`] is what a caller
//! sees on failure.
//!
//! # Example
//!
//! ```
//! use carpool::{BatchCollector, BatchLoaderConfig};
//!
//! struct Squares;
//!
//! impl BatchCollector for Squares {
//!     type Input = u64;
//!     type Output = u64;
//!     type Key = u64;
//!     type Error = std::convert::Infallible;
//!
//!     fn key(&self, input: &u64) -> u64 {
//!         *input
//!     }
//!
//!     async fn load(&self, inputs: Vec<u64>) -> Result<Vec<u64>, Self::Error> {
//!         Ok(inputs.iter().map(|n| n * n).collect())
//!     }
//! }
//!
//! // Equal inputs share a key, so they collapse into one downstream slot.
//! let squares = Squares;
//! assert_eq!(squares.key(&7), squares.key(&7));
//! assert_eq!(BatchLoaderConfig::default().max_batch_size.get(), 1024);
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

mod collector;
mod config;
mod error;

pub use collector::BatchCollector;
pub use config::BatchLoaderConfig;
pub use error::Error;
