//! Deduplicate and batch concurrent async requests.
//!
//! Concurrent requests within a collection window are merged into a single downstream batch,
//! and duplicate inputs share one result.
//! No cache; built on `tokio`.
//!
//! This version ships [`queue`] - the pending-request queue underneath the batching side;
//! the layers on top are not released yet.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod queue;
