//! Deduplicate and batch concurrent async requests.
//!
//! Many concurrent `load(input)` calls are merged within a collection window
//! into a single downstream batch, and duplicate inputs share one result.
//! There is no cache; the API is trait-based and built on `tokio`.
//!
//! The public API is not available yet;
//! this version ships the crate skeleton only.

#![forbid(unsafe_code)]
