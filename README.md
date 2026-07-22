# carpool

Deduplicate and batch concurrent async requests.
Concurrent requests within a collection window are merged into a single downstream batch,
and duplicate inputs share one result. No cache; built on `tokio`.

## Status

The crate is built and released feature by feature.

- [x] `queue` - the pending-request queue underneath the batching side:
      entries pair an input with its `oneshot` reply sender,
      liveness is read lazily from the channel,
      batches are sliced by timer or by threshold. First release.
- [ ] `Deduplicator` - single-flight per input over a user-implemented `Fetcher`;
      a flight lives while at least one caller is still waiting.
- [ ] `Batcher` - collection-window batching over a user-implemented `BatchCollector`,
      with an input-addressed result contract.
- [ ] `Loader` - deduplication in front of batching.
- [ ] Metrics - instrumentation of windows, batches, and flights.
- [ ] Tracing - OpenTelemetry spans linked to individual `load` calls.

An earlier prototype of the whole stack lives on the `draft` branch.

## Usage

```console
cargo add carpool
cargo add tokio --features macros,rt-multi-thread,sync
```

The queue stores pending requests and slices batches; delivery stays with the consumer:

```rust
use carpool::queue::Queue;
use tokio::sync::oneshot;

#[tokio::main]
async fn main() {
    let queue = Queue::default();

    let (tx, rx) = oneshot::channel();
    queue.push("carp", tx);

    for (input, tx) in queue.take(usize::MAX) { // drain everything alive
        // a closed sender means the caller left; skipping it is deliberate
        let _ = tx.send(input.len());
    }
    assert_eq!(rx.await, Ok(4));
}
```

A runnable walkthrough lives in the repository: `cargo run --example queue`.
Full API documentation is on [docs.rs](https://docs.rs/carpool).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
