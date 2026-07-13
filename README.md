# carpool

Deduplicate and batch concurrent async requests. Many concurrent `load(input)`
calls are merged within a collection window into a single downstream batch, and
duplicate inputs share one result. No cache, trait-based API, built on `tokio`.

## Status

Pre-release. The crate is built and released feature by feature;
nothing is published yet.

- [ ] `queue` - the pending-request queue underneath the batching side:
      entries pair an input with its `oneshot` reply sender, liveness is
      read lazily from the channel, batches are sliced by timer or by
      threshold. First release.
- [ ] `Deduplicator` - single-flight per input over a user-implemented `Fetcher`;
      a flight lives while at least one caller is still waiting.
- [ ] `Batcher` - collection-window batching over a user-implemented `BatchCollector`,
      with an input-addressed result contract.
- [ ] `Loader` - deduplication in front of batching.
- [ ] Metrics - instrumentation of windows, batches, and flights.
- [ ] Tracing - OpenTelemetry spans linked to individual `load` calls.

An earlier prototype of the whole stack lives on the `draft` branch.
Installation and usage will be documented as features are released.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
