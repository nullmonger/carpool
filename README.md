# carpool

Deduplicate and batch concurrent async requests. Many concurrent `load(input)`
calls are merged within a collection window into a single downstream batch, and
duplicate keys share one result. No cache, trait-based API, built on `tokio`.

## Status

Early development, pre-release. The public API is not available yet.
Installation and usage are documented once it stabilizes.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
