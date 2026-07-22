# Changelog

All notable changes to this project are documented in this file.

The format follows Keep a Changelog, and the project uses Semantic Versioning.

## [Unreleased]

## [0.1.0] - 2026-07-22

### Added

- `queue`: the pending-request queue underneath the batching side.
  `Queue` pairs each input with its `oneshot` reply sender,
  reads liveness lazily from the channel,
  and slices batches by threshold (`take_if`) or by timer (`reached` + `take`).

[Unreleased]: https://github.com/nullmonger/carpool/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/nullmonger/carpool/releases/tag/v0.1.0
