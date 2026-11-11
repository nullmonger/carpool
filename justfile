
check:
  cargo fmt --check
  cargo check
  cargo clippy

precommit: check
  cargo test
  cargo build

prepush: precommit
  cargo audit
  cargo build --release
