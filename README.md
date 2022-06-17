# hybrid-lock

Hybrid locking, or [`parking_lot::RwLock`](https://crates.io/crates/parking_lot) with support for optimistic locking.

See [the paper](https://dl.acm.org/doi/abs/10.1145/3399666.3399908) for details.