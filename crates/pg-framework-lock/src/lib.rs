//! 基于 PostgreSQL 和单调 fencing token 的分布式租约锁。

mod config;
mod error;
mod guard;
mod locker;
mod types;

pub use config::{
    DEFAULT_ACQUIRE_BACKOFF, DEFAULT_LEASE_SAFETY_MARGIN, DEFAULT_MAX_LEASE,
    DEFAULT_MAX_REENTRANT_DEPTH, DEFAULT_MIN_LEASE, LockConfig,
};
pub use error::LockError;
pub use guard::LeaseGuard;
pub use locker::Locker;
pub use pg_framework_core::{
    DEFAULT_MAX_KEY_BYTES, DEFAULT_POOL_MAX_SIZE, HARD_MAX_KEY_BYTES, KeyHasher,
    build_default_pool, build_pool,
};
pub use types::{AcquireOutcome, FencingToken, LeaseState, LeaseTtl, LockKey, RenewOutcome};
