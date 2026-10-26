//! 基于 PostgreSQL 的固定窗口限频。

mod config;
mod error;
mod limiter;
mod types;

pub use config::RateLimitConfig;
pub use error::RateLimitError;
pub use limiter::RateLimiter;
pub use pg_framework_core::{
    DEFAULT_MAX_KEY_BYTES, DEFAULT_POOL_MAX_SIZE, HARD_MAX_KEY_BYTES, KeyHasher,
    build_default_pool, build_pool,
};
pub use types::{RateDecision, RateKey, RateSnapshot};
