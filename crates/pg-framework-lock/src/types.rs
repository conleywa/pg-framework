use std::fmt;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use pg_framework_core::{KeyId, duration_millis};

use crate::LockError;

/// 已校验的租约 TTL。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaseTtl(Duration);

impl LeaseTtl {
    /// 使用秒数创建租约 TTL。
    ///
    /// # 错误
    ///
    /// 时长无法表示时返回错误。
    pub fn from_secs(seconds: u64) -> Result<Self, LockError> {
        Self::new(Duration::from_secs(seconds))
    }

    /// 使用时长创建租约 TTL。
    ///
    /// # 错误
    ///
    /// 时长为 0 或过大时返回错误。
    pub fn new(duration: Duration) -> Result<Self, LockError> {
        if duration.is_zero() {
            return Err(LockError::InvalidConfig("lease TTL must be positive"));
        }
        let _ = duration_millis(duration)?;
        Ok(Self(duration))
    }

    /// 返回内部时长。
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
}

/// PostgreSQL 生成的单调 fencing token。
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct FencingToken(i64);

impl FencingToken {
    /// 构造 token。
    #[must_use]
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    /// 返回内部 token。
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// 由 namespace 和 canonical key 派生出的逻辑锁标识。
#[derive(Clone, Copy, Eq)]
pub struct LockKey(KeyId);

impl LockKey {
    /// 使用默认 SHA-256 哈希器派生锁 key。
    #[must_use]
    pub fn derive(namespace: &str, canonical_key: &[u8]) -> Self {
        Self(KeyId::derive(namespace, canonical_key))
    }

    /// 返回稳定 key 标识。
    #[must_use]
    pub const fn key_id(self) -> KeyId {
        self.0
    }
}

impl Hash for LockKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.as_bytes().hash(state);
    }
}

impl PartialEq for LockKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl fmt::Debug for LockKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LockKey")
            .field("key_id", &self.0.to_hex())
            .finish()
    }
}

/// 租约 Guard 的当前状态。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseState {
    /// 租约仍处于本地安全 deadline 内，且最近一次数据库操作已确认。
    Active,
    /// 续租或释放结果无法确认。
    Uncertain,
    /// 已确认租约过期或归其他进程所有。
    Lost,
    /// owner 已显式释放租约。
    Released,
}

/// 非阻塞获取锁的结果。
#[derive(Debug)]
#[non_exhaustive]
pub enum AcquireOutcome {
    /// 已获取锁。
    Acquired(crate::LeaseGuard),
    /// 其他 owner 当前持有锁。
    Contended,
}

/// 租约续期结果。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RenewOutcome {
    /// 租约已续期。
    Renewed {
        /// 数据库侧续期后的剩余安全时长。
        remaining: Duration,
    },
    /// 当前 Guard 已不再拥有该租约。
    Lost,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_key_debug_does_not_expose_raw_key() {
        let key = LockKey::derive("tenant", b"secret-resource");
        let debug = format!("{key:?}");

        assert!(debug.contains("key_id"));
        assert!(!debug.contains("secret-resource"));
    }

    #[test]
    fn ttl_requires_positive_duration() {
        assert!(matches!(
            LeaseTtl::new(Duration::ZERO),
            Err(LockError::InvalidConfig(_))
        ));
    }
}
