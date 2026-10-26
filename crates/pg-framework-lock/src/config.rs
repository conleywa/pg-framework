use std::time::Duration;

use pg_framework_core::{DEFAULT_MAX_KEY_BYTES, KeyHasher, validate_key_size};

use crate::LockError;

/// 默认最小租约 TTL。
pub const DEFAULT_MIN_LEASE: Duration = Duration::from_secs(2);
/// 默认最大租约 TTL。
pub const DEFAULT_MAX_LEASE: Duration = Duration::from_secs(60 * 60);
/// 默认从数据库租约 deadline 中扣除的安全余量。
pub const DEFAULT_LEASE_SAFETY_MARGIN: Duration = Duration::from_secs(1);
/// 默认获取重试的最大退避时长。
pub const DEFAULT_ACQUIRE_BACKOFF: Duration = Duration::from_secs(2);

/// 分布式锁组件配置。
#[derive(Clone, Debug)]
pub struct LockConfig {
    /// 所有锁 key 都包含的逻辑 namespace。
    pub namespace: String,
    /// 允许的最小租约 TTL。
    pub min_lease: Duration,
    /// 允许的最大租约 TTL。
    pub max_lease: Duration,
    /// 从数据库 deadline 中扣除的安全余量。
    pub safety_margin: Duration,
    /// 指数退避的最大等待时长。
    pub acquire_backoff_max: Duration,
    /// 锁标识使用的稳定哈希策略。
    pub key_hasher: KeyHasher,
    /// canonical 锁 key 最大长度。
    pub max_key_bytes: usize,
    /// 是否在哈希旁额外保存原始锁 key。
    pub store_raw_keys: bool,
}

impl LockConfig {
    /// 为一个 namespace 创建配置。
    #[must_use]
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            min_lease: DEFAULT_MIN_LEASE,
            max_lease: DEFAULT_MAX_LEASE,
            safety_margin: DEFAULT_LEASE_SAFETY_MARGIN,
            acquire_backoff_max: DEFAULT_ACQUIRE_BACKOFF,
            key_hasher: KeyHasher::default(),
            max_key_bytes: DEFAULT_MAX_KEY_BYTES,
            store_raw_keys: false,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), LockError> {
        if self.namespace.is_empty() {
            return Err(LockError::InvalidConfig("namespace is empty"));
        }
        if self.min_lease.is_zero() || self.max_lease < self.min_lease {
            return Err(LockError::InvalidConfig("lease bounds are invalid"));
        }
        if self.acquire_backoff_max.is_zero() {
            return Err(LockError::InvalidConfig("backoff must be positive"));
        }
        validate_key_size(1, self.max_key_bytes)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_key_limit_above_schema_bound() {
        let mut config = LockConfig::new("locks");
        config.max_key_bytes = 2048;

        assert!(matches!(
            config.validate(),
            Err(LockError::Core(
                pg_framework_core::CoreError::InvalidMaxKeyBytes
            ))
        ));
    }
}
