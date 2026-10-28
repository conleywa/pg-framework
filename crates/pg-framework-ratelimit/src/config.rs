use std::time::Duration;

use pg_framework_core::{
    DEFAULT_MAX_KEY_BYTES, DEFAULT_OPERATION_TIMEOUT, DEFAULT_SHARD_COUNT, KeyHasher,
    duration_millis, validate_key_size,
};

use crate::RateLimitError;

/// 固定窗口限频器配置。
#[derive(Clone, Debug)]
pub struct RateLimitConfig {
    /// 项目、环境、租户和策略共同组成的 namespace。
    pub namespace: String,
    /// 数据库 schema generation。
    pub schema_generation: String,
    /// 数据库物理分区数。
    pub physical_partition_count: u16,
    /// 固定窗口时长。
    pub window: Duration,
    /// 窗口结束后的清理宽限期。
    pub cleanup_grace: Duration,
    /// 限频 key 使用的稳定哈希策略。
    pub key_hasher: KeyHasher,
    /// canonical 限频 key 最大长度。
    pub max_key_bytes: usize,
    /// 是否在哈希旁额外保存原始限频 key。
    pub store_raw_keys: bool,
    /// 单次数据库操作总超时。
    pub operation_timeout: Duration,
}

impl RateLimitConfig {
    /// 创建固定窗口配置。
    #[must_use]
    pub fn new(namespace: impl Into<String>, window: Duration) -> Self {
        Self {
            namespace: namespace.into(),
            schema_generation: "v1".to_owned(),
            physical_partition_count: DEFAULT_SHARD_COUNT,
            window,
            cleanup_grace: Duration::from_secs(60),
            key_hasher: KeyHasher::default(),
            max_key_bytes: DEFAULT_MAX_KEY_BYTES,
            store_raw_keys: false,
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), RateLimitError> {
        if self.namespace.is_empty() {
            return Err(RateLimitError::InvalidConfig("namespace is empty"));
        }
        if self.window.is_zero() {
            return Err(RateLimitError::InvalidConfig(
                "window duration must be positive",
            ));
        }
        if self.schema_generation.is_empty() || self.physical_partition_count == 0 {
            return Err(RateLimitError::InvalidConfig(
                "schema generation and partition count are invalid",
            ));
        }
        if self.operation_timeout.is_zero() {
            return Err(RateLimitError::InvalidConfig(
                "operation timeout must be positive",
            ));
        }
        let _ = duration_millis(self.window)?;
        let _ = duration_millis(self.cleanup_grace)?;
        validate_key_size(1, self.max_key_bytes)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_window_is_rejected() {
        let config = RateLimitConfig::new("login", Duration::ZERO);
        assert!(matches!(
            config.validate(),
            Err(RateLimitError::InvalidConfig(_))
        ));
    }

    #[test]
    fn rejects_key_limit_above_schema_bound() {
        let mut config = RateLimitConfig::new("login", Duration::from_secs(60));
        config.max_key_bytes = 2048;

        assert!(matches!(
            config.validate(),
            Err(RateLimitError::Core(
                pg_framework_core::CoreError::InvalidMaxKeyBytes
            ))
        ));
    }
}
