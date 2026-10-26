use std::fmt;
use std::time::Duration;

use pg_framework_core::KeyId;

/// 单次原子限频决策结果。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateDecision {
    /// 请求的 cost 是否被接受。
    pub allowed: bool,
    /// 当前窗口配置的 limit。
    pub limit: u64,
    /// 剩余可用额度。
    pub remaining: u64,
    /// 距离当前固定窗口重置的时长。
    pub reset_after: Duration,
    /// 被拒绝请求建议的重试等待时长。
    pub retry_after: Option<Duration>,
}

/// 未消费额度时读取到的限频 key 当前状态。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateSnapshot {
    /// 当前窗口配置的 limit。
    pub limit: u64,
    /// 当前窗口已经消费的额度。
    pub used: u64,
    /// 剩余可用额度。
    pub remaining: u64,
    /// 距离当前固定窗口重置的时长。
    pub reset_after: Duration,
}

/// 稳定的限频 key。
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct RateKey(KeyId);

impl RateKey {
    /// 使用默认 SHA-256 哈希器派生限频 key。
    #[must_use]
    pub fn derive(namespace: &str, canonical_key: &[u8]) -> Self {
        Self(KeyId::derive(namespace, canonical_key))
    }

    /// 返回内部稳定标识。
    #[must_use]
    pub const fn key_id(self) -> KeyId {
        self.0
    }
}

impl fmt::Debug for RateKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RateKey")
            .field("key_id", &self.0.to_hex())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_does_not_expose_raw_rate_key() {
        let key = RateKey::derive("login", b"user@example.com");
        let debug = format!("{key:?}");

        assert!(debug.contains("key_id"));
        assert!(!debug.contains("user@example.com"));
    }
}
