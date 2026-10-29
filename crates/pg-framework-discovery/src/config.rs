use std::time::Duration;

use pg_framework_core::{
    DEFAULT_DISCOVERY_CHANNEL, DEFAULT_MAX_KEY_BYTES, DEFAULT_OPERATION_TIMEOUT,
    DEFAULT_SHARD_COUNT, DEFAULT_SHUTDOWN_TIMEOUT, KeyHasher,
};

use crate::DiscoveryError;

/// 服务发现默认 lease TTL。
pub const DEFAULT_SERVICE_LEASE: Duration = Duration::from_secs(30);
/// 默认心跳间隔。
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
/// 默认 endpoint 长度上限。
pub const DEFAULT_MAX_ENDPOINT_BYTES: usize = 512;
/// 默认 metadata 长度上限。
pub const DEFAULT_MAX_METADATA_BYTES: usize = 4096;
/// SQL schema 允许的最大 endpoint。
pub const HARD_MAX_ENDPOINT_BYTES: usize = 2048;
/// SQL schema 允许的最大 metadata。
pub const HARD_MAX_METADATA_BYTES: usize = 8192;

/// 服务注册与发现配置。
#[derive(Clone, Debug)]
pub struct RegistryConfig {
    /// 项目、环境或集群 namespace。
    pub namespace: String,
    /// 数据库 schema generation。
    pub schema_generation: String,
    /// 物理分区数。
    pub physical_partition_count: u16,
    /// 服务标识稳定哈希策略。
    pub key_hasher: KeyHasher,
    /// PostgreSQL 服务变更通知 channel。
    pub notification_channel: String,
    /// 服务标识最大长度。
    pub max_service_bytes: usize,
    /// 默认 lease TTL。
    pub lease_ttl: Duration,
    /// 后台心跳间隔。
    pub heartbeat_interval: Duration,
    /// endpoint 最大长度。
    pub max_endpoint_bytes: usize,
    /// metadata 最大长度。
    pub max_metadata_bytes: usize,
    /// 单次数据库操作总超时。
    pub operation_timeout: Duration,
    /// 后台任务优雅关闭总超时。
    pub shutdown_timeout: Duration,
}

impl RegistryConfig {
    /// 为一个 namespace 创建默认配置。
    #[must_use]
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            schema_generation: "v2".to_owned(),
            physical_partition_count: DEFAULT_SHARD_COUNT,
            key_hasher: KeyHasher::default(),
            notification_channel: DEFAULT_DISCOVERY_CHANNEL.to_owned(),
            max_service_bytes: DEFAULT_MAX_KEY_BYTES,
            lease_ttl: DEFAULT_SERVICE_LEASE,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            max_endpoint_bytes: DEFAULT_MAX_ENDPOINT_BYTES,
            max_metadata_bytes: DEFAULT_MAX_METADATA_BYTES,
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), DiscoveryError> {
        if self.namespace.is_empty() {
            return Err(DiscoveryError::InvalidConfig("namespace is empty"));
        }
        if self.schema_generation.is_empty() || self.physical_partition_count == 0 {
            return Err(DiscoveryError::InvalidConfig(
                "schema generation or partition count is invalid",
            ));
        }
        if self.notification_channel.is_empty()
            || self.notification_channel.len() > 63
            || !self
                .notification_channel
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(DiscoveryError::InvalidConfig(
                "notification channel must be a safe identifier up to 63 bytes",
            ));
        }
        if self.max_service_bytes == 0 || self.max_service_bytes > 1024 {
            return Err(DiscoveryError::InvalidConfig(
                "max service bytes must be between 1 and 1024",
            ));
        }
        if self.lease_ttl.is_zero() || self.heartbeat_interval.is_zero() {
            return Err(DiscoveryError::InvalidConfig(
                "lease TTL and heartbeat interval must be positive",
            ));
        }
        if self.heartbeat_interval >= self.lease_ttl {
            return Err(DiscoveryError::InvalidConfig(
                "heartbeat interval must be shorter than lease TTL",
            ));
        }
        if self.max_endpoint_bytes == 0 || self.max_endpoint_bytes > HARD_MAX_ENDPOINT_BYTES {
            return Err(DiscoveryError::InvalidConfig(
                "max endpoint bytes are outside schema bounds",
            ));
        }
        if self.max_metadata_bytes > HARD_MAX_METADATA_BYTES {
            return Err(DiscoveryError::InvalidConfig(
                "max metadata bytes exceed schema bounds",
            ));
        }
        if self.operation_timeout.is_zero() || self.shutdown_timeout.is_zero() {
            return Err(DiscoveryError::InvalidConfig("timeouts must be positive"));
        }
        Ok(())
    }
}
