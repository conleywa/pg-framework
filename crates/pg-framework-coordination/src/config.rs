use std::time::Duration;

use pg_framework_core::{
    DEFAULT_COORDINATION_CHANNEL, DEFAULT_OPERATION_TIMEOUT, DEFAULT_SHUTDOWN_TIMEOUT,
};

use crate::CoordinationError;

/// 默认协调会话 lease。
pub const DEFAULT_COORDINATION_LEASE: Duration = Duration::from_secs(30);
/// 默认协调会话心跳间隔。
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// 默认从数据库 lease 中扣除的本地安全余量。
pub const DEFAULT_COORDINATION_SAFETY_MARGIN: Duration = Duration::from_secs(2);
/// 默认 watch 周期刷新间隔。
pub const DEFAULT_WATCH_REFRESH: Duration = Duration::from_secs(5);
/// 默认节点路径长度上限。
pub const DEFAULT_MAX_COORDINATION_PATH_BYTES: usize = 1024;
/// SQL schema 允许的最大节点路径长度。
pub const HARD_MAX_COORDINATION_PATH_BYTES: usize = 4096;
/// 默认节点数据长度上限，与 ZooKeeper 常见 1 MiB 限制一致。
pub const DEFAULT_MAX_COORDINATION_DATA_BYTES: usize = 1024 * 1024;
/// SQL schema 允许的最大节点数据长度。
pub const HARD_MAX_COORDINATION_DATA_BYTES: usize = 1024 * 1024;
/// 默认单次读取的直接子节点数量上限。
pub const DEFAULT_MAX_COORDINATION_CHILDREN: usize = 10_000;

const HARD_MAX_COORDINATION_CHILDREN: usize = 100_000;
const MIN_SESSION_LEASE: Duration = Duration::from_secs(2);
const MAX_SESSION_LEASE: Duration = Duration::from_secs(60 * 60);

/// 协调节点客户端配置。
#[derive(Clone, Debug)]
pub struct CoordinationConfig {
    /// 项目、环境或集群 namespace。
    pub namespace: String,
    /// 数据库 schema generation。
    pub schema_generation: String,
    /// 协调变更通知使用的 PostgreSQL channel。
    pub notification_channel: String,
    /// 节点路径最大字节数。
    pub max_path_bytes: usize,
    /// 单个节点数据最大字节数。
    pub max_data_bytes: usize,
    /// 单次 children 查询允许返回的最大节点数。
    pub max_children: usize,
    /// 新会话默认 lease。
    pub lease_ttl: Duration,
    /// 后台会话心跳间隔。
    pub heartbeat_interval: Duration,
    /// 从数据库剩余 lease 中扣除的安全余量。
    pub safety_margin: Duration,
    /// watch 未收到 NOTIFY 时的兜底刷新间隔。
    pub watch_refresh_interval: Duration,
    /// 单次数据库操作总超时。
    pub operation_timeout: Duration,
    /// 后台任务优雅关闭总超时。
    pub shutdown_timeout: Duration,
}

impl CoordinationConfig {
    /// 为一个 namespace 创建默认配置。
    #[must_use]
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            schema_generation: "v1".to_owned(),
            notification_channel: DEFAULT_COORDINATION_CHANNEL.to_owned(),
            max_path_bytes: DEFAULT_MAX_COORDINATION_PATH_BYTES,
            max_data_bytes: DEFAULT_MAX_COORDINATION_DATA_BYTES,
            max_children: DEFAULT_MAX_COORDINATION_CHILDREN,
            lease_ttl: DEFAULT_COORDINATION_LEASE,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            safety_margin: DEFAULT_COORDINATION_SAFETY_MARGIN,
            watch_refresh_interval: DEFAULT_WATCH_REFRESH,
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), CoordinationError> {
        if self.namespace.is_empty() || self.namespace.len() > 1024 {
            return Err(CoordinationError::InvalidConfig(
                "namespace must contain between 1 and 1024 bytes",
            ));
        }
        if self.schema_generation.is_empty() {
            return Err(CoordinationError::InvalidConfig(
                "schema generation is empty",
            ));
        }
        if self.notification_channel.is_empty()
            || self.notification_channel.len() > 63
            || !self
                .notification_channel
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(CoordinationError::InvalidConfig(
                "notification channel must be a safe identifier up to 63 bytes",
            ));
        }
        if self.max_path_bytes == 0 || self.max_path_bytes > HARD_MAX_COORDINATION_PATH_BYTES {
            return Err(CoordinationError::InvalidConfig(
                "max path bytes are outside schema bounds",
            ));
        }
        if self.max_data_bytes > HARD_MAX_COORDINATION_DATA_BYTES {
            return Err(CoordinationError::InvalidConfig(
                "max data bytes exceed schema bounds",
            ));
        }
        if self.max_children == 0 || self.max_children > HARD_MAX_COORDINATION_CHILDREN {
            return Err(CoordinationError::InvalidConfig(
                "max children is outside supported bounds",
            ));
        }
        self.validate_session_ttl(self.lease_ttl)?;
        if self.heartbeat_interval.is_zero() || self.watch_refresh_interval.is_zero() {
            return Err(CoordinationError::InvalidConfig(
                "heartbeat and watch refresh intervals must be positive",
            ));
        }
        if self.safety_margin.is_zero() || self.safety_margin >= self.lease_ttl {
            return Err(CoordinationError::InvalidConfig(
                "lease safety margin must be positive and shorter than the lease",
            ));
        }
        if self.operation_timeout.is_zero() || self.shutdown_timeout.is_zero() {
            return Err(CoordinationError::InvalidConfig(
                "timeouts must be positive",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_session_ttl(
        &self,
        lease_ttl: Duration,
    ) -> Result<(), CoordinationError> {
        if lease_ttl < MIN_SESSION_LEASE || lease_ttl > MAX_SESSION_LEASE {
            return Err(CoordinationError::InvalidConfig(
                "session lease is outside configured bounds",
            ));
        }
        if self.heartbeat_interval >= lease_ttl {
            return Err(CoordinationError::InvalidConfig(
                "heartbeat interval must be shorter than session lease",
            ));
        }
        if self.safety_margin >= lease_ttl {
            return Err(CoordinationError::InvalidConfig(
                "session lease must exceed safety margin",
            ));
        }
        if lease_ttl.as_millis() > i64::MAX as u128 {
            return Err(CoordinationError::InvalidConfig(
                "session lease is too large",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_must_be_shorter_than_lease() {
        let mut config = CoordinationConfig::new("services");
        config.heartbeat_interval = config.lease_ttl;

        assert!(matches!(
            config.validate(),
            Err(CoordinationError::InvalidConfig(_))
        ));
    }

    #[test]
    fn rejects_path_limit_above_schema() {
        let mut config = CoordinationConfig::new("services");
        config.max_path_bytes = HARD_MAX_COORDINATION_PATH_BYTES + 1;

        assert!(matches!(
            config.validate(),
            Err(CoordinationError::InvalidConfig(_))
        ));
    }
}
