use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// 一次服务注册的稳定实例标识。
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct RegistrationId([u8; 16]);

impl RegistrationId {
    /// 从 16 字节原始数据构造实例标识。
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// 返回 16 字节原始标识。
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// 将实例标识编码为 32 个十六进制字符。
    #[must_use]
    pub fn to_hex(self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(32);
        for byte in self.0 {
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        encoded
    }
}

impl fmt::Debug for RegistrationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

impl fmt::Display for RegistrationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

/// 发现接口返回的一个健康服务实例。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceInstance {
    /// 实例注册标识。
    pub registration_id: RegistrationId,
    /// 调用方用于连接实例的 endpoint。
    pub endpoint: String,
    /// 业务附加元数据。
    pub metadata: Vec<u8>,
    /// 注册内容版本。
    pub version: i64,
    /// 根据数据库时钟计算的剩余 lease。
    pub remaining: Duration,
}

/// 服务发现 watch 通道发布的最新快照。
#[derive(Clone, Debug)]
pub struct ServiceSnapshot {
    /// 未过期实例集合。
    pub instances: Arc<[ServiceInstance]>,
    /// 进程内单调递增快照版本。
    pub revision: u64,
    /// 最近一次刷新失败时标记为 stale；保留上一次成功快照。
    pub stale: bool,
}

/// 本进程注册实例的当前状态。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistrationStatus {
    /// 最近一次心跳或注册已确认。
    Active,
    /// 网络或数据库结果暂时无法确认。
    Uncertain,
    /// 本地 lease deadline 已过期。
    Lost,
    /// 调用方已主动停止注册。
    Closed,
}

/// 注册实例的快照。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegistrationSnapshot {
    /// 实例标识。
    pub registration_id: RegistrationId,
    /// 当前状态。
    pub status: RegistrationStatus,
    /// 最近一次确认的注册内容版本。
    pub version: i64,
    /// 本地安全 deadline 的剩余时长。
    pub remaining: Duration,
}
