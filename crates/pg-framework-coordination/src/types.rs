use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::NodePath;

/// 一次协调会话的稳定标识。
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct SessionId([u8; 16]);

impl SessionId {
    /// 从 16 字节原始数据构造会话标识。
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// 返回 16 字节原始标识。
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// 将标识编码为 32 个十六进制字符。
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

impl fmt::Debug for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

/// 节点数据版本，从 0 开始，每次成功更新加 1。
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct NodeVersion(i64);

impl NodeVersion {
    /// 构造版本值。
    #[must_use]
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    /// 返回内部版本值。
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl fmt::Display for NodeVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0.to_string())
    }
}

/// 节点生命周期类型。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeKind {
    /// 只有显式删除才会消失。
    Persistent,
    /// 跟随创建它的协调会话 lease 存活。
    Ephemeral,
}

/// 节点创建模式。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreateMode {
    /// 普通持久节点。
    Persistent,
    /// 跟随当前会话 lease 的临时节点。
    Ephemeral,
    /// 在调用方给出的父路径下创建十进制顺序持久节点。
    PersistentSequential,
    /// 在调用方给出的父路径下创建十进制顺序临时节点。
    EphemeralSequential,
}

impl CreateMode {
    /// 返回节点生命周期类型。
    #[must_use]
    pub(crate) const fn kind(self) -> NodeKind {
        match self {
            Self::Persistent | Self::PersistentSequential => NodeKind::Persistent,
            Self::Ephemeral | Self::EphemeralSequential => NodeKind::Ephemeral,
        }
    }

    /// 返回是否使用父路径顺序计数器。
    #[must_use]
    pub(crate) const fn is_sequential(self) -> bool {
        matches!(self, Self::PersistentSequential | Self::EphemeralSequential)
    }

    /// 返回是否要求活动协调会话。
    #[must_use]
    pub(crate) const fn is_ephemeral(self) -> bool {
        matches!(self, Self::Ephemeral | Self::EphemeralSequential)
    }
}

/// 协调树中的一个节点。
#[derive(Clone, Eq, PartialEq)]
pub struct CoordinationNode {
    /// 规范化绝对路径。
    pub path: NodePath,
    /// 节点二进制数据。
    pub data: Arc<[u8]>,
    /// 节点数据版本。
    pub version: NodeVersion,
    /// 生命周期类型。
    pub kind: NodeKind,
    /// 首次创建时间。
    pub created_at: SystemTime,
    /// 最近更新时间。
    pub updated_at: SystemTime,
}

impl fmt::Debug for CoordinationNode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoordinationNode")
            .field("path", &self.path)
            .field("data_len", &self.data.len())
            .field("version", &self.version)
            .field("kind", &self.kind)
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .finish()
    }
}

/// 协调会话当前状态。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionStatus {
    /// 最近一次数据库心跳已确认，且未超过本地安全 deadline。
    Active,
    /// 最近一次心跳结果暂时无法确认，但本地安全 deadline 尚未到期。
    Uncertain,
    /// 数据库 lease 已过期、行已不存在，或本地安全 deadline 已到期。
    Lost,
    /// 调用方已经显式关闭会话。
    Closed,
}

/// 本进程协调会话状态快照。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionSnapshot {
    /// 会话标识。
    pub session_id: SessionId,
    /// 当前状态。
    pub status: SessionStatus,
    /// 最近一次成功心跳对应的会话版本。
    pub version: i64,
    /// 本地安全 deadline 的剩余时长。
    pub remaining: Duration,
}

/// watch 推送的路径快照。
///
/// `node` 是目标路径本身，`children` 是按名称排序的直接子节点。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoordinationSnapshot {
    /// 被 watch 的目标路径。
    pub path: NodePath,
    /// 目标节点；不存在时为 `None`。
    pub node: Option<CoordinationNode>,
    /// 直接子节点名称。
    pub children: Arc<[String]>,
    /// 进程内单调递增快照版本。
    pub revision: u64,
    /// 最近一次刷新失败并保留旧快照时为 true。
    pub stale: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_id_hex_has_fixed_size() {
        let id = SessionId::from_bytes([0xab; 16]);

        assert_eq!(id.to_hex().len(), 32);
        assert_eq!(id.to_string(), id.to_hex());
    }

    #[test]
    fn create_modes_derive_lifetime_and_orderability() {
        assert_eq!(CreateMode::Persistent.kind(), NodeKind::Persistent);
        assert!(CreateMode::Ephemeral.is_ephemeral());
        assert!(CreateMode::PersistentSequential.is_sequential());
        assert!(CreateMode::EphemeralSequential.is_ephemeral());
    }

    #[test]
    fn node_debug_does_not_expose_payload() {
        let node = CoordinationNode {
            path: NodePath::parse("/secret").expect("path"),
            data: Arc::from(b"credential".as_slice()),
            version: NodeVersion::new(3),
            kind: NodeKind::Persistent,
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
        };
        let debug = format!("{node:?}");

        assert!(debug.contains("data_len"));
        assert!(!debug.contains("credential"));
    }
}
