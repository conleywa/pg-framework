use deadpool_postgres::PoolError;
use pg_framework_core::CoreError;
use thiserror::Error;

use crate::NodeVersion;

/// PostgreSQL 协调节点组件错误。
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CoordinationError {
    /// 配置或输入非法。
    #[error("协调节点配置非法：{0}")]
    InvalidConfig(&'static str),
    /// 节点路径不是规范绝对路径。
    #[error("节点路径非法：{0}")]
    InvalidPath(&'static str),
    /// 节点路径超过配置上限。
    #[error("节点路径超过 {max} 字节上限")]
    PathTooLarge {
        /// 配置的路径上限。
        max: usize,
    },
    /// 节点数据超过配置上限。
    #[error("节点数据超过 {max} 字节上限")]
    DataTooLarge {
        /// 配置的数据上限。
        max: usize,
    },
    /// 无法获取 PostgreSQL 连接。
    #[error("获取 PostgreSQL 连接失败")]
    Pool(#[from] PoolError),
    /// PostgreSQL 操作失败。
    #[error("PostgreSQL 协调节点操作失败")]
    Postgres(#[from] tokio_postgres::Error),
    /// 共享配置失败。
    #[error(transparent)]
    Core(#[from] CoreError),
    /// 安全随机数生成失败。
    #[error("生成协调会话凭证失败：{0}")]
    Random(String),
    /// 数据库操作超时。
    #[error("协调节点操作超时：{0}")]
    Timeout(&'static str),
    /// 当前会话已过期或已被删除。
    #[error("协调会话已失效")]
    SessionLost,
    /// 当前会话已经关闭。
    #[error("协调会话已关闭")]
    SessionClosed,
    /// 会话身份随机碰撞。
    #[error("协调会话身份冲突")]
    SessionConflict,
    /// 节点路径已存在。
    #[error("节点已存在")]
    NodeExists,
    /// 节点不存在或处于失效临时会话下。
    #[error("节点不存在")]
    NodeMissing,
    /// 节点仍包含有效或尚未清理的子节点。
    #[error("节点仍有子节点")]
    NodeNotEmpty,
    /// 临时节点不能作为父节点。
    #[error("临时节点不能拥有子节点")]
    EphemeralParent,
    /// 根节点不能删除。
    #[error("不能删除协调树根节点")]
    RootCannotBeDeleted,
    /// 版本化 CAS 冲突。
    #[error("节点版本冲突：期望 {expected}，实际存在版本 {actual}")]
    VersionConflict {
        /// 调用方期望的版本。
        expected: NodeVersion,
        /// 数据库当前版本。
        actual: NodeVersion,
    },
    /// children 数量超过配置上限。
    #[error("子节点数量超过 {max} 上限")]
    ChildrenLimitExceeded {
        /// 配置的子节点数量上限。
        max: usize,
    },
    /// 父路径的顺序编号已经耗尽。
    #[error("顺序节点编号已耗尽")]
    SequenceExhausted,
    /// 后台任务 join 失败。
    #[error("协调会话后台任务失败")]
    Task(#[from] tokio::task::JoinError),
    /// watch 未能在启动时限内完成 LISTEN 和初始快照。
    #[error("协调节点 watch 启动失败")]
    WatchStartup,
}
