use deadpool_postgres::PoolError;
use pg_framework_core::CoreError;
use thiserror::Error;

/// 分布式锁组件返回的错误。
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum LockError {
    /// 锁配置或输入非法。
    #[error("锁配置非法：{0}")]
    InvalidConfig(&'static str),
    /// 无法获取 PostgreSQL 连接。
    #[error("获取 PostgreSQL 连接失败")]
    Pool(#[from] PoolError),
    /// PostgreSQL 返回错误。
    #[error("PostgreSQL 锁操作失败")]
    Postgres(#[from] tokio_postgres::Error),
    /// 共享配置失败。
    #[error(transparent)]
    Core(#[from] CoreError),
    /// 安全 owner token 生成失败。
    #[error("生成锁 owner token 失败：{0}")]
    Random(String),
    /// 调用方的锁获取 deadline 已到期。
    #[error("获取锁超时")]
    TimedOut,
    /// 无法归并已提交的数据库操作结果。
    #[error("锁操作 {operation} 的结果未知")]
    OutcomeUnknown {
        /// 无法归并结果的操作名称。
        operation: &'static str,
    },
    /// 释放前租约已丢失。
    #[error("锁租约已丢失")]
    AlreadyLost,
    /// 操作执行超时。
    #[error("锁操作超时：{0}")]
    Timeout(&'static str),
    /// 重入深度达到配置上限。
    #[error("锁重入深度超过上限 {max}")]
    ReentrantLimitExceeded {
        /// 配置的最大重入深度。
        max: usize,
    },
}
