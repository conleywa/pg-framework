use deadpool_postgres::PoolError;
use pg_framework_core::CoreError;
use thiserror::Error;

/// 服务注册与发现错误。
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DiscoveryError {
    /// 配置或输入非法。
    #[error("服务发现配置非法：{0}")]
    InvalidConfig(&'static str),
    /// endpoint 超过配置上限。
    #[error("服务 endpoint 超过上限")]
    EndpointTooLarge,
    /// metadata 超过配置上限。
    #[error("服务 metadata 超过上限")]
    MetadataTooLarge,
    /// 无法获取 PostgreSQL 连接。
    #[error("获取 PostgreSQL 连接失败")]
    Pool(#[from] PoolError),
    /// PostgreSQL 操作失败。
    #[error("PostgreSQL 服务发现操作失败")]
    Postgres(#[from] tokio_postgres::Error),
    /// 共享配置失败。
    #[error(transparent)]
    Core(#[from] CoreError),
    /// 安全随机数生成失败。
    #[error("生成服务实例 token 失败：{0}")]
    Random(String),
    /// 操作超时。
    #[error("服务发现操作超时：{0}")]
    Timeout(&'static str),
    /// 注册身份已被其他有效 owner 占用。
    #[error("服务注册身份发生冲突")]
    RegistrationConflict,
    /// 后台任务 join 失败。
    #[error("服务注册后台任务失败")]
    Task(#[from] tokio::task::JoinError),
    /// watch 未能在超时前建立 LISTEN 和初始快照。
    #[error("服务发现 watch 启动失败")]
    WatchStartup,
}
