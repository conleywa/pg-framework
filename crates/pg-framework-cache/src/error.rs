use deadpool_postgres::PoolError;
use pg_framework_core::CoreError;
use thiserror::Error;

/// 共享缓存返回的错误。
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CacheError {
    /// 缓存配置非法。
    #[error("缓存配置非法：{0}")]
    InvalidConfig(&'static str),
    /// value 超过配置上限。
    #[error("缓存 value 超过配置上限")]
    ValueTooLarge,
    /// 有界写缓冲已满。
    #[error("缓存写缓冲已满")]
    BufferFull,
    /// 无法获取 PostgreSQL 连接。
    #[error("获取 PostgreSQL 连接失败")]
    Pool(#[from] PoolError),
    /// PostgreSQL 返回错误。
    #[error("PostgreSQL 缓存操作失败")]
    Postgres(#[from] tokio_postgres::Error),
    /// 后台任务 join 失败。
    #[error("缓存后台任务失败")]
    Task(#[from] tokio::task::JoinError),
    /// 共享 key 或连接池配置失败。
    #[error(transparent)]
    Core(#[from] CoreError),
    /// 同步操作超过配置超时。
    #[error("缓存操作超时：{0}")]
    Timeout(&'static str),
    /// listener 尚未完成首次订阅和恢复。
    #[error("缓存仍在恢复，当前不可读写")]
    Recovering,
    /// namespace 正在执行清理。
    #[error("缓存 namespace 正在清理")]
    NamespaceClearing,
    /// listener 未能在超时前完成首次恢复。
    #[error("缓存 listener 启动或首次恢复失败")]
    ListenerStartup,
}
