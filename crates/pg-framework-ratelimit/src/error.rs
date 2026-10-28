use deadpool_postgres::PoolError;
use pg_framework_core::CoreError;
use thiserror::Error;

/// 限频器返回的错误。
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RateLimitError {
    /// 配置或输入值非法。
    #[error("限频配置非法：{0}")]
    InvalidConfig(&'static str),
    /// 无法获取 PostgreSQL 连接。
    #[error("获取 PostgreSQL 连接失败")]
    Pool(#[from] PoolError),
    /// PostgreSQL 返回错误。
    #[error("PostgreSQL 限频操作失败")]
    Postgres(#[from] tokio_postgres::Error),
    /// 共享配置失败。
    #[error(transparent)]
    Core(#[from] CoreError),
    /// 操作执行超时。
    #[error("限频操作超时：{0}")]
    Timeout(&'static str),
}
