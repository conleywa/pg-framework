//! PostgreSQL 通用连接、key 路由和分片基础能力。

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::{
    BuildError, Manager, ManagerConfig, Pool, PoolError, RecyclingMethod, Runtime,
};
use hmac::{Hmac, Mac, digest::KeyInit};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_postgres::NoTls;

/// 缓存和限频表默认使用的物理分区数。
pub const DEFAULT_SHARD_COUNT: u16 = 64;

/// key 级缓存失效默认使用的 PostgreSQL 通知 channel。
pub const DEFAULT_CACHE_CHANNEL: &str = "pgfw_cache_v1";
/// 缓存 namespace 控制事件默认使用的 PostgreSQL 通知 channel。
pub const DEFAULT_CACHE_CONTROL_CHANNEL: &str = "pgfw_cache_v1_control";
/// 服务发现默认使用的 PostgreSQL 通知 channel。
pub const DEFAULT_DISCOVERY_CHANNEL: &str = "pgfw_discovery_v1";

/// 每个应用进程默认允许使用的 PostgreSQL 最大连接数。
///
/// 缓存、限频和锁组件应共享同一个连接池。建议按
/// `ceil(QPS × P99 延迟) + 突发余量` 估算，并确保所有实例连接上限之和小于
/// PostgreSQL 的 `max_connections`，同时预留管理和主备切换连接。对于普通低延迟
/// 应用进程，默认值 8 是较保守的配置。
pub const DEFAULT_POOL_MAX_SIZE: usize = 8;

/// 默认连接 PostgreSQL 的超时。
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// 默认等待连接池空闲连接的超时。
pub const DEFAULT_POOL_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
/// 默认创建连接池连接的预热超时。
pub const DEFAULT_POOL_CREATE_TIMEOUT: Duration = Duration::from_secs(5);
/// 默认回收连接池连接的预热超时。
pub const DEFAULT_POOL_RECYCLE_TIMEOUT: Duration = Duration::from_secs(2);
/// 默认为每个会话设置的 PostgreSQL statement timeout。
pub const DEFAULT_STATEMENT_TIMEOUT: Duration = Duration::from_secs(5);
/// 默认为每个会话设置的 PostgreSQL lock timeout。
pub const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(2);
/// 组件单次操作建议使用的默认总超时。
pub const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(10);
/// 后台任务优雅关闭的默认总超时。
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);

/// 各组件默认接受的最大 canonical key 长度。
pub const DEFAULT_MAX_KEY_BYTES: usize = 512;

/// SQL schema 强制执行的 key 长度硬上限。
pub const HARD_MAX_KEY_BYTES: usize = 1024;

/// 由 namespace 和 canonical key 派生出的稳定 256 位标识。
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct KeyId([u8; 32]);

impl KeyId {
    /// 派生 key 标识，并避免直接暴露原始 key。
    #[must_use]
    pub fn derive(namespace: &str, canonical_key: &[u8]) -> Self {
        KeyHasher::default().derive(namespace, canonical_key)
    }

    /// 从 32 字节原始数据构造标识。
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// 返回 32 字节原始标识。
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// 以固定长度数组返回标识。
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }

    /// 将标识编码为 64 个小写十六进制字符。
    #[must_use]
    pub fn to_hex(self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(64);
        for byte in self.0 {
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        encoded
    }
}

/// 派生稳定的 cache namespace 控制标识。
#[must_use]
pub fn namespace_id(namespace: &str) -> KeyId {
    KeyId::derive("pgfw_cache_namespace_v1", namespace.as_bytes())
}

/// 稳定的 key 哈希策略。
///
/// 高熵 key 使用 SHA-256；邮箱、手机号、用户名、IP 等低熵 key 使用
/// HMAC-SHA256。
#[derive(Clone, Default)]
pub enum KeyHasher {
    /// 无盐 SHA-256。
    #[default]
    Sha256,
    /// 使用部署密钥的 HMAC-SHA256。
    HmacSha256(Arc<[u8]>),
}

impl KeyHasher {
    /// 创建 HMAC key 哈希器。
    ///
    /// # 错误
    ///
    /// 密钥为空时返回错误。
    pub fn hmac_sha256(secret: impl Into<Arc<[u8]>>) -> Result<Self, CoreError> {
        let secret = secret.into();
        if secret.is_empty() {
            return Err(CoreError::EmptyHmacSecret);
        }
        Ok(Self::HmacSha256(secret))
    }

    /// 派生稳定 key 标识。
    #[must_use]
    pub fn derive(&self, namespace: &str, canonical_key: &[u8]) -> KeyId {
        match self {
            Self::Sha256 => {
                let mut hasher = Sha256::new();
                hasher.update((namespace.len() as u64).to_be_bytes());
                hasher.update(namespace.as_bytes());
                hasher.update((canonical_key.len() as u64).to_be_bytes());
                hasher.update(canonical_key);
                KeyId(hasher.finalize().into())
            }
            Self::HmacSha256(secret) => {
                let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(secret)
                    .expect("HMAC accepts keys of every length");
                mac.update(&(namespace.len() as u64).to_be_bytes());
                mac.update(namespace.as_bytes());
                mac.update(&(canonical_key.len() as u64).to_be_bytes());
                mac.update(canonical_key);
                KeyId(mac.finalize().into_bytes().into())
            }
        }
    }
}

impl fmt::Debug for KeyHasher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sha256 => formatter.write_str("KeyHasher::Sha256"),
            Self::HmacSha256(_) => formatter.write_str("KeyHasher::HmacSha256([REDACTED])"),
        }
    }
}

impl AsRef<[u8]> for KeyId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for KeyId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

/// 稳定的 key 到本地 stripe 路由策略。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardRouter {
    shard_count: u16,
}

impl ShardRouter {
    /// 使用非零 stripe 数量创建路由器。
    ///
    /// # 错误
    ///
    /// `shard_count` 为 0 时返回错误。
    pub fn new(shard_count: u16) -> Result<Self, CoreError> {
        if shard_count == 0 {
            return Err(CoreError::InvalidShardCount);
        }
        Ok(Self { shard_count })
    }

    /// 返回配置的 stripe 数量。
    #[must_use]
    pub const fn shard_count(self) -> u16 {
        self.shard_count
    }

    /// 使用标识前 8 字节计算本地 stripe。
    #[must_use]
    pub fn shard_id(self, key: KeyId) -> u16 {
        let first = key
            .as_bytes()
            .get(..8)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_be_bytes)
            .unwrap_or_default();
        u16::try_from(first % u64::from(self.shard_count)).unwrap_or_default()
    }
}

impl Default for ShardRouter {
    fn default() -> Self {
        Self {
            shard_count: DEFAULT_SHARD_COUNT,
        }
    }
}

/// 将时长转换为 PostgreSQL 可用的有符号毫秒数。
///
/// # 错误
///
/// 时长无法用 `i64` 表示时返回错误。
pub fn duration_millis(duration: Duration) -> Result<i64, CoreError> {
    i64::try_from(duration.as_millis()).map_err(|_| CoreError::DurationTooLarge)
}

/// 按运行时配置和 schema 硬上限校验 canonical key 长度。
///
/// # 错误
///
/// key 为空、过大或配置上限非法时返回错误。
pub fn validate_key_size(key_len: usize, max_key_bytes: usize) -> Result<(), CoreError> {
    if max_key_bytes == 0 || max_key_bytes > HARD_MAX_KEY_BYTES {
        return Err(CoreError::InvalidMaxKeyBytes);
    }
    if key_len == 0 {
        return Err(CoreError::EmptyKey);
    }
    if key_len > max_key_bytes {
        return Err(CoreError::KeyTooLarge { max: max_key_bytes });
    }
    Ok(())
}

/// 构建不启用 TLS 的最小 Tokio PostgreSQL 连接池。
///
/// 需要 TLS 的应用应自行构建 [`deadpool_postgres::Pool`]，再传给组件构造函数。
///
/// # 错误
///
/// 连接池大小非法或构建失败时返回错误。
pub fn build_pool(
    mut postgres_config: tokio_postgres::Config,
    max_size: usize,
) -> Result<Pool, CoreError> {
    if max_size == 0 {
        return Err(CoreError::InvalidPoolSize);
    }

    if postgres_config.get_connect_timeout().is_none() {
        postgres_config.connect_timeout(DEFAULT_CONNECT_TIMEOUT);
    }
    let mut options = postgres_config.get_options().unwrap_or_default().to_owned();
    if !options.contains("statement_timeout") {
        if !options.is_empty() {
            options.push(' ');
        }
        options.push_str(&format!(
            "-c statement_timeout={}",
            DEFAULT_STATEMENT_TIMEOUT.as_millis()
        ));
    }
    if !options.contains("lock_timeout") {
        if !options.is_empty() {
            options.push(' ');
        }
        options.push_str(&format!(
            "-c lock_timeout={}",
            DEFAULT_LOCK_TIMEOUT.as_millis()
        ));
    }
    postgres_config.options(options);

    let manager = Manager::from_config(
        postgres_config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    Pool::builder(manager)
        .max_size(max_size)
        .wait_timeout(Some(DEFAULT_POOL_WAIT_TIMEOUT))
        .create_timeout(Some(DEFAULT_POOL_CREATE_TIMEOUT))
        .recycle_timeout(Some(DEFAULT_POOL_RECYCLE_TIMEOUT))
        .runtime(Runtime::Tokio1)
        .build()
        .map_err(CoreError::PoolBuild)
}

/// 使用 [`DEFAULT_POOL_MAX_SIZE`] 构建 PostgreSQL 连接池。
///
/// # 错误
///
/// 无法构建连接池时返回错误。
pub fn build_default_pool(postgres_config: tokio_postgres::Config) -> Result<Pool, CoreError> {
    build_pool(postgres_config, DEFAULT_POOL_MAX_SIZE)
}

/// 校验组件 schema generation 和物理分区数。
///
/// # 错误
///
/// PostgreSQL 查询失败、元数据缺失或配置不匹配时返回错误。
pub async fn verify_schema(
    pool: &Pool,
    component: &str,
    schema_generation: &str,
    shard_count: u16,
) -> Result<(), CoreError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT schema_generation, shard_count
             FROM pgfw_schema_meta
             WHERE component = $1",
            &[&component],
        )
        .await?;
    let Some(row) = row else {
        return Err(CoreError::SchemaMissing {
            component: component.to_owned(),
        });
    };
    let actual_generation: String = row.get(0);
    let actual_shards: i32 = row.get(1);
    let expected_shards = i32::from(shard_count);
    let table = match component {
        "cache" => Some("pgfw_cache_entry"),
        "ratelimit" => Some("pgfw_rate_limit_counter"),
        "lock" => Some("pgfw_lock"),
        _ => None,
    };
    let Some(table) = table else {
        return Err(CoreError::SchemaMismatch {
            component: component.to_owned(),
            expected_generation: schema_generation.to_owned(),
            actual_generation,
            expected_shards,
            actual_shards,
        });
    };
    let physical_partitions = client
        .query_one(
            "SELECT COUNT(*)::bigint
             FROM pg_inherits
             WHERE inhparent = to_regclass($1)",
            &[&table],
        )
        .await?
        .get::<_, i64>(0);
    let physical_partitions = i32::try_from(physical_partitions).unwrap_or(i32::MAX);
    if actual_generation != schema_generation
        || actual_shards != expected_shards
        || (shard_count > 0 && physical_partitions != expected_shards)
    {
        return Err(CoreError::SchemaMismatch {
            component: component.to_owned(),
            expected_generation: schema_generation.to_owned(),
            actual_generation,
            expected_shards,
            actual_shards: if shard_count > 0 {
                physical_partitions
            } else {
                actual_shards
            },
        });
    }
    Ok(())
}

/// pg-framework 组件共享错误。
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CoreError {
    /// stripe 数量为 0，无法路由 key。
    #[error("stripe 数量必须大于 0")]
    InvalidShardCount,
    /// 连接池容量不能为 0。
    #[error("PostgreSQL 连接池大小必须大于 0")]
    InvalidPoolSize,
    /// 时长无法表示为 PostgreSQL 毫秒数。
    #[error("时长过大")]
    DurationTooLarge,
    /// deadpool manager 构建失败。
    #[error("构建 PostgreSQL 连接池失败")]
    PoolBuild(#[source] BuildError),
    /// HMAC key 哈希器不能使用空密钥。
    #[error("HMAC key 哈希密钥不能为空")]
    EmptyHmacSecret,
    /// 配置的最大 key 长度超出 schema 范围。
    #[error("最大 key 字节数必须在 1 到 1024 之间")]
    InvalidMaxKeyBytes,
    /// canonical key 不能为空。
    #[error("canonical key 不能为空")]
    EmptyKey,
    /// canonical key 超过配置上限。
    #[error("canonical key 超过 {max} 字节上限")]
    KeyTooLarge {
        /// 配置的最大 key 长度。
        max: usize,
    },
    /// 无法从连接池获取连接。
    #[error("获取 PostgreSQL 连接失败")]
    Pool(#[from] PoolError),
    /// schema 元数据查询失败。
    #[error("PostgreSQL schema 校验失败")]
    Postgres(#[from] tokio_postgres::Error),
    /// schema 元数据不存在。
    #[error("组件 {component} 的 schema 元数据不存在")]
    SchemaMissing {
        /// 组件名称。
        component: String,
    },
    /// schema generation 或物理分区数不匹配。
    #[error(
        "组件 {component} 的 schema 不匹配：期望 {expected_generation}/{expected_shards}，实际 {actual_generation}/{actual_shards}"
    )]
    SchemaMismatch {
        /// 组件名称。
        component: String,
        /// 期望 schema generation。
        expected_generation: String,
        /// 实际 schema generation。
        actual_generation: String,
        /// 期望分区数。
        expected_shards: i32,
        /// 实际分区数。
        actual_shards: i32,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_derivation_is_stable_and_namespace_scoped() {
        let first = KeyId::derive("tenant:a", b"orders/1");
        let second = KeyId::derive("tenant:a", b"orders/1");
        let other_namespace = KeyId::derive("tenant:b", b"orders/1");

        assert_eq!(first, second);
        assert_ne!(first, other_namespace);
        assert_eq!(first.to_hex().len(), 64);
    }

    #[test]
    fn shard_routing_is_deterministic() {
        let router = ShardRouter::default();
        let key = KeyId::derive("cache", b"key");

        assert_eq!(router.shard_id(key), router.shard_id(key));
        assert!(router.shard_id(key) < DEFAULT_SHARD_COUNT);
    }

    #[test]
    fn rejects_zero_shards_and_zero_pool_size() {
        assert!(matches!(
            ShardRouter::new(0),
            Err(CoreError::InvalidShardCount)
        ));

        let config = tokio_postgres::Config::new();
        assert!(matches!(
            build_pool(config, 0),
            Err(CoreError::InvalidPoolSize)
        ));
    }

    #[test]
    fn default_pool_size_is_non_zero() {
        assert_eq!(DEFAULT_POOL_MAX_SIZE, 8);
        let pool = build_default_pool(tokio_postgres::Config::new()).expect("pool");
        let timeouts = pool.timeouts();
        assert_eq!(timeouts.wait, Some(DEFAULT_POOL_WAIT_TIMEOUT));
        assert_eq!(timeouts.create, Some(DEFAULT_POOL_CREATE_TIMEOUT));
        assert_eq!(timeouts.recycle, Some(DEFAULT_POOL_RECYCLE_TIMEOUT));
    }

    #[test]
    fn hmac_hasher_is_secret_dependent() {
        let first = KeyHasher::hmac_sha256(vec![1, 2, 3]).expect("secret");
        let second = KeyHasher::hmac_sha256(vec![3, 2, 1]).expect("secret");

        assert_ne!(
            first.derive("namespace", b"low-entropy"),
            second.derive("namespace", b"low-entropy")
        );
        assert!(!format!("{first:?}").contains('1'));
    }

    #[test]
    fn key_size_validation_enforces_runtime_and_schema_limits() {
        assert!(validate_key_size(512, 512).is_ok());
        assert!(matches!(
            validate_key_size(0, 512),
            Err(CoreError::EmptyKey)
        ));
        assert!(matches!(
            validate_key_size(513, 512),
            Err(CoreError::KeyTooLarge { max: 512 })
        ));
        assert!(matches!(
            validate_key_size(1, 2048),
            Err(CoreError::InvalidMaxKeyBytes)
        ));
    }
}
