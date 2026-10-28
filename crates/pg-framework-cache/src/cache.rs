use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use deadpool_postgres::{GenericClient, Pool};
use pg_framework_core::{KeyId, ShardRouter, validate_key_size};
use tokio::sync::Notify;

use crate::config::CacheConfig;
use crate::error::CacheError;
use crate::state::{L1Shard, PendingEntry};

/// 带分片本地读缓存和有界写缓冲的 PostgreSQL 缓存。
pub struct Cache {
    pub(crate) pool: Pool,
    pub(crate) config: CacheConfig,
    pub(crate) router: ShardRouter,
    pub(crate) l1_capacity_per_shard: usize,
    pub(crate) l1_bytes_per_shard: usize,
    pub(crate) l1: Box<[Mutex<L1Shard>]>,
    pub(crate) pending: Box<[Mutex<HashMap<KeyId, PendingEntry>>]>,
    pub(crate) pending_keys: AtomicUsize,
    pub(crate) pending_bytes: AtomicUsize,
    pub(crate) generation: AtomicUsize,
    pub(crate) flush_gate: tokio::sync::Mutex<()>,
    pub(crate) clear_gate: tokio::sync::Mutex<()>,
    pub(crate) flush_notify: Notify,
    pub(crate) ready: AtomicBool,
    pub(crate) clearing: AtomicBool,
    pub(crate) namespace_epoch: AtomicU64,
}

impl Cache {
    /// 基于调用方管理的共享 PostgreSQL 连接池创建缓存。
    ///
    /// # 错误
    ///
    /// 配置非法时返回错误。
    pub fn new(pool: Pool, config: CacheConfig) -> Result<Self, CacheError> {
        Self::validate_config(&config)?;
        let router = ShardRouter::new(config.shard_count)?;
        let shard_count = usize::from(config.shard_count);
        let l1_capacity_per_shard = config.l1_capacity.div_ceil(shard_count).max(1);
        let l1_bytes_per_shard = config.l1_max_bytes.div_ceil(shard_count).max(1);
        let l1 = (0..shard_count)
            .map(|_| Mutex::new(L1Shard::default()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let pending = (0..shard_count)
            .map(|_| Mutex::new(HashMap::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Ok(Self {
            pool,
            config,
            router,
            l1_capacity_per_shard,
            l1_bytes_per_shard,
            l1,
            pending,
            pending_keys: AtomicUsize::new(0),
            pending_bytes: AtomicUsize::new(0),
            generation: AtomicUsize::new(1),
            flush_gate: tokio::sync::Mutex::new(()),
            clear_gate: tokio::sync::Mutex::new(()),
            flush_notify: Notify::new(),
            ready: AtomicBool::new(false),
            clearing: AtomicBool::new(false),
            namespace_epoch: AtomicU64::new(0),
        })
    }

    /// 创建缓存并立即校验数据库 schema generation。
    ///
    /// # 错误
    ///
    /// 配置非法、schema 缺失或不匹配时返回错误。
    pub async fn connect(pool: Pool, config: CacheConfig) -> Result<Self, CacheError> {
        let cache = Self::new(pool, config)?;
        cache.verify_schema().await?;
        Ok(cache)
    }

    fn validate_config(config: &CacheConfig) -> Result<(), CacheError> {
        if config.namespace.is_empty() {
            return Err(CacheError::InvalidConfig("namespace is empty"));
        }
        if config.schema_generation.is_empty() || config.physical_partition_count == 0 {
            return Err(CacheError::InvalidConfig(
                "schema generation and partition count are invalid",
            ));
        }
        if !valid_channel(&config.notification_channel)
            || !valid_channel(&config.control_channel)
            || config.notification_channel == config.control_channel
        {
            return Err(CacheError::InvalidConfig(
                "notification and control channels must be distinct safe identifiers up to 63 bytes",
            ));
        }
        if config.max_value_bytes == 0 {
            return Err(CacheError::InvalidConfig("max_value_bytes is zero"));
        }
        if config.l1_capacity == 0 {
            return Err(CacheError::InvalidConfig("l1_capacity is zero"));
        }
        if config.l1_max_bytes == 0 {
            return Err(CacheError::InvalidConfig("l1_max_bytes is zero"));
        }
        if config.recovery_rows_per_shard == 0 {
            return Err(CacheError::InvalidConfig("recovery_rows_per_shard is zero"));
        }
        if !config.buffer.validate() {
            return Err(CacheError::InvalidConfig("buffer limits are invalid"));
        }
        validate_key_size(1, config.max_key_bytes)?;
        if config.default_ttl.is_some_and(|ttl| ttl.is_zero()) {
            return Err(CacheError::InvalidConfig("default TTL must be positive"));
        }
        if config.operation_timeout.is_zero() || config.shutdown_timeout.is_zero() {
            return Err(CacheError::InvalidConfig("cache timeouts must be positive"));
        }
        Ok(())
    }

    pub(crate) fn pending_for(&self, key: KeyId) -> Option<PendingEntry> {
        let shard = self.shard_id(key);
        self.pending
            .get(usize::from(shard))
            .and_then(|lock| lock.lock().ok()?.get(&key).cloned())
    }

    pub(crate) fn key_id(&self, key: &[u8]) -> Result<KeyId, CacheError> {
        if self.config.require_listener_ready && !self.ready.load(Ordering::Acquire) {
            return Err(CacheError::Recovering);
        }
        if self.clearing.load(Ordering::Acquire) {
            return Err(CacheError::NamespaceClearing);
        }
        validate_key_size(key.len(), self.config.max_key_bytes)?;
        Ok(self.config.key_hasher.derive(&self.config.namespace, key))
    }

    pub(crate) fn stored_raw_key(&self, key: &[u8]) -> Option<Arc<[u8]>> {
        self.config.store_raw_keys.then(|| Arc::from(key))
    }

    pub(crate) fn shard_id(&self, key: KeyId) -> u16 {
        self.router.shard_id(key)
    }

    pub(crate) fn next_generation(&self) -> u64 {
        u64::try_from(self.generation.fetch_add(1, Ordering::Relaxed)).unwrap_or(u64::MAX)
    }

    /// 校验运行时配置与数据库 schema 元数据一致。
    ///
    /// # 错误
    ///
    /// schema 缺失、generation 或分区数不匹配时返回错误。
    pub async fn verify_schema(&self) -> Result<(), CacheError> {
        self.timed("schema", async {
            pg_framework_core::verify_schema(
                &self.pool,
                "cache",
                &self.config.schema_generation,
                self.config.physical_partition_count,
            )
            .await?;
            let client = self.pool.get().await?;
            let namespace_table_exists: bool = client
                .query_one(
                    "SELECT to_regclass('pgfw_cache_namespace') IS NOT NULL",
                    &[],
                )
                .await?
                .get(0);
            if !namespace_table_exists {
                return Err(CacheError::InvalidConfig(
                    "cache namespace table does not exist",
                ));
            }
            Ok(())
        })
        .await?;
        Ok(())
    }

    pub(crate) async fn ensure_namespace_writable<C>(&self, client: &C) -> Result<(), CacheError>
    where
        C: GenericClient,
    {
        client
            .execute(
                "INSERT INTO pgfw_cache_namespace (namespace)
                 VALUES ($1)
                 ON CONFLICT (namespace) DO NOTHING",
                &[&self.config.namespace],
            )
            .await?;
        let row = client
            .query_opt(
                "SELECT clearing
                 FROM pgfw_cache_namespace
                 WHERE namespace = $1
                 FOR SHARE",
                &[&self.config.namespace],
            )
            .await?;
        if row.is_some_and(|row| row.get::<_, bool>(0)) {
            return Err(CacheError::NamespaceClearing);
        }
        Ok(())
    }

    pub(crate) async fn ensure_namespace_writable_tokio(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
    ) -> Result<(), CacheError> {
        transaction
            .execute(
                "INSERT INTO pgfw_cache_namespace (namespace)
                 VALUES ($1)
                 ON CONFLICT (namespace) DO NOTHING",
                &[&self.config.namespace],
            )
            .await?;
        let row = transaction
            .query_opt(
                "SELECT clearing
                 FROM pgfw_cache_namespace
                 WHERE namespace = $1
                 FOR SHARE",
                &[&self.config.namespace],
            )
            .await?;
        if row.is_some_and(|row| row.get::<_, bool>(0)) {
            return Err(CacheError::NamespaceClearing);
        }
        Ok(())
    }

    pub(crate) async fn timed<T, F>(
        &self,
        operation: &'static str,
        future: F,
    ) -> Result<T, CacheError>
    where
        F: Future<Output = Result<T, CacheError>>,
    {
        tokio::time::timeout(self.config.operation_timeout, future)
            .await
            .map_err(|_| CacheError::Timeout(operation))?
    }

    /// 返回缓存是否已完成 LISTEN 和首次恢复。
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// 返回 namespace 是否正在清理。
    #[must_use]
    pub fn is_clearing(&self) -> bool {
        self.clearing.load(Ordering::Acquire)
    }

    pub(crate) fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }

    pub(crate) fn set_clearing(&self, clearing: bool) {
        self.clearing.store(clearing, Ordering::Release);
    }

    pub(crate) fn apply_namespace_state(&self, clearing: bool, epoch: u64) {
        let previous = self.namespace_epoch.swap(epoch, Ordering::AcqRel);
        if previous != 0 && previous != epoch {
            self.discard_pending();
        }
        self.set_clearing(clearing);
        self.clear_l1();
    }
}

fn valid_channel(channel: &str) -> bool {
    !channel.is_empty()
        && channel.len() <= 63
        && channel
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BufferConfig;
    use crate::config::WriteMode;
    use crate::state::Mutation;
    use pg_framework_core::build_pool;

    fn test_cache() -> Cache {
        let pool = build_pool(tokio_postgres::Config::new(), 1).expect("pool config");
        let config = CacheConfig::new("test")
            .with_write_mode(WriteMode::Buffered)
            .with_listener_requirement(false);
        Cache::new(pool, config).expect("cache config")
    }

    #[tokio::test]
    async fn repeated_buffered_writes_coalesce_to_latest_value() {
        let cache = test_cache();

        for index in 0..100u32 {
            cache
                .set(b"hot", index.to_be_bytes().to_vec(), None)
                .await
                .expect("enqueue");
        }

        let stats = cache.stats();
        assert_eq!(stats.pending_keys, 1);
        let key = cache.key_id(b"hot").expect("key");
        let pending = cache.pending_for(key).expect("pending write");
        let Mutation::Set { value, .. } = pending.mutation else {
            panic!("expected set mutation");
        };
        assert_eq!(value.as_ref(), &99u32.to_be_bytes());
    }

    #[test]
    fn notification_payload_round_trips_key_id() {
        let key = KeyId::derive("test", b"key");
        let payload = key.to_hex();

        assert_eq!(crate::maintenance::key_id_from_hex(&payload), Some(key));
        assert!(crate::maintenance::key_id_from_hex("short").is_none());
    }

    #[tokio::test]
    async fn optional_raw_key_is_retained_for_management() {
        let pool = build_pool(tokio_postgres::Config::new(), 1).expect("pool");
        let mut config = CacheConfig::new("test")
            .with_write_mode(WriteMode::Buffered)
            .with_listener_requirement(false);
        config.store_raw_keys = true;
        let cache = Cache::new(pool, config).expect("cache");

        cache
            .set(b"original-key", vec![1], None)
            .await
            .expect("enqueue");

        let key = cache.key_id(b"original-key").expect("key");
        let pending = cache.pending_for(key).expect("pending");
        assert_eq!(pending.raw_key.as_deref(), Some(b"original-key".as_slice()));
    }

    #[tokio::test]
    async fn concurrent_buffer_admission_respects_hard_key_limit() {
        let pool = build_pool(tokio_postgres::Config::new(), 1).expect("pool");
        let mut config = CacheConfig::new("test")
            .with_write_mode(WriteMode::Buffered)
            .with_listener_requirement(false);
        config.shard_count = 8;
        config.buffer = BufferConfig {
            max_pending_keys: 5,
            max_pending_bytes: 1024,
            max_batch_keys: 5,
            max_batch_bytes: 1024,
            ..BufferConfig::default()
        };
        let cache = Arc::new(Cache::new(pool, config).expect("cache"));
        let mut tasks = Vec::new();
        for index in 0u32..64 {
            let cache = Arc::clone(&cache);
            tasks.push(tokio::spawn(async move {
                cache.set(&index.to_be_bytes(), vec![1], None).await
            }));
        }
        for task in tasks {
            let _ = task.await.expect("task");
        }

        let stats = cache.stats();
        assert!(stats.pending_keys <= 5);
        assert!(stats.pending_bytes <= 1024);
    }

    #[tokio::test]
    async fn namespace_control_event_toggles_clearing_state() {
        let cache = test_cache();
        let namespace = pg_framework_core::namespace_id("test").to_hex();

        cache
            .namespace_notification(&format!("1{namespace}"))
            .expect("start event");
        assert!(cache.is_clearing());

        cache
            .namespace_notification(&format!("0{namespace}"))
            .expect("finish event");
        assert!(!cache.is_clearing());
    }

    #[tokio::test]
    async fn strict_cache_rejects_access_before_listener_ready() {
        let pool = build_pool(tokio_postgres::Config::new(), 1).expect("pool");
        let config = CacheConfig::new("test").with_write_mode(WriteMode::Buffered);
        let cache = Cache::new(pool, config).expect("cache");

        assert!(matches!(
            cache.set(b"key", vec![1], None).await,
            Err(CacheError::Recovering)
        ));

        cache.set_ready(true);
        assert!(cache.set(b"key", vec![1], None).await.is_ok());
    }

    #[tokio::test]
    async fn namespace_epoch_change_discards_old_pending_writes() {
        let cache = test_cache();
        cache.apply_namespace_state(false, 1);
        cache.set(b"stale", vec![1], None).await.expect("enqueue");
        assert_eq!(cache.stats().pending_keys, 1);

        cache.apply_namespace_state(false, 2);

        assert_eq!(cache.stats().pending_keys, 0);
    }
}
