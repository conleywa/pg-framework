use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use deadpool_postgres::Pool;
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
    pub(crate) l1: Box<[Mutex<L1Shard>]>,
    pub(crate) pending: Box<[Mutex<HashMap<KeyId, PendingEntry>>]>,
    pub(crate) pending_keys: AtomicUsize,
    pub(crate) pending_bytes: AtomicUsize,
    pub(crate) generation: AtomicUsize,
    pub(crate) flush_gate: tokio::sync::Mutex<()>,
    pub(crate) flush_notify: Notify,
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
            l1,
            pending,
            pending_keys: AtomicUsize::new(0),
            pending_bytes: AtomicUsize::new(0),
            generation: AtomicUsize::new(1),
            flush_gate: tokio::sync::Mutex::new(()),
            flush_notify: Notify::new(),
        })
    }

    fn validate_config(config: &CacheConfig) -> Result<(), CacheError> {
        if config.namespace.is_empty() {
            return Err(CacheError::InvalidConfig("namespace is empty"));
        }
        if config.notification_channel.is_empty()
            || config.notification_channel.len() > 63
            || !config
                .notification_channel
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(CacheError::InvalidConfig(
                "notification channel must be a safe identifier up to 63 bytes",
            ));
        }
        if config.max_value_bytes == 0 {
            return Err(CacheError::InvalidConfig("max_value_bytes is zero"));
        }
        if config.l1_capacity == 0 {
            return Err(CacheError::InvalidConfig("l1_capacity is zero"));
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
        Ok(())
    }

    pub(crate) fn pending_for(&self, key: KeyId) -> Option<PendingEntry> {
        let shard = self.shard_id(key);
        self.pending
            .get(usize::from(shard))
            .and_then(|lock| lock.lock().ok()?.get(&key).cloned())
    }

    pub(crate) fn key_id(&self, key: &[u8]) -> Result<KeyId, CacheError> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WriteMode;
    use crate::state::Mutation;
    use pg_framework_core::build_pool;

    fn test_cache() -> Cache {
        let pool = build_pool(tokio_postgres::Config::new(), 1).expect("pool config");
        let config = CacheConfig::new("test").with_write_mode(WriteMode::Buffered);
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
        let mut config = CacheConfig::new("test").with_write_mode(WriteMode::Buffered);
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
}
