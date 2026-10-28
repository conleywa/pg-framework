use std::sync::Arc;
use std::time::{Duration, Instant};

use pg_framework_core::KeyId;

use crate::Cache;
use crate::config::ReadMode;
use crate::error::CacheError;
use crate::state::{L1Entry, L2Entry, Mutation};
use crate::types::TtlResult;

impl Cache {
    /// 使用指定一致性模式读取 value。
    ///
    /// # 错误
    ///
    /// 存储失败或 pending 写入刷盘失败时返回错误。
    pub async fn get(&self, key: &[u8], mode: ReadMode) -> Result<Option<Arc<[u8]>>, CacheError> {
        let key_id = self.key_id(key)?;
        if mode == ReadMode::Shared {
            if self.pending_for(key_id).is_some() {
                self.flush().await?;
            }
            return self
                .timed("shared read", self.read_l2(key_id))
                .await
                .map(|entry| entry.map(|item| item.value));
        }

        if let Some(entry) = self.pending_for(key_id) {
            return self.read_with_pending(key_id, entry.mutation).await;
        }
        if let Some(value) = self.read_l1(key_id) {
            return Ok(Some(value));
        }
        let entry = self.timed("read", self.load_l2_into_l1(key_id)).await?;
        Ok(entry.map(|item| item.value))
    }

    /// 返回 key 的剩余 TTL。
    ///
    /// # 错误
    ///
    /// 存储失败时返回错误。
    pub async fn ttl(&self, key: &[u8]) -> Result<TtlResult, CacheError> {
        let key_id = self.key_id(key)?;
        if let Some(entry) = self.pending_for(key_id) {
            return Ok(match entry.mutation {
                Mutation::Delete { .. } => TtlResult::Missing,
                Mutation::Persist { .. } => TtlResult::Persistent,
                Mutation::Set {
                    ttl_ms,
                    local_deadline,
                    ..
                } => match (ttl_ms, local_deadline) {
                    (None, _) => TtlResult::Persistent,
                    (Some(_), Some(deadline)) => deadline
                        .checked_duration_since(Instant::now())
                        .map_or(TtlResult::Missing, TtlResult::Expiring),
                    (Some(_), None) => TtlResult::Missing,
                },
                Mutation::Expire { local_deadline, .. } => local_deadline
                    .checked_duration_since(Instant::now())
                    .map_or(TtlResult::Missing, TtlResult::Expiring),
            });
        }
        if let Some(result) = self.ttl_from_l1(key_id) {
            return Ok(result);
        }
        self.timed("ttl", self.ttl_from_l2(key_id)).await
    }

    async fn read_with_pending(
        &self,
        key: KeyId,
        mutation: Mutation,
    ) -> Result<Option<Arc<[u8]>>, CacheError> {
        match mutation {
            Mutation::Set {
                value,
                local_deadline,
                ..
            } => {
                if local_deadline.is_some_and(|deadline| deadline <= Instant::now()) {
                    return Ok(None);
                }
                Ok(Some(value))
            }
            Mutation::Delete { .. } => Ok(None),
            Mutation::Expire { local_deadline, .. } => {
                if local_deadline <= Instant::now() {
                    return Ok(None);
                }
                if let Some(value) = self.read_l1(key) {
                    return Ok(Some(value));
                }
                Ok(self
                    .timed("read expire", self.load_l2_into_l1(key))
                    .await?
                    .map(|item| item.value))
            }
            Mutation::Persist { .. } => {
                if let Some(value) = self.read_l1(key) {
                    return Ok(Some(value));
                }
                Ok(self
                    .timed("read persist", self.load_l2_into_l1(key))
                    .await?
                    .map(|item| item.value))
            }
        }
    }

    async fn load_l2_into_l1(&self, key: KeyId) -> Result<Option<L2Entry>, CacheError> {
        let shard = self.shard_id(key);
        let epoch = self
            .l1
            .get(usize::from(shard))
            .and_then(|lock| lock.lock().ok().map(|state| state.epoch))
            .unwrap_or_default();
        let Some(entry) = self.read_l2(key).await? else {
            return Ok(None);
        };
        if self.pending_for(key).is_none() {
            self.insert_if_epoch(shard, epoch, key, &entry);
        }
        Ok(Some(entry))
    }

    async fn read_l2(&self, key: KeyId) -> Result<Option<L2Entry>, CacheError> {
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                "SELECT value, version,
                    CASE WHEN expires_at IS NULL THEN NULL
                         ELSE GREATEST(
                            0,
                            CEIL(EXTRACT(EPOCH FROM
                                (expires_at - clock_timestamp())) * 1000)
                         )::bigint
                    END AS ttl_ms
                 FROM pgfw_cache_entry
                 WHERE namespace = $1 AND key_hash = $2
                   AND (expires_at IS NULL OR expires_at > clock_timestamp())",
                &[&self.config.namespace, &key.as_bytes().as_slice()],
            )
            .await?;
        Ok(row.map(|row| {
            let value: Vec<u8> = row.get(0);
            let version: i64 = row.get(1);
            let ttl_ms: Option<i64> = row.get(2);
            L2Entry {
                value: Arc::from(value),
                version,
                ttl_ms,
            }
        }))
    }

    async fn ttl_from_l2(&self, key: KeyId) -> Result<TtlResult, CacheError> {
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                "SELECT
                    CASE WHEN expires_at IS NULL THEN NULL
                         ELSE GREATEST(
                            0,
                            CEIL(EXTRACT(EPOCH FROM
                                (expires_at - clock_timestamp())) * 1000)
                         )::bigint
                    END AS ttl_ms
                 FROM pgfw_cache_entry
                 WHERE namespace = $1 AND key_hash = $2
                   AND (expires_at IS NULL OR expires_at > clock_timestamp())",
                &[&self.config.namespace, &key.as_bytes().as_slice()],
            )
            .await?;
        Ok(match row {
            None => TtlResult::Missing,
            Some(row) => match row.get::<_, Option<i64>>(0) {
                None => TtlResult::Persistent,
                Some(millis) => u64::try_from(millis)
                    .map(|millis| TtlResult::Expiring(Duration::from_millis(millis)))
                    .unwrap_or(TtlResult::Missing),
            },
        })
    }

    fn read_l1(&self, key: KeyId) -> Option<Arc<[u8]>> {
        let shard = self.shard_id(key);
        self.l1
            .get(usize::from(shard))
            .and_then(|lock| lock.lock().ok()?.get(key))
    }

    fn ttl_from_l1(&self, key: KeyId) -> Option<TtlResult> {
        let shard = self.shard_id(key);
        let state = self.l1.get(usize::from(shard))?.lock().ok()?;
        let entry = state.entries.get(&key)?;
        Some(match entry.expires_at {
            None => TtlResult::Persistent,
            Some(deadline) => deadline
                .checked_duration_since(Instant::now())
                .map_or(TtlResult::Missing, TtlResult::Expiring),
        })
    }

    pub(crate) fn insert_if_epoch(&self, shard: u16, epoch: u64, key: KeyId, entry: &L2Entry) {
        let expires_at = entry
            .ttl_ms
            .and_then(|millis| u64::try_from(millis).ok())
            .and_then(|millis| Instant::now().checked_add(Duration::from_millis(millis)));
        let Some(lock) = self.l1.get(usize::from(shard)) else {
            return;
        };
        let Ok(mut state) = lock.lock() else {
            return;
        };
        if state.epoch != epoch {
            return;
        }
        state.insert(
            key,
            L1Entry {
                value: Arc::clone(&entry.value),
                version: entry.version,
                expires_at,
                inserted_at: Instant::now(),
            },
            self.l1_capacity_per_shard,
            self.l1_bytes_per_shard,
        );
    }
}
