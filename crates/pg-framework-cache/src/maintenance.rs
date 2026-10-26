use std::sync::Arc;
use std::sync::atomic::Ordering;

use pg_framework_core::KeyId;
use tracing::warn;

use crate::Cache;
use crate::error::CacheError;
use crate::notify::notify_key;
use crate::state::L2Entry;
use crate::types::{CacheKeyPage, CacheStats, StoredCacheKey};

impl Cache {
    pub(crate) fn notification_channel(&self) -> &str {
        &self.config.notification_channel
    }

    /// 从 L1 删除 key，并推进所在 stripe 的失效 epoch。
    pub fn invalidate(&self, key: KeyId) {
        let shard = self.shard_id(key);
        if let Some(lock) = self.l1.get(usize::from(shard))
            && let Ok(mut state) = lock.lock()
        {
            state.invalidate(key);
        }
    }

    /// 解码 64 字符十六进制通知并失效对应 key。
    ///
    /// # 错误
    ///
    /// payload 不是合法 key 标识时返回错误。
    pub fn invalidate_notification(&self, payload: &str) -> Result<(), CacheError> {
        let key = key_id_from_hex(payload)
            .ok_or(CacheError::InvalidConfig("invalid invalidation payload"))?;
        self.invalidate(key);
        Ok(())
    }

    /// 清空所有本地条目，并推进每个 stripe 的 epoch。
    pub fn clear_l1(&self) {
        for shard in &self.l1 {
            if let Ok(mut state) = shard.lock() {
                state.clear();
            }
        }
    }

    /// 返回当前诊断计数器。
    #[must_use]
    pub fn stats(&self) -> CacheStats {
        let l1_entries = self
            .l1
            .iter()
            .filter_map(|shard| shard.lock().ok().map(|state| state.entries.len()))
            .sum();
        CacheStats {
            pending_keys: self.pending_keys.load(Ordering::Acquire),
            pending_bytes: self.pending_bytes.load(Ordering::Acquire),
            l1_entries,
        }
    }

    /// 按有界批次删除当前缓存 namespace 的全部条目。
    ///
    /// # 错误
    ///
    /// 任一批次失败时返回存储错误。
    pub async fn clear_namespace(&self) -> Result<(), CacheError> {
        self.flush().await?;
        self.clear_l1();

        loop {
            let mut client = self.pool.get().await?;
            let transaction = client.transaction().await?;
            let rows = transaction
                .query(
                    "WITH batch AS (
                        SELECT ctid
                        FROM pgfw_cache_entry
                        WHERE namespace = $1
                        LIMIT $2
                     ),
                     deleted AS (
                        DELETE FROM pgfw_cache_entry AS cache
                        USING batch
                        WHERE cache.ctid = batch.ctid
                        RETURNING cache.key_hash
                     )
                     SELECT key_hash FROM deleted",
                    &[
                        &self.config.namespace,
                        &i64::try_from(self.config.buffer.max_batch_keys).unwrap_or(i64::MAX),
                    ],
                )
                .await?;
            if rows.is_empty() {
                transaction.commit().await?;
                break;
            }
            for row in &rows {
                let bytes: Vec<u8> = row.get(0);
                if let Ok(array) = <[u8; 32]>::try_from(bytes) {
                    notify_key(
                        &transaction,
                        &self.config.notification_channel,
                        KeyId::from_bytes(array),
                    )
                    .await?;
                }
            }
            transaction.commit().await?;
        }
        Ok(())
    }

    /// 在缓存初始化或 listener 重连后，从有效 L2 行预热 L1。
    ///
    /// # 错误
    ///
    /// 任一预热查询失败时返回存储错误。
    pub async fn warm_reload(&self) -> Result<(), CacheError> {
        self.clear_l1();
        let epochs = self
            .l1
            .iter()
            .filter_map(|lock| lock.lock().ok().map(|state| state.epoch))
            .collect::<Vec<_>>();
        let client = self.pool.get().await?;
        let limit = self
            .config
            .recovery_rows_per_shard
            .saturating_mul(self.l1.len())
            .max(1);
        let rows = client
            .query(
                "SELECT key_hash, value,
                    CASE WHEN expires_at IS NULL THEN NULL
                         ELSE GREATEST(
                            0,
                            CEIL(EXTRACT(EPOCH FROM
                                (expires_at - clock_timestamp())) * 1000)
                         )::bigint
                    END AS ttl_ms
                 FROM pgfw_cache_entry
                 WHERE namespace = $1
                   AND (expires_at IS NULL OR expires_at > clock_timestamp())
                 ORDER BY updated_at DESC
                 LIMIT $2",
                &[
                    &self.config.namespace,
                    &i64::try_from(limit).unwrap_or(i64::MAX),
                ],
            )
            .await?;
        for row in rows {
            let key_bytes: Vec<u8> = row.get(0);
            let value: Vec<u8> = row.get(1);
            let ttl_ms: Option<i64> = row.get(2);
            let Ok(key_array) = <[u8; 32]>::try_from(key_bytes) else {
                warn!("预热重载时跳过格式错误的缓存 key hash");
                continue;
            };
            let key = KeyId::from_bytes(key_array);
            if self.pending_for(key).is_some() {
                continue;
            }
            let shard = self.shard_id(key);
            let epoch = epochs.get(usize::from(shard)).copied().unwrap_or_default();
            self.insert_if_epoch(
                shard,
                epoch,
                key,
                &L2Entry {
                    value: Arc::from(value),
                    ttl_ms,
                },
            );
        }
        Ok(())
    }

    /// 使用有界 keyset 分页扫描有效缓存 key。
    ///
    /// 查询会命中声明式分区父表，因此可能扫描所有分区。调用方必须按页遍历
    /// namespace。
    ///
    /// # 错误
    ///
    /// limit 为 0 或 PostgreSQL 失败时返回错误。
    pub async fn scan_keys(
        &self,
        after: Option<KeyId>,
        limit: usize,
    ) -> Result<CacheKeyPage, CacheError> {
        if limit == 0 {
            return Err(CacheError::InvalidConfig("scan limit must be positive"));
        }
        let limit = i64::try_from(limit.min(10_000))
            .map_err(|_| CacheError::InvalidConfig("scan limit is too large"))?;
        let cursor = after.map(|key| key.as_bytes().to_vec());
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "WITH scan_cursor AS (
                    SELECT $2::bytea AS after
                 )
                 SELECT cache.key_hash, cache.raw_key
                 FROM pgfw_cache_entry AS cache
                 CROSS JOIN scan_cursor
                 WHERE cache.namespace = $1
                   AND (scan_cursor.after IS NULL
                        OR cache.key_hash > scan_cursor.after)
                   AND (cache.expires_at IS NULL
                        OR cache.expires_at > clock_timestamp())
                 ORDER BY cache.key_hash
                 LIMIT $3",
                &[&self.config.namespace, &cursor, &limit],
            )
            .await?;
        let keys = rows
            .into_iter()
            .filter_map(|row| {
                let hash: Vec<u8> = row.get(0);
                let raw_key: Option<Vec<u8>> = row.get(1);
                <[u8; 32]>::try_from(hash).ok().map(|bytes| StoredCacheKey {
                    key_id: KeyId::from_bytes(bytes),
                    raw_key,
                })
            })
            .collect::<Vec<_>>();
        let next_after = (keys.len() == usize::try_from(limit).unwrap_or_default())
            .then(|| keys.last().map(|key| key.key_id))
            .flatten();
        Ok(CacheKeyPage { keys, next_after })
    }

    /// 按有界批次删除过期行。
    ///
    /// # 错误
    ///
    /// batch size 为 0 或 PostgreSQL 失败时返回错误。
    pub async fn cleanup_expired(&self, batch_size: usize) -> Result<u64, CacheError> {
        if batch_size == 0 {
            return Err(CacheError::InvalidConfig("batch size must be positive"));
        }
        let mut total = 0u64;
        loop {
            let client = self.pool.get().await?;
            let deleted = client
                .execute(
                    "WITH expired AS (
                        SELECT ctid
                        FROM pgfw_cache_entry
                        WHERE expires_at IS NOT NULL
                          AND expires_at <= clock_timestamp()
                        ORDER BY expires_at
                        LIMIT $1
                        FOR UPDATE SKIP LOCKED
                     )
                     DELETE FROM pgfw_cache_entry AS cache
                     USING expired
                     WHERE cache.ctid = expired.ctid",
                    &[&i64::try_from(batch_size).unwrap_or(i64::MAX)],
                )
                .await?;
            total = total.saturating_add(deleted);
            if deleted < u64::try_from(batch_size).unwrap_or(u64::MAX) {
                break;
            }
        }
        Ok(total)
    }
}

pub(crate) fn key_id_from_hex(payload: &str) -> Option<KeyId> {
    if payload.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (index, chunk) in payload.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let high = hex_value(chunk[0])?;
        let low = hex_value(chunk[1])?;
        bytes[index] = (high << 4) | low;
    }
    Some(KeyId::from_bytes(bytes))
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
