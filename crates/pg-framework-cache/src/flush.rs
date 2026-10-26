use std::sync::atomic::Ordering;
use std::time::Duration;

use pg_framework_core::KeyId;
use tokio::sync::Notify;
use tracing::debug;

use crate::Cache;
use crate::error::CacheError;
use crate::notify::notify_keys;
use crate::state::{Mutation, PendingEntry};

impl Cache {
    /// 刷盘当前所有缓冲写入。
    ///
    /// # 错误
    ///
    /// 返回第一个存储错误；失败的变更继续保留在缓冲区。
    pub async fn flush(&self) -> Result<(), CacheError> {
        let _gate = self.flush_gate.lock().await;
        loop {
            let mut made_progress = false;
            for shard in 0..self.pending.len() {
                let batch = self.snapshot_pending_shard(shard);
                if batch.is_empty() {
                    continue;
                }
                self.flush_batch(&batch).await?;
                self.commit_pending_batch(shard, &batch);
                made_progress = true;
            }
            if !made_progress || self.pending_keys.load(Ordering::Acquire) == 0 {
                break;
            }
        }
        Ok(())
    }

    /// 通知后台 flusher 立即处理缓冲写入。
    pub fn wake_flusher(&self) {
        self.flush_notify.notify_one();
    }

    pub(crate) fn flusher_signal(&self) -> &Notify {
        &self.flush_notify
    }

    pub(crate) fn flush_interval(&self) -> Duration {
        self.config.buffer.flush_interval
    }

    fn snapshot_pending_shard(&self, shard: usize) -> Vec<(KeyId, PendingEntry)> {
        let mut batch = Vec::with_capacity(self.config.buffer.max_batch_keys);
        let mut bytes = 0usize;
        let Ok(state) = self.pending[shard].lock() else {
            return batch;
        };
        for (key, entry) in state.iter() {
            let mutation_bytes = entry.retained_bytes();
            if !batch.is_empty()
                && (batch.len() >= self.config.buffer.max_batch_keys
                    || bytes.saturating_add(mutation_bytes) > self.config.buffer.max_batch_bytes)
            {
                break;
            }
            bytes = bytes.saturating_add(mutation_bytes);
            batch.push((*key, entry.clone()));
        }
        batch
    }

    fn commit_pending_batch(&self, shard: usize, batch: &[(KeyId, PendingEntry)]) {
        let Ok(mut state) = self.pending[shard].lock() else {
            return;
        };
        for (key, entry) in batch {
            let should_remove = state
                .get(key)
                .is_some_and(|current| current.generation() == entry.generation());
            if should_remove && let Some(removed) = state.remove(key) {
                self.pending_keys.fetch_sub(1, Ordering::AcqRel);
                self.pending_bytes
                    .fetch_sub(removed.retained_bytes(), Ordering::AcqRel);
            }
        }
    }

    async fn flush_batch(&self, batch: &[(KeyId, PendingEntry)]) -> Result<(), CacheError> {
        let mut upserts = Vec::new();
        let mut deletes = Vec::new();
        let mut expires = Vec::new();
        let mut persists = Vec::new();
        let mut keys = Vec::with_capacity(batch.len());

        for (key, entry) in batch {
            let key_bytes = key.as_bytes().to_vec();
            keys.push(key_bytes.clone());
            match &entry.mutation {
                Mutation::Set { value, ttl_ms, .. } => {
                    upserts.push((
                        key_bytes,
                        entry.raw_key.clone(),
                        value.as_ref().to_vec(),
                        *ttl_ms,
                    ));
                }
                Mutation::Delete { .. } => deletes.push(key_bytes),
                Mutation::Expire { ttl_ms, .. } => {
                    expires.push((key_bytes, *ttl_ms));
                }
                Mutation::Persist { .. } => persists.push(key_bytes),
            }
        }

        let mut client = self.pool.get().await?;
        let transaction = client.transaction().await?;

        if !upserts.is_empty() {
            let raw_keys = upserts
                .iter()
                .map(|(_, raw_key, _, _)| raw_key.as_deref().map(<[u8]>::to_vec))
                .collect::<Vec<_>>();
            let upsert_keys = upserts
                .iter()
                .map(|(key, _, _, _)| key.clone())
                .collect::<Vec<_>>();
            let values = upserts
                .iter()
                .map(|(_, _, value, _)| value.clone())
                .collect::<Vec<_>>();
            let ttls = upserts
                .iter()
                .map(|(_, _, _, ttl)| *ttl)
                .collect::<Vec<_>>();
            transaction
                .execute(
                    "INSERT INTO pgfw_cache_entry (
                        namespace, raw_key, key_hash, value, version, expires_at
                     )
                     SELECT
                        $1, input.raw_key, input.key_hash, input.value, 1,
                        CASE WHEN input.ttl_ms IS NULL THEN NULL
                             ELSE clock_timestamp()
                                  + input.ttl_ms * interval '1 millisecond'
                        END
                     FROM unnest(
                          $2::bytea[], $3::bytea[], $4::bytea[], $5::bigint[]
                     ) AS input(raw_key, key_hash, value, ttl_ms)
                     ON CONFLICT (namespace, key_hash)
                     DO UPDATE SET
                        raw_key = EXCLUDED.raw_key,
                        value = EXCLUDED.value,
                        version = pgfw_cache_entry.version + 1,
                        expires_at = EXCLUDED.expires_at,
                        updated_at = clock_timestamp()",
                    &[
                        &self.config.namespace,
                        &raw_keys,
                        &upsert_keys,
                        &values,
                        &ttls,
                    ],
                )
                .await?;
        }

        if !deletes.is_empty() {
            transaction
                .execute(
                    "DELETE FROM pgfw_cache_entry
                     WHERE namespace = $1
                       AND key_hash = ANY($2::bytea[])",
                    &[&self.config.namespace, &deletes],
                )
                .await?;
        }

        if !expires.is_empty() {
            let expire_keys = expires
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            let ttls = expires.iter().map(|(_, ttl)| *ttl).collect::<Vec<_>>();
            transaction
                .execute(
                    "UPDATE pgfw_cache_entry AS cache
                     SET expires_at =
                            clock_timestamp()
                            + input.ttl_ms * interval '1 millisecond',
                         updated_at = clock_timestamp()
                     FROM unnest($2::bytea[], $3::bigint[])
                          AS input(key_hash, ttl_ms)
                     WHERE cache.namespace = $1
                       AND cache.key_hash = input.key_hash
                       AND (cache.expires_at IS NULL
                            OR cache.expires_at > clock_timestamp())",
                    &[&self.config.namespace, &expire_keys, &ttls],
                )
                .await?;
        }

        if !persists.is_empty() {
            transaction
                .execute(
                    "UPDATE pgfw_cache_entry
                     SET expires_at = NULL, updated_at = clock_timestamp()
                     WHERE namespace = $1
                       AND key_hash = ANY($2::bytea[])
                       AND (expires_at IS NULL OR expires_at > clock_timestamp())",
                    &[&self.config.namespace, &persists],
                )
                .await?;
        }

        notify_keys(&transaction, &self.config.notification_channel, &keys).await?;
        transaction.commit().await?;
        debug!(
            keys = batch.len(),
            upserts = upserts.len(),
            deletes = deletes.len(),
            expires = expires.len(),
            persists = persists.len(),
            "缓存写缓冲已刷盘"
        );
        Ok(())
    }
}
