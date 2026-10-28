use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use pg_framework_core::{KeyId, duration_millis};

use crate::Cache;
use crate::config::WriteMode;
use crate::error::CacheError;
use crate::notify::notify_key;
use crate::state::{Mutation, PendingEntry};

impl Cache {
    /// 写入 value；`ttl` 为 `None` 时使用配置的默认 TTL。
    ///
    /// # 错误
    ///
    /// value 过大、写缓冲耗尽或存储失败时返回错误。
    pub async fn set(
        &self,
        key: &[u8],
        value: impl Into<Arc<[u8]>>,
        ttl: Option<Duration>,
    ) -> Result<(), CacheError> {
        let key_id = self.key_id(key)?;
        let value = value.into();
        if value.len() > self.config.max_value_bytes {
            return Err(CacheError::ValueTooLarge);
        }
        let ttl = ttl.or(self.config.default_ttl);
        match self.config.write_mode {
            WriteMode::Immediate => {
                self.timed(
                    "set",
                    self.set_immediate(key_id, self.stored_raw_key(key), value, ttl),
                )
                .await
            }
            WriteMode::Buffered => {
                let ttl_ms = ttl.map(duration_millis).transpose()?;
                if ttl_ms.is_some_and(|millis| millis <= 0) {
                    return Err(CacheError::InvalidConfig("TTL must be positive"));
                }
                let local_deadline = match ttl {
                    None => None,
                    Some(ttl) => Some(
                        Instant::now()
                            .checked_add(ttl)
                            .ok_or(CacheError::InvalidConfig("TTL is too large"))?,
                    ),
                };
                self.enqueue(
                    key_id,
                    self.stored_raw_key(key),
                    Mutation::Set {
                        value,
                        ttl_ms,
                        local_deadline,
                        generation: self.next_generation(),
                    },
                )
            }
        }
    }

    /// 在调用方事务中写入 value。
    ///
    /// 本地 L1 会立即失效。若调用方随后回滚，后续读取会从未改变的 L2 安全重建
    /// L1。
    ///
    /// # 错误
    ///
    /// value 过大或 PostgreSQL 失败时返回错误。
    pub async fn set_in(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
        key: &[u8],
        value: impl Into<Arc<[u8]>>,
        ttl: Option<Duration>,
    ) -> Result<(), CacheError> {
        let value = value.into();
        if value.len() > self.config.max_value_bytes {
            return Err(CacheError::ValueTooLarge);
        }
        let key_id = self.key_id(key)?;
        let ttl_ms = ttl
            .or(self.config.default_ttl)
            .map(duration_millis)
            .transpose()?;
        if ttl_ms.is_some_and(|millis| millis <= 0) {
            return Err(CacheError::InvalidConfig("TTL must be positive"));
        }
        let raw_key = self.stored_raw_key(key);
        let raw_key_param = raw_key.as_deref();
        self.ensure_namespace_writable_tokio(transaction).await?;
        transaction
            .execute(
                "INSERT INTO pgfw_cache_entry (
                    namespace, raw_key, key_hash, value, version, expires_at
                 )
                 VALUES (
                    $1, $2, $3, $4, 1,
                    CASE WHEN $5::bigint IS NULL THEN NULL
                         ELSE clock_timestamp() + $5 * interval '1 millisecond' END
                 )
                 ON CONFLICT (namespace, key_hash)
                 DO UPDATE SET
                    raw_key = EXCLUDED.raw_key,
                    value = EXCLUDED.value,
                    version = pgfw_cache_entry.version + 1,
                    expires_at = EXCLUDED.expires_at,
                    updated_at = clock_timestamp()",
                &[
                    &self.config.namespace,
                    &raw_key_param,
                    &key_id.as_bytes().as_slice(),
                    &value.as_ref(),
                    &ttl_ms,
                ],
            )
            .await?;
        notify_key(transaction, &self.config.notification_channel, key_id).await?;
        self.invalidate(key_id);
        Ok(())
    }

    /// key 不存在或已过期时原子写入 value。
    ///
    /// # 错误
    ///
    /// value 过大或存储失败时返回错误。
    pub async fn set_if_absent(
        &self,
        key: &[u8],
        value: impl Into<Arc<[u8]>>,
        ttl: Option<Duration>,
    ) -> Result<bool, CacheError> {
        let value = value.into();
        if value.len() > self.config.max_value_bytes {
            return Err(CacheError::ValueTooLarge);
        }
        let key_id = self.key_id(key)?;
        let ttl_ms = ttl
            .or(self.config.default_ttl)
            .map(duration_millis)
            .transpose()?;
        if ttl_ms.is_some_and(|millis| millis <= 0) {
            return Err(CacheError::InvalidConfig("TTL must be positive"));
        }
        let raw_key = self.stored_raw_key(key);
        let raw_key_param = raw_key.as_deref();

        let inserted = self
            .timed("set_if_absent", async {
                let mut client = self.pool.get().await?;
                let transaction = client.transaction().await?;
                self.ensure_namespace_writable(&transaction).await?;
                let row = transaction
                    .query_opt(
                        "INSERT INTO pgfw_cache_entry (
                    namespace, raw_key, key_hash, value, version, expires_at
                 )
                 VALUES (
                    $1, $2, $3, $4, 1,
                    CASE WHEN $5::bigint IS NULL THEN NULL
                         ELSE clock_timestamp() + $5 * interval '1 millisecond' END
                 )
                 ON CONFLICT (namespace, key_hash)
                 DO UPDATE SET
                    raw_key = EXCLUDED.raw_key,
                    value = EXCLUDED.value,
                    version = pgfw_cache_entry.version + 1,
                    expires_at = EXCLUDED.expires_at,
                    updated_at = clock_timestamp()
                 WHERE pgfw_cache_entry.expires_at IS NULL
                    OR pgfw_cache_entry.expires_at <= clock_timestamp()
                 RETURNING version",
                        &[
                            &self.config.namespace,
                            &raw_key_param,
                            &key_id.as_bytes().as_slice(),
                            &value.as_ref(),
                            &ttl_ms,
                        ],
                    )
                    .await?;
                let inserted = row.is_some();
                if inserted {
                    notify_key(&transaction, &self.config.notification_channel, key_id).await?;
                }
                transaction.commit().await?;
                Ok(inserted)
            })
            .await?;
        self.invalidate(key_id);
        Ok(inserted)
    }

    /// 在调用方事务中原子执行 set-if-absent。
    ///
    /// # 错误
    ///
    /// value 过大或 PostgreSQL 失败时返回错误。
    pub async fn set_if_absent_in(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
        key: &[u8],
        value: impl Into<Arc<[u8]>>,
        ttl: Option<Duration>,
    ) -> Result<bool, CacheError> {
        let value = value.into();
        if value.len() > self.config.max_value_bytes {
            return Err(CacheError::ValueTooLarge);
        }
        let key_id = self.key_id(key)?;
        let ttl_ms = ttl
            .or(self.config.default_ttl)
            .map(duration_millis)
            .transpose()?;
        if ttl_ms.is_some_and(|millis| millis <= 0) {
            return Err(CacheError::InvalidConfig("TTL must be positive"));
        }
        let raw_key = self.stored_raw_key(key);
        let raw_key_param = raw_key.as_deref();
        self.ensure_namespace_writable_tokio(transaction).await?;
        let row = transaction
            .query_opt(
                "INSERT INTO pgfw_cache_entry (
                    namespace, raw_key, key_hash, value, version, expires_at
                 )
                 VALUES (
                    $1, $2, $3, $4, 1,
                    CASE WHEN $5::bigint IS NULL THEN NULL
                         ELSE clock_timestamp() + $5 * interval '1 millisecond' END
                 )
                 ON CONFLICT (namespace, key_hash)
                 DO UPDATE SET
                    raw_key = EXCLUDED.raw_key,
                    value = EXCLUDED.value,
                    version = pgfw_cache_entry.version + 1,
                    expires_at = EXCLUDED.expires_at,
                    updated_at = clock_timestamp()
                 WHERE pgfw_cache_entry.expires_at IS NULL
                    OR pgfw_cache_entry.expires_at <= clock_timestamp()
                 RETURNING version",
                &[
                    &self.config.namespace,
                    &raw_key_param,
                    &key_id.as_bytes().as_slice(),
                    &value.as_ref(),
                    &ttl_ms,
                ],
            )
            .await?;
        let inserted = row.is_some();
        if inserted {
            notify_key(transaction, &self.config.notification_channel, key_id).await?;
        }
        self.invalidate(key_id);
        Ok(inserted)
    }

    /// 删除 key。
    ///
    /// # 错误
    ///
    /// 写缓冲耗尽或存储失败时返回错误。
    pub async fn delete(&self, key: &[u8]) -> Result<(), CacheError> {
        let key_id = self.key_id(key)?;
        match self.config.write_mode {
            WriteMode::Immediate => self.timed("delete", self.delete_immediate(key_id)).await,
            WriteMode::Buffered => self.enqueue(
                key_id,
                self.stored_raw_key(key),
                Mutation::Delete {
                    generation: self.next_generation(),
                },
            ),
        }
    }

    /// 在调用方事务中删除 key。
    ///
    /// # 错误
    ///
    /// PostgreSQL 失败时返回错误。
    pub async fn delete_in(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
        key: &[u8],
    ) -> Result<(), CacheError> {
        let key_id = self.key_id(key)?;
        self.ensure_namespace_writable_tokio(transaction).await?;
        transaction
            .execute(
                "DELETE FROM pgfw_cache_entry
                 WHERE namespace = $1 AND key_hash = $2",
                &[&self.config.namespace, &key_id.as_bytes().as_slice()],
            )
            .await?;
        notify_key(transaction, &self.config.notification_channel, key_id).await?;
        self.invalidate(key_id);
        Ok(())
    }

    /// 设置或替换 key TTL。
    ///
    /// # 错误
    ///
    /// TTL 非法或存储失败时返回错误。
    pub async fn expire(&self, key: &[u8], ttl: Duration) -> Result<(), CacheError> {
        if ttl.is_zero() {
            return Err(CacheError::InvalidConfig("TTL must be positive"));
        }
        let key_id = self.key_id(key)?;
        let ttl_ms = duration_millis(ttl)?;
        match self.config.write_mode {
            WriteMode::Immediate => {
                self.timed(
                    "expire",
                    self.persist_or_expire_immediate(key_id, Some(ttl_ms)),
                )
                .await
            }
            WriteMode::Buffered => self.enqueue(
                key_id,
                self.stored_raw_key(key),
                Mutation::Expire {
                    ttl_ms,
                    local_deadline: Instant::now()
                        .checked_add(ttl)
                        .ok_or(CacheError::InvalidConfig("TTL is too large"))?,
                    generation: self.next_generation(),
                },
            ),
        }
    }

    /// 移除 key TTL。
    ///
    /// # 错误
    ///
    /// 写缓冲耗尽或存储失败时返回错误。
    pub async fn persist(&self, key: &[u8]) -> Result<(), CacheError> {
        let key_id = self.key_id(key)?;
        match self.config.write_mode {
            WriteMode::Immediate => {
                self.timed("persist", self.persist_or_expire_immediate(key_id, None))
                    .await
            }
            WriteMode::Buffered => self.enqueue(
                key_id,
                self.stored_raw_key(key),
                Mutation::Persist {
                    generation: self.next_generation(),
                },
            ),
        }
    }

    /// 在调用方事务中更新 key TTL。
    ///
    /// # 错误
    ///
    /// TTL 非法或 PostgreSQL 失败时返回错误。
    pub async fn expire_in(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
        key: &[u8],
        ttl: Duration,
    ) -> Result<(), CacheError> {
        if ttl.is_zero() {
            return Err(CacheError::InvalidConfig("TTL must be positive"));
        }
        self.persist_or_expire_in(transaction, key, Some(duration_millis(ttl)?))
            .await
    }

    /// 在调用方事务中移除 key TTL。
    ///
    /// # 错误
    ///
    /// PostgreSQL 失败时返回错误。
    pub async fn persist_in(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
        key: &[u8],
    ) -> Result<(), CacheError> {
        self.persist_or_expire_in(transaction, key, None).await
    }

    async fn set_immediate(
        &self,
        key: KeyId,
        raw_key: Option<Arc<[u8]>>,
        value: Arc<[u8]>,
        ttl: Option<Duration>,
    ) -> Result<(), CacheError> {
        let ttl_ms = ttl.map(duration_millis).transpose()?;
        if ttl_ms.is_some_and(|millis| millis <= 0) {
            return Err(CacheError::InvalidConfig("TTL must be positive"));
        }
        let raw_key_param = raw_key.as_deref();
        let mut client = self.pool.get().await?;
        let transaction = client.transaction().await?;
        self.ensure_namespace_writable(&transaction).await?;
        transaction
            .execute(
                "INSERT INTO pgfw_cache_entry (
                    namespace, raw_key, key_hash, value, version, expires_at
                 )
                 VALUES (
                    $1, $2, $3, $4, 1,
                    CASE WHEN $5::bigint IS NULL THEN NULL
                         ELSE clock_timestamp() + $5 * interval '1 millisecond' END
                 )
                 ON CONFLICT (namespace, key_hash)
                 DO UPDATE SET
                    raw_key = EXCLUDED.raw_key,
                    value = EXCLUDED.value,
                    version = pgfw_cache_entry.version + 1,
                    expires_at = EXCLUDED.expires_at,
                    updated_at = clock_timestamp()",
                &[
                    &self.config.namespace,
                    &raw_key_param,
                    &key.as_bytes().as_slice(),
                    &value.as_ref(),
                    &ttl_ms,
                ],
            )
            .await?;
        notify_key(&transaction, &self.config.notification_channel, key).await?;
        transaction.commit().await?;
        self.invalidate(key);
        Ok(())
    }

    async fn delete_immediate(&self, key: KeyId) -> Result<(), CacheError> {
        let mut client = self.pool.get().await?;
        let transaction = client.transaction().await?;
        self.ensure_namespace_writable(&transaction).await?;
        transaction
            .execute(
                "DELETE FROM pgfw_cache_entry
                 WHERE namespace = $1 AND key_hash = $2",
                &[&self.config.namespace, &key.as_bytes().as_slice()],
            )
            .await?;
        notify_key(&transaction, &self.config.notification_channel, key).await?;
        transaction.commit().await?;
        self.invalidate(key);
        Ok(())
    }

    async fn persist_or_expire_immediate(
        &self,
        key: KeyId,
        ttl_ms: Option<i64>,
    ) -> Result<(), CacheError> {
        let mut client = self.pool.get().await?;
        let transaction = client.transaction().await?;
        self.ensure_namespace_writable(&transaction).await?;
        transaction
            .execute(
                "UPDATE pgfw_cache_entry
                 SET expires_at = CASE WHEN $3::bigint IS NULL THEN NULL
                                       ELSE clock_timestamp() + $3 * interval '1 millisecond' END,
                     version = version + 1,
                     updated_at = clock_timestamp()
                 WHERE namespace = $1 AND key_hash = $2
                   AND (expires_at IS NULL OR expires_at > clock_timestamp())",
                &[&self.config.namespace, &key.as_bytes().as_slice(), &ttl_ms],
            )
            .await?;
        notify_key(&transaction, &self.config.notification_channel, key).await?;
        transaction.commit().await?;
        self.invalidate(key);
        Ok(())
    }

    async fn persist_or_expire_in(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
        key: &[u8],
        ttl_ms: Option<i64>,
    ) -> Result<(), CacheError> {
        let key_id = self.key_id(key)?;
        self.ensure_namespace_writable_tokio(transaction).await?;
        transaction
            .execute(
                "UPDATE pgfw_cache_entry
                 SET expires_at = CASE WHEN $3::bigint IS NULL THEN NULL
                                       ELSE clock_timestamp() + $3 * interval '1 millisecond' END,
                     version = version + 1,
                     updated_at = clock_timestamp()
                 WHERE namespace = $1 AND key_hash = $2
                   AND (expires_at IS NULL OR expires_at > clock_timestamp())",
                &[
                    &self.config.namespace,
                    &key_id.as_bytes().as_slice(),
                    &ttl_ms,
                ],
            )
            .await?;
        notify_key(transaction, &self.config.notification_channel, key_id).await?;
        self.invalidate(key_id);
        Ok(())
    }

    fn enqueue(
        &self,
        key: KeyId,
        raw_key: Option<Arc<[u8]>>,
        mutation: Mutation,
    ) -> Result<(), CacheError> {
        let shard = self.shard_id(key);
        let mut state = self
            .pending
            .get(usize::from(shard))
            .ok_or(CacheError::InvalidConfig("invalid shard"))?
            .lock()
            .map_err(|_| CacheError::InvalidConfig("pending buffer lock poisoned"))?;
        let (entry, old_bytes) = if let Some(existing) = state.get(&key) {
            let entry = PendingEntry {
                raw_key: raw_key.or_else(|| existing.raw_key.clone()),
                mutation: existing.mutation.clone().merged_with(mutation),
            };
            let old_bytes = existing.retained_bytes();
            (entry, old_bytes)
        } else {
            let entry = PendingEntry { raw_key, mutation };
            (entry, 0)
        };
        let new_bytes = entry.retained_bytes();
        let key_was_reserved = if state.contains_key(&key) {
            false
        } else {
            if !self.reserve_pending_key() {
                return Err(CacheError::BufferFull);
            }
            true
        };
        if !self.update_pending_bytes(old_bytes, new_bytes) {
            if key_was_reserved {
                self.pending_keys.fetch_sub(1, Ordering::AcqRel);
            }
            return Err(CacheError::BufferFull);
        }
        state.insert(key, entry);
        drop(state);
        self.invalidate(key);
        if self.pending_keys.load(Ordering::Acquire) >= self.config.buffer.max_batch_keys
            || self.pending_bytes.load(Ordering::Acquire) >= self.config.buffer.max_batch_bytes
        {
            self.wake_flusher();
        }
        Ok(())
    }

    fn reserve_pending_key(&self) -> bool {
        let mut current = self.pending_keys.load(Ordering::Acquire);
        loop {
            if current >= self.config.buffer.max_pending_keys {
                return false;
            }
            match self.pending_keys.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn update_pending_bytes(&self, old_bytes: usize, new_bytes: usize) -> bool {
        let mut current = self.pending_bytes.load(Ordering::Acquire);
        loop {
            let projected = current.saturating_sub(old_bytes).saturating_add(new_bytes);
            if projected > self.config.buffer.max_pending_bytes {
                return false;
            }
            match self.pending_bytes.compare_exchange_weak(
                current,
                projected,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }
}
