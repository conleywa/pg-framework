use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::Pool;
use pg_framework_core::{duration_millis, validate_key_size};
use tracing::debug;

use crate::{RateDecision, RateLimitConfig, RateLimitError, RateSnapshot};

/// PostgreSQL 固定窗口限频器。
#[derive(Clone)]
pub struct RateLimiter {
    pub(crate) pool: Pool,
    pub(crate) config: RateLimitConfig,
}

impl RateLimiter {
    /// 基于调用方管理的共享 PostgreSQL 连接池创建限频器。
    ///
    /// # 错误
    ///
    /// 配置非法时返回错误。
    pub fn new(pool: Pool, config: RateLimitConfig) -> Result<Self, RateLimitError> {
        config.validate()?;
        Ok(Self { pool, config })
    }

    /// 创建限频器并立即校验数据库 schema generation。
    ///
    /// # 错误
    ///
    /// 配置非法、schema 缺失或不匹配时返回错误。
    pub async fn connect(pool: Pool, config: RateLimitConfig) -> Result<Self, RateLimitError> {
        let limiter = Self::new(pool, config)?;
        limiter.verify_schema().await?;
        Ok(limiter)
    }

    /// 校验运行时配置与数据库 schema 元数据一致。
    ///
    /// # 错误
    ///
    /// schema 缺失、generation 或分区数不匹配时返回错误。
    pub async fn verify_schema(&self) -> Result<(), RateLimitError> {
        self.timed("schema", async {
            pg_framework_core::verify_schema(
                &self.pool,
                "ratelimit",
                &self.config.schema_generation,
                self.config.physical_partition_count,
            )
            .await
            .map_err(RateLimitError::from)
        })
        .await?;
        Ok(())
    }

    async fn timed<T, F>(&self, operation: &'static str, future: F) -> Result<T, RateLimitError>
    where
        F: Future<Output = Result<T, RateLimitError>>,
    {
        tokio::time::timeout(self.config.operation_timeout, future)
            .await
            .map_err(|_| RateLimitError::Timeout(operation))?
    }

    /// 原子消费 `cost` 额度并返回决策。
    ///
    /// # 错误
    ///
    /// 输入非法或 PostgreSQL 失败时返回错误。
    pub async fn check(
        &self,
        key: &[u8],
        cost: u64,
        limit: u64,
    ) -> Result<RateDecision, RateLimitError> {
        if cost == 0 {
            return Err(RateLimitError::InvalidConfig("cost must be positive"));
        }
        if limit == 0 || limit >= i64::MAX as u64 {
            return Err(RateLimitError::InvalidConfig(
                "limit must be between 1 and i64::MAX - 1",
            ));
        }
        validate_key_size(key.len(), self.config.max_key_bytes)?;
        let key_id = self.config.key_hasher.derive(&self.config.namespace, key);
        let raw_key = self.config.store_raw_keys.then(|| Arc::<[u8]>::from(key));
        let window_ms = duration_millis(self.config.window)?;
        let grace_ms = duration_millis(self.config.cleanup_grace)?;
        let cost =
            i64::try_from(cost).map_err(|_| RateLimitError::InvalidConfig("cost is too large"))?;
        let limit = i64::try_from(limit)
            .map_err(|_| RateLimitError::InvalidConfig("limit is too large"))?;
        let row = self
            .timed("check", async {
                let client = self.pool.get().await?;
                let row = client
                    .query_one(
                        "WITH current_time AS (
                    SELECT clock_timestamp() AS now
                 ),
                 bounds AS (
                    SELECT
                        to_timestamp(
                            floor(EXTRACT(EPOCH FROM now) * 1000.0 / $6)
                            * $6 / 1000.0
                        ) AS window_start
                    FROM current_time
                 )
                 INSERT INTO pgfw_rate_limit_counter (
                    namespace, raw_key, key_hash, window_start, used, expires_at
                 )
                 SELECT
                    $1, $2, $3, bounds.window_start, $4,
                    bounds.window_start + $6 * interval '1 millisecond'
                        + $7 * interval '1 millisecond'
                 FROM bounds
                 ON CONFLICT (namespace, key_hash, window_start)
                 DO UPDATE SET
                    raw_key = EXCLUDED.raw_key,
                    used = CASE
                        WHEN pgfw_rate_limit_counter.used <= $5 - $4
                        THEN pgfw_rate_limit_counter.used + $4
                        ELSE $5 + 1
                    END
                 RETURNING
                    used,
                    GREATEST(
                        0,
                        CEIL(EXTRACT(EPOCH FROM (
                            window_start
                            + $6 * interval '1 millisecond'
                            - clock_timestamp()
                        )) * 1000)
                    )::bigint AS reset_ms",
                        &[
                            &self.config.namespace,
                            &raw_key.as_deref(),
                            &key_id.as_bytes().as_slice(),
                            &cost,
                            &limit,
                            &window_ms,
                            &grace_ms,
                        ],
                    )
                    .await?;
                Ok(row)
            })
            .await?;
        let used: i64 = row.get(0);
        let reset_ms: i64 = row.get(1);
        let allowed = used <= limit;
        let remaining = if allowed {
            u64::try_from(limit - used).unwrap_or_default()
        } else {
            0
        };
        let reset_after = Duration::from_millis(u64::try_from(reset_ms).unwrap_or_default());
        debug!(
            allowed,
            remaining,
            used = u64::try_from(used).unwrap_or(u64::MAX),
            reset_ms,
            "限频决策"
        );
        Ok(RateDecision {
            allowed,
            limit: u64::try_from(limit).unwrap_or_default(),
            remaining,
            reset_after,
            retry_after: (!allowed).then_some(reset_after),
        })
    }

    /// 读取当前窗口且不消费额度。
    ///
    /// # 错误
    ///
    /// 输入非法或 PostgreSQL 失败时返回错误。
    pub async fn get(
        &self,
        key: &[u8],
        limit: u64,
    ) -> Result<Option<RateSnapshot>, RateLimitError> {
        validate_key_size(key.len(), self.config.max_key_bytes)?;
        if limit == 0 || limit >= i64::MAX as u64 {
            return Err(RateLimitError::InvalidConfig(
                "limit must be between 1 and i64::MAX - 1",
            ));
        }
        let key_id = self.config.key_hasher.derive(&self.config.namespace, key);
        let window_ms = duration_millis(self.config.window)?;
        let row = self
            .timed("get", async {
                let client = self.pool.get().await?;
                let row = client
                    .query_opt(
                        "WITH current_time AS (
                    SELECT clock_timestamp() AS now
                 ),
                 bounds AS (
                    SELECT to_timestamp(
                        floor(EXTRACT(EPOCH FROM now) * 1000.0 / $3)
                        * $3 / 1000.0
                    ) AS window_start
                    FROM current_time
                 )
                 SELECT counter.used,
                    GREATEST(
                        0,
                        CEIL(EXTRACT(EPOCH FROM (
                            counter.window_start
                            + $3 * interval '1 millisecond'
                            - clock_timestamp()
                        )) * 1000)
                    )::bigint AS reset_ms
                 FROM pgfw_rate_limit_counter AS counter
                 JOIN bounds
                   ON counter.window_start = bounds.window_start
                 WHERE counter.namespace = $1 AND counter.key_hash = $2",
                        &[
                            &self.config.namespace,
                            &key_id.as_bytes().as_slice(),
                            &window_ms,
                        ],
                    )
                    .await?;
                Ok(row)
            })
            .await?;
        Ok(row.map(|row| {
            let used = u64::try_from(row.get::<_, i64>(0)).unwrap_or(u64::MAX);
            let reset_ms = u64::try_from(row.get::<_, i64>(1)).unwrap_or_default();
            let capped_used = used.min(limit);
            RateSnapshot {
                limit,
                used: capped_used,
                remaining: limit.saturating_sub(capped_used),
                reset_after: Duration::from_millis(reset_ms),
            }
        }))
    }

    /// 清除指定 key 的所有窗口记录。
    ///
    /// # 错误
    ///
    /// PostgreSQL 失败时返回错误。
    pub async fn reset(&self, key: &[u8]) -> Result<u64, RateLimitError> {
        validate_key_size(key.len(), self.config.max_key_bytes)?;
        let key_id = self.config.key_hasher.derive(&self.config.namespace, key);
        self.timed("reset", async {
            let client = self.pool.get().await?;
            let deleted = client
                .execute(
                    "DELETE FROM pgfw_rate_limit_counter
                     WHERE namespace = $1 AND key_hash = $2",
                    &[&self.config.namespace, &key_id.as_bytes().as_slice()],
                )
                .await?;
            Ok(deleted)
        })
        .await
    }

    /// 按有界批次删除过期窗口。
    ///
    /// # 错误
    ///
    /// batch size 非法或 PostgreSQL 失败时返回错误。
    pub async fn cleanup_expired(&self, batch_size: usize) -> Result<u64, RateLimitError> {
        if batch_size == 0 {
            return Err(RateLimitError::InvalidConfig("batch size must be positive"));
        }
        let mut total = 0u64;
        loop {
            let deleted = self
                .timed("cleanup", async {
                    let client = self.pool.get().await?;
                    let deleted = client
                        .execute(
                            "WITH expired AS (
                        SELECT namespace, key_hash, window_start
                        FROM pgfw_rate_limit_counter
                        WHERE expires_at <= clock_timestamp()
                        ORDER BY expires_at
                        LIMIT $1
                        FOR UPDATE SKIP LOCKED
                     )
                     DELETE FROM pgfw_rate_limit_counter AS counter
                     USING expired
                     WHERE counter.namespace = expired.namespace
                       AND counter.key_hash = expired.key_hash
                       AND counter.window_start = expired.window_start
                       AND counter.expires_at <= clock_timestamp()",
                            &[&i64::try_from(batch_size).unwrap_or(i64::MAX)],
                        )
                        .await?;
                    Ok(deleted)
                })
                .await?;
            total = total.saturating_add(deleted);
            if deleted < u64::try_from(batch_size).unwrap_or(u64::MAX) {
                break;
            }
        }
        Ok(total)
    }
}
