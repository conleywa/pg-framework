use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use deadpool_postgres::Pool;
use pg_framework_core::{KeyId, duration_millis, validate_key_size};
use tracing::warn;

use crate::{AcquireOutcome, FencingToken, LeaseGuard, LeaseTtl, LockConfig, LockError};

pub(crate) struct LockerInner {
    pub(crate) pool: Pool,
    pub(crate) config: LockConfig,
}

/// PostgreSQL 分布式租约锁。
#[derive(Clone)]
pub struct Locker {
    pub(crate) inner: Arc<LockerInner>,
}

impl Locker {
    /// 基于调用方管理的共享 PostgreSQL 连接池创建锁实例。
    ///
    /// # 错误
    ///
    /// 配置非法时返回错误。
    pub fn new(pool: Pool, config: LockConfig) -> Result<Self, LockError> {
        config.validate()?;
        Ok(Self {
            inner: Arc::new(LockerInner { pool, config }),
        })
    }

    /// 不等待地尝试获取租约。
    ///
    /// # 错误
    ///
    /// TTL 非法、存储失败、随机数生成失败或结果无法归并时返回错误。
    pub async fn try_acquire(
        &self,
        key: &[u8],
        ttl: LeaseTtl,
    ) -> Result<AcquireOutcome, LockError> {
        self.validate_ttl(ttl)?;
        validate_key_size(key.len(), self.inner.config.max_key_bytes)?;
        let key_id = self
            .inner
            .config
            .key_hasher
            .derive(&self.inner.config.namespace, key);
        let owner_token = random_owner_token()?;
        let ttl_ms = duration_millis(ttl.get())?;
        let raw_key = self
            .inner
            .config
            .store_raw_keys
            .then(|| Arc::<[u8]>::from(key));

        match self
            .execute_acquire(key_id, raw_key, owner_token, ttl_ms)
            .await
        {
            Ok(Some(guard)) => Ok(AcquireOutcome::Acquired(guard)),
            Ok(None) => Ok(AcquireOutcome::Contended),
            Err(error) => {
                warn!(error = %error, "锁获取结果未知，正在根据 owner token 归并");
                match self.recover_owner(key_id, owner_token).await {
                    Ok(Some((fencing_token, remaining))) => Ok(AcquireOutcome::Acquired(
                        self.guard(key_id, owner_token, fencing_token, ttl.get(), remaining),
                    )),
                    Ok(None) => Err(LockError::OutcomeUnknown {
                        operation: "acquire",
                    }),
                    Err(recovery_error) => {
                        warn!(
                            error = %recovery_error,
                            "锁获取结果归并失败"
                        );
                        Err(LockError::OutcomeUnknown {
                            operation: "acquire",
                        })
                    }
                }
            }
        }
    }

    /// 获取租约，并使用有界抖动退避重试。
    ///
    /// # 错误
    ///
    /// 到达调用方 deadline 或存储失败时返回错误。
    pub async fn acquire(
        &self,
        key: &[u8],
        ttl: LeaseTtl,
        deadline: Instant,
    ) -> Result<LeaseGuard, LockError> {
        let mut backoff = Duration::from_millis(25);
        loop {
            match self.try_acquire(key, ttl).await? {
                AcquireOutcome::Acquired(guard) => return Ok(guard),
                AcquireOutcome::Contended => {}
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(LockError::TimedOut);
            }
            let remaining = deadline.saturating_duration_since(now);
            tokio::time::sleep(jitter(backoff.min(remaining))).await;
            backoff = backoff
                .saturating_mul(2)
                .min(self.inner.config.acquire_backoff_max);
        }
    }

    /// 按有界批次删除过期锁行。
    ///
    /// # 错误
    ///
    /// 存储失败时返回错误。
    pub async fn cleanup_expired(&self, batch_size: usize) -> Result<u64, LockError> {
        if batch_size == 0 {
            return Err(LockError::InvalidConfig("batch size must be positive"));
        }
        let mut total = 0u64;
        loop {
            let client = self.inner.pool.get().await?;
            let deleted = client
                .execute(
                    "WITH expired AS (
                        SELECT ctid
                        FROM pgfw_lock
                        WHERE lease_expires_at <= clock_timestamp()
                        ORDER BY lease_expires_at
                        LIMIT $1
                        FOR UPDATE SKIP LOCKED
                     )
                     DELETE FROM pgfw_lock AS lock
                     USING expired
                     WHERE lock.ctid = expired.ctid",
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

    fn validate_ttl(&self, ttl: LeaseTtl) -> Result<(), LockError> {
        if ttl.get() < self.inner.config.min_lease || ttl.get() > self.inner.config.max_lease {
            return Err(LockError::InvalidConfig(
                "lease TTL is outside configured bounds",
            ));
        }
        if ttl.get() <= self.inner.config.safety_margin {
            return Err(LockError::InvalidConfig(
                "lease TTL must exceed the safety margin",
            ));
        }
        Ok(())
    }

    async fn execute_acquire(
        &self,
        key_id: KeyId,
        raw_key: Option<Arc<[u8]>>,
        owner_token: [u8; 32],
        ttl_ms: i64,
    ) -> Result<Option<LeaseGuard>, LockError> {
        let client = self.inner.pool.get().await?;
        let row = client
            .query_opt(
                "WITH candidate AS (
                    SELECT nextval('pgfw_lock_fencing_seq') AS fencing_token
                 )
                 INSERT INTO pgfw_lock AS lock (
                    namespace, raw_key, key_hash, owner_token, fencing_token,
                    lease_expires_at, acquired_at
                 )
                 SELECT
                    $1, $2, $3, $4, candidate.fencing_token,
                    clock_timestamp() + $5 * interval '1 millisecond',
                    clock_timestamp()
                 FROM candidate
                 ON CONFLICT (namespace, key_hash)
                 DO UPDATE SET
                    raw_key = EXCLUDED.raw_key,
                    owner_token = EXCLUDED.owner_token,
                    fencing_token = EXCLUDED.fencing_token,
                    lease_expires_at = EXCLUDED.lease_expires_at,
                    acquired_at = EXCLUDED.acquired_at
                 WHERE lock.lease_expires_at <= clock_timestamp()
                 RETURNING
                    fencing_token,
                    GREATEST(
                        0,
                        CEIL(EXTRACT(EPOCH FROM
                            (lease_expires_at - clock_timestamp())) * 1000)
                    )::bigint AS remaining_ms",
                &[
                    &self.inner.config.namespace,
                    &raw_key.as_deref(),
                    &key_id.as_bytes().as_slice(),
                    &owner_token.as_slice(),
                    &ttl_ms,
                ],
            )
            .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        let fencing_token = FencingToken::new(row.get(0));
        let remaining = positive_duration(row.get(1))?;
        Ok(Some(self.guard(
            key_id,
            owner_token,
            fencing_token,
            Duration::from_millis(u64::try_from(ttl_ms).unwrap_or_default()),
            remaining,
        )))
    }

    async fn recover_owner(
        &self,
        key_id: KeyId,
        owner_token: [u8; 32],
    ) -> Result<Option<(FencingToken, Duration)>, LockError> {
        let client = self.inner.pool.get().await?;
        let row = client
            .query_opt(
                "SELECT
                    fencing_token,
                    GREATEST(
                        0,
                        CEIL(EXTRACT(EPOCH FROM
                            (lease_expires_at - clock_timestamp())) * 1000)
                    )::bigint
                 FROM pgfw_lock
                 WHERE namespace = $1
                   AND key_hash = $2
                   AND owner_token = $3
                   AND lease_expires_at > clock_timestamp()",
                &[
                    &self.inner.config.namespace,
                    &key_id.as_bytes().as_slice(),
                    &owner_token.as_slice(),
                ],
            )
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some((
            FencingToken::new(row.get(0)),
            positive_duration(row.get(1))?,
        )))
    }

    fn guard(
        &self,
        key_id: KeyId,
        owner_token: [u8; 32],
        fencing_token: FencingToken,
        lease_ttl: Duration,
        database_remaining: Duration,
    ) -> LeaseGuard {
        let safe_remaining = database_remaining.saturating_sub(self.inner.config.safety_margin);
        LeaseGuard::new(
            Arc::clone(&self.inner),
            key_id,
            owner_token,
            fencing_token,
            lease_ttl,
            safe_remaining,
            database_remaining,
        )
    }
}

pub(crate) fn random_owner_token() -> Result<[u8; 32], LockError> {
    let mut token = [0u8; 32];
    getrandom::fill(&mut token).map_err(|error| LockError::Random(error.to_string()))?;
    Ok(token)
}

pub(crate) fn positive_duration(milliseconds: i64) -> Result<Duration, LockError> {
    let milliseconds = u64::try_from(milliseconds)
        .map_err(|_| LockError::InvalidConfig("invalid database lease duration"))?;
    if milliseconds == 0 {
        return Err(LockError::AlreadyLost);
    }
    Ok(Duration::from_millis(milliseconds))
}

fn jitter(duration: Duration) -> Duration {
    if duration.is_zero() {
        return duration;
    }
    let base = duration / 2;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.subsec_nanos());
    let spread = base.as_nanos().max(1);
    let offset = u128::from(nanos) % spread;
    base.saturating_add(Duration::from_nanos(
        u64::try_from(offset).unwrap_or_default(),
    ))
}
