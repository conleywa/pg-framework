use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pg_framework_core::{KeyId, duration_millis};
use tracing::info;

use crate::locker::{LockerInner, positive_duration};
use crate::{FencingToken, LeaseState, LockError, RenewOutcome};

/// 不可 Clone 的租约 Guard；drop 时不会执行数据库 I/O。
#[must_use = "the lease must be renewed or released explicitly"]
pub struct LeaseGuard {
    inner: Arc<LockerInner>,
    key_id: KeyId,
    owner_token: [u8; 32],
    fencing_token: FencingToken,
    lease_ttl: Duration,
    deadline: Option<Instant>,
    database_remaining: Duration,
    state: LeaseState,
}

impl LeaseGuard {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        inner: Arc<LockerInner>,
        key_id: KeyId,
        owner_token: [u8; 32],
        fencing_token: FencingToken,
        lease_ttl: Duration,
        safe_remaining: Duration,
        database_remaining: Duration,
    ) -> Self {
        Self {
            inner,
            key_id,
            owner_token,
            fencing_token,
            lease_ttl,
            deadline: Instant::now().checked_add(safe_remaining),
            database_remaining,
            state: if safe_remaining.is_zero() {
                LeaseState::Lost
            } else {
                LeaseState::Active
            },
        }
    }

    /// 返回被保护资源必须校验的 fencing token。
    #[must_use]
    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    /// 返回 Guard 当前状态。
    #[must_use]
    pub fn state(&self) -> LeaseState {
        if matches!(self.state, LeaseState::Active | LeaseState::Uncertain)
            && self
                .deadline
                .is_none_or(|deadline| Instant::now() >= deadline)
        {
            return LeaseState::Lost;
        }
        self.state
    }

    /// 返回本地安全 deadline 是否尚未过期。
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.state() == LeaseState::Active
    }

    /// 返回剩余安全时长。
    #[must_use]
    pub fn safe_remaining(&self) -> Duration {
        self.deadline.map_or(Duration::ZERO, |deadline| {
            deadline.saturating_duration_since(Instant::now())
        })
    }

    /// 续租。
    ///
    /// # 错误
    ///
    /// 无法确认续租结果时返回存储错误。
    pub async fn renew(&mut self) -> Result<RenewOutcome, LockError> {
        match self.state {
            LeaseState::Lost | LeaseState::Released => return Ok(RenewOutcome::Lost),
            LeaseState::Uncertain | LeaseState::Active => {}
        }
        if self
            .deadline
            .is_none_or(|deadline| Instant::now() >= deadline)
        {
            self.state = LeaseState::Lost;
            return Ok(RenewOutcome::Lost);
        }

        let client = match self.inner.pool.get().await {
            Ok(client) => client,
            Err(error) => {
                self.state = LeaseState::Uncertain;
                return Err(LockError::Pool(error));
            }
        };
        let ttl_ms = duration_millis(self.lease_ttl)?;
        let row = match client
            .query_opt(
                "UPDATE pgfw_lock
                 SET lease_expires_at =
                        clock_timestamp() + $4 * interval '1 millisecond'
                 WHERE namespace = $1
                   AND key_hash = $2
                   AND owner_token = $3
                   AND fencing_token = $5
                   AND lease_expires_at > clock_timestamp()
                 RETURNING GREATEST(
                    0,
                    CEIL(EXTRACT(EPOCH FROM
                        (lease_expires_at - clock_timestamp())) * 1000)
                 )::bigint",
                &[
                    &self.inner.config.namespace,
                    &self.key_id.as_bytes().as_slice(),
                    &self.owner_token.as_slice(),
                    &ttl_ms,
                    &self.fencing_token.get(),
                ],
            )
            .await
        {
            Ok(row) => row,
            Err(error) => {
                self.state = LeaseState::Uncertain;
                return Err(LockError::Postgres(error));
            }
        };

        let Some(row) = row else {
            self.state = LeaseState::Lost;
            return Ok(RenewOutcome::Lost);
        };
        let database_remaining = positive_duration(row.get(0))?;
        let safe_remaining = database_remaining.saturating_sub(self.inner.config.safety_margin);
        self.database_remaining = database_remaining;
        self.deadline = Instant::now().checked_add(safe_remaining);
        self.state = if safe_remaining.is_zero() {
            LeaseState::Lost
        } else {
            LeaseState::Active
        };

        if self.state == LeaseState::Lost {
            Ok(RenewOutcome::Lost)
        } else {
            Ok(RenewOutcome::Renewed {
                remaining: safe_remaining,
            })
        }
    }

    /// 释放租约。
    ///
    /// # 错误
    ///
    /// 无法确认释放结果时返回错误。
    pub async fn release(&mut self) -> Result<(), LockError> {
        match self.state {
            LeaseState::Released => return Ok(()),
            LeaseState::Lost => return Err(LockError::AlreadyLost),
            LeaseState::Active | LeaseState::Uncertain => {}
        }
        let client = match self.inner.pool.get().await {
            Ok(client) => client,
            Err(error) => {
                self.state = LeaseState::Uncertain;
                return Err(LockError::Pool(error));
            }
        };
        let deleted = match client
            .execute(
                "DELETE FROM pgfw_lock
                 WHERE namespace = $1
                   AND key_hash = $2
                   AND owner_token = $3
                   AND fencing_token = $4",
                &[
                    &self.inner.config.namespace,
                    &self.key_id.as_bytes().as_slice(),
                    &self.owner_token.as_slice(),
                    &self.fencing_token.get(),
                ],
            )
            .await
        {
            Ok(deleted) => deleted,
            Err(error) => {
                self.state = LeaseState::Uncertain;
                return Err(LockError::Postgres(error));
            }
        };
        if deleted == 0 {
            self.state = LeaseState::Lost;
            return Err(LockError::AlreadyLost);
        }
        self.state = LeaseState::Released;
        info!(fencing_token = self.fencing_token.get(), "锁已释放");
        Ok(())
    }
}

impl fmt::Debug for LeaseGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeaseGuard")
            .field("key_id", &self.key_id.to_hex())
            .field("fencing_token", &self.fencing_token)
            .field("state", &self.state())
            .field("safe_remaining", &self.safe_remaining())
            .finish()
    }
}
