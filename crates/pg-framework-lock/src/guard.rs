use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use pg_framework_core::{KeyId, duration_millis};
use tracing::info;

use crate::locker::{LockerInner, positive_duration};
use crate::{FencingToken, LeaseState, LockError, RenewOutcome};

struct LeaseRuntime {
    state: LeaseState,
    deadline: Option<Instant>,
    database_remaining: Duration,
    reentrant_depth: usize,
    release_in_progress: bool,
}

struct LeaseShared {
    key_id: KeyId,
    owner_token: [u8; 32],
    fencing_token: FencingToken,
    lease_ttl: Duration,
    runtime: Mutex<LeaseRuntime>,
}

impl LeaseShared {
    fn effective_state(runtime: &LeaseRuntime) -> LeaseState {
        if matches!(runtime.state, LeaseState::Active | LeaseState::Uncertain)
            && runtime
                .deadline
                .is_none_or(|deadline| Instant::now() >= deadline)
        {
            LeaseState::Lost
        } else {
            runtime.state
        }
    }
}

/// 不可 Clone 的租约 Guard；drop 时不会执行数据库 I/O。
///
/// `reenter()` 创建共享同一 owner/fencing token 的嵌套 Guard。只有最后一个
/// 未释放 Guard 显式 `release` 时才会删除数据库锁行。
#[must_use = "租约 Guard 必须显式续租或释放"]
pub struct LeaseGuard {
    inner: Arc<LockerInner>,
    shared: Arc<LeaseShared>,
    released: bool,
}

impl LeaseGuard {
    pub(crate) fn new(
        inner: Arc<LockerInner>,
        key_id: KeyId,
        owner_token: [u8; 32],
        fencing_token: FencingToken,
        lease_ttl: Duration,
        safe_remaining: Duration,
        database_remaining: Duration,
    ) -> Self {
        let initial_state = if safe_remaining.is_zero() {
            LeaseState::Lost
        } else {
            LeaseState::Active
        };
        Self {
            inner,
            shared: Arc::new(LeaseShared {
                key_id,
                owner_token,
                fencing_token,
                lease_ttl,
                runtime: Mutex::new(LeaseRuntime {
                    state: initial_state,
                    deadline: Instant::now().checked_add(safe_remaining),
                    database_remaining,
                    reentrant_depth: 1,
                    release_in_progress: false,
                }),
            }),
            released: false,
        }
    }

    /// 从当前 Guard 派生一个重入 Guard。
    ///
    /// 重入不访问 PostgreSQL，也不生成新的 fencing token。
    ///
    /// # 错误
    ///
    /// 当前租约不再有效、正在释放或达到重入上限时返回错误。
    pub fn reenter(&self) -> Result<Self, LockError> {
        let mut runtime = self.runtime()?;
        let state = LeaseShared::effective_state(&runtime);
        if self.released || runtime.release_in_progress || state != LeaseState::Active {
            return Err(LockError::AlreadyLost);
        }
        if runtime.reentrant_depth >= self.inner.config.max_reentrant_depth {
            return Err(LockError::ReentrantLimitExceeded {
                max: self.inner.config.max_reentrant_depth,
            });
        }
        runtime.reentrant_depth += 1;
        drop(runtime);
        Ok(Self {
            inner: Arc::clone(&self.inner),
            shared: Arc::clone(&self.shared),
            released: false,
        })
    }

    /// 返回当前 Guard 派生链的重入深度。
    #[must_use]
    pub fn reentrant_depth(&self) -> usize {
        self.runtime().map_or(0, |runtime| runtime.reentrant_depth)
    }

    /// 返回被保护资源必须校验的 fencing token。
    #[must_use]
    pub fn fencing_token(&self) -> FencingToken {
        self.shared.fencing_token
    }

    /// 返回 Guard 当前状态。
    #[must_use]
    pub fn state(&self) -> LeaseState {
        if self.released {
            return LeaseState::Released;
        }
        self.runtime().map_or(LeaseState::Lost, |runtime| {
            LeaseShared::effective_state(&runtime)
        })
    }

    /// 返回本地安全 deadline 是否尚未过期。
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.state() == LeaseState::Active
    }

    /// 返回剩余安全时长。
    #[must_use]
    pub fn safe_remaining(&self) -> Duration {
        if self.released {
            return Duration::ZERO;
        }
        self.runtime().map_or(Duration::ZERO, |runtime| {
            runtime.deadline.map_or(Duration::ZERO, |deadline| {
                deadline.saturating_duration_since(Instant::now())
            })
        })
    }

    /// 续租。所有重入 Guard 共享更新后的 deadline。
    ///
    /// # 错误
    ///
    /// 无法确认续租结果时返回存储错误。
    pub async fn renew(&mut self) -> Result<RenewOutcome, LockError> {
        if self.released {
            return Ok(RenewOutcome::Lost);
        }
        let state = self.state();
        if matches!(state, LeaseState::Lost | LeaseState::Released) {
            self.mark_state(LeaseState::Lost)?;
            return Ok(RenewOutcome::Lost);
        }

        let ttl_ms = duration_millis(self.shared.lease_ttl)?;
        let renew_result = match tokio::time::timeout(self.inner.config.operation_timeout, async {
            let client = self.inner.pool.get().await?;
            client
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
                        &self.shared.key_id.as_bytes().as_slice(),
                        &self.shared.owner_token.as_slice(),
                        &ttl_ms,
                        &self.shared.fencing_token.get(),
                    ],
                )
                .await
                .map_err(LockError::from)
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(LockError::Timeout("renew")),
        };

        match renew_result {
            Ok(Some(row)) => {
                let database_remaining = positive_duration(row.get(0))?;
                Ok(self.apply_renewal(database_remaining)?)
            }
            Ok(None) => {
                self.mark_state(LeaseState::Lost)?;
                Ok(RenewOutcome::Lost)
            }
            Err(original_error) => {
                self.mark_state(LeaseState::Uncertain)?;
                match self.reconcile_owned().await {
                    Ok(Some(database_remaining)) => Ok(self.apply_renewal(database_remaining)?),
                    Ok(None) => {
                        self.mark_state(LeaseState::Lost)?;
                        Ok(RenewOutcome::Lost)
                    }
                    Err(_) => Err(original_error),
                }
            }
        }
    }

    /// 释放当前重入层级。
    ///
    /// 嵌套 Guard 只减少本地深度；最后一个 Guard 释放时才删除数据库锁行。
    ///
    /// # 错误
    ///
    /// 无法确认最终释放结果时返回错误，Guard 可继续用于重试。
    pub async fn release(&mut self) -> Result<(), LockError> {
        if self.released {
            return Ok(());
        }
        let state = self.state();
        if state == LeaseState::Lost {
            return Err(LockError::AlreadyLost);
        }

        let final_release = {
            let mut runtime = self.runtime()?;
            if runtime.release_in_progress {
                return Err(LockError::OutcomeUnknown {
                    operation: "release",
                });
            }
            match runtime.reentrant_depth {
                0 => return Err(LockError::AlreadyLost),
                1 => {
                    runtime.release_in_progress = true;
                    true
                }
                _ => {
                    runtime.reentrant_depth -= 1;
                    false
                }
            }
        };
        if !final_release {
            self.released = true;
            return Ok(());
        }

        let release_result =
            match tokio::time::timeout(self.inner.config.operation_timeout, async {
                let client = self.inner.pool.get().await?;
                client
                    .execute(
                        "DELETE FROM pgfw_lock
                         WHERE namespace = $1
                           AND key_hash = $2
                           AND owner_token = $3
                           AND fencing_token = $4",
                        &[
                            &self.inner.config.namespace,
                            &self.shared.key_id.as_bytes().as_slice(),
                            &self.shared.owner_token.as_slice(),
                            &self.shared.fencing_token.get(),
                        ],
                    )
                    .await
                    .map_err(LockError::from)
            })
            .await
            {
                Ok(result) => result,
                Err(_) => Err(LockError::Timeout("release")),
            };

        match release_result {
            Ok(0) => match self.reconcile_owned().await {
                Ok(None) => {
                    self.finish_release()?;
                    Ok(())
                }
                Ok(Some(_)) => {
                    self.abort_release(LeaseState::Uncertain)?;
                    Err(LockError::OutcomeUnknown {
                        operation: "release",
                    })
                }
                Err(error) => {
                    self.abort_release(LeaseState::Uncertain)?;
                    Err(error)
                }
            },
            Ok(_) => {
                self.finish_release()?;
                info!(fencing_token = self.shared.fencing_token.get(), "锁已释放");
                Ok(())
            }
            Err(original_error) => match self.reconcile_owned().await {
                Ok(None) => {
                    self.finish_release()?;
                    Ok(())
                }
                Ok(Some(_)) => {
                    self.abort_release(LeaseState::Uncertain)?;
                    Err(original_error)
                }
                Err(error) => {
                    self.abort_release(LeaseState::Uncertain)?;
                    Err(error)
                }
            },
        }
    }

    fn runtime(&self) -> Result<MutexGuard<'_, LeaseRuntime>, LockError> {
        self.shared
            .runtime
            .lock()
            .map_err(|_| LockError::InvalidConfig("lease guard state is poisoned"))
    }

    fn mark_state(&self, state: LeaseState) -> Result<(), LockError> {
        self.runtime()?.state = state;
        Ok(())
    }

    fn apply_renewal(&self, database_remaining: Duration) -> Result<RenewOutcome, LockError> {
        let mut runtime = self.runtime()?;
        let safe_remaining = database_remaining.saturating_sub(self.inner.config.safety_margin);
        runtime.database_remaining = database_remaining;
        runtime.deadline = Instant::now().checked_add(safe_remaining);
        runtime.state = if safe_remaining.is_zero() {
            LeaseState::Lost
        } else {
            LeaseState::Active
        };
        if runtime.state == LeaseState::Lost {
            Ok(RenewOutcome::Lost)
        } else {
            Ok(RenewOutcome::Renewed {
                remaining: safe_remaining,
            })
        }
    }

    fn finish_release(&mut self) -> Result<(), LockError> {
        let mut runtime = self.runtime()?;
        runtime.state = LeaseState::Released;
        runtime.reentrant_depth = 0;
        runtime.release_in_progress = false;
        drop(runtime);
        self.released = true;
        Ok(())
    }

    fn abort_release(&self, state: LeaseState) -> Result<(), LockError> {
        let mut runtime = self.runtime()?;
        runtime.state = state;
        runtime.release_in_progress = false;
        Ok(())
    }

    async fn reconcile_owned(&self) -> Result<Option<Duration>, LockError> {
        tokio::time::timeout(self.inner.config.operation_timeout, async {
            let client = self.inner.pool.get().await?;
            let row = client
                .query_opt(
                    "SELECT
                        owner_token,
                        fencing_token,
                        lease_expires_at > clock_timestamp(),
                        GREATEST(
                            0,
                            CEIL(EXTRACT(EPOCH FROM
                                (lease_expires_at - clock_timestamp())) * 1000)
                        )::bigint
                     FROM pgfw_lock
                     WHERE namespace = $1 AND key_hash = $2",
                    &[
                        &self.inner.config.namespace,
                        &self.shared.key_id.as_bytes().as_slice(),
                    ],
                )
                .await
                .map_err(LockError::from)?;
            let Some(row) = row else {
                return Ok(None);
            };
            let owner: Vec<u8> = row.get(0);
            let fencing: i64 = row.get(1);
            let active: bool = row.get(2);
            if owner.as_slice() != self.shared.owner_token
                || fencing != self.shared.fencing_token.get()
                || !active
            {
                return Ok(None);
            }
            positive_duration(row.get(3)).map(Some)
        })
        .await
        .map_err(|_| LockError::Timeout("reconcile"))?
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        if let Ok(mut runtime) = self.shared.runtime.lock()
            && !runtime.release_in_progress
            && runtime.reentrant_depth > 0
        {
            runtime.reentrant_depth -= 1;
        }
    }
}

impl fmt::Debug for LeaseGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeaseGuard")
            .field("key_id", &self.shared.key_id.to_hex())
            .field("fencing_token", &self.shared.fencing_token)
            .field("state", &self.state())
            .field("safe_remaining", &self.safe_remaining())
            .field("reentrant_depth", &self.reentrant_depth())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LockConfig;

    fn guard(max_reentrant_depth: usize) -> LeaseGuard {
        let pool = pg_framework_core::build_pool(tokio_postgres::Config::new(), 1).expect("pool");
        let mut config = LockConfig::new("test");
        config.max_reentrant_depth = max_reentrant_depth;
        let inner = Arc::new(LockerInner { pool, config });
        LeaseGuard::new(
            inner,
            KeyId::derive("test", b"key"),
            [7; 32],
            FencingToken::new(1),
            Duration::from_secs(10),
            Duration::from_secs(9),
            Duration::from_secs(10),
        )
    }

    #[tokio::test]
    async fn reentrant_release_only_releases_one_logical_level() {
        let root = guard(4);
        let mut nested = root.reenter().expect("reenter");
        assert_eq!(root.reentrant_depth(), 2);
        assert_eq!(nested.fencing_token(), root.fencing_token());

        nested.release().await.expect("release nested");

        assert_eq!(nested.state(), LeaseState::Released);
        assert_eq!(root.reentrant_depth(), 1);
        assert!(root.is_active());
    }

    #[test]
    fn reenter_enforces_configured_depth_limit() {
        let root = guard(1);

        assert!(matches!(
            root.reenter(),
            Err(LockError::ReentrantLimitExceeded { max: 1 })
        ));
    }
}
