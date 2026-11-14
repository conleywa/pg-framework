use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pg_framework_core::duration_millis;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::client::{CoordinationClient, SessionCredentials, notify_namespace};
use crate::error::CoordinationError;
use crate::path::NodePath;
use crate::types::{CoordinationNode, CreateMode, SessionId, SessionSnapshot, SessionStatus};

struct SessionRuntime {
    status: SessionStatus,
    version: i64,
    deadline: Option<Instant>,
}

struct SessionShared {
    client: CoordinationClient,
    session_id: SessionId,
    owner_token: [u8; 32],
    lease_ttl: Duration,
    runtime: Mutex<SessionRuntime>,
}

struct SessionAck {
    version: i64,
    remaining: Duration,
}

/// 已在 PostgreSQL 建立 lease 并由后台心跳续约的协调会话。
///
/// 该会话创建的所有临时节点都属于同一 lease。显式 [`shutdown`](Self::shutdown)
/// 会立即删除会话及其全部临时节点；drop 只停止心跳，节点在数据库 lease
/// 到期后失效。
#[must_use = "drop 后临时节点只会等待 session lease 过期"]
pub struct CoordinationSession {
    shared: Arc<SessionShared>,
    shutdown: watch::Sender<bool>,
    handle: Option<JoinHandle<()>>,
}

impl CoordinationSession {
    pub(crate) async fn start(
        client: CoordinationClient,
        lease_ttl: Option<Duration>,
    ) -> Result<Self, CoordinationError> {
        let lease_ttl = lease_ttl.unwrap_or(client.inner.config.lease_ttl);
        client.inner.config.validate_session_ttl(lease_ttl)?;
        let shared = Arc::new(SessionShared {
            client,
            session_id: SessionId::from_bytes(random_bytes()?),
            owner_token: random_bytes()?,
            lease_ttl,
            runtime: Mutex::new(SessionRuntime {
                status: SessionStatus::Uncertain,
                version: 0,
                deadline: None,
            }),
        });
        let ack = shared
            .client
            .timed("session create", create_once(&shared))
            .await?;
        update_runtime(&shared, ack)?;
        info!(session_id = %shared.session_id, "协调会话已建立");

        let (shutdown, receiver) = watch::channel(false);
        let handle = tokio::spawn(heartbeat_loop(Arc::clone(&shared), receiver));
        Ok(Self {
            shared,
            shutdown,
            handle: Some(handle),
        })
    }

    /// 返回会话标识。
    #[must_use]
    pub fn session_id(&self) -> SessionId {
        self.shared.session_id
    }

    /// 返回当前会话状态快照。
    #[must_use]
    pub fn snapshot(&self) -> SessionSnapshot {
        let runtime = self.shared.runtime.lock();
        let (status, version, remaining) =
            runtime.map_or((SessionStatus::Lost, 0, Duration::ZERO), |runtime| {
                let status = effective_status(&runtime);
                let remaining = runtime.deadline.map_or(Duration::ZERO, |deadline| {
                    deadline.saturating_duration_since(Instant::now())
                });
                (status, runtime.version, remaining)
            });
        SessionSnapshot {
            session_id: self.shared.session_id,
            status,
            version,
            remaining,
        }
    }

    /// 当前本地安全 deadline 内是否可创建临时节点。
    #[must_use]
    pub fn is_active(&self) -> bool {
        matches!(
            self.snapshot().status,
            SessionStatus::Active | SessionStatus::Uncertain
        )
    }

    /// 立即执行一次数据库心跳。
    ///
    /// 数据库 lease 已过期时，会话永久进入 [`SessionStatus::Lost`]；
    /// 不能通过后续心跳复活。
    ///
    /// # 错误
    ///
    /// PostgreSQL 失败、会话已丢失或已关闭时返回错误。
    pub async fn heartbeat(&self) -> Result<SessionSnapshot, CoordinationError> {
        match self.snapshot().status {
            SessionStatus::Active | SessionStatus::Uncertain => {}
            SessionStatus::Lost => return Err(CoordinationError::SessionLost),
            SessionStatus::Closed => return Err(CoordinationError::SessionClosed),
        }
        let result = self
            .shared
            .client
            .timed("session heartbeat", heartbeat_once(&self.shared))
            .await;
        match result {
            Ok(Some(ack)) => {
                update_runtime(&self.shared, ack)?;
                Ok(self.snapshot())
            }
            Ok(None) => {
                set_runtime_status(&self.shared, SessionStatus::Lost);
                Err(CoordinationError::SessionLost)
            }
            Err(error) => {
                set_runtime_status(&self.shared, failure_status(&self.shared));
                Err(error)
            }
        }
    }

    /// 在当前会话下创建临时节点。
    ///
    /// # 错误
    ///
    /// 会话失效、路径或数据非法、父节点不存在、父节点为临时节点或路径
    /// 已存在时返回错误。
    pub async fn create_ephemeral(
        &self,
        path: &NodePath,
        data: impl Into<Vec<u8>>,
    ) -> Result<CoordinationNode, CoordinationError> {
        self.require_active()?;
        self.shared
            .client
            .create_internal(
                path,
                data.into(),
                CreateMode::Ephemeral,
                Some(self.credentials()),
            )
            .await
    }

    /// 在指定父路径下创建顺序临时节点。
    ///
    /// `prefix` 是完整非根路径前缀，例如 `/services/orders/worker-`。
    /// 返回值中的实际路径带有零填充的十进制后缀。
    ///
    /// # 错误
    ///
    /// 会话失效、前缀或数据非法、父节点不存在或 PostgreSQL 失败时返回错误。
    pub async fn create_ephemeral_sequential(
        &self,
        prefix: &NodePath,
        data: impl Into<Vec<u8>>,
    ) -> Result<CoordinationNode, CoordinationError> {
        self.require_active()?;
        self.shared
            .client
            .create_internal(
                prefix,
                data.into(),
                CreateMode::EphemeralSequential,
                Some(self.credentials()),
            )
            .await
    }

    /// 停止后台心跳并立即删除会话及其全部临时节点。
    ///
    /// # 错误
    ///
    /// 后台任务 join、关闭超时或 PostgreSQL 删除失败时返回错误。
    pub async fn shutdown(mut self) -> Result<(), CoordinationError> {
        self.set_status(SessionStatus::Closed);
        let _ = self.shutdown.send(true);
        let timeout = self.shared.client.inner.config.shutdown_timeout;
        if let Some(mut handle) = self.handle.take() {
            match tokio::time::timeout(timeout, &mut handle).await {
                Ok(result) => result?,
                Err(_) => {
                    handle.abort();
                    return Err(CoordinationError::Timeout("session shutdown"));
                }
            }
        }
        self.shared
            .client
            .timed("session close", close_once(&self.shared))
            .await?;
        info!(session_id = %self.shared.session_id, "协调会话已关闭");
        Ok(())
    }

    fn credentials(&self) -> SessionCredentials<'_> {
        SessionCredentials {
            session_id: self.shared.session_id.as_bytes(),
            owner_token: &self.shared.owner_token,
        }
    }

    fn require_active(&self) -> Result<(), CoordinationError> {
        let status = self.snapshot().status;
        match status {
            SessionStatus::Active | SessionStatus::Uncertain => Ok(()),
            SessionStatus::Closed => Err(CoordinationError::SessionClosed),
            SessionStatus::Lost => Err(CoordinationError::SessionLost),
        }
    }

    fn set_status(&self, status: SessionStatus) {
        set_runtime_status(&self.shared, status);
    }
}

impl fmt::Debug for CoordinationSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoordinationSession")
            .field("session_id", &self.shared.session_id)
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl Drop for CoordinationSession {
    fn drop(&mut self) {
        self.set_status(SessionStatus::Closed);
        let _ = self.shutdown.send(true);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

async fn create_once(shared: &SessionShared) -> Result<SessionAck, CoordinationError> {
    let lease_ms = duration_millis(shared.lease_ttl)?;
    let client = shared.client.inner.pool.get().await?;
    let row = client
        .query_opt(
            "INSERT INTO pgfw_coordination_session (
                namespace,
                session_id,
                owner_token,
                version,
                lease_expires_at,
                created_at,
                updated_at
             )
             VALUES (
                $1,
                $2,
                $3,
                1,
                clock_timestamp() + $4 * interval '1 millisecond',
                clock_timestamp(),
                clock_timestamp()
             )
             ON CONFLICT (namespace, session_id) DO NOTHING
             RETURNING
                version,
                GREATEST(
                    0,
                    CEIL(EXTRACT(EPOCH FROM
                        (lease_expires_at - clock_timestamp())) * 1000)
                )::bigint",
            &[
                &shared.client.inner.config.namespace,
                &shared.session_id.as_bytes().as_slice(),
                &shared.owner_token.as_slice(),
                &lease_ms,
            ],
        )
        .await?;
    let Some(row) = row else {
        return Err(CoordinationError::SessionConflict);
    };
    Ok(SessionAck {
        version: row.get(0),
        remaining: millis_to_duration(row.get(1))?,
    })
}

async fn heartbeat_once(shared: &SessionShared) -> Result<Option<SessionAck>, CoordinationError> {
    let lease_ms = duration_millis(shared.lease_ttl)?;
    let client = shared.client.inner.pool.get().await?;
    let row = client
        .query_opt(
            "UPDATE pgfw_coordination_session
             SET lease_expires_at =
                    clock_timestamp() + $4 * interval '1 millisecond',
                 version = version + 1,
                 updated_at = clock_timestamp()
             WHERE namespace = $1
               AND session_id = $2
               AND owner_token = $3
               AND lease_expires_at > clock_timestamp()
             RETURNING
                version,
                GREATEST(
                    0,
                    CEIL(EXTRACT(EPOCH FROM
                        (lease_expires_at - clock_timestamp())) * 1000)
                )::bigint",
            &[
                &shared.client.inner.config.namespace,
                &shared.session_id.as_bytes().as_slice(),
                &shared.owner_token.as_slice(),
                &lease_ms,
            ],
        )
        .await?;
    row.map(|row| {
        Ok(SessionAck {
            version: row.get(0),
            remaining: millis_to_duration(row.get(1))?,
        })
    })
    .transpose()
}

async fn close_once(shared: &SessionShared) -> Result<(), CoordinationError> {
    let mut client = shared.client.inner.pool.get().await?;
    let transaction = client.transaction().await?;
    let deleted = transaction
        .execute(
            "DELETE FROM pgfw_coordination_session
             WHERE namespace = $1
               AND session_id = $2
               AND owner_token = $3",
            &[
                &shared.client.inner.config.namespace,
                &shared.session_id.as_bytes().as_slice(),
                &shared.owner_token.as_slice(),
            ],
        )
        .await?;
    if deleted > 0 {
        notify_namespace(
            &transaction,
            &shared.client.inner.config.notification_channel,
            shared.client.notification_id(),
        )
        .await?;
    }
    transaction.commit().await?;
    Ok(())
}

async fn heartbeat_loop(shared: Arc<SessionShared>, mut shutdown: watch::Receiver<bool>) {
    let interval_duration = shared.client.inner.config.heartbeat_interval;
    let mut interval = tokio::time::interval(interval_duration);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    let mut backoff = interval_duration;

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            _ = interval.tick() => {}
        }
        let result = shared
            .client
            .timed("session heartbeat", heartbeat_once(&shared))
            .await;
        match result {
            Ok(Some(ack)) => {
                if let Err(error) = update_runtime(&shared, ack) {
                    warn!(error = %error, "协调会话心跳安全窗口已耗尽");
                    set_runtime_status(&shared, SessionStatus::Lost);
                    break;
                }
                backoff = interval_duration;
            }
            Ok(None) => {
                warn!(session_id = %shared.session_id, "协调会话 lease 已丢失");
                set_runtime_status(&shared, SessionStatus::Lost);
                break;
            }
            Err(error) => {
                let status = failure_status(&shared);
                set_runtime_status(&shared, status);
                warn!(error = %error, "协调会话心跳失败");
                if status == SessionStatus::Lost {
                    break;
                }
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
            }
        }
    }
}

fn update_runtime(shared: &SessionShared, ack: SessionAck) -> Result<(), CoordinationError> {
    let safe_remaining = ack
        .remaining
        .saturating_sub(shared.client.inner.config.safety_margin);
    let mut runtime = shared
        .runtime
        .lock()
        .map_err(|_| CoordinationError::InvalidConfig("session state is poisoned"))?;
    match runtime.status {
        SessionStatus::Closed => return Err(CoordinationError::SessionClosed),
        SessionStatus::Lost => return Err(CoordinationError::SessionLost),
        SessionStatus::Active | SessionStatus::Uncertain => {}
    }
    if runtime
        .deadline
        .is_some_and(|deadline| Instant::now() >= deadline)
    {
        runtime.status = SessionStatus::Lost;
        runtime.deadline = None;
        return Err(CoordinationError::SessionLost);
    }
    if safe_remaining.is_zero() {
        runtime.status = SessionStatus::Lost;
        runtime.deadline = None;
        return Err(CoordinationError::SessionLost);
    }
    runtime.status = SessionStatus::Active;
    runtime.version = ack.version;
    runtime.deadline = Instant::now().checked_add(safe_remaining);
    Ok(())
}

fn set_runtime_status(shared: &SessionShared, status: SessionStatus) {
    if let Ok(mut runtime) = shared.runtime.lock() {
        runtime.status = status;
        if status == SessionStatus::Lost {
            runtime.deadline = None;
        }
    }
}

fn effective_status(runtime: &SessionRuntime) -> SessionStatus {
    if matches!(
        runtime.status,
        SessionStatus::Active | SessionStatus::Uncertain
    ) && runtime
        .deadline
        .is_none_or(|deadline| Instant::now() >= deadline)
    {
        SessionStatus::Lost
    } else {
        runtime.status
    }
}

fn failure_status(shared: &SessionShared) -> SessionStatus {
    shared
        .runtime
        .lock()
        .map_or(SessionStatus::Lost, |runtime| {
            if runtime
                .deadline
                .is_none_or(|deadline| Instant::now() >= deadline)
            {
                SessionStatus::Lost
            } else {
                SessionStatus::Uncertain
            }
        })
}

fn millis_to_duration(milliseconds: i64) -> Result<Duration, CoordinationError> {
    let milliseconds = u64::try_from(milliseconds)
        .map_err(|_| CoordinationError::InvalidConfig("invalid database lease duration"))?;
    if milliseconds == 0 {
        return Err(CoordinationError::SessionLost);
    }
    Ok(Duration::from_millis(milliseconds))
}

fn random_bytes<const N: usize>() -> Result<[u8; N], CoordinationError> {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes).map_err(|error| CoordinationError::Random(error.to_string()))?;
    Ok(bytes)
}
