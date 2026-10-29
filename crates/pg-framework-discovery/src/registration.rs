use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pg_framework_core::{KeyId, duration_millis};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::error::DiscoveryError;
use crate::notify::notify_service;
use crate::registry::ServiceRegistry;
use crate::types::{RegistrationId, RegistrationSnapshot, RegistrationStatus};

pub(crate) struct RegistrationSpec {
    pub(crate) service_name: String,
    pub(crate) service_hash: KeyId,
    pub(crate) registration_id: RegistrationId,
    pub(crate) owner_token: [u8; 32],
    pub(crate) endpoint: String,
    pub(crate) metadata: Vec<u8>,
    pub(crate) lease_ttl: Duration,
}

struct RegistrationRuntime {
    status: RegistrationStatus,
    version: i64,
    deadline: Option<Instant>,
}

struct RegistrationShared {
    registry: ServiceRegistry,
    spec: RegistrationSpec,
    runtime: Mutex<RegistrationRuntime>,
}

struct RegistrationAck {
    version: i64,
    remaining: Duration,
}

/// 已在 PostgreSQL 注册、并由后台心跳续约的服务实例。
#[must_use = "drop 后实例将在 lease 到期后从发现结果消失"]
pub struct ServiceRegistration {
    shared: Arc<RegistrationShared>,
    shutdown: watch::Sender<bool>,
    handle: Option<JoinHandle<()>>,
}

impl ServiceRegistration {
    pub(crate) async fn start(
        registry: ServiceRegistry,
        spec: RegistrationSpec,
    ) -> Result<Self, DiscoveryError> {
        let shared = Arc::new(RegistrationShared {
            registry,
            spec,
            runtime: Mutex::new(RegistrationRuntime {
                status: RegistrationStatus::Uncertain,
                version: 0,
                deadline: None,
            }),
        });
        let ack = shared
            .registry
            .timed("register", register_once(&shared))
            .await?;
        update_runtime(&shared, ack)?;
        info!(
            registration_id = %shared.spec.registration_id,
            "服务实例已注册"
        );

        let (shutdown, receiver) = watch::channel(false);
        let handle = tokio::spawn(heartbeat_loop(Arc::clone(&shared), receiver));
        Ok(Self {
            shared,
            shutdown,
            handle: Some(handle),
        })
    }

    /// 返回实例注册标识。
    #[must_use]
    pub fn registration_id(&self) -> RegistrationId {
        self.shared.spec.registration_id
    }

    /// 返回当前注册快照。
    #[must_use]
    pub fn snapshot(&self) -> RegistrationSnapshot {
        let runtime = self.shared.runtime.lock();
        let (status, version, remaining) =
            runtime.map_or((RegistrationStatus::Lost, 0, Duration::ZERO), |runtime| {
                let status = effective_status(&runtime);
                let remaining = runtime.deadline.map_or(Duration::ZERO, |deadline| {
                    deadline.saturating_duration_since(Instant::now())
                });
                (status, runtime.version, remaining)
            });
        RegistrationSnapshot {
            registration_id: self.shared.spec.registration_id,
            status,
            version,
            remaining,
        }
    }

    /// 立即执行一次心跳；锁行不存在时使用同一身份重新注册。
    ///
    /// # 错误
    ///
    /// PostgreSQL 操作失败或注册身份冲突时返回错误。
    pub async fn heartbeat(&self) -> Result<RegistrationSnapshot, DiscoveryError> {
        let result = self
            .shared
            .registry
            .timed("heartbeat", refresh_once(&self.shared))
            .await;
        match result {
            Ok(ack) => {
                update_runtime(&self.shared, ack)?;
                Ok(self.snapshot())
            }
            Err(error) => {
                set_runtime_status(&self.shared, failure_status(&self.shared));
                Err(error)
            }
        }
    }

    /// 停止心跳并删除注册行。
    ///
    /// # 错误
    ///
    /// 后台任务 join、关闭超时或 PostgreSQL 删除失败时返回错误。
    pub async fn shutdown(mut self) -> Result<(), DiscoveryError> {
        self.set_status(RegistrationStatus::Closed);
        let _ = self.shutdown.send(true);
        let timeout = self.shared.registry.inner.config.shutdown_timeout;
        if let Some(mut handle) = self.handle.take() {
            match tokio::time::timeout(timeout, &mut handle).await {
                Ok(result) => result?,
                Err(_) => {
                    handle.abort();
                    return Err(DiscoveryError::Timeout("registration shutdown"));
                }
            }
        }
        self.shared
            .registry
            .timed("deregister", deregister_once(&self.shared))
            .await?;
        info!(
            registration_id = %self.shared.spec.registration_id,
            "服务实例已注销"
        );
        Ok(())
    }

    fn set_status(&self, status: RegistrationStatus) {
        if let Ok(mut runtime) = self.shared.runtime.lock() {
            runtime.status = status;
        }
    }
}

impl Drop for ServiceRegistration {
    fn drop(&mut self) {
        self.set_status(RegistrationStatus::Closed);
        let _ = self.shutdown.send(true);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

async fn heartbeat_loop(shared: Arc<RegistrationShared>, mut shutdown: watch::Receiver<bool>) {
    let interval_duration = shared.registry.inner.config.heartbeat_interval;
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
            .registry
            .timed("heartbeat", refresh_once(&shared))
            .await;
        match result {
            Ok(ack) => {
                if let Err(error) = update_runtime(&shared, ack) {
                    set_runtime_status(&shared, RegistrationStatus::Lost);
                    warn!(error = %error, "服务注册心跳状态更新失败");
                    break;
                }
                backoff = interval_duration;
            }
            Err(error) => {
                set_runtime_status(&shared, failure_status(&shared));
                warn!(error = %error, "服务注册心跳失败");
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

async fn refresh_once(shared: &RegistrationShared) -> Result<RegistrationAck, DiscoveryError> {
    if let Some(ack) = heartbeat_once(shared).await? {
        return Ok(ack);
    }
    register_once(shared).await
}

async fn heartbeat_once(
    shared: &RegistrationShared,
) -> Result<Option<RegistrationAck>, DiscoveryError> {
    let ttl_ms = duration_millis(shared.spec.lease_ttl)?;
    let mut client = shared.registry.inner.pool.get().await?;
    let transaction = client.transaction().await?;
    let row = transaction
        .query_opt(
            "UPDATE pgfw_service_instance
             SET lease_expires_at =
                    clock_timestamp() + $5 * interval '1 millisecond',
                 updated_at = clock_timestamp()
             WHERE namespace = $1
               AND service_hash = $2
               AND registration_id = $3
               AND owner_token = $4
               AND lease_expires_at > clock_timestamp()
             RETURNING
                version,
                GREATEST(
                    0,
                    CEIL(EXTRACT(EPOCH FROM
                        (lease_expires_at - clock_timestamp())) * 1000)
                )::bigint",
            &[
                &shared.registry.inner.config.namespace,
                &shared.spec.service_hash.as_bytes().as_slice(),
                &shared.spec.registration_id.as_bytes().as_slice(),
                &shared.spec.owner_token.as_slice(),
                &ttl_ms,
            ],
        )
        .await?;
    Ok(row.map(|row| RegistrationAck {
        version: row.get(0),
        remaining: Duration::from_millis(u64::try_from(row.get::<_, i64>(1)).unwrap_or_default()),
    }))
}

async fn register_once(shared: &RegistrationShared) -> Result<RegistrationAck, DiscoveryError> {
    let ttl_ms = duration_millis(shared.spec.lease_ttl)?;
    let mut client = shared.registry.inner.pool.get().await?;
    let transaction = client.transaction().await?;
    let row = transaction
        .query_opt(
            "INSERT INTO pgfw_service_instance (
                namespace,
                service_name,
                service_hash,
                registration_id,
                owner_token,
                endpoint,
                metadata,
                version,
                registered_at,
                lease_expires_at,
                updated_at
             )
             VALUES (
                $1, $2, $3, $4, $5, $6, $7, 1,
                clock_timestamp(),
                clock_timestamp() + $8 * interval '1 millisecond',
                clock_timestamp()
             )
             ON CONFLICT (service_hash, registration_id)
             DO UPDATE SET
                owner_token = EXCLUDED.owner_token,
                service_name = EXCLUDED.service_name,
                endpoint = EXCLUDED.endpoint,
                metadata = EXCLUDED.metadata,
                version = pgfw_service_instance.version + 1,
                lease_expires_at = EXCLUDED.lease_expires_at,
                updated_at = clock_timestamp()
             WHERE pgfw_service_instance.owner_token = EXCLUDED.owner_token
                OR pgfw_service_instance.lease_expires_at <= clock_timestamp()
             RETURNING
                version,
                GREATEST(
                    0,
                    CEIL(EXTRACT(EPOCH FROM
                        (lease_expires_at - clock_timestamp())) * 1000)
                )::bigint",
            &[
                &shared.registry.inner.config.namespace,
                &shared.spec.service_name,
                &shared.spec.service_hash.as_bytes().as_slice(),
                &shared.spec.registration_id.as_bytes().as_slice(),
                &shared.spec.owner_token.as_slice(),
                &shared.spec.endpoint,
                &shared.spec.metadata,
                &ttl_ms,
            ],
        )
        .await?;
    let Some(row) = row else {
        return Err(DiscoveryError::RegistrationConflict);
    };
    notify_service(
        &transaction,
        &shared.registry.inner.config.notification_channel,
        shared.spec.service_hash,
    )
    .await?;
    transaction.commit().await?;
    Ok(RegistrationAck {
        version: row.get(0),
        remaining: Duration::from_millis(u64::try_from(row.get::<_, i64>(1)).unwrap_or_default()),
    })
}

async fn deregister_once(shared: &RegistrationShared) -> Result<(), DiscoveryError> {
    let mut client = shared.registry.inner.pool.get().await?;
    let transaction = client.transaction().await?;
    let deleted = transaction
        .execute(
            "DELETE FROM pgfw_service_instance
             WHERE namespace = $1
               AND service_hash = $2
               AND registration_id = $3
               AND owner_token = $4",
            &[
                &shared.registry.inner.config.namespace,
                &shared.spec.service_hash.as_bytes().as_slice(),
                &shared.spec.registration_id.as_bytes().as_slice(),
                &shared.spec.owner_token.as_slice(),
            ],
        )
        .await?;
    if deleted > 0 {
        notify_service(
            &transaction,
            &shared.registry.inner.config.notification_channel,
            shared.spec.service_hash,
        )
        .await?;
    }
    transaction.commit().await?;
    Ok(())
}

fn update_runtime(shared: &RegistrationShared, ack: RegistrationAck) -> Result<(), DiscoveryError> {
    let mut runtime = shared
        .runtime
        .lock()
        .map_err(|_| DiscoveryError::InvalidConfig("registration state is poisoned"))?;
    runtime.status = RegistrationStatus::Active;
    runtime.version = ack.version;
    runtime.deadline = Instant::now().checked_add(ack.remaining);
    Ok(())
}

fn set_runtime_status(shared: &RegistrationShared, status: RegistrationStatus) {
    if let Ok(mut runtime) = shared.runtime.lock() {
        runtime.status = status;
    }
}

fn effective_status(runtime: &RegistrationRuntime) -> RegistrationStatus {
    if matches!(
        runtime.status,
        RegistrationStatus::Active | RegistrationStatus::Uncertain
    ) && runtime
        .deadline
        .is_none_or(|deadline| Instant::now() >= deadline)
    {
        RegistrationStatus::Lost
    } else {
        runtime.status
    }
}

fn failure_status(shared: &RegistrationShared) -> RegistrationStatus {
    shared
        .runtime
        .lock()
        .ok()
        .map_or(RegistrationStatus::Lost, |runtime| {
            if runtime
                .deadline
                .is_none_or(|deadline| Instant::now() >= deadline)
            {
                RegistrationStatus::Lost
            } else {
                RegistrationStatus::Uncertain
            }
        })
}
