use std::sync::Arc;
use std::time::Duration;

use pg_framework_core::KeyId;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio_postgres::{AsyncMessage, NoTls};
use tracing::{info, warn};

use crate::error::DiscoveryError;
use crate::registry::ServiceRegistry;
use crate::types::{ServiceInstance, ServiceSnapshot};

/// 持续推送服务实例变化的 watch 通道。
#[must_use = "drop 后服务发现 watch 会停止"]
pub struct ServiceWatch {
    receiver: watch::Receiver<ServiceSnapshot>,
    shutdown: watch::Sender<bool>,
    handle: Option<JoinHandle<()>>,
    shutdown_timeout: Duration,
}

impl ServiceWatch {
    /// 返回最新快照。
    #[must_use]
    pub fn latest(&self) -> ServiceSnapshot {
        self.receiver.borrow().clone()
    }

    /// 克隆一个独立 receiver。
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<ServiceSnapshot> {
        self.receiver.clone()
    }

    /// 等待下一次快照更新。
    ///
    /// # 错误
    ///
    /// watch 任务已结束时返回错误。
    pub async fn changed(&mut self) -> Result<(), DiscoveryError> {
        self.receiver
            .changed()
            .await
            .map_err(|_| DiscoveryError::WatchStartup)
    }

    /// 停止 watch 后台任务。
    ///
    /// # 错误
    ///
    /// 后台任务 panic 或关闭超时时返回错误。
    pub async fn shutdown(mut self) -> Result<(), DiscoveryError> {
        let _ = self.shutdown.send(true);
        if let Some(mut handle) = self.handle.take() {
            match tokio::time::timeout(self.shutdown_timeout, &mut handle).await {
                Ok(result) => result?,
                Err(_) => {
                    handle.abort();
                    return Err(DiscoveryError::Timeout("watch shutdown"));
                }
            }
        }
        Ok(())
    }
}

impl Drop for ServiceWatch {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

impl ServiceRegistry {
    /// 监听 service 实例变化，并在返回前完成 LISTEN 和初始快照。
    ///
    /// # 错误
    ///
    /// service/limit/interval 非法，或启动超时时返回错误。
    pub async fn watch(
        &self,
        service: &str,
        postgres_config: tokio_postgres::Config,
        limit: usize,
        refresh_interval: Option<Duration>,
    ) -> Result<ServiceWatch, DiscoveryError> {
        if limit == 0 {
            return Err(DiscoveryError::InvalidConfig(
                "watch limit must be positive",
            ));
        }
        let service_hash = self.service_hash(service)?;
        let refresh_interval = refresh_interval.unwrap_or(self.inner.config.heartbeat_interval);
        if refresh_interval.is_zero() {
            return Err(DiscoveryError::InvalidConfig(
                "watch refresh interval must be positive",
            ));
        }

        let (snapshot_tx, receiver) = watch::channel(ServiceSnapshot {
            instances: Arc::from(Vec::<ServiceInstance>::new()),
            revision: 0,
            stale: true,
        });
        let (ready_tx, ready_rx) = oneshot::channel();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let registry = self.clone();
        let handle = tokio::spawn(run(
            registry,
            postgres_config,
            service_hash,
            limit,
            refresh_interval,
            snapshot_tx,
            shutdown_rx,
            Some(ready_tx),
        ));
        let mut service_watch = ServiceWatch {
            receiver,
            shutdown,
            handle: Some(handle),
            shutdown_timeout: self.inner.config.shutdown_timeout,
        };

        let startup_timeout = self.inner.config.shutdown_timeout;
        match tokio::time::timeout(startup_timeout, ready_rx).await {
            Ok(Ok(())) => Ok(service_watch),
            Ok(Err(_)) => {
                service_watch.abort();
                Err(DiscoveryError::WatchStartup)
            }
            Err(_) => {
                service_watch.abort();
                Err(DiscoveryError::Timeout("watch startup"))
            }
        }
    }
}

impl ServiceWatch {
    fn abort(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run(
    registry: ServiceRegistry,
    postgres_config: tokio_postgres::Config,
    service_hash: KeyId,
    limit: usize,
    refresh_interval: Duration,
    snapshot_tx: watch::Sender<ServiceSnapshot>,
    mut shutdown: watch::Receiver<bool>,
    mut ready_tx: Option<oneshot::Sender<()>>,
) {
    let mut revision = 0u64;
    let mut current: Arc<[ServiceInstance]> = Arc::from(Vec::<ServiceInstance>::new());
    let mut backoff = Duration::from_millis(200);
    loop {
        if *shutdown.borrow() {
            break;
        }
        if let Err(error) = registry.verify_schema().await {
            warn!(error = %error, "服务发现 watch schema 校验失败");
            mark_stale(&snapshot_tx, &mut revision, Arc::clone(&current));
            if !sleep_or_shutdown(backoff, &mut shutdown).await {
                break;
            }
            backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
            continue;
        }

        let connect = postgres_config.connect(NoTls);
        let connected = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
            result = tokio::time::timeout(
                registry.inner.config.operation_timeout,
                connect,
            ) => result,
        };
        let (client, connection) = match connected {
            Ok(Ok(connection)) => connection,
            Ok(Err(error)) => {
                warn!(error = %error, "服务发现 watch 连接失败");
                if !sleep_or_shutdown(backoff, &mut shutdown).await {
                    break;
                }
                backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
                continue;
            }
            Err(_) => {
                warn!("服务发现 watch 连接超时");
                if !sleep_or_shutdown(backoff, &mut shutdown).await {
                    break;
                }
                backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
                continue;
            }
        };

        let (notification_tx, mut notifications) = tokio::sync::mpsc::channel(256);
        let connection_task = tokio::spawn(async move {
            let mut connection = connection;
            loop {
                let message =
                    std::future::poll_fn(|context| connection.poll_message(context)).await;
                match message {
                    Some(Ok(AsyncMessage::Notification(notification))) => {
                        if notification_tx.send(notification).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(AsyncMessage::Notice(_))) | Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        warn!(error = %error, "服务发现 watch PostgreSQL 连接已结束");
                        break;
                    }
                    None => break,
                }
            }
        });

        let listen = format!("LISTEN \"{}\"", registry.inner.config.notification_channel);
        if let Err(error) = client.batch_execute(&listen).await {
            warn!(error = %error, "服务发现 watch LISTEN 失败");
            connection_task.abort();
            if !sleep_or_shutdown(backoff, &mut shutdown).await {
                break;
            }
            backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
            continue;
        }

        match registry.discover_by_hash(service_hash, limit).await {
            Ok(instances) => {
                current = Arc::from(instances);
                publish(&snapshot_tx, &mut revision, Arc::clone(&current), false);
                if let Some(sender) = ready_tx.take() {
                    let _ = sender.send(());
                }
                info!("服务发现 watch 已订阅并完成初始快照");
            }
            Err(error) => {
                warn!(error = %error, "服务发现 watch 初始快照失败");
                connection_task.abort();
                if !sleep_or_shutdown(backoff, &mut shutdown).await {
                    break;
                }
                backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
                continue;
            }
        }
        backoff = Duration::from_millis(200);

        let mut interval = tokio::time::interval(refresh_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            let should_refresh = tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        connection_task.abort();
                        return;
                    }
                    false
                }
                notification = notifications.recv() => {
                    let Some(notification) = notification else {
                        warn!("服务发现 watch 通知流已结束");
                        connection_task.abort();
                        break;
                    };
                    notification.payload() == service_hash.to_hex()
                }
                _ = interval.tick() => true,
            };
            if !should_refresh {
                continue;
            }
            match registry.discover_by_hash(service_hash, limit).await {
                Ok(instances) => {
                    current = Arc::from(instances);
                    publish(&snapshot_tx, &mut revision, Arc::clone(&current), false);
                }
                Err(error) => {
                    warn!(error = %error, "服务发现 watch 刷新失败");
                    mark_stale(&snapshot_tx, &mut revision, Arc::clone(&current));
                }
            }
        }
        mark_stale(&snapshot_tx, &mut revision, Arc::clone(&current));
        if !sleep_or_shutdown(backoff, &mut shutdown).await {
            break;
        }
        backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
    }
}

fn publish(
    sender: &watch::Sender<ServiceSnapshot>,
    revision: &mut u64,
    instances: Arc<[ServiceInstance]>,
    stale: bool,
) {
    *revision = revision.wrapping_add(1);
    let _ = sender.send(ServiceSnapshot {
        instances,
        revision: *revision,
        stale,
    });
}

fn mark_stale(
    sender: &watch::Sender<ServiceSnapshot>,
    revision: &mut u64,
    instances: Arc<[ServiceInstance]>,
) {
    publish(sender, revision, instances, true);
}

async fn sleep_or_shutdown(duration: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        changed = shutdown.changed() => {
            changed.is_ok() && !*shutdown.borrow()
        }
        () = tokio::time::sleep(duration) => true,
    }
}
