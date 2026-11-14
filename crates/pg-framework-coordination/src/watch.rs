use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_postgres::{AsyncMessage, NoTls};
use tracing::{info, warn};

use crate::client::{CoordinationClient, empty_snapshot};
use crate::error::CoordinationError;
use crate::path::NodePath;
use crate::types::CoordinationSnapshot;

/// 持续推送目标路径及直接子节点变化的 watch。
#[must_use = "drop 后协调节点 watch 会停止"]
pub struct CoordinationWatch {
    receiver: watch::Receiver<CoordinationSnapshot>,
    shutdown: watch::Sender<bool>,
    handle: Option<JoinHandle<()>>,
    shutdown_timeout: Duration,
}

impl CoordinationWatch {
    /// 返回最新快照。
    #[must_use]
    pub fn latest(&self) -> CoordinationSnapshot {
        self.receiver.borrow().clone()
    }

    /// 克隆一个独立 receiver。
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<CoordinationSnapshot> {
        self.receiver.clone()
    }

    /// 等待下一次快照更新。
    ///
    /// # 错误
    ///
    /// watch 后台任务已结束时返回错误。
    pub async fn changed(&mut self) -> Result<(), CoordinationError> {
        self.receiver
            .changed()
            .await
            .map_err(|_| CoordinationError::WatchStartup)
    }

    /// 停止 watch 后台任务。
    ///
    /// # 错误
    ///
    /// 后台任务 panic 或关闭超时时返回错误。
    pub async fn shutdown(mut self) -> Result<(), CoordinationError> {
        let _ = self.shutdown.send(true);
        if let Some(mut handle) = self.handle.take() {
            match tokio::time::timeout(self.shutdown_timeout, &mut handle).await {
                Ok(result) => result?,
                Err(_) => {
                    handle.abort();
                    return Err(CoordinationError::Timeout("watch shutdown"));
                }
            }
        }
        Ok(())
    }

    fn abort(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

impl Drop for CoordinationWatch {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

impl CoordinationClient {
    /// 监听目标节点和它的直接子节点。
    ///
    /// 返回前已建立 LISTEN 并完成初始快照。临时节点因会话过期而消失时，
    /// 即使没有显式 DELETE，也会由周期刷新发现。
    ///
    /// # 错误
    ///
    /// 路径或刷新间隔非法，或启动超时时返回错误。
    pub async fn watch(
        &self,
        path: &NodePath,
        postgres_config: tokio_postgres::Config,
        refresh_interval: Option<Duration>,
    ) -> Result<CoordinationWatch, CoordinationError> {
        self.validate_path(path)?;
        let refresh_interval = refresh_interval.unwrap_or(self.inner.config.watch_refresh_interval);
        if refresh_interval.is_zero() {
            return Err(CoordinationError::InvalidConfig(
                "watch refresh interval must be positive",
            ));
        }

        let (snapshot_tx, receiver) = watch::channel(empty_snapshot(path.clone()));
        let (ready_tx, ready_rx) = oneshot::channel();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let client = self.clone();
        let target = path.clone();
        let handle = tokio::spawn(run(
            client,
            postgres_config,
            target,
            refresh_interval,
            snapshot_tx,
            shutdown_rx,
            Some(ready_tx),
        ));
        let mut coordination_watch = CoordinationWatch {
            receiver,
            shutdown,
            handle: Some(handle),
            shutdown_timeout: self.inner.config.shutdown_timeout,
        };

        match tokio::time::timeout(self.inner.config.shutdown_timeout, ready_rx).await {
            Ok(Ok(())) => Ok(coordination_watch),
            Ok(Err(_)) => {
                coordination_watch.abort();
                Err(CoordinationError::WatchStartup)
            }
            Err(_) => {
                coordination_watch.abort();
                Err(CoordinationError::Timeout("watch startup"))
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run(
    client: CoordinationClient,
    postgres_config: tokio_postgres::Config,
    target: NodePath,
    refresh_interval: Duration,
    snapshot_tx: watch::Sender<CoordinationSnapshot>,
    mut shutdown: watch::Receiver<bool>,
    mut ready_tx: Option<oneshot::Sender<()>>,
) {
    let mut revision = 0u64;
    let mut current = empty_snapshot(target.clone());
    let mut backoff = Duration::from_millis(200);

    loop {
        if *shutdown.borrow() {
            break;
        }
        if let Err(error) = client.verify_schema().await {
            warn!(error = %error, "协调节点 watch schema 校验失败");
            publish(&snapshot_tx, &mut revision, current.clone(), true);
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
                client.inner.config.operation_timeout,
                connect,
            ) => result,
        };
        let (watch_client, connection) = match connected {
            Ok(Ok(connection)) => connection,
            Ok(Err(error)) => {
                warn!(error = %error, "协调节点 watch 连接失败");
                if !sleep_or_shutdown(backoff, &mut shutdown).await {
                    break;
                }
                backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
                continue;
            }
            Err(_) => {
                warn!("协调节点 watch 连接超时");
                if !sleep_or_shutdown(backoff, &mut shutdown).await {
                    break;
                }
                backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
                continue;
            }
        };

        let (notification_tx, mut notifications) = mpsc::channel(256);
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
                        warn!(error = %error, "协调节点 watch PostgreSQL 连接已结束");
                        break;
                    }
                    None => break,
                }
            }
        });

        let listen = format!("LISTEN \"{}\"", client.inner.config.notification_channel);
        match tokio::time::timeout(
            client.inner.config.operation_timeout,
            watch_client.batch_execute(&listen),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                warn!(error = %error, "协调节点 watch LISTEN 失败");
                connection_task.abort();
                if !sleep_or_shutdown(backoff, &mut shutdown).await {
                    break;
                }
                backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
                continue;
            }
            Err(_) => {
                warn!("协调节点 watch LISTEN 超时");
                connection_task.abort();
                if !sleep_or_shutdown(backoff, &mut shutdown).await {
                    break;
                }
                backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
                continue;
            }
        }

        match refresh_snapshot(&client, &target).await {
            Ok(snapshot) => {
                current = snapshot;
                publish(&snapshot_tx, &mut revision, current.clone(), false);
                if let Some(sender) = ready_tx.take() {
                    let _ = sender.send(());
                }
                info!(path = %target, "协调节点 watch 已订阅并完成初始快照");
            }
            Err(error) => {
                warn!(error = %error, "协调节点 watch 初始快照失败");
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
                        warn!("协调节点 watch 通知流已结束");
                        connection_task.abort();
                        break;
                    };
                    notification.payload() == client.notification_id()
                }
                _ = interval.tick() => true,
            };
            if !should_refresh {
                continue;
            }
            match refresh_snapshot(&client, &target).await {
                Ok(snapshot) => {
                    current = snapshot;
                    publish(&snapshot_tx, &mut revision, current.clone(), false);
                }
                Err(error) => {
                    warn!(error = %error, "协调节点 watch 刷新失败");
                    publish(&snapshot_tx, &mut revision, current.clone(), true);
                }
            }
        }
        connection_task.abort();
        publish(&snapshot_tx, &mut revision, current.clone(), true);
        if !sleep_or_shutdown(backoff, &mut shutdown).await {
            break;
        }
        backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
    }
}

async fn refresh_snapshot(
    client: &CoordinationClient,
    path: &NodePath,
) -> Result<CoordinationSnapshot, CoordinationError> {
    let (node, children) = client
        .timed("watch snapshot", client.read_snapshot(path))
        .await?;
    Ok(CoordinationSnapshot {
        path: path.clone(),
        node,
        children,
        revision: 0,
        stale: false,
    })
}

fn publish(
    sender: &watch::Sender<CoordinationSnapshot>,
    revision: &mut u64,
    mut snapshot: CoordinationSnapshot,
    stale: bool,
) {
    *revision = revision.wrapping_add(1);
    snapshot.revision = *revision;
    snapshot.stale = stale;
    let _ = sender.send(snapshot);
}

async fn sleep_or_shutdown(duration: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        changed = shutdown.changed() => {
            changed.is_ok() && !*shutdown.borrow()
        }
        () = tokio::time::sleep(duration) => true,
    }
}
