use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio_postgres::{AsyncMessage, NoTls};
use tracing::{info, warn};

use crate::Cache;
use crate::error::CacheError;

/// 专用 PostgreSQL LISTEN 任务，支持重连、首次恢复和 namespace 控制事件。
#[must_use = "drop 后 listener 会停止接收失效事件"]
pub struct CacheListener {
    cache: Arc<Cache>,
    shutdown: watch::Sender<bool>,
    handle: Option<JoinHandle<()>>,
}

impl CacheListener {
    /// 启动 listener，并等待 LISTEN、schema 校验和首次 L1 恢复完成。
    ///
    /// # 错误
    ///
    /// 启动超时、首次恢复失败或后台任务提前退出时返回错误。
    pub async fn start(
        cache: Arc<Cache>,
        postgres_config: tokio_postgres::Config,
    ) -> Result<Self, CacheError> {
        let (shutdown, receiver) = watch::channel(false);
        let (ready_tx, ready_rx) = oneshot::channel();
        let handle = tokio::spawn(run(
            Arc::clone(&cache),
            postgres_config,
            receiver,
            Some(ready_tx),
        ));
        let mut listener = Self {
            cache,
            shutdown,
            handle: Some(handle),
        };

        let startup_timeout = listener.cache.config.shutdown_timeout;
        match tokio::time::timeout(startup_timeout, ready_rx).await {
            Ok(Ok(())) => Ok(listener),
            Ok(Err(_)) => {
                listener.abort();
                Err(CacheError::ListenerStartup)
            }
            Err(_) => {
                listener.abort();
                Err(CacheError::Timeout("listener startup"))
            }
        }
    }

    /// 停止 listener。
    ///
    /// # 错误
    ///
    /// 后台任务 panic 或关闭超时时返回错误。
    pub async fn shutdown(mut self) -> Result<(), CacheError> {
        self.cache.set_ready(false);
        let _ = self.shutdown.send(true);
        let timeout = self.cache.config.shutdown_timeout;
        if let Some(mut handle) = self.handle.take() {
            match tokio::time::timeout(timeout, &mut handle).await {
                Ok(result) => result?,
                Err(_) => {
                    handle.abort();
                    return Err(CacheError::Timeout("listener shutdown"));
                }
            }
        }
        Ok(())
    }

    fn abort(&mut self) {
        self.cache.set_ready(false);
        let _ = self.shutdown.send(true);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

impl Drop for CacheListener {
    fn drop(&mut self) {
        self.abort();
    }
}

async fn run(
    cache: Arc<Cache>,
    postgres_config: tokio_postgres::Config,
    mut shutdown: watch::Receiver<bool>,
    mut ready_tx: Option<oneshot::Sender<()>>,
) {
    let mut backoff = Duration::from_millis(200);
    loop {
        cache.set_ready(false);
        if *shutdown.borrow() {
            break;
        }

        if let Err(error) = cache.verify_schema().await {
            warn!(error = %error, "缓存 listener schema 校验失败");
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
            result = tokio::time::timeout(cache.config.operation_timeout, connect) => {
                match result {
                    Ok(result) => result,
                    Err(_) => {
                        warn!("缓存 listener 连接超时");
                        if !sleep_or_shutdown(backoff, &mut shutdown).await {
                            break;
                        }
                        backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
                        continue;
                    }
                }
            },
        };
        let (client, connection) = match connected {
            Ok(connection) => connection,
            Err(error) => {
                warn!(error = %error, "缓存 listener 连接失败");
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
                    Some(Ok(AsyncMessage::Notice(_))) => {}
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        warn!(error = %error, "缓存 listener PostgreSQL 连接已结束");
                        break;
                    }
                    None => break,
                }
            }
        });

        let listen = format!(
            "LISTEN \"{}\"; LISTEN \"{}\"",
            cache.notification_channel(),
            cache.control_channel()
        );
        if let Err(error) = client.batch_execute(&listen).await {
            warn!(error = %error, "缓存 listener 订阅失败");
            connection_task.abort();
            if !sleep_or_shutdown(backoff, &mut shutdown).await {
                break;
            }
            backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
            continue;
        }

        let (clearing, epoch) = match query_namespace_state(&cache, &client).await {
            Ok(state) => state,
            Err(error) => {
                warn!(error = %error, "缓存 listener 读取 namespace 状态失败");
                connection_task.abort();
                if !sleep_or_shutdown(backoff, &mut shutdown).await {
                    break;
                }
                backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
                continue;
            }
        };
        cache.apply_namespace_state(clearing, epoch);
        if !clearing {
            match cache.warm_reload().await {
                Ok(()) => {
                    mark_ready(&cache, &mut ready_tx);
                    info!("缓存 listener 已订阅并完成 L1 预热");
                }
                Err(error) => {
                    warn!(error = %error, "缓存 listener 预热重载失败");
                    connection_task.abort();
                    if !sleep_or_shutdown(backoff, &mut shutdown).await {
                        break;
                    }
                    backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
                    continue;
                }
            }
        }

        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        cache.set_ready(false);
                        connection_task.abort();
                        return;
                    }
                }
                notification = notifications.recv() => {
                    let Some(notification) = notification else {
                        warn!("缓存 listener 通知流已结束");
                        cache.set_ready(false);
                        connection_task.abort();
                        break;
                    };
                    if notification.channel() == cache.notification_channel() {
                        if let Err(error) =
                            cache.invalidate_notification(notification.payload())
                        {
                            warn!(error = %error, "缓存失效 payload 被拒绝");
                        }
                    } else if notification.channel() == cache.control_channel() {
                        match cache.namespace_notification(notification.payload()) {
                            Ok(true) if cache.is_clearing() => {
                                cache.set_ready(false);
                            }
                            Ok(true) => {
                                if let Err(error) = cache.warm_reload().await {
                                    warn!(error = %error, "namespace 清理完成后重载失败");
                                    cache.set_ready(false);
                                    connection_task.abort();
                                    break;
                                }
                                mark_ready(&cache, &mut ready_tx);
                            }
                            Ok(false) => {}
                            Err(error) => {
                                warn!(error = %error, "namespace 控制 payload 被拒绝");
                            }
                        }
                    }
                }
            }
        }

        cache.set_ready(false);
        if !sleep_or_shutdown(backoff, &mut shutdown).await {
            break;
        }
        backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
    }
    cache.set_ready(false);
}

async fn query_namespace_state(
    cache: &Cache,
    client: &tokio_postgres::Client,
) -> Result<(bool, u64), CacheError> {
    let row = tokio::time::timeout(
        cache.config.operation_timeout,
        client.query_opt(
            "SELECT clearing, epoch
             FROM pgfw_cache_namespace
             WHERE namespace = $1",
            &[&cache.config.namespace],
        ),
    )
    .await
    .map_err(|_| CacheError::Timeout("namespace state"))??;
    Ok(row.map_or((false, 0), |row| {
        (
            row.get::<_, bool>(0),
            u64::try_from(row.get::<_, i64>(1)).unwrap_or_default(),
        )
    }))
}

fn mark_ready(cache: &Cache, ready_tx: &mut Option<oneshot::Sender<()>>) {
    cache.set_ready(true);
    if let Some(sender) = ready_tx.take() {
        let _ = sender.send(());
    }
}

async fn sleep_or_shutdown(duration: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        changed = shutdown.changed() => {
            changed.is_ok() && !*shutdown.borrow()
        }
        () = tokio::time::sleep(duration) => true,
    }
}
