use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_postgres::{AsyncMessage, NoTls};
use tracing::{info, warn};

use crate::Cache;

/// 专用 PostgreSQL LISTEN 任务，支持重连和 L1 重载。
#[must_use = "dropping the listener stops invalidation processing"]
pub struct CacheListener {
    shutdown: watch::Sender<bool>,
    handle: Option<JoinHandle<()>>,
}

impl CacheListener {
    /// 使用专用 PostgreSQL 连接启动 listener。
    ///
    /// 传入的配置不能使用 transaction-pooling 代理。
    pub fn spawn(cache: Arc<Cache>, postgres_config: tokio_postgres::Config) -> Self {
        let (shutdown, receiver) = watch::channel(false);
        let handle = tokio::spawn(run(cache, postgres_config, receiver));
        Self {
            shutdown,
            handle: Some(handle),
        }
    }

    /// 停止 listener。
    ///
    /// # 错误
    ///
    /// listener task panic 时返回 join 错误。
    pub async fn shutdown(mut self) -> Result<(), tokio::task::JoinError> {
        let _ = self.shutdown.send(true);
        if let Some(handle) = self.handle.take() {
            return handle.await;
        }
        Ok(())
    }
}

impl Drop for CacheListener {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

async fn run(
    cache: Arc<Cache>,
    postgres_config: tokio_postgres::Config,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut backoff = Duration::from_millis(200);
    loop {
        if *shutdown.borrow() {
            break;
        }

        let connect = postgres_config.connect(NoTls);
        let connected = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
            result = connect => result,
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

        let listen = format!("LISTEN \"{}\"", cache.notification_channel());
        if let Err(error) = client.batch_execute(&listen).await {
            warn!(error = %error, "缓存 listener 订阅失败");
            connection_task.abort();
            if !sleep_or_shutdown(backoff, &mut shutdown).await {
                break;
            }
            backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
            continue;
        }

        backoff = Duration::from_millis(200);
        cache.clear_l1();
        if let Err(error) = cache.warm_reload().await {
            warn!(error = %error, "缓存 listener 预热重载失败");
        } else {
            info!("缓存 listener 已订阅并完成 L1 预热");
        }

        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        connection_task.abort();
                        return;
                    }
                }
                notification = notifications.recv() => {
                    match notification {
                        Some(notification) => {
                            if let Err(error) =
                                cache.invalidate_notification(notification.payload())
                            {
                                warn!(error = %error, "缓存失效 payload 被拒绝");
                            }
                        }
                        None => {
                            warn!("缓存 listener 通知流已结束");
                            connection_task.abort();
                            break;
                        }
                    }
                }
            }
        }

        if !sleep_or_shutdown(backoff, &mut shutdown).await {
            break;
        }
        backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
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
