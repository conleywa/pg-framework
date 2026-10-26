//! 基于 PostgreSQL、带合并写缓冲的共享缓存。

mod cache;
mod config;
mod error;
mod flush;
mod listener;
mod maintenance;
mod notify;
mod read;
mod state;
mod types;
mod write;

pub use cache::Cache;
pub use config::{BufferConfig, CacheConfig, ReadMode, WriteMode};
pub use error::CacheError;
pub use listener::CacheListener;
pub use pg_framework_core::{
    DEFAULT_MAX_KEY_BYTES, DEFAULT_POOL_MAX_SIZE, DEFAULT_SHARD_COUNT, HARD_MAX_KEY_BYTES,
    KeyHasher, KeyId, ShardRouter, build_default_pool, build_pool,
};
pub use types::{CacheKeyPage, CacheStats, StoredCacheKey, TtlResult};

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::warn;

/// 后台定时缓存刷盘任务。
#[must_use = "dropping the task handle aborts the background flusher"]
pub struct CacheFlusher {
    cache: Arc<Cache>,
    shutdown: watch::Sender<bool>,
    handle: Option<JoinHandle<()>>,
}

impl CacheFlusher {
    /// 为共享缓存实例启动后台 flusher。
    pub fn spawn(cache: Arc<Cache>) -> Self {
        let (shutdown, mut receiver) = watch::channel(false);
        let task_cache = Arc::clone(&cache);
        let handle = tokio::spawn(async move {
            let interval_duration = task_cache.flush_interval();
            let mut interval = tokio::time::interval(interval_duration);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut retry_delay = interval_duration;

            loop {
                tokio::select! {
                    changed = receiver.changed() => {
                        if changed.is_err() || *receiver.borrow() {
                            break;
                        }
                    }
                    () = task_cache.flusher_signal().notified() => {}
                    _ = interval.tick() => {}
                }

                if task_cache.stats().pending_keys == 0 {
                    retry_delay = interval_duration;
                    continue;
                }
                match task_cache.flush().await {
                    Ok(()) => retry_delay = interval_duration,
                    Err(error) => {
                        warn!(error = %error, "缓存后台刷盘失败");
                        tokio::select! {
                            changed = receiver.changed() => {
                                if changed.is_err() || *receiver.borrow() {
                                    break;
                                }
                            }
                            () = tokio::time::sleep(retry_delay) => {}
                        }
                        retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(30));
                    }
                }
            }
        });

        Self {
            cache,
            shutdown,
            handle: Some(handle),
        }
    }

    /// 刷盘剩余缓冲写入并停止后台任务。
    ///
    /// # 错误
    ///
    /// 后台任务 panic 或最终刷盘失败时返回错误。
    pub async fn shutdown(mut self) -> Result<(), CacheError> {
        let _ = self.shutdown.send(true);
        if let Some(handle) = self.handle.take() {
            handle.await?;
        }
        self.cache.flush().await
    }
}

impl Drop for CacheFlusher {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pg_framework_core::build_pool;

    use super::*;

    #[tokio::test]
    async fn buffered_update_replaces_pending_value() {
        let pool = build_pool(tokio_postgres::Config::new(), 1).expect("pool");
        let config = CacheConfig::new("tests").with_write_mode(WriteMode::Buffered);
        let cache = Arc::new(Cache::new(pool, config).expect("cache"));

        cache.set(b"key", vec![1], None).await.expect("first write");
        cache
            .set(b"key", vec![2], None)
            .await
            .expect("second write");

        assert_eq!(cache.stats().pending_keys, 1);
        assert_eq!(
            cache.get(b"key", ReadMode::LocalFirst).await.expect("read"),
            Some(Arc::from(vec![2]))
        );
    }
}
