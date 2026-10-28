use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pg_framework_cache::{Cache, CacheConfig, CacheListener, ReadMode, build_default_pool};
use pg_framework_lock::{AcquireOutcome, LeaseTtl, LockConfig, Locker};
use pg_framework_ratelimit::{RateLimitConfig, RateLimiter};

fn database_config() -> tokio_postgres::Config {
    let url = std::env::var("PGFRAMEWORK_TEST_DATABASE_URL")
        .expect("必须设置 PGFRAMEWORK_TEST_DATABASE_URL");
    url.parse().expect("测试数据库 URL 非法")
}

async fn migrate(pool: &deadpool_postgres::Pool) {
    let client = pool.get().await.expect("连接测试数据库");
    client
        .batch_execute(include_str!("../../../sql/000_meta.sql"))
        .await
        .expect("执行 meta migration");
    client
        .batch_execute(include_str!("../../../sql/001_cache.sql"))
        .await
        .expect("执行 cache migration");
    client
        .batch_execute(include_str!("../../../sql/002_lock.sql"))
        .await
        .expect("执行 lock migration");
    client
        .batch_execute(include_str!("../../../sql/003_ratelimit.sql"))
        .await
        .expect("执行 ratelimit migration");
}

fn unique_namespace(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("系统时间")
        .as_nanos();
    format!("{prefix}:{nanos}")
}

#[tokio::test]
#[ignore = "需要设置 PGFRAMEWORK_TEST_DATABASE_URL 并使用专用测试数据库"]
async fn cache_rate_limit_and_lock_work_against_postgresql() {
    let pool = build_default_pool(database_config()).expect("连接池");
    migrate(&pool).await;

    let cache_namespace = unique_namespace("cache");
    let other_namespace = unique_namespace("cache_other");
    let mut cache_config = CacheConfig::new(cache_namespace.clone());
    cache_config.store_raw_keys = true;
    let cache = Arc::new(
        Cache::connect(pool.clone(), cache_config)
            .await
            .expect("创建 cache"),
    );
    let cache_listener = CacheListener::start(Arc::clone(&cache), database_config())
        .await
        .expect("启动 cache listener");
    let mut other_config = CacheConfig::new(other_namespace.clone());
    other_config.store_raw_keys = true;
    let other_cache = Arc::new(
        Cache::connect(pool.clone(), other_config)
            .await
            .expect("创建另一 namespace cache"),
    );
    let other_listener = CacheListener::start(Arc::clone(&other_cache), database_config())
        .await
        .expect("启动另一 cache listener");

    for index in 0u32..128 {
        cache
            .set(&index.to_be_bytes(), vec![1], None)
            .await
            .expect("cache set");
        other_cache
            .set(&index.to_be_bytes(), vec![2], None)
            .await
            .expect("other cache set");
    }
    cache
        .set(&0u32.to_be_bytes(), vec![3], None)
        .await
        .expect("cache update");
    let page = cache.scan_keys(None, 128).await.expect("扫描 key");
    assert!(page.keys.iter().any(|key| key.version >= 2));
    cache.clear_namespace().await.expect("清理 namespace");
    assert!(
        cache
            .scan_keys(None, 1)
            .await
            .expect("清理后扫描")
            .keys
            .is_empty()
    );
    for index in 0u32..128 {
        assert_eq!(
            other_cache
                .get(&index.to_be_bytes(), ReadMode::Shared)
                .await
                .expect("other cache get")
                .as_deref(),
            Some([2].as_slice())
        );
    }
    other_cache
        .clear_namespace()
        .await
        .expect("清理另一 namespace");
    cache_listener
        .shutdown()
        .await
        .expect("关闭 cache listener");
    other_listener
        .shutdown()
        .await
        .expect("关闭另一 cache listener");

    let rate_namespace = unique_namespace("ratelimit");
    let rate_config = RateLimitConfig::new(rate_namespace, Duration::from_secs(60));
    let limiter = Arc::new(
        RateLimiter::connect(pool.clone(), rate_config)
            .await
            .expect("创建 limiter"),
    );
    let mut checks = tokio::task::JoinSet::new();
    for _ in 0..200 {
        let limiter = Arc::clone(&limiter);
        checks.spawn(async move {
            limiter
                .check(b"subject", 1, 100)
                .await
                .expect("限频检查")
                .allowed
        });
    }
    let mut allowed = 0;
    while let Some(result) = checks.join_next().await {
        if result.expect("限频任务") {
            allowed += 1;
        }
    }
    assert_eq!(allowed, 100);
    limiter.cleanup_expired(100).await.expect("限频清理");
    let snapshot = limiter
        .get(b"subject", 100)
        .await
        .expect("限频读取")
        .expect("当前窗口存在");
    assert_eq!(snapshot.limit, 100);
    assert_eq!(snapshot.used, 100);
    assert_eq!(snapshot.remaining, 0);
    limiter.reset(b"subject").await.expect("重置限频");

    let lock = Locker::connect(pool.clone(), LockConfig::new(unique_namespace("lock")))
        .await
        .expect("创建锁");
    let mut first = match lock
        .try_acquire(b"resource", LeaseTtl::from_secs(5).expect("TTL"))
        .await
        .expect("获取锁")
    {
        AcquireOutcome::Acquired(guard) => guard,
        AcquireOutcome::Contended => panic!("首次获取不应竞争"),
        _ => panic!("未知的获取结果"),
    };
    assert!(first.is_active());
    let mut nested = first.reenter().expect("同 Guard 重入");
    let mut deepest = nested.reenter().expect("再次重入");
    assert_eq!(first.reentrant_depth(), 3);
    assert!(matches!(
        lock.try_acquire(b"resource", LeaseTtl::from_secs(5).expect("TTL"))
            .await
            .expect("竞争检查"),
        AcquireOutcome::Contended
    ));
    deepest.release().await.expect("释放最深重入");
    nested.release().await.expect("释放嵌套重入");
    assert!(first.is_active());
    first.renew().await.expect("续租");
    first.release().await.expect("释放锁");
    let mut reacquired = match lock
        .try_acquire(b"resource", LeaseTtl::from_secs(5).expect("TTL"))
        .await
        .expect("重新获取锁")
    {
        AcquireOutcome::Acquired(guard) => guard,
        AcquireOutcome::Contended => panic!("释放后重新获取不应竞争"),
        _ => panic!("未知的重新获取结果"),
    };
    reacquired.release().await.expect("释放重新获取的锁");
    lock.cleanup_expired(100).await.expect("锁清理");
}
