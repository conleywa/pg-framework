use std::time::Duration;

use pg_framework_core::{
    DEFAULT_CACHE_CHANNEL, DEFAULT_MAX_KEY_BYTES, DEFAULT_SHARD_COUNT, KeyHasher,
};

/// 缓存写入行为。
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum WriteMode {
    /// 返回前提交 L2 变更和通知。
    #[default]
    Immediate,
    /// 在内存中合并变更并异步刷盘。
    Buffered,
}

/// 缓存读取行为。
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReadMode {
    /// 优先读取 L1，未命中后加载 L2。
    #[default]
    LocalFirst,
    /// 等待该 key 的 pending 写入刷盘，并始终读取 L2。
    Shared,
}

/// 有界内存写缓冲配置。
#[derive(Clone, Debug)]
pub struct BufferConfig {
    /// pending key 数量上限。
    pub max_pending_keys: usize,
    /// pending value 字节总数上限。
    pub max_pending_bytes: usize,
    /// 单次 stripe 刷盘选择的 key 数量上限。
    pub max_batch_keys: usize,
    /// 单次 stripe 刷盘选择的 value 字节上限。
    pub max_batch_bytes: usize,
    /// 后台 flusher 的定时刷盘间隔。
    pub flush_interval: Duration,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            max_pending_keys: 10_000,
            max_pending_bytes: 32 * 1024 * 1024,
            max_batch_keys: 1_000,
            max_batch_bytes: 8 * 1024 * 1024,
            flush_interval: Duration::from_millis(50),
        }
    }
}

impl BufferConfig {
    pub(crate) fn validate(&self) -> bool {
        self.max_pending_keys > 0
            && self.max_pending_bytes > 0
            && self.max_batch_keys > 0
            && self.max_batch_bytes > 0
            && self.max_batch_keys <= self.max_pending_keys
            && self.max_batch_bytes <= self.max_pending_bytes
            && !self.flush_interval.is_zero()
    }
}

/// 单个缓存实例的配置。
#[derive(Clone, Debug)]
pub struct CacheConfig {
    /// 缓存逻辑 namespace。
    pub namespace: String,
    /// PostgreSQL 通知 channel，必须是不超过 63 字节的安全标识符。
    pub notification_channel: String,
    /// 进程内 L1 和写缓冲的 stripe 数量。
    ///
    /// 该值不会写入 PostgreSQL；数据库分区始终由稳定 `key_hash` 选择。
    pub shard_count: u16,
    /// 缓存写入行为。
    pub write_mode: WriteMode,
    /// 允许写入的最大 value 长度。
    pub max_value_bytes: usize,
    /// canonical key 最大长度。
    pub max_key_bytes: usize,
    /// 是否在哈希旁额外保存原始 key，供管理查询使用。
    pub store_raw_keys: bool,
    /// 所有 stripe 的 L1 总容量。
    pub l1_capacity: usize,
    /// 调用方未提供 TTL 时使用的默认 TTL。
    pub default_ttl: Option<Duration>,
    /// key 和分区使用的稳定哈希策略。
    pub key_hasher: KeyHasher,
    /// listener 重连后每个本地 stripe 预热的 L1 行数。
    pub recovery_rows_per_shard: usize,
    /// 内存写缓冲限制。
    pub buffer: BufferConfig,
}

impl CacheConfig {
    /// 为一个 namespace 创建缓存配置。
    #[must_use]
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            notification_channel: DEFAULT_CACHE_CHANNEL.to_owned(),
            shard_count: DEFAULT_SHARD_COUNT,
            write_mode: WriteMode::Immediate,
            max_value_bytes: 1024 * 1024,
            max_key_bytes: DEFAULT_MAX_KEY_BYTES,
            store_raw_keys: false,
            l1_capacity: 100_000,
            default_ttl: None,
            key_hasher: KeyHasher::default(),
            recovery_rows_per_shard: 1_000,
            buffer: BufferConfig::default(),
        }
    }

    /// 选择缓存写入行为。
    #[must_use]
    pub fn with_write_mode(mut self, write_mode: WriteMode) -> Self {
        self.write_mode = write_mode;
        self
    }
}
