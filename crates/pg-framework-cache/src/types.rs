use std::time::Duration;

use pg_framework_core::KeyId;

/// 缓存 TTL 查询结果。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TtlResult {
    /// key 不存在或已经过期。
    Missing,
    /// key 存在且没有过期时间。
    Persistent,
    /// key 将在返回值表示的时长后过期。
    Expiring(Duration),
}

/// 用于诊断的运行时计数器。
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheStats {
    /// 存在 pending 缓冲变更的 key 数量。
    pub pending_keys: usize,
    /// pending value 的字节总数。
    pub pending_bytes: usize,
    /// 当前 L1 条目数。
    pub l1_entries: usize,
}

/// 管理扫描接口返回的 key。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredCacheKey {
    /// 用于路由和失效的稳定哈希。
    pub key_id: KeyId,
    /// 开启 `store_raw_keys` 时保存的原始 key。
    pub raw_key: Option<Vec<u8>>,
}

/// 一页 keyset 分页缓存 key。
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CacheKeyPage {
    /// 按哈希升序排列的有效且未过期 key。
    pub keys: Vec<StoredCacheKey>,
    /// 传给下一次扫描的 cursor。
    pub next_after: Option<KeyId>,
}
