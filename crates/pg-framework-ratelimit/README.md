# pg-framework-ratelimit

`pg-framework-ratelimit` 是基于 PostgreSQL 的多实例限频组件。它通过数据库内单行原子更新协调所有应用实例，按稳定 key hash 做物理分区，不使用进程本地计数作为决策依据，也不依赖缓存 pub/sub。

第一版使用固定窗口算法，目标是部署简单和决策可解释，不追求单 key 极限 QPS。

## 设计目标

- 多实例共享额度，不因实例数量增加而放大 limit。
- 单 key 的检查和扣减在 PostgreSQL 内原子完成。
- TTL 自动回收过期窗口。
- 按稳定 key hash 分片，单 key 始终路由到同一分区。
- 分片用于隔离容量、索引与清理负载，不改变单 key 的强一致决策。
- 提供放行、剩余额度、重置时间和重试时间。
- PostgreSQL 失败时默认 fail-closed。
- 后续可以在不修改业务调用方式的情况下增加 Token Bucket 等算法。

## 数据模型

建议使用普通日志表。限频数据在数据库异常重启或主备切换后重置会削弱安全策略，因此默认不使用 `UNLOGGED`。如果某类非关键限频明确接受主备切换后清零，可以单独提供非日志表配置。

```sql
CREATE TABLE pgfw_rate_limit_counter (
    namespace    text        NOT NULL,
    raw_key      bytea,
    key_hash     bytea       NOT NULL CHECK (octet_length(key_hash) = 32),
    window_start timestamptz NOT NULL,
    used         bigint      NOT NULL CHECK (used >= 0),
    expires_at   timestamptz NOT NULL,
    PRIMARY KEY (namespace, key_hash, window_start),
    CHECK (raw_key IS NULL OR octet_length(raw_key) BETWEEN 1 AND 1024)
) PARTITION BY HASH (key_hash);

CREATE TABLE pgfw_rate_limit_counter_p00
    PARTITION OF pgfw_rate_limit_counter
    FOR VALUES WITH (MODULUS 64, REMAINDER 0);

-- 其余分区 p01 ... p63 由迁移脚本统一创建。

CREATE INDEX pgfw_rate_limit_counter_expires_idx
    ON pgfw_rate_limit_counter (expires_at);
```

字段含义：

- `namespace`：租户、业务域、环境和策略版本的隔离维度。
- `raw_key`：可选的原始 key，仅用于管理查询；默认不保存。
- `key_hash`：限频主体的稳定摘要。不要直接把 IP、用户 ID 或 token 写入表。
- `window_start`：由数据库时间计算出的窗口起点。
- `used`：当前窗口已经消费的额度。被拒绝的请求允许饱和在 `limit + 1`，避免继续累加。
- `expires_at`：窗口结束时间加短暂宽限期，用于清理。

## Key 设计

对外使用 `RateKey` 表达主体，内部 key 至少组合：

```text
namespace = project / environment / tenant / policy
key_hash  = SHA-256(namespace || canonical_rate_key)
```

要求：

- canonical encoding 跨实例稳定，不依赖 Rust `Hash`。
- 策略 ID 或策略版本应进入 namespace，避免在线修改 limit 时混用不同规则。
- 低熵敏感主体应配置 `KeyHasher::HmacSha256`，而不是裸 SHA-256。
- key 默认最大 512 字节，SQL 硬上限 1024 字节；超限在数据库操作前拒绝。
- `store_raw_keys = false` 是默认值。开启后会把原始主体写入数据库，必须按敏感数据管理；日志和指标仍不记录原始 key。

### 分区

- `namespace` 负责策略隔离，不单独决定物理分布。
- PostgreSQL 对包含 namespace 的 `key_hash` 做 HASH 分区，同一个 `RateKey` 始终落到同一分区。
- 按 key 的 `check`、`get`、`reset` 携带 `namespace + key_hash`，PostgreSQL 自动裁剪到目标分区。
- 分页巡检可以查询分区父表并遍历全部 key；这种查询会扫描所有分区，必须设置 limit 和 cursor。
- 分区不是对计数做 local shard 后求和。单 key 的额度仍由单个目标行的原子更新串行化。
- `SHARD_COUNT` 在同一个 schema generation 内固定。改变分区数必须创建新表并通过双写/切换或计划停写迁移。

## 固定窗口 DML

### `check`

`check` 必须使用一条原子 upsert 完成窗口创建和额度消费。窗口时间、数据库时钟、`expires_at` 应在一个 SQL 语句内确定。

概念 SQL：

```sql
INSERT INTO pgfw_rate_limit_counter (
    namespace, key_hash, window_start, used, expires_at
)
VALUES (
    $namespace,
    $key_hash,
    $window_start,
    LEAST($cost, $limit + 1),
    $window_end + $grace
)
ON CONFLICT (namespace, key_hash, window_start)
DO UPDATE SET
    used = LEAST(
        pgfw_rate_limit_counter.used + $cost,
        $limit + 1
    )
RETURNING
    used <= $limit AS allowed,
    used,
    expires_at;
```

实现时还需：

- 使用数据库时间计算 `window_start` 和 `window_end`。
- 对 `limit`、`cost`、TTL 和加法做范围校验，避免整数溢出。
- `cost` 大于剩余额度时返回拒绝，不消费部分额度。
- `remaining = max(limit - used, 0)`。
- `retry_after` 根据数据库时间和重置边界计算。

数据库行锁会让同一 key 的并发更新串行化。这保证所有实例共享同一个额度，不需要额外锁或 pub/sub。

### `get`

只读返回当前窗口状态，用于管理后台和诊断，不消费额度。查询必须同时携带 namespace 和 key hash，并使用数据库时间过滤过期窗口。

### `reset`

删除指定 namespace 和 key 的计数状态。该操作用于管理场景，不应被普通请求路径调用。

### 批量检查

第一版不提供“多个 key 全部成功后一起提交”的隐式行为。多 key 限频如果需要原子性，应在单个数据库事务中按稳定顺序锁定并更新，或设计专门的批量接口，避免调用方误以为依次 `check` 是原子的。

## TTL 和清理

限频记录必须设置 `expires_at`：

- 固定窗口通常使用 `window_end + cleanup_grace`。
- 读取时使用数据库时间过滤过期窗口。
- 后台任务按物理分区轮询或在父表分批删除过期行。
- 每个实例都可以执行清理，使用 `FOR UPDATE SKIP LOCKED` 避免争抢。
- 清理失败不影响正确性，但会持续占用表和索引空间。

示例逻辑：

```sql
WITH expired AS (
    SELECT namespace, key_hash, window_start
    FROM pgfw_rate_limit_counter
    WHERE expires_at <= clock_timestamp()
    ORDER BY expires_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
DELETE FROM pgfw_rate_limit_counter AS counter
USING expired
WHERE counter.namespace = expired.namespace
  AND counter.key_hash = expired.key_hash
  AND counter.window_start = expired.window_start
  AND counter.expires_at <= clock_timestamp();
```

建议监控：

- 每个分区的过期行数量、物理表/索引大小。
- 每个分区的清理批次耗时、删除行数和失败次数。
- 单个热点 key 的 PG 等待时间。
- `check` 的 P95/P99 延迟。
- allowed、denied 和 store-error 数量。

## 容量和性能

精确限频的代价是每个 `check` 都至少执行一次 PostgreSQL 写操作，并且同一个 key 会竞争同一行。

分片将不同 key 的写入、索引更新和 autovacuum 分散到多个物理分区。必须监控最大分区而不是只看父表总量；任一分区接近运维阈值时，应通过下一 schema generation 增加分片数，而不是把同一 key 再拆成多个计数行。

适用范围：

- 登录、短信、支付、风控等需要精确额度的中低频场景。
- 单 key QPS 不会长期打满单个 PostgreSQL 行的场景。
- 能接受 PostgreSQL 作为限频关键路径的场景。

不适合直接使用的方式：

- 每个实例在本地独立计数后偶尔回写 PG。这会产生最多 `实例数 × 本地批次` 的超额放行。
- 将计数拆成多个 shard 后在本地简单求和。没有全局预留协议时无法保证不超额。
- 对超高 QPS 热点 key 继续逐请求写 PG。此时应增加额度预租，或改用更合适的专用限频设施。

### 高 QPS 扩展

后续可增加额度预租：

1. 实例从 PostgreSQL 原子预租一批 token。
2. 本地只消费已经预租的 token。
3. 额度耗尽后再次向 PG 预租。

预租不会让全局放行超过 limit，但实例崩溃会浪费部分额度。它适合热点 key，同时保持 PG 为唯一协调者。

## 一致性和失败策略

- 单 key 的 `check` 是强一致的。
- 不同 key 之间没有隐式原子性。
- PostgreSQL 超时、连接失败或无法确认写入结果时默认 fail-closed。
- fail-open 必须由调用方显式配置，并记录结构化指标。
- 不要自动在数据库失败后切换到本地计数，这会破坏策略上界且难以观测。
- 请求超时应短于业务整体超时，避免限频故障耗尽连接池。
- `RateLimiter::connect` 在启动时校验 `pgfw_schema_meta`、schema generation 和物理分区数。
- 默认连接池包含 wait/create/recycle timeout；`operation_timeout` 为组件级总超时，外部连接池也必须配置等价限制。
- 网络抖动或进程重连后不加载本地计数快照。每次决策重新访问 PostgreSQL，因此不存在需要恢复的 cache L1。
- `RateLimiter::connect` 在启动时校验 schema generation、分区数和目标表；长生命周期进程在确认数据库会话或 primary 重建后应再次调用 `verify_schema`，不匹配时保持 fail-closed。
- `check` 在 PostgreSQL 返回前断线时，提交结果可能未知。默认不自动重试写请求，因为盲目重试可能导致同一业务请求重复消费额度；需要安全重试时，应在后续版本增加显式 idempotency token。

## Rust API

```rust
use std::time::Duration;
use pg_framework_ratelimit::{
    build_default_pool, RateLimitConfig, RateLimiter,
};

pub struct RateDecision {
    pub allowed: bool,
    pub limit: u64,
    pub remaining: u64,
    pub reset_after: Duration,
    pub retry_after: Option<Duration>,
}

let pool = build_default_pool(pg_config)?;
let config = RateLimitConfig::new(
    "project:prod:tenant-1:login",
    Duration::from_secs(60),
);
let limiter = RateLimiter::connect(pool, config).await?;
let decision = limiter.check(b"user-42", 1, 100).await?;
```

配置通过公开字段或构造函数提供：

- PostgreSQL pool。
- namespace。
- schema generation 和物理分区数。
- 稳定哈希策略和 key 上限。
- 单次数据库操作超时。
- 清理间隔、批次大小和宽限期。
- fail-closed 由调用方错误处理完成。

## 并发验收场景

- 两个以上实例同时发起请求，总放行数不超过同一个 limit。
- 例如 1000 个并发请求、limit 为 100、cost 为 1，最终恰好放行 100 个。
- `cost > remaining` 时拒绝，不留下部分扣减。
- 窗口结束后新请求使用新窗口，旧窗口最终被 TTL 清理。
- 数据库失败时默认拒绝，并返回可区分的存储错误。
- 热点 key 压测时连接池不会被无限等待占满。
- 同一个 `RateKey` 在不同实例和重连后始终由相同 `key_hash` 路由到同一个分区。
- 按 key 的 `check` 和读取 SQL 带 `namespace + key_hash`，执行计划只访问目标分区。
- 任一单个分区数据量保持在配置阈值内，而不是全部窗口记录集中到一张物理表。
- PostgreSQL 重连后不会使用旧本地计数；schema/分区配置不匹配时拒绝决策。
