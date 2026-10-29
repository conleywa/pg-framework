# pg-framework-cache

`pg-framework-cache` 是面向多实例部署的 PostgreSQL 共享缓存组件。它用每实例分片 HashMap 作为 L1，用 PostgreSQL `UNLOGGED` 声明式分区表作为共享 L2，用有界内存写缓冲合并频繁写入，并用 `LISTEN/NOTIFY` 完成跨实例失效。

该组件的目标是替代 Redis 在本项目中的普通缓存职责，而不是追求极限吞吐或兼容 Redis 协议。

## 设计目标

- 多个应用实例共享同一份缓存 value。
- 使用 PostgreSQL 作为唯一共享基础设施，减少 Redis 的部署和维护成本。
- 支持 TTL、原子写入和显式失效。
- 同一 key 的短时间重复写入在内存中合并，并按批次刷入 L2。
- 写缓冲有明确的内存上限和背压，不允许高频写入无限堆积。
- L2 按稳定 key hash 分片，避免单个物理表持续膨胀。
- NOTIFY 丢失、监听断线或进程重启后，不允许无限期返回旧数据。
- PostgreSQL 重连后先恢复订阅，再按有界批次重新加载 L2 到 L1。
- 缓存数据可丢失；数据库故障切换后允许全部 cache miss。

## 存储模型

### L2：PostgreSQL 非日志表

```sql
CREATE UNLOGGED TABLE pgfw_cache_entry (
    namespace   text        NOT NULL,
    raw_key     bytea,
    key_hash    bytea       NOT NULL CHECK (octet_length(key_hash) = 32),
    value       bytea       NOT NULL,
    version     bigint      NOT NULL CHECK (version > 0),
    expires_at  timestamptz,
    updated_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace, key_hash),
    CHECK (raw_key IS NULL OR octet_length(raw_key) BETWEEN 1 AND 1024)
) PARTITION BY HASH (key_hash);

CREATE UNLOGGED TABLE pgfw_cache_namespace (
    namespace   text        NOT NULL PRIMARY KEY,
    clearing    boolean     NOT NULL DEFAULT false,
    epoch       bigint      NOT NULL DEFAULT 1 CHECK (epoch > 0),
    updated_at  timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE UNLOGGED TABLE pgfw_cache_entry_p00
    PARTITION OF pgfw_cache_entry
    FOR VALUES WITH (MODULUS 64, REMAINDER 0);

-- 其余分区 p01 ... p63 由迁移脚本统一创建。

CREATE INDEX pgfw_cache_entry_expires_idx
    ON pgfw_cache_entry (expires_at)
    WHERE expires_at IS NOT NULL;
```

`expires_at IS NULL` 表示该 key 没有 TTL。调用方应通过 namespace 默认 TTL 或业务配置限制无 TTL key 的数量。

上例使用 64 个物理分区。`key_hash` 已包含 namespace，因此按 key 查询只携带 `namespace + key_hash` 即可裁剪到单个分区。查询父表且不带 key 条件时会扫描所有分区，适合配置管理、迁移和分页巡检；不适合作为请求热路径或执行无界 `SELECT *`。

`UNLOGGED` 的语义：

- 同一个 primary 上的多个实例可以共享数据。
- 不写 WAL，能够降低缓存写入产生的 WAL 和复制开销。
- 数据库异常重启后会截断；物理 standby 不复制该表。
- 主备切换后缓存变冷，不能把表内容当作业务事实。

### 分区

分区用于限制单个物理表、索引和 autovacuum 的工作集，不改变缓存的单 key 语义。

- `namespace` 只负责逻辑隔离，不保证数据分布均匀。
- `key_hash` 由 namespace 和 canonical key 共同派生，PostgreSQL 对 `key_hash` 做 HASH 分区。
- 按 key 查询包含 `namespace` 和 `key_hash`，PostgreSQL 自动裁剪到目标分区。
- 查询全部 key 时访问分区父表，PostgreSQL 会扫描所有分区。管理接口必须使用 keyset/cursor 分页并设置最大 limit。
- 批量写入可以按应用内部 buffer stripe 分组，但 `stripe_id` 不持久化，也不进入主键；物理路由始终由 `key_hash` 完成。
- `SHARD_COUNT` 在同一个 schema generation 内固定。改变分区数必须创建新 generation；缓存可以切换后冷启动，不能静默复用旧映射。
- 后台清理、统计和容量告警按物理分区或分批巡检，不能依赖全表 `count(*)` 作为热路径指标。

### 数据库可读视图

`pgfw_cache_entry_readable` 把 `key_hash` 显示为十六进制文本，并通过
`pgfw_display_text` 显示合法 UTF-8 的 `raw_key` 和 value；非法 UTF-8 自动回退为
PostgreSQL escape 编码。底表继续使用 `bytea`，避免把任意二进制缓存值限制为字符串。

只有启用 `store_raw_keys` 时 `raw_key_text` 才有值。视图用于受控排障和管理查询，
不带 key 条件时会扫描所有分区，必须设置 `LIMIT` 或使用 keyset 分页。

### L1：分片 HashMap

每个缓存实例维护独立的：

```text
L1Shard[N] {
    HashMap<KeyId, CacheEntry>,
    invalidation_epoch: u64,
}
CacheEntry {
    value: Arc<Bytes>,
    version: i64,
    expires_at: Option<Instant>,
    inserted_at: Instant,
}
```

实际数据结构可以调整，但必须满足以下约束：

- value 使用 `Arc<[u8]>`，读取时尽快释放标准库 Mutex guard。
- L1 不跨 `.await` 持有分片锁引用。
- TTL 使用从数据库剩余时间换算出的 `Instant`，避免应用实例墙钟偏差。
- L1 同时限制条目数和 value 总字节数，并在任一上限触达时淘汰最旧记录；组件自己负责 TTL。
- L1 安装前比较 L2 `version`；旧快照不得覆盖已有的新版本。

## Key 和命名空间

公开 API 接受稳定编码的业务 key，组件内部转换为：

```text
KeyId = SHA-256(namespace || canonical_key_bytes)   # high-entropy key
KeyId = HMAC-SHA256(secret, namespace || key_bytes) # low-entropy key
```

要求：

- 编码必须跨进程、跨版本稳定，不能依赖 Rust `RandomState`。
- namespace 用于租户、业务域或部署环境隔离。
- NOTIFY 只携带 32 字节摘要，不携带原始 key 和 value。
- NOTIFY 摘要就是 `KeyId`；分区信息不放入 payload，PostgreSQL 根据 `key_hash` 自动路由。
- `store_raw_keys = false` 时，数据库只保存不可逆哈希；默认配置即为该模式。
- `store_raw_keys = true` 时，`raw_key` 列额外保存原始字节，便于管理查询和排障。原始 key 可能包含凭据、PII 或长 URL，会扩大数据库、备份和监控的敏感数据边界。
- 默认最大 key 为 512 字节，SQL schema 硬上限为 1024 字节。超限在发起数据库操作前拒绝。
- 低熵敏感 key 必须配置 `KeyHasher::HmacSha256`；HMAC secret 轮换会改变全部分区映射，必须按新 schema generation 处理。
- 哈希用于缩短 key 和降低直接暴露风险，不等价于加密。保存 `raw_key` 后应以敏感业务数据处理。
- 组件限制单个 value 大小，避免单个缓存项占用过多内存或 TOAST 空间。

### 版本

- `pgfw_cache_entry.version` 是单个 key 的单调递增数据库版本。
- `set`、TTL 修改和过期行接管都会在提交事务中递增 version。
- L2 读取和 warm reload 必须携带 version；L1 只在版本不旧于当前条目时安装。
- 版本冲突按“保留较大版本”处理，不能按请求返回顺序覆盖。
- version 不放入 key-only NOTIFY payload。每次实际版本变化仍发送对应 `KeyId` 失效事件；接收方采取保守失效，不使用通知推断版本。
- 缓存数据本身允许故障后丢失，version 只用于同一数据库生命周期内的新旧比较，不作为业务全局版本号。
- 重连时同时读取 namespace epoch；epoch 变化表示可能遗漏了清理事件，必须丢弃旧 pending 写缓冲并清空 L1。

## 读取路径

### LocalFirst

普通缓存默认采用 `LocalFirst`：

1. 如果该 key 有尚未刷盘的本地 pending mutation，先按最新 mutation 返回 value 或 miss；pending delete 不能回退到旧 L2。
2. 否则读取 L1；未过期则返回。
3. L1 miss 或 expired 时读取 PostgreSQL L2。
4. SQL 使用数据库时钟过滤已过期记录。
5. 将结果写回 L1，并保留 L2 version 和数据库给出的剩余 TTL。

该模式语义是最终一致。多实例写成功后，其他实例通常会在 NOTIFY 到达后失效 L1，但无法保证写后立即读到新值。

### Shared

需要跨实例 read-after-write 时使用 `Shared` 读取：

1. 如果该 key 有 pending mutation，先等待该 key 的写缓冲完成 flush；超时或失败则返回存储错误。
2. 始终读取 PostgreSQL L2。
3. 未过期时顺便刷新 L1。
4. L1 只作为后续 `LocalFirst` 读取的预热数据。

`Shared` 不读取只读副本，避免复制延迟破坏语义。

### 回源与单飞

如果组件提供 `get_or_load`：

- 同一个 key 的并发 L1 miss 在单实例内合并为一次 L2 读取或业务 loader。
- 不同实例允许并发回源，第一版不依赖 PostgreSQL advisory lock 做分布式 singleflight。
- 如需降低跨实例击穿，可后续增加基于 `pg_try_advisory_xact_lock` 的可选模式。
- loader 返回后必须先确认该 key 的失效 generation 未变化，再安装 L1，避免旧值在失效事件之后被重新写回。

## 写入与失效

每次实际刷入 L2 的 DML 和对应 NOTIFY 必须在同一事务中提交：

```text
BEGIN
  INSERT / UPDATE / DELETE pgfw_cache_entry_pNN
  SELECT pg_notify('pgfw_cache_v1', encode(key_id, 'hex'))
    FROM unnest($key_ids::bytea[]);
COMMIT
```

PostgreSQL 在事务提交后才投递 NOTIFY；事务回滚时数据修改和通知都不会生效。

组件同时提供两种入口：

- `set` / `delete`：组件内部创建事务，可以选择立即写入或进入内存合并缓冲。
- `*_in(&tx, ...)`：加入调用方已有业务事务，保证业务数据与缓存变更同提交。

不要在业务事务提交后再单独调用 `pg_notify`。进程可能在两步之间崩溃，造成数据库已更新但其他实例永远没有收到失效事件。

`Immediate` 写入提交后，写实例应主动清理自己的 L1。`Buffered` 写入进入队列时，也要立即用 pending overlay 遮蔽旧 L1。不要直接把 pending 新值塞回普通 L1，否则并发失效事件、TTL 和 version 的排序容易出错。

### 写入模式

提供两种明确的写入模式，不通过隐藏配置改变调用语义：

- `Immediate`：调用返回前完成 L2 DML 和 NOTIFY 提交。适合低频写入、需要提交确认，以及紧接 `Shared` 读取的场景。
- `Buffered`：调用只完成有界内存入队，由后台 flusher 合并同 key 写入并批量刷盘。适合高频覆盖写、状态刷新和能接受短时间 L2 延迟的场景。

`*_in(&tx, ...)` 和 `set_if_absent` 必须使用 `Immediate`。前者属于调用方事务，不能延后；后者的结果依赖 L2 当前状态，无法在本地先合并后决定。

`Buffered` 模式下，`set` 成功表示写入已进入内存缓冲，不表示 L2 已可见。需要“已刷盘”屏障时，调用方必须等待 `flush()`，或者使用 `Immediate`。进程崩溃会丢失尚未刷盘的 `Buffered` 写入；缓存数据本身可重建，因此这是有意的可用性取舍。

### 内存合并与批量刷盘

写缓冲至少维护：

```text
HashMap<KeyId, PendingMutation>
bytes
in_flight_batch
```

合并规则：

- `HashMap` 以完整 `KeyId` 为 key，同一 key 只保留最后一个 mutation。
- `set` 和 `delete` 是绝对操作，后续出现时直接替换前序状态。
- `set` 后接 `expire` 保留新 value 并替换 TTL；`set` 后接 `persist` 保留新 value 并移除 TTL。
- `delete` 后接 `expire` 或 `persist` 必须继续保留 delete tombstone，不能复活旧 L2 行。
- 较早的 `set` 后接 `delete` 时，必须保留 delete tombstone，不能因为“删除不存在的行”而丢掉失效事件。
- 新写入只有在所属 buffer stripe 的刷盘事务成功后才从 pending 状态移除。
- 不同 buffer stripe 可以轮流刷盘，但单个 key 的操作顺序不能被重排。
- pending mutation 同时作为本实例的读覆盖层：`set` 返回队列中的最新 value，`delete` 返回 miss，不能用旧 L1/L2 覆盖它。

触发器至少包括：

| 条件 | 行为 |
| --- | --- |
| pending key 数达到批次上限 | 立即唤醒 flusher |
| pending value 字节数达到批次上限 | 立即唤醒 flusher |
| 距离上次成功刷盘达到 `flush_interval` | 定时刷盘 |
| 进程正常关闭 | 停止接收新写入并完成最后一次刷盘 |
| PG 重连或恢复完成 | 优先冲刷 pending overlay，再允许 L1 进入 Ready |

需要独立配置而不是无限增长：

```text
max_pending_keys        = 10_000
max_pending_bytes       = 32 MiB
max_batch_keys          = 1_000
max_batch_bytes         = 8 MiB
flush_interval          = 50 ms
```

表中的数值只是起点，必须根据 value 大小、分片数和 PostgreSQL 延迟压测后调整。pending key 和字节额度通过原子比较交换预留，达到硬上限时返回 `BufferFull`；不能通过扩大无界 channel 规避背压。

一次 flush 的流程：

1. 在短临界区内取出不超过上限的 pending mutation，记录每个 key 的 generation，立即释放锁。
2. 按内部 buffer stripe 分组，生成有界批量 upsert/delete 参数；PostgreSQL 再按 `key_hash` 路由物理分区。
3. 在数据库事务中写入，并为该事务内每个 distinct `KeyId` 调用一次 `pg_notify`。
4. 提交成功后，仅移除 generation 未变化的 pending 项；同一 key 在 I/O 期间的新写入保留到下一批。
5. 提交失败时合并回显式重试队列，使用带抖动的指数退避；不能静默丢弃，也不能覆盖期间到达的新写入。

刷盘任务不能在持有写缓冲、L1 或批量状态锁时执行数据库 `.await`。批次过大时应拆分，避免触发 PostgreSQL 绑定参数数量或单事务锁范围上限。

### NOTIFY payload

key 级失效事件只通知主键摘要，不传 namespace 原文、key 原文、operation、value 或 TTL：

```text
channel: pgfw_cache_v1_<project_or_environment_hash>
payload: 64 个小写十六进制字符，表示 32 字节 KeyId
```

约束：

- key 级 payload 固定为 64 字节文本；即使 namespace 或业务 key 很长，也不会接近 PostgreSQL `NOTIFY` 的 8000 字节限制。
- 一个刷盘事务只对每个 distinct key 发送一次通知；PostgreSQL 会折叠事务内相同 channel 和 payload 的重复通知。
- 不允许把一批 key 用逗号拼接、JSON 数组或 envelope 装入一个 payload。批处理优化写入，不等于把通知也合并成一个大包。
- 所有 key 级变更都语义化为“该 key 失效”；接收方只按 `KeyId` 清除 L1，不需要在 payload 中携带 operation。
- channel 必须包含项目、环境或 schema generation 维度，避免同一数据库上的不同部署互相干扰。
- 每个进程只需要一个专用 `PgListener`，再通过有界进程内 broadcast 分发给多个缓存实例。

`clear_namespace` 使用独立控制 channel。payload 固定为 `1` 或 `0` 加 64 字符 namespace hash，共 65 字节：`1` 表示开始清理，`0` 表示清理完成。接收方遇到开始事件后清空 L1 和 pending 写缓冲，并拒绝新读写；完成事件到达后重新预热。任何控制事件都不得携带 value 或无限长 key 列表。

## DML

### `set`

```sql
INSERT INTO pgfw_cache_entry (
    namespace, raw_key, key_hash, value, version, expires_at
)
VALUES ($1, $2, $3, $4, 1, $5)
ON CONFLICT (namespace, key_hash)
DO UPDATE SET
    value = EXCLUDED.value,
    version = pgfw_cache_entry.version + 1,
    expires_at = EXCLUDED.expires_at,
    updated_at = clock_timestamp()
RETURNING version, expires_at;
```

`expires_at` 由数据库时间计算。若提供 TTL，则使用数据库时钟加上 TTL；未提供时使用 namespace 默认 TTL 或 SQL `NULL`。

`Buffered` 模式使用批量 upsert，不为每个被合并的旧 value 返回 version。TTL 从实际刷盘时的数据库时间开始计算，因此最多会比调用时间晚一个 flush interval；需要严格 TTL 起点时使用 `Immediate`。

### `set_if_absent`

使用单条 `INSERT ... ON CONFLICT DO UPDATE ... WHERE` 实现原子写入：

- 从未存在的 key 写入成功。
- 已存在且未过期的 key 写入失败。
- 已存在但已过期的 key 可以覆盖。

该操作必须直接访问目标分区，不能进入写缓冲。其成功或失败只能在 PostgreSQL 返回后确定。

这是 cache 语义的 NX，不应宣传为分布式锁。锁场景需要唯一 owner、fencing token 和明确的释放协议。

### `get`

`Shared` 读取的 SQL 必须同时判断过期：

```sql
SELECT value, version, expires_at
FROM pgfw_cache_entry
WHERE namespace = $1
  AND key_hash = $2
  AND (expires_at IS NULL OR expires_at > clock_timestamp());
```

过期记录视为不存在。读取时可以惰性删除，但不要把惰性删除当作唯一清理手段。

### 查询全部 key

管理或巡检需要遍历 namespace 下全部有效 key 时，查询声明式分区父表：

```sql
SELECT key_hash
FROM pgfw_cache_entry
WHERE namespace = $1
  AND ($2::bytea IS NULL OR key_hash > $2)
  AND (expires_at IS NULL OR expires_at > clock_timestamp())
ORDER BY key_hash
LIMIT $3;
```

- 该查询会跨所有物理分区，结果仍按 `key_hash` 全局排序。
- 调用方把最后一行作为下一次 cursor，并设置固定最大 limit；不能无界拉取整个 namespace。
- `scan_keys` 同时返回 `key_id` 和可选 `raw_key`；未开启原始 key 存储时 `raw_key` 为 `None`。
- 内部 L1 不使用该查询；L1 恢复使用有上限的 warm reload。

### `delete`

1. 删除 L2 记录。
2. 同一事务提交 NOTIFY。
3. 清理本实例 L1。

`Buffered` 模式下 delete 以 tombstone 保存到写缓冲。只有实际刷盘事务提交后才发送 NOTIFY；即使 L2 原本不存在该行，只要 delete 可能覆盖其他实例的 L1，就仍需传播该 key 的失效。

### `expire` / `persist`

- `expire` 更新绝对过期时间，并发送失效事件，防止其他实例保留旧 TTL。
- `persist` 将 `expires_at` 设置为 `NULL`，并发送失效事件。
- 两者都在同一事务中递增 version。
- 输入 TTL 必须在组件允许的上下界内校验。

### `ttl`

TTL 从 PostgreSQL 读取，使用数据库时钟计算剩余时间：

- key 不存在：返回 `None`。
- key 无过期时间：返回无 TTL 状态。
- key 已过期：视为不存在。

### `clear_namespace`

大 namespace 不应在一次事务中删除无限多记录，也不能把所有 key 放入一个 NOTIFY。推荐流程：

1. 将 `pgfw_cache_namespace.clearing` 设置为 `true` 并递增 epoch，发送 65 字节控制事件。
2. 清空本实例 L1 和 pending 写缓冲；其他实例收到事件后执行相同操作。
3. 按 `(namespace, key_hash)` 分批删除，避免依赖分区表内非全局唯一的 `ctid`。
4. 写事务先对 namespace 行执行 `FOR SHARE`；clearing 期间返回 `NamespaceClearing`，防止新写入与清理交错。
5. 完成后把 `clearing` 设为 `false`、再次递增 epoch 并发送完成事件。中途失败时保持 clearing，调用方可重试清理。

该操作跨越多个物理分区，不承诺所有分区在同一事务内完成，因此通过 namespace epoch 而不是“最后一批删除通知”表达正确状态。

## Rust API

```rust
use std::sync::Arc;
use std::time::Duration;
use pg_framework_cache::{
    build_default_pool, Cache, CacheConfig, CacheFlusher, CacheListener,
    WriteMode,
};

let mut config = CacheConfig::new("project:prod:orders")
    .with_write_mode(WriteMode::Buffered);
config.buffer.flush_interval = Duration::from_millis(50);

let pool = build_default_pool(pg_config.clone())?;
let cache = Arc::new(Cache::connect(pool, config).await?);
let listener = CacheListener::start(Arc::clone(&cache), pg_config).await?;
let flusher = CacheFlusher::spawn(Arc::clone(&cache));

cache.set(b"order:42", payload, Some(Duration::from_secs(60))).await?;
```

`CacheConfig::require_listener_ready` 默认为 `true`，因此推荐先启动 `CacheListener`。关闭时应先停止接收业务写入，再依次关闭 flusher 和 listener。

`build_default_pool` 默认设置 connect、wait、create、recycle、statement 和 lock timeout；`CacheConfig.operation_timeout` 与 `shutdown_timeout` 负责组件级总超时。使用外部连接池时需要提供等价保护。

## TTL 和清理

过期判断始终在逻辑读取时完成，后台任务只负责回收存储：

- SQL 使用 `clock_timestamp()`，不使用应用实例时间。
- L1 保存根据数据库返回剩余 TTL 换算的 monotonic deadline。
- 清理任务按物理分区轮询或在父表分批删除，例如每批 1000 行，使用 `FOR UPDATE SKIP LOCKED`。
- 每个实例都可以运行清理任务；任务必须幂等并且能容忍并发。
- 清理频率、批次大小和最大执行时间可配置。
- 监控每个分区的 expired row 数量、物理大小、清理耗时和单批删除行数。

示例逻辑：

```sql
WITH expired AS (
    SELECT namespace, key_hash
    FROM pgfw_cache_entry
    WHERE expires_at IS NOT NULL
      AND expires_at <= clock_timestamp()
    ORDER BY expires_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
DELETE FROM pgfw_cache_entry AS cache
USING expired
WHERE cache.namespace = expired.namespace
  AND cache.key_hash = expired.key_hash
  AND cache.expires_at <= clock_timestamp();
```

## 监听与恢复

缓存实例使用明确的状态机：

```text
Disconnected -> Listening -> Recovering -> Ready
                    ^            |
                    +------------+
```

恢复规则：

- 使用异步 `CacheListener::start(...)` 启动；只有 schema 校验、LISTEN、namespace 状态读取和首次 warm reload 全部成功后，方法才返回并设置 `Ready`。
- `require_listener_ready` 默认开启。未进入 `Ready` 时普通缓存读写返回 `Recovering`；仅管理扫描和 namespace 清理允许继续执行。
- 每次断线、schema 不匹配、namespace 进入 clearing 或 listener 关闭时，状态立即退回非 Ready。
- 每个进程使用一个持久 PG 会话执行 LISTEN；不能使用 transaction-pooling 的 PgBouncer。
- 启动或重连时先完成 LISTEN，再读取 L2 快照。顺序反过来会在“快照完成”和“开始监听”之间留下失效缺口。
- 监听连接断开、schema generation 变化或数据库重连后，立即离开 `Ready` 状态，清除受影响 namespace 的 L1，并递增本地 recovery generation。
- 连接池发生全量断连重连或健康检查确认数据库会话重建时，同样进入恢复流程。单条池连接被替换但 LISTEN 订阅仍连续时，可以保留 L1，因为事件流没有出现缺口。
- L1 恢复期间不得安装旧快照。`require_listener_ready = false` 时可按需读取 L2；开启时拒绝普通读写，直到订阅和重载完成。
- 从 L2 读取有上限的未过期记录，过滤 `expires_at <= clock_timestamp()` 后按本地 stripe 加载到 L1。
- 全量数据大于 L1 容量时，只预热到配置容量；剩余记录按需加载。不能以“重新加载”为理由突破 L1 的内存上限。
- 读取快照前记录 per-key invalidation generation；安装前 generation 必须未变化。恢复期间收到该 key 的 NOTIFY 时，将 key 标记为 dirty 并禁止安装旧快照。
- pending 写缓冲中的 key 不能被 L2 快照覆盖。连接恢复后先冲刷 pending overlay，或在 Ready 前持续排除这些 key。
- 完成配置范围内的有界 warm reload 后才能回到 `Ready`；超过 L1 容量的数据继续按需加载。
- 网络抖动恢复后使用带抖动的指数退避重连；每次重连都重复“恢复订阅、清 L1、重载”的完整流程。
- NOTIFY 不持久化，不提供消息确认和断线重放。
- TTL 是 NOTIFY 丢失时的最终陈旧时间上界。无 TTL 的 key 依赖重连清空和主动重载，因此不适合安全敏感数据。

重载任务和业务读可以并行，但 L1 安装必须经过 generation 校验。不能在持有 L1 guard 时查询 PostgreSQL。

### 主备切换

网络中断和 primary 切换必须区分处理：

- 同一 primary 上的短暂网络抖动：L2 内容仍在，清 L1 后重新加载可以恢复缓存命中率。
- primary 切换或 PostgreSQL 异常重启：`UNLOGGED` 表可能为空，重载得到一个空缓存，随后按需回源。
- 每次重连都会先退出 Ready 并清空 L1，不沿用旧连接周期中的缓存状态。
- pending 写批次在提交结果不确定时按幂等 upsert/delete 重试；重复 NOTIFY 可以接受，重复删除和 upsert 不改变缓存语义。

## 并发和竞态

必须覆盖以下竞态：

1. 实例 A 从 L2 读到 version N。
2. 实例 B 写入 version N+1 并发送失效。
3. 实例 A 在失效之后才把 version N 写入 L1。

写缓冲还会出现以下交错：

1. flusher 取出 key K 的 generation G。
2. 业务线程写入 K 的 generation G+1。
3. flusher 成功提交 G。
4. 如果直接删除 K 的 pending 项，会丢失 G+1。

处理方式：

- 回源开始和安装前比较 per-key invalidation generation。
- generation 变化时丢弃结果，重新读取或返回 miss。
- L1/pending 分片锁只在较短同步区间持有，不跨数据库 I/O 或任何 `.await`。
- 同一个 key 的进程内 singleflight 不应阻塞其他 key 的请求。
- flush 批次只移除 generation 与快照相同的 pending 项；更新版本保留到下一批。
- flush 失败回填时使用 per-key generation 做 last-write-wins，不能覆盖失败期间到达的新 mutation。
- NOTIFY 处理、快照安装和 pending 合并共享同一个 per-key generation/epoch 判定。
- 内部 L1/buffer stripe 在调用开始处只计算一次；数据库物理分区始终由稳定 `key_hash` 决定。

## 失败语义

- L1 命中且未过期：PG 短暂不可用时仍可返回。
- L2 miss 时 PG 不可用：返回错误，不伪造 miss。
- 监听断开但 L1 未过期：`require_listener_ready = false` 时可按 TTL 策略继续服务。
- `require_listener_ready = true`：监听断开、订阅不完整或首次恢复未完成时返回 `Recovering`。
- `Buffered` 写入在 PG 不可用时保留在重试队列；达到硬上限后通过背压或显式错误拒绝新写入。
- PG 重连：先恢复 LISTEN，清除不可信 L1，再进行有上限的 L2 warm reload；pending overlay 不能被快照覆盖。
- PG 主备切换：清空 L1，等待新 primary 上的空 `UNLOGGED` 表可用后按需重建。
- 刷盘事务结果未知：按幂等 mutation 重试并记录不确定提交指标，不能把结果当作确定失败后覆盖更新的 pending 值。

## 验收场景

- 两个独立实例写入和读取同一 key，能够看到共享 L2 数据。
- 实例 A 写入后，实例 B 的 L1 收到失效并重新加载新值。
- 业务事务回滚时，缓存数据和 NOTIFY 都不生效。
- 监听断线期间发生写入，重连后 L1 被清空，不返回无限期旧值。
- PG 短暂断网恢复后，实例重新完成 LISTEN，并从 L2 有界重载有效数据。
- 重载过程中收到 key 失效事件，旧 L2 快照不会在事件之后重新安装到 L1。
- TTL 到期后，L1 和 L2 都不能再返回该 key。
- 并发 `set_if_absent` 只有一个调用成功。
- 回源与失效交错时，旧值不会在失效事件之后长期驻留。
- 同一 key 短时间内写入 10,000 次，只产生有上限的批量 L2 写入，且每个刷盘事务对该 key 至多发送一次 NOTIFY。
- namespace 和业务 key 超长时，key 级 NOTIFY payload 仍固定为 64 字节文本。
- 写缓冲达到内存硬上限时产生背压或错误，进程内存不持续增长。
- flush 与同 key 新写入交错时，新 mutation 不丢失。
- 按 key 的业务查询带 `namespace + key_hash`，执行计划只扫描对应分区。
- 查询全部 key 时使用有界游标分页，结果可以跨分区连续遍历。
- 调整 `SHARD_COUNT` 时切换到新 schema generation，不复用旧物理分区。
- 数据库异常重启后，缓存表为空但业务功能能够冷启动。
