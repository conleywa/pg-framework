# pg-framework-lock

`pg-framework-lock` 是基于 PostgreSQL 的多实例分布式租约锁组件。它为每个成功获取的锁生成单调 fencing token，用于阻止租约过期、网络分区或 GC 停顿后的旧 owner 写入共享资源。

目标不是提供一个“永远不会失联”的锁服务，而是在 PostgreSQL 是唯一共享基础设施的前提下，让锁竞争、过期、重连和主备切换具有明确、可验收的语义。

## 设计目标

- 同一个 namespace 和 lock key 在任何时刻最多只有一个有效 lease owner。
- 锁使用 PostgreSQL 日志表保存，主备切换后不能像 `UNLOGGED` 缓存一样静默消失。
- 租约时间由数据库时钟决定，应用使用剩余时长换算单调时钟 deadline。
- 每次重新获取锁都获得严格递增的 fencing token。
- 网络抖动、提交结果未知或续租失败时，不盲目假设仍持有锁。
- 等待锁不长期占用数据库连接，不使用无界轮询或惊群唤醒。
- 锁 key、owner token 和业务敏感信息不进入日志。

## 非目标

- 不提供 FIFO 公平性；竞争失败者可能多次落后。
- 不保证跨多个锁的原子获取。
- 不把锁替代为业务幂等、唯一约束或 fencing 校验。
- 不允许依赖 GC、协程调度或进程存活来判断锁是否仍有效。
- 第一版不提供可重入锁。同一逻辑任务再次获取同一 key 会被视为另一个 owner。
- 不使用 PostgreSQL session advisory lock 实现跨资源长事务租约。advisory lock 随会话和数据库重启消失，也不能携带 fencing token。

## 一致性模型

锁由以下三部分共同构成：

```text
Lease     = (LockId, OwnerToken, FencingToken, DB expires_at)
Guard     = 本进程对 Lease 的持有状态和单调时钟 deadline
Fencing   = 共享资源拒绝比已见 token 更旧或相等的写入
```

- 数据库行只能被一个 owner 在租约有效期内占用。
- owner 在数据库租约过期后可能不知道自己已失去锁，因此不能仅凭“我没有收到释放通知”继续写业务资源。
- 新 owner 获取锁时使用更大的 fencing token；被保护资源必须原子比较或持久化最后接受的 token。
- 正确性依赖 fencing，而不是应用进程之间的时钟完全一致。
- 如果不具备 fencing 能力，只能把锁当作降低重复执行的优化，不能当作强互斥保证。

### 为什么需要 fencing

考虑以下顺序：

1. owner A 获取 token 10，随后发生长时间 GC。
2. A 的数据库租约过期。
3. owner B 获取同一锁，得到 token 11。
4. A 恢复运行，并在检查本地状态前向共享资源写入。

数据库锁已经属于 B，但 A 仍可能执行写入。只有共享资源拒绝 token 10，才能保证 A 不会覆盖 B。仅有 TTL 或“心跳超时”不能解决该问题。

## 存储模型

锁表必须使用普通日志表，不能使用 `UNLOGGED`。

```sql
CREATE SEQUENCE pgfw_lock_fencing_seq
    AS bigint
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    NO CYCLE
    CACHE 1;

CREATE TABLE pgfw_lock (
    namespace      text        NOT NULL,
    raw_key        bytea,
    key_hash       bytea       NOT NULL CHECK (octet_length(key_hash) = 32),
    owner_token    bytea       NOT NULL CHECK (octet_length(owner_token) = 32),
    fencing_token  bigint      NOT NULL CHECK (fencing_token > 0),
    lease_expires_at timestamptz NOT NULL,
    acquired_at    timestamptz NOT NULL,
    PRIMARY KEY (namespace, key_hash),
    CHECK (raw_key IS NULL OR octet_length(raw_key) BETWEEN 1 AND 1024)
);

CREATE INDEX pgfw_lock_expires_idx
    ON pgfw_lock (lease_expires_at);
```

约束：

- 只有仍有效的锁行保存在表中；release 和过期清理都会删除行。
- fencing token 来自全局 non-cycling sequence，所以删除并重新创建锁行不会让 token 归零。
- sequence 显式使用 `CACHE 1`，降低进程崩溃时未使用 cached value 的丢失风险。空洞可以接受，倒退不可以接受。
- sequence 必须和锁表处于同一个恢复边界内。物理 standby 通过 WAL 恢复 sequence；逻辑恢复或手工重建时必须显式恢复 sequence 状态。
- 普通异步 copy 的 standby 可能缺少最近一次成功 acquire 的 lock/sequence WAL。需要跨主备继续安全签发 fencing token 时，部署必须提供同步复制或等价的“已确认 WAL 不丢失”保证；否则恢复后必须 fail-closed，等待人工验证。
- `owner_token` 由应用使用 CSPRNG 固定生成 32 字节。它不得出现在普通日志或指标标签中。
- `namespace` 包含项目、环境、租户或资源域，避免不同业务误用同一个 key。
- `raw_key` 默认不保存；需要管理查询时可显式开启，但锁资源 ID 常包含敏感业务信息。
- 锁表默认不按业务 key 分表。锁行数应受“同时存活的租约数量”限制；如果 key 基数长期很大，可以按 `key_hash` 做稳定 hash 分区，但同一个 key 必须始终路由到一个分区。

## Key 设计

```text
namespace = project / environment / tenant / resource_type
key_hash  = SHA-256(namespace || canonical_lock_key)
LockId    = (namespace, key_hash)
```

- canonical encoding 必须跨实例和版本稳定，不能依赖 Rust `DefaultHasher` 或 `RandomState`。
- 原始资源 ID 默认不写表、不写指标；低熵敏感 key 使用 HMAC。
- 默认最大 key 为 512 字节，SQL 硬上限为 1024 字节。
- `store_raw_keys = true` 时原始 key 会额外写入 `raw_key`，必须配套数据库权限、备份脱敏和保留策略。
- namespace 必须包含锁语义版本，避免发布时旧 owner 和新 owner 使用不同协议却竞争同一资源。
- 不同业务即使 key 文本相同，也必须使用不同 namespace。

## DML

### `try_acquire`

应用先生成新的 `owner_token`，数据库在一条语句中分配 fencing token 并尝试插入或接管已过期锁：

```sql
WITH candidate AS (
    SELECT nextval('pgfw_lock_fencing_seq') AS fencing_token
)
INSERT INTO pgfw_lock AS lock (
    namespace,
    key_hash,
    owner_token,
    fencing_token,
    lease_expires_at,
    acquired_at
)
SELECT
    $namespace,
    $key_hash,
    $owner_token,
    candidate.fencing_token,
    clock_timestamp() + $lease,
    clock_timestamp()
FROM candidate
ON CONFLICT (namespace, key_hash)
DO UPDATE SET
    owner_token = EXCLUDED.owner_token,
    fencing_token = EXCLUDED.fencing_token,
    lease_expires_at = EXCLUDED.lease_expires_at,
    acquired_at = EXCLUDED.acquired_at
WHERE lock.lease_expires_at <= clock_timestamp()
RETURNING
    fencing_token,
    lease_expires_at,
    clock_timestamp() AS database_now;
```

返回空行表示锁仍被其他 owner 持有。`ON CONFLICT` 加条件更新由 PostgreSQL 行锁串行化同一个 key 的竞争者；不同 key 不互相阻塞。

- 应用不能在冲突时自己把 expiry 改成过去来“抢占”锁。
- 输入 lease TTL 必须有上下界，避免 0、负数或过长租约。
- `nextval` 失败或 statement 回滚可以产生 token 空洞，这是允许的；fencing 只要求单调递增，不要求连续。
- 成功后，应用根据 `lease_expires_at - database_now` 计算本地剩余时长，并减去网络和调度安全余量。
- 本地 deadline 使用 `Instant` 等单调时钟，不能直接使用应用机器墙钟。

### `renew`

```sql
UPDATE pgfw_lock
SET lease_expires_at = clock_timestamp() + $lease
WHERE namespace = $namespace
  AND key_hash = $key_hash
  AND owner_token = $owner_token
  AND fencing_token = $fencing_token
  AND lease_expires_at > clock_timestamp()
RETURNING lease_expires_at, clock_timestamp() AS database_now;
```

- 续租频率建议为 lease TTL 的 `1/3`，并加入抖动，避免所有 owner 同时续租。
- 返回空行表示租约已丢失，不管本地 deadline 是否还没到。
- 续租请求不能无界重试。重试总时间必须小于本地安全 deadline；超过后进入 `Lost` 状态。
- I/O 期间不能持有进程内全局 Mutex。Guard 状态只在短临界区内更新。

### `release`

```sql
DELETE FROM pgfw_lock
WHERE namespace = $namespace
  AND key_hash = $key_hash
  AND owner_token = $owner_token
  AND fencing_token = $fencing_token;
```

- release 必须同时匹配 owner 和 fencing token。
- 旧 owner 延迟到达的 release 不能删除新 owner 的锁行。
- `DELETE` 条件保证旧 owner 不会删除新 owner 的锁行。返回 0 行可能表示已经释放，也可能表示 owner 已变化；状态上报需要另行查询当前行，但不能据此放宽删除条件。
- 不要在异步 `Drop` 中隐式发起数据库 I/O。Guard drop 不做网络操作，未显式 release 的锁等待 TTL 清理。

### `acquire`

`acquire(deadline, lease_ttl)` 在 `try_acquire` 之上执行有界等待：

- 每次失败后 sleep 带抖动的指数退避，不能紧密循环。
- 每次重试重新查询数据库，不使用旧 contention 结果。
- 等待期间释放数据库连接；不要用 `SELECT ... FOR UPDATE` 占着一个连接等待租约到期。
- deadline 到期返回 `TimedOut`，与数据库错误和取消明确区分。
- 等待 future 必须能被取消。取消只停止后续尝试，已经成功但响应丢失的租约通过 owner token 回查处理。
- 不承诺公平顺序；业务不能依赖先到先得。

## 网络重连与未知结果

### acquire 响应丢失

请求可能已经提交，但客户端只看到连接错误。`try_acquire` 不能直接返回“未获取”：

1. 尝试在重连后的会话中按 `(LockId, owner_token)` 查询锁行。
2. 如果找到且 `lease_expires_at > clock_timestamp()`，恢复 fencing token 和 deadline，返回同一个 Guard。
3. 如果行不存在，说明此前请求未提交或租约已被清理，可以按新 owner token 再次尝试。
4. 如果 owner token 不匹配，说明已有其他 owner，返回 contention 或丢失。

owner token 必须在第一次发出 SQL 前生成并保留，直到完成结果归并。
如果数据库持续不可用，无法完成回查，`try_acquire` 必须返回 `OutcomeUnknown` 类错误，不能把连接错误映射成 `Contended`。

### renew 响应丢失

- 重连后按 owner token 和 fencing token 回查。
- 同一 owner 仍有效时，恢复最新 expiry 和状态。
- owner 已变化或租约已过期时进入 `Lost`，不能继续执行未被 fencing 保护的关键写入。
- 数据库持续不可用直至本地 deadline 时同样进入 `Lost`，网络恢复后也不能让旧 Guard 复活。

### release 响应丢失

- 重连后按 owner/fencing token 查询。
- 行不存在表示已释放。
- 仍是同一 owner 时可以重试删除。
- 已是其他 owner 时，旧 release 应返回成功或 `AlreadyLost`，绝不能删除新行。

### 主备切换

- 锁表和 sequence 使用 WAL，正常情况下可随 PostgreSQL primary 恢复。
- 新 primary 上必须重新校验 schema generation、表和 sequence 是否存在；不匹配时 fail-closed。
- 异步复制可能让新 primary 丢失旧 primary 已返回的 fencing token。没有同步复制保证时，不能自动继续签发新 token，否则下游可能观察到 token 倒退。
- 切换期间数据库不可用时，现有 owner 继续消耗本地 deadline。到期即停止；不能因为旧连接对象还在就延长租约。
- primary 角色变化、server identity 变化或会话重建都会清理连接级缓存和未归并请求状态，但不会修改已持久化的锁行。
- advisory lock 不参与该恢复模型，因此不作为默认锁实现。

## Lease 安全窗口

数据库给出的 lease TTL 不等于应用可安全执行关键操作的完整时长。

- 从数据库返回到客户端收到响应之间存在网络延迟。
- 进程可能发生 GC、调度暂停或 CPU 饥饿。
- 时钟推进不代表 owner 的代码能及时停止。

建议：

```text
renew_interval = lease_ttl / 3
safe_deadline  = database_remaining_ttl - max(network_p99, scheduler_pause_budget)
```

- `safe_deadline` 不能大于数据库 expiry。
- 每次关键操作前检查 Guard 是否仍为 `Active` 且当前单调时间早于 safe deadline。
- 任何续租错误都只能把状态转为 `Uncertain` 或 `Lost`，不能乐观延长本地 deadline。
- 被保护资源仍必须执行 fencing token 比较。仅检查本地 deadline 无法约束恢复运行后的旧 owner。

## Fencing 集成

下游资源至少采用以下一种模式：

- PostgreSQL 表中保存 `last_fencing_token`，在同一语句中执行 `WHERE last_fencing_token < $fencing_token` 后更新。
- 对象存储使用带版本或条件的写入，并把更高 token 作为一个单调资源版本提交。
- 消息队列或外部系统只在 token 单调增加时接受变更。

错误示例：

```text
检查 guard.is_active()
执行一个没有 fencing 条件的远程写请求
```

在这两步之间进程可能暂停到租约过期，远程写仍由旧 owner 完成。正确方案必须让共享资源在一次原子决策中拒绝旧 token。

### 同库事务内的严格互斥

如果临界区完全位于一个 PostgreSQL 事务内，可以使用 `pg_try_advisory_xact_lock`：

- 锁随事务提交或回滚自动释放。
- 不需要 lease heartbeat。
- 不能跨事务、跨资源或跨数据库故障保持，也不携带 fencing token。

该模式可以作为后续独立 API，不能与租约锁混合后宣称同类语义。

## 锁顺序与死锁

- 优先只获取一个锁。
- 如果必须获取多个锁，所有调用方按稳定 key 排序获取，并按相反顺序释放。
- 多锁获取不是原子的；中途失败必须释放已经获取的 Guard。
- 不要在持有一个锁时无超时等待另一个可能被反向持有的锁。
- 连接池中执行锁 DML 的操作应有 SQL timeout，并短于业务总超时。

## API 方向

```rust
pub enum AcquireOutcome {
    Acquired(LeaseGuard),
    Contended,
}

#[must_use = "a lease guard must be renewed or released explicitly"]
pub struct LeaseGuard {
    // lock_id, owner_token, fencing_token, monotonic deadline, state
}

let outcome = locker.try_acquire(&lock_key, LeaseTtl::from_secs(30)).await?;
let mut lease = match outcome {
    AcquireOutcome::Acquired(lease) => lease,
    AcquireOutcome::Contended => return Ok(()),
};

lease.renew().await?;
let fencing_token = lease.fencing_token();
// 共享资源必须原子拒绝小于或等于 fencing_token 的旧写入。
lease.release().await?;
```

公开类型建议：

- `LockKey`、`LockId`、`LeaseTtl`、`FencingToken` 使用 newtype，避免与普通整数和字符串混用。
- `AcquireOutcome` 只表示已确认的 `Acquired` / `Contended`；“结果无法归并”必须通过结构化错误返回。
- `RenewOutcome` 区分 `Renewed`、`Lost`、`Uncertain`。
- `LeaseGuard` 不可 `Clone`，不能被静默丢弃后仍被当成有效锁。
- `release(self)` 消费 Guard，防止重复释放。
- 所有错误使用结构化 enum，并标记 `#[non_exhaustive]`。
- 锁 key 的 Debug 输出只显示 namespace 和 hash，不显示原始 key 或 owner token。

第一版建议显式续租，不隐藏后台 heartbeat 任务。自动续租器必须在无法确认租约时可靠通知业务停止；简单后台任务容易让业务继续运行并误以为锁仍有效。

## 失败策略

| 场景 | 行为 |
| --- | --- |
| 锁被其他有效 owner 占用 | 返回 `Contended`，按 deadline 和 jitter 重试 |
| acquire 响应丢失 | 按 owner token 回查；有效则恢复 Guard，否则重新尝试 |
| acquire 响应丢失且无法回查 | 返回 `OutcomeUnknown`，不声称成功或竞争失败 |
| renew 返回空行 | 立即 `Lost`，停止依赖该租约的写入 |
| renew 连接错误但尚未超过 deadline | `Uncertain`，短暂回查；期间不声称续租成功 |
| 超过本地 safe deadline | 永久 `Lost`，网络恢复后不能复活 |
| release 响应丢失 | 回查 owner/fencing 后再重试，旧 owner 不能删除新锁 |
| PostgreSQL 不可用 | fail-closed；不创建本地锁替代 |
| primary 切换 | 重新校验 schema/sequence；旧 Guard 继续受本地 deadline 约束 |
| 进程崩溃 | 行保留到 lease 到期，新 owner 获得更大 fencing token |

## 可观测性

建议指标：

- try_acquire 成功、竞争、错误和取消次数。
- acquire 等待时长 P50/P95/P99。
- renew 成功、丢失、不确定和超时次数。
- 超过 safe deadline 后仍尝试续租的次数。
- 活跃锁行数、过期未清理行数、清理批次耗时。
- 每个 namespace 的锁竞争和持有时间，不记录原始 key 或 owner token。
- PG 重连、提交结果未知和 owner-token reconciliation 次数。

## 清理

- release 后立即删除行。
- 崩溃或网络断开留下的行由后台任务按 `lease_expires_at` 分批删除。
- 清理只回收存储，不参与锁是否有效的判断；acquire SQL 本身必须检查 database clock。
- 使用 `FOR UPDATE SKIP LOCKED` 避免多个清理任务争抢同一行。
- 删除过期行不需要保留 fencing 状态，因为 token 来自全局 non-cycling sequence。

## 验收场景

- 两个实例并发 `try_acquire` 同一 key，最多一个返回 Acquired。
- 不同 key 可以并发持有，不因全局 mutex 串行化。
- lease 到期后新 owner 可以获取锁，fencing token 严格大于旧 token。
- 旧 owner 在租约过期后提交的写入被下游 fencing 条件拒绝。
- renew 返回空行后 Guard 立即进入 Lost，不再报告 active。
- renew 超时并在 safe deadline 前无法回查时进入 Uncertain；超过 deadline 后永久 Lost。
- acquire 已提交但响应丢失时，通过 owner token 回查到同一个 fencing token 并恢复 Guard。
- 旧 owner 延迟 release 不能删除新 owner 的锁行。
- owner 崩溃后锁在 TTL 到期时可被接管，清理任务与 acquire 并发不会产生双 owner。
- PostgreSQL network jitter 恢复后，未过期的 Guard 通过回查恢复状态；已过期的 Guard 不复活。
- primary 切换后锁表和 sequence 可恢复，schema 不匹配时 fail-closed。
- 等待锁时不长期占用数据库连接，取消 future 能停止后续重试。
- 所有关键资源写入都能验证单调 fencing token，而不只是检查进程内 `is_active`。
- 异步复制丢失已确认 fencing WAL 时，恢复流程拒绝自动继续签发 token。
