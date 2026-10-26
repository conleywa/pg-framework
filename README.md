# pg-framework

`pg-framework` 是一组以 PostgreSQL 为共享基础设施的 Rust 通用组件，目标是在多实例部署中承担缓存、限频和分布式租约锁职责，减少额外中间件的部署、监控和运维成本。

项目不追求极限吞吐。优先目标是语义清晰、故障可预期、部署简单，并且不要求业务方引入 Redis 客户端、集群或独立的高可用方案。

## 组件

| 组件 | PostgreSQL 中的角色 | 一致性模型 |
| --- | --- | --- |
| [缓存](./crates/pg-framework-cache/README.md) | 分片非日志缓存表 + 合并写缓冲 + key 失效通知 | L1 默认最终一致，可显式选择共享强读 |
| [分布式锁](./crates/pg-framework-lock/README.md) | 日志锁表 + 全局 fencing token 序列 | 有界租约；下游校验 fencing 后阻止旧 owner |
| [限频](./crates/pg-framework-ratelimit/README.md) | 分片原子计数和窗口状态 | 单 key 操作强一致 |

三个组件共享 PostgreSQL 连接、数据库时钟、TTL 清理思路和可观测性约定，但不强行共用一套存储抽象。缓存的核心是“读取和失效”，限频的核心是“原子累加和决策”，锁的核心是“租约归属和旧 owner fencing”，三者的数据一致性要求不同。

## 总体架构

```text
                         PostgreSQL
                  +---------------------------+
                  | unlogged cache partitions |
                  | logged rate-limit parts   |
                  | logged lock + fencing seq |
                  | LISTEN / NOTIFY           |
                  +-------------+-------------+
                              |
            +-----------------+-----------------+
            |                                   |
      Application A                       Application B
   +---------------------+             +---------------------+
   | Shard Router        |             | Shard Router        |
   | Striped L1          |             | Striped L1          |
   | Cache Write Buffer  |             | Cache Write Buffer  |
   | PgListener/Recovery |             | PgListener/Recovery |
   | Lease Guard         |             | Lease Guard         |
   +---------------------+             +---------------------+
```

### 缓存

缓存采用三级路径：

1. 每实例按固定 stripe 分片的 HashMap 作为 L1。
2. 有界内存写缓冲按 `KeyId` 合并同一 key 的频繁写入，并按稳定 hash 分片批量刷入 PostgreSQL。
3. PostgreSQL `UNLOGGED` 声明式分区表作为所有实例共享的 L2。
4. 每次刷盘事务中的缓存 DML 与 `pg_notify` 一起提交，通知其他实例清除对应 L1。

缓存 value 只存储在 PostgreSQL 非日志表和进程内 L1，不发送到 NOTIFY payload。key 级 NOTIFY 只发送固定 32 字节 `KeyId` 的十六进制文本，即 64 字节 payload；不发送 namespace 原文、operation、TTL 或批量 key 列表，避免超过 PostgreSQL `NOTIFY` 的 8000 字节上限。

`UNLOGGED` 表适合可重建的缓存数据，但不写 WAL，也不参与物理复制。数据库异常重启或主备切换后缓存会变冷；这是用可接受的缓存丢失换取更低的 PG 写入成本。

缓存支持 `Immediate` 和 `Buffered` 两种写入模式。`Buffered` 写入成功只代表进入有界内存队列；后台 flusher 按时间、key 数或字节数触发批量提交。需要 L2 已提交语义时必须使用 `Immediate` 或等待显式 `flush()`。

### 限频

限频完全依赖 PostgreSQL 行级原子更新完成并发协调，不使用 pub/sub 传播计数，也不在本地合并计数。同一个限频 key 的请求在数据库内串行化，因此多实例之间不会因为本地计数产生超额放行。

cache 和 ratelimit 都按稳定 hash 将 key 映射到固定物理分区。不同 key 的写入、索引更新和清理分布在多个分区，避免默认全部落到一张大表；单个 key 仍只对应一个分区，不改变一致性语义。

声明式分区父表仍支持全量查询：按 key 查询会因 `key_hash` 条件裁剪到单分区，不带 key 条件的巡检会扫描全部分区。管理接口必须使用 limit 和 keyset/cursor 分页，不能把无界扫描放进请求热路径。

`namespace` 只负责逻辑隔离，`key_hash` 负责物理路由，不额外存储 `shard_id`。数据库默认只保存哈希；可显式开启 `store_raw_keys` 保存原始 key，默认 key 上限为 512 字节，SQL 硬上限为 1024 字节。原始 key 会扩大敏感数据边界，低熵 key 应使用 `KeyHasher::HmacSha256`。

固定窗口是默认算法。Token Bucket、滑动窗口和额度预租作为后续扩展，但 API 不应提前泄露具体算法细节。

### 分布式锁

锁使用普通日志表和全局 non-cycling sequence，不使用 `UNLOGGED` 表或 session advisory lock 作为默认实现：

1. `try_acquire` 在目标锁行上原子插入，或者接管已于数据库时钟过期的行。
2. 每次成功获取都从 sequence 取得严格递增的 fencing token。
3. holder 按 lease TTL 的三分之一续租，并在连接错误时回查 owner token 和 fencing token。
4. release 必须同时匹配 owner token 与 fencing token，旧 owner 不能删除新 owner 的锁行。
5. 被保护资源原子拒绝小于或等于已见 fencing token 的写入。

租约到期意味着新 owner 可以接管，但旧 owner 可能因网络分区或长时间暂停而不知道。因此锁组件不能只提供 `is_locked()` 或 `is_active()`；关键写入必须携带 fencing token。PostgreSQL 重连后，过期的 Guard 永久进入 `Lost`，不能因为数据库恢复而复活。

## TTL

三个组件都使用数据库时钟产生绝对 deadline，避免不同应用实例的墙钟偏差。

- 缓存支持可选 TTL；未设置 TTL 的记录保持到显式删除。组件可以配置 namespace 默认 TTL。
- 限频计数必须设置 TTL，窗口结束后经过短暂宽限期即可清理。
- 锁使用有上下界的 lease TTL；本地只保存根据数据库剩余时长换算出的单调时钟安全 deadline。
- 过期判断发生在读取时。后台清理只负责回收空间，不能让已经过期的数据重新可见。
- 缓存和限频清理按分片轮询；锁清理只删除已经过期的活跃锁行。所有清理都使用分批删除和 `FOR UPDATE SKIP LOCKED`。
- 本地 L1 使用根据数据库剩余 TTL 换算出的单调时钟 deadline，不直接使用应用机器墙钟。

## DML

组件公开的 DML 是数据库表操作的受控封装，不仅修改数据，还负责维护 L1、Guard 状态、TTL 和失效通知。

### 缓存 DML

- `set`：写入或覆盖 value，携带可选 TTL。
- `get`：按 `LocalFirst` 或 `Shared` 读取模式获取 value。
- `set_if_absent`：原子写入，已存在且未过期的 key 不覆盖。
- `delete`：删除共享记录并失效所有实例的 L1。
- `expire` / `persist`：修改或移除 TTL。
- `ttl`：返回剩余 TTL。
- `clear_namespace`：清理一个 namespace，并广播 namespace 级失效。

`Buffered` 模式只合并可安全覆盖的普通 `set` / `delete` / TTL 变更。`*_in`、`set_if_absent` 等依赖当前数据库状态或调用方事务的操作必须立即执行。

每次实际刷盘的缓存 DML 必须在同一事务中安排 NOTIFY；批处理按 key 发送小 payload，不能把整个批次编码成一个通知。绕过组件直接修改缓存表会导致其他实例的 L1 无法及时失效。

### 限频 DML

- `check`：原子消费额度并返回放行结果、剩余额度、重置时间和重试时间。
- `get`：只读查看当前窗口状态，不消费额度。
- `reset`：管理操作，清除指定 key 的当前状态。

限频计数不使用缓存 L1，避免近似计数破坏多实例下的一致性。

### 分布式锁 DML

- `try_acquire`：原子占用空闲或已过期锁行，返回 Guard 中的 fencing token 和单调安全 deadline；owner token 保持内部私有。
- `acquire`：在调用方 deadline 内使用抖动退避等待，不长期占用数据库连接。
- `renew`：同时匹配 owner token、fencing token 和未过期条件，延长租约。
- `release`：同时匹配 owner token 和 fencing token，删除当前 owner 的锁行。

提交结果未知时，组件使用预先生成的 owner token 回查。acquire 可能已经提交，因此不能把连接错误直接解释成竞争失败；renew 超过本地安全 deadline 后必须进入 `Lost`。

## 一致性边界

缓存不是分布式锁：

- `LocalFirst` 读可能短暂返回旧值，正常情况下由 NOTIFY 快速失效，异常情况下由 TTL 限制最大陈旧时间。
- `Shared` 读每次访问 PostgreSQL，不读取未经验证的 L1，适合跨实例 read-after-write 场景。
- `Buffered` 写入在刷盘前只对写实例的 pending overlay 可见，其他实例仍可能读到旧 L2/L1；`Shared` 读取遇到 pending key 时先等待 flush。
- NOTIFY 不是持久消息，不支持断线重放。监听重连或事件流出现缺口时必须清空对应 L1。
- PG 重连后必须先重新 LISTEN，再从 L2 有界重载；重载期间到达的失效事件必须阻止旧快照重新进入 L1。
- 需要锁语义时使用独立锁组件，而不是缓存 writer ownership、`set_if_absent` 或空闲 TTL。

限频的单 key 决策是强一致的，但不同 key 之间不提供跨 key 原子事务。业务如果需要一次消费多个 key，应使用专门的事务接口，而不是依次调用普通 `check`。

锁提供有界 lease 和 fencing token，但不提供魔法式的绝对互斥：

- 租约过期后新 owner 可以接管，即使旧 owner 进程仍存活。
- 旧 owner 的所有关键写入都必须被下游资源的 fencing 条件拒绝。
- 只检查本地 Guard 是否 active、只记录日志或只依赖 TTL，都不能阻止恢复运行后的旧 owner 写入。
- 锁不保证 FIFO 公平性，也不提供跨多个 key 的原子获取。

## 故障策略

| 场景 | 推荐行为 |
| --- | --- |
| 缓存监听连接断开 | 默认允许 L1 继续服务到 TTL；严格 namespace 可配置为 fail-closed，并在恢复时清除和重载 |
| 缓存 PostgreSQL 不可用 | `LocalFirst` 可返回仍有效的 L1；L2 miss 返回存储错误 |
| 缓存写缓冲接近上限 | 触发同步刷盘和背压；PG 仍不可用时返回显式错误 |
| PG 网络抖动恢复 | 缓存重新 LISTEN、清 L1 并有界重载；限频重新校验 schema 后直接继续读 PG |
| PG 主备切换 | 接受非日志缓存变冷，重新建立监听并清空 L1；限频依赖日志表恢复 |
| 限频 PostgreSQL 不可用 | 默认 fail-closed；业务必须显式选择其他策略 |
| 锁 acquire 响应丢失 | 按预生成 owner token 回查；有效则恢复同一 fencing token，否则重新尝试 |
| 锁 acquire 结果无法归并 | 返回 `OutcomeUnknown`，不把网络错误误报为竞争失败 |
| 锁 renew 失败或超时 | 在本地安全 deadline 前回查；到期后永久 `Lost`，不恢复旧租约 |
| 锁 release 响应丢失 | 按 owner/fencing token 回查并幂等重试，旧 owner 不删除新行 |
| 锁 PostgreSQL 不可用 | fail-closed，不退化成本地锁；现有 Guard 只运行到安全 deadline |
| 锁主备切换 | 重新校验锁表和 sequence；异步复制可能丢 token 时必须 fail-closed，过期的 Guard 不复活 |
| 事件消费者落后 | 丢弃事件流并清空对应 L1，随后从 L2 有界重载 |

## 部署约束

- 所有需要共享缓存的应用实例必须连接同一个 PostgreSQL 数据库。
- cache、ratelimit、lock 默认共享同一个进程级 PostgreSQL 连接池，`DEFAULT_POOL_MAX_SIZE = 8`。
- 池大小按 `ceil(QPS × P99 数据库延迟) + burst margin` 评估，并保证所有实例池上限之和低于 PostgreSQL `max_connections`，预留管理连接和主备切换余量。高延迟查询不能只靠扩大池解决。
- LISTEN 使用 PG 会话，必须使用专用直连或 session pooling；不能使用 PgBouncer transaction pooling。
- 迁移建表时区分 `UNLOGGED` 缓存表与普通日志表（限频、锁）。
- 锁表必须与 non-cycling fencing sequence 一起备份和恢复；不能把锁表建成 `UNLOGGED`。
- cache/ratelimit 的 schema generation 固定物理分区数；`namespace` 只负责隔离，`key_hash` 负责物理路由，不额外存储 `shard_id`。改变分区数必须迁移或切换 generation。
- 锁的 schema generation 固定锁表和 fencing sequence；普通异步复制不能自动证明已确认的 fencing token 不会倒退。
- 容量告警必须覆盖每个物理分区及其索引；只监控父表总量会掩盖单分区膨胀。
- `UNLOGGED` 表在主备切换、异常重启后为空，监控和容量规划必须接受冷启动。
- 缓存表只保存可重建数据，不能保存唯一业务事实。

## 非目标

- 不兼容 Redis 协议，也不迁移任意 Redis 数据结构。
- 不保证缓存跨 PostgreSQL 故障切换持久化。
- 不为所有 key 提供最高吞吐；热点限频 key 受 PostgreSQL 单行锁能力上限约束。
- 不把缓存当作锁、队列或持久化消息系统。
- 不把锁组件当作公平队列、跨 key 原子锁或业务幂等机制。
- 下游不具备 fencing 校验能力时不承诺旧 owner 的关键写入一定被阻止。

## 当前状态

三个组件已实现 Async Rust API、PostgreSQL 原子 DML、TTL 清理、稳定 key 路由、连接池接入和 `tracing` 观测。SQL 迁移统一保存在 [`sql/`](./sql/README.md)。

依赖只在 workspace 根声明，且只声明 major/minor 兼容版本，不在各 crate 固定 patch 版本：

- `tokio`、`tokio-postgres`、`deadpool-postgres`
- `sha2`、`hmac`、`getrandom`
- `thiserror`、`tracing`

默认进程级连接池上限为 `DEFAULT_POOL_MAX_SIZE = 8`，三个组件应共享同一个池。当前自动验证覆盖 key 路由、buffer 合并、配置边界和敏感 Debug 输出；接入真实 PostgreSQL 的集成测试需要在部署环境运行 SQL 迁移后补充执行。
