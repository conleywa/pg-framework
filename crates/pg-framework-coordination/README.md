# pg-framework-coordination

`pg-framework-coordination` 使用 PostgreSQL 实现接近 ZooKeeper 的轻量协调能力：

- 规范化绝对路径和层级节点。
- 持久节点，只有显式删除才消失。
- 与会话 lease 绑定的临时节点，服务崩溃或失联后自动失效。
- 持久顺序节点和临时顺序节点。
- 基于节点版本的 compare-and-set。
- 同时监听目标节点和直接子节点的 watch。

它面向服务成员列表、leader election、工作分片和配置分发等低频协调场景。目标不是
兼容 ZooKeeper 协议，也不提供 ZooKeeper 的 ensemble、事务日志和 session 语义全部细节。

## 核心语义

```text
CoordinationSession
  = (namespace, session_id, owner_token, DB lease_expires_at)

EphemeralNode
  = 会话有效时可见；会话关闭或 lease 过期后不可见

WatchSnapshot
  = (目标节点, 直接子节点名称, revision, stale)
```

- 根路径 `/` 始终存在，不能删除。
- 临时节点不能拥有子节点，子树应挂载在持久节点下。
- 节点 `version` 从 0 开始，每次 `set_data` 成功后加 1。
- `set_data(..., Some(expected))` 和 `delete(..., Some(expected))` 提供乐观并发控制。
- 临时节点是否可见只由 PostgreSQL 时钟下的会话 lease 决定；后台清理不参与正确性判断。
- 会话显式 `shutdown` 会立即删除会话和全部临时节点；drop 会话只停止心跳，等待 lease 到期。

## 路径

`NodePath` 在进入数据库前完成校验：

- 必须是以 `/` 开头的绝对路径。
- 根路径为 `/`。
- 非根路径不能以 `/` 结尾，不能包含 `//`、空段、`.` 或 `..`。
- 默认最长 1024 字节，SQL 硬上限 4096 字节。
- 拒绝 ASCII 控制字符。

顺序节点的“路径”参数是完整前缀，而不是父节点。例如前缀
`/services/orders/worker-` 第一次创建的实际路径是
`/services/orders/worker-0000000001`。每个 namespace 和父路径维护独立递增计数器。

## 存储模型

```text
pgfw_coordination_session
  namespace, session_id, owner_token, version,
  lease_expires_at, created_at, updated_at

pgfw_coordination_node
  namespace, path, parent_path, name, data, version,
  node_type, session_id, created_at, updated_at

pgfw_coordination_sequence
  namespace, parent_path, next_sequence
```

节点表使用普通日志表，随 PostgreSQL WAL 恢复。`session_id` 外键在删除过期会话时
级联删除其临时节点；持久节点的 `session_id` 为 `NULL`。数据库约束还拒绝把临时节点
作为父节点。

数据默认和硬上限均为 1 MiB，与常见 ZooKeeper 节点值限制一致。路径和数据长度限制
避免单个协调请求无限扩大 WAL、连接和客户端内存占用。

`pgfw_coordination_session_readable` 和 `pgfw_coordination_node_readable` 用于受控排障。
前者不暴露 owner token；后者的文本数据仅用于查看，业务读取仍使用 `bytea`。

## 会话与临时节点

```rust
use pg_framework_coordination::{
    CoordinationClient, CoordinationConfig, NodePath, build_default_pool,
};

let pool = build_default_pool(pg_config)?;
let client = CoordinationClient::connect(
    pool,
    CoordinationConfig::new("project:prod:services"),
).await?;

let services = NodePath::parse("/services/orders")?;
client.ensure_path(&services, Vec::new()).await?;

let session = client.start_session(None).await?;
let member = NodePath::parse("/services/orders/worker-a")?;
session
    .create_ephemeral(&member, br#"{"endpoint":"10.0.0.8:9000"}"#.to_vec())
    .await?;

// 正常退出时立即撤销成员身份。
session.shutdown().await?;
```

后台心跳按配置间隔延长数据库 lease。心跳连接短暂失败时会进入 `Uncertain`，但仍允许
在本地安全 deadline 内执行操作；一旦数据库确认会话行不存在或 lease 已过期，会话永久
进入 `Lost`，不能由后续心跳复活。

如果进程崩溃，PostgreSQL 会话行仍保留到 `lease_expires_at`。所有节点读取都会 JOIN
会话表并过滤 `lease_expires_at <= clock_timestamp()` 的临时节点，因此清理任务滞后
不会让失效节点重新可见。

## 顺序节点与选举

```rust
let election_prefix = NodePath::parse("/services/orders/leader-")?;
let candidate = session
    .create_ephemeral_sequential(&election_prefix, b"node-a".to_vec())
    .await?;

// 所有候选项按名称排序，最小者可以作为当前 leader。
let children = client.children(
    &NodePath::parse("/services/orders")?,
).await?;
```

顺序号持久保存在 `pgfw_coordination_sequence`，删除父路径不会自动重置。空洞可以接受，
调用方不应把顺序号解释为当前节点数量。每个父路径最多分配
`9_999_999_999`，固定十位宽度保证字符串排序与分配顺序一致。

临时节点本身不能作为锁。leader election 仍可能出现旧 leader 在停顿后恢复执行的情况；
如果要保护会修改共享资源的 leader，应结合 `pg-framework-lock` 的 fencing token，或让
目标资源原子校验一个单调 token。

## Watch

```rust
let mut watch = client
    .watch(&services, pg_config.clone(), None)
    .await?;

let snapshot = watch.latest();
for child in snapshot.children.iter() {
    println!("{child}");
}

watch.changed().await?;
```

`watch` 在返回前完成 `LISTEN` 和第一次快照读取，返回失败不会留下“已经启动但尚未订阅”
的假象。快照包括目标节点本身和按名称排序的直接子节点。

每次节点变更在同一事务中发送 namespace 级 NOTIFY。payload 是固定 namespace 哈希，
不包含路径、数据、会话 ID 或 owner token。watch 收到本 namespace 的事件后重新读取
目标路径，并通过周期刷新发现没有显式通知的临时节点过期。

NOTIFY 不是持久事件流。连接断开后组件会重新 LISTEN 并读取完整快照；`stale = true`
表示最近一次刷新失败并保留了上一份快照。调用方必须检查该标志，不能把 stale 快照
当作当前拓扑。

## 失败策略

| 场景 | 行为 |
| --- | --- |
| 服务进程崩溃 | 心跳停止，数据库 lease 到期后临时节点不可见 |
| 会话网络中断但未超过 deadline | 状态为 `Uncertain`，数据库操作仍会校验 lease |
| 会话 lease 过期 | 永久 `Lost`；临时节点失效，后续心跳不能复活 |
| 显式关闭会话 | 停止心跳并删除 session 行，外键级联删除全部临时节点 |
| watch 首次启动失败 | 不返回 watch，避免调用方误以为已经订阅 |
| LISTEN 连接断开 | 标记 stale，退避后重新 LISTEN 并读取完整快照 |
| NOTIFY 丢失或后端重启 | 周期刷新兜底；事件流不作为完整历史 |
| 节点 CAS 冲突 | 返回期望版本和数据库实际版本 |
| 写请求响应丢失或超时 | 可能已经提交；按路径读取版本/存在性归并，不能直接假定未生效 |
| 父节点已删除 | 返回 `NodeMissing`，不会自动伪造层级 |
| 临时节点有子节点 | 数据库和客户端同时拒绝 |
| PostgreSQL 不可用 | 返回存储错误，不使用进程内目录树替代 |

## 运维

- 多实例必须使用相同 namespace 和 schema generation。
- 会话心跳是普通连接池查询，不需要为每个临时节点单独心跳。
- 定期运行 `cleanup_expired` 回收过期会话，节点通过外键级联删除。
- 临时节点的最大实际数量受“活动会话数 × 每会话节点数”约束，业务仍应限制成员数量。
- watch 的 LISTEN 连接必须使用直连或 session pooling，不能使用 transaction pooling。
- 节点表使用普通日志表，备份恢复后应重新校验 schema 和协调状态。
- namespace 级通知在高频变更下会让同 namespace 的 watch 刷新更频繁；大量独立服务应拆成
  合理 namespace。

真实数据库行为测试位于 `pg-framework-integration-tests`，覆盖持久路径、临时节点、
会话关闭、lease 过期、CAS、顺序节点、可读视图和 watch 更新。
