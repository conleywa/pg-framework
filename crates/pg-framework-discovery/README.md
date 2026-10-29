# pg-framework-discovery

`pg-framework-discovery` 是基于 PostgreSQL 的多实例服务注册与发现组件。服务实例通过短租约注册，后台心跳续约，发现查询只返回数据库时钟下仍有效的实例。

## 设计目标

- 不依赖 Redis、Consul、etcd 或独立注册中心。
- 同一个 namespace 和 service 可以有多个健康实例。
- 实例 crash、网络隔离或心跳停止后，租约过期即从发现结果消失。
- `service_hash` 做稳定 hash 分区，发现查询裁剪到单分区。
- 网络恢复后，同一注册句柄使用原 registration ID 和 owner token 重新注册。
- 发现结果不读本地陈旧快照；PostgreSQL 不可用时默认返回错误。
- endpoint、metadata、版本和剩余 lease 具备明确上限和结构化输出。

## 非目标

- 不提供 Kubernetes Service、DNS 或负载均衡器替代。
- 不提供跨服务事务、分布式配置中心或消息总线。
- 不承诺实例在 lease TTL 内一定在线；进程可能在心跳后立即崩溃。
- 不提供基于业务 service 名称推断 owner 的重入语义。
- 不把注册表当作强一致服务拓扑事实；发现结果是租约视角下的健康实例集合。

## 数据模型

注册表使用普通日志表。主备切换后仍保留最近 committed 注册行，由租约和心跳纠正旧实例。

```sql
CREATE TABLE pgfw_service_instance (
    namespace         text        NOT NULL,
    service_name      text,
    service_hash      bytea       NOT NULL CHECK (octet_length(service_hash) = 32),
    registration_id   bytea       NOT NULL CHECK (octet_length(registration_id) = 16),
    owner_token       bytea       NOT NULL CHECK (octet_length(owner_token) = 32),
    endpoint          text        NOT NULL,
    metadata          bytea       NOT NULL,
    version           bigint      NOT NULL CHECK (version > 0),
    registered_at     timestamptz NOT NULL,
    lease_expires_at  timestamptz NOT NULL,
    updated_at        timestamptz NOT NULL,
    PRIMARY KEY (service_hash, registration_id)
) PARTITION BY HASH (service_hash);
```

- `service_hash = HMAC 或 SHA-256(namespace || service)`。
- `service_name` 保存注册时的原始 service 文本，便于数据库排障；迁移前遗留行可以为空。
- `registration_id` 是每次注册句柄的随机 128 位实例标识。
- `owner_token` 是随机 256 位续约凭证，不对外公开，也不写入日志。
- `endpoint` 默认最大 512 字节，SQL 硬上限 2048 字节。
- `metadata` 默认最大 4096 字节，SQL 硬上限 8192 字节。
- `version` 表示注册内容版本；心跳只延长 lease，不递增 version，重新注册或内容更新时递增。
- 所有时间使用 PostgreSQL `clock_timestamp()`。

## 分片

- 同一个 service 的所有实例位于同一个物理分区，发现查询按 `service_hash` 裁剪。
- 不同 service 通过稳定 hash 分散到最多 64 个分区。
- 分区数属于 schema generation；修改分区数必须创建新 generation。
- 清理按主键 `(service_hash, registration_id)` 分批，不能只依赖分区内唯一的 `ctid`。
- 大流量服务仍受单分区和单服务集合规模约束，应监控实例数量和分区大小。

### 数据库可读视图

`pgfw_service_instance_readable` 显示原始 service name、十六进制 service hash 和
registration ID、endpoint、可读 metadata 及 lease 时间。视图不包含 `owner_token`，
也不能用于续约或注销判断。该视图用于受控排障；正常请求仍应通过 `discover()` 并携带
`namespace + service_hash` 以裁剪到单个物理分区。

## 注册

`ServiceRegistry::register` 接收 service、endpoint、metadata 和可选 lease TTL：

1. 校验 service、endpoint 和 metadata 上限。
2. 生成 registration ID 和 owner token，同时保存原始 service name 供数据库排障。
3. 提交初始注册行。
4. 启动后台心跳任务。
5. 只有初始注册 commit 成功后才返回 `ServiceRegistration`。

初始 SQL 使用 `INSERT ... ON CONFLICT`：

- 新 registration ID 创建 version 1。
- 同一 registration ID 且 owner token 相同可以更新注册内容。
- 已过期行可以由同一注册句柄重新接管。
- 同一 registration ID 被其他有效 owner 占用时返回 `RegistrationConflict`。

## 心跳

- 心跳间隔默认 10 秒，lease TTL 默认 30 秒。
- 配置必须保证 `heartbeat_interval < lease_ttl`。
- 心跳只更新 `lease_expires_at` 和 `updated_at`，不创建新 registration ID，也不增加 fencing token。
- 心跳 SQL 同时匹配 service hash、registration ID、owner token 和未过期条件。
- 返回空行表示注册行已过期或被替换；后台任务使用同一 ID/token 尝试重新注册。
- 网络失败时状态变为 `Uncertain`；本地 lease deadline 到期后变为 `Lost`。
- 数据库恢复后仍可重新注册，但网络故障期间发现结果可能已经暂时看不到该实例。
- 后台使用有界指数退避，不无限紧密重试。

状态：

```text
Uncertain -> Active
     |          |
     v          v
   Lost <------+
```

主动 `shutdown` 后状态为 `Closed`。

## 发现

```sql
SELECT registration_id, endpoint, metadata, version, remaining_lease
FROM pgfw_service_instance
WHERE namespace = $1
  AND service_hash = $2
  AND lease_expires_at > clock_timestamp()
ORDER BY updated_at DESC, registration_id
LIMIT $3;
```

- 发现每次访问 PostgreSQL，不维护本地陈旧服务快照。
- 过期实例不会返回；清理任务只负责释放存储。
- 默认最大返回 10,000 条，业务应设置更小的合理 limit。
- 结果不保证公平或轮询顺序。
- 调用方负责客户端负载均衡、健康检查重试和熔断。
- PostgreSQL 不可用时返回存储错误，不伪造空列表；空列表只表示没有有效注册实例。

### 动态感知新实例

单次 `discover()` 不会主动推送后续加入的实例。需要持续感知时使用
`ServiceRegistry::watch()`：

1. 使用专用 PostgreSQL 会话执行 `LISTEN`。
2. LISTEN 成功后执行一次 `discover()`，形成初始快照。
3. 注册、重新注册或注销事务提交时发送 `pg_notify`；payload 只包含 64 字符
   `service_hash` 十六进制摘要，不包含 endpoint、metadata 或批量实例列表。
4. watch 收到目标 service 的事件后重新查询数据库，并通过 Tokio `watch` channel
   广播新的 `ServiceSnapshot`。
5. 周期刷新按 `refresh_interval` 执行，用于发现 lease 自然过期，因为实例
   崩溃不会产生注销通知。
6. PostgreSQL 查询失败时保留上一次成功快照并标记 `stale = true`；连接恢复后
   重新 LISTEN 并刷新。

```rust
let mut watch = registry
    .watch(
        "orders",
        pg_config,
        100,
        Some(Duration::from_secs(5)),
    )
    .await?;

loop {
    let snapshot = watch.latest();
    // 根据 snapshot.instances 更新客户端连接池
    watch.changed().await?;
}
```

`watch()` 只有在 LISTEN 和初始快照都成功后才返回。drop 会停止后台监听；
优雅关闭使用 `shutdown()`。

## 网络重连

- registration 句柄保存固定的 registration ID、owner token、service name、service hash、endpoint 和 metadata。
- 连接池恢复后心跳先尝试延长原 lease；原行不存在时再使用同一身份注册。
- 如果 PostgreSQL 主备切换保留了注册行，心跳可以继续。
- 如果切换或网络中断超过 lease TTL，发现端已经看不到该实例；恢复后的重新注册会创建新 version。
- 服务发现的默认 schema generation 为 `v2`，其中包含可读 `service_name` 列；升级已有数据库时必须执行 `005_readable_views.sql`。
- `ServiceRegistry::connect` 校验 schema generation 和物理分区数；长生命周期进程确认 primary 或连接池重建后应再次调用 `verify_schema`。

## Rust API

```rust
use std::time::Duration;
use pg_framework_discovery::{
    build_default_pool, RegistryConfig, ServiceRegistry,
};

let config = RegistryConfig::new("project:prod:cluster");
let registry = ServiceRegistry::connect(
    build_default_pool(pg_config)?,
    config,
).await?;

let registration = registry
    .register(
        "orders",
        "http://10.0.0.12:8080",
        b"zone=cn-east-1".to_vec(),
        Some(Duration::from_secs(30)),
    )
    .await?;

let instances = registry.discover("orders", 100).await?;

registration.shutdown().await?;
```

## 连接池和超时

- 默认使用 `DEFAULT_POOL_MAX_SIZE = 8`。
- 默认池包含 connect、wait、create、recycle、statement 和 lock timeout。
- `RegistryConfig.operation_timeout` 限制注册、心跳、发现和清理的单次操作。
- `RegistryConfig.shutdown_timeout` 限制后台 heartbeat join。
- 心跳任务使用独立 Tokio task；drop 只 abort，不执行数据库 I/O。

## 可观测性

建议指标：

- 注册成功、冲突和失败次数。
- 心跳成功、失败、重新注册次数和延迟。
- 注册状态为 Active/Uncertain/Lost 的实例数。
- 每次发现返回实例数、查询延迟和 limit 命中次数。
- 过期行数量、清理批次和每分区物理大小。
- 注册 version 分布。

不得记录 owner token、原始敏感 service 或完整 metadata。

## 验收场景

- 多个实例注册同一 service 后，发现返回全部未过期实例。
- 一个实例停止心跳并超过 lease 后从发现结果消失。
- 同一 registration ID 使用错误 owner token 不能续约或删除注册行。
- PostgreSQL 网络中断恢复后，心跳在 lease 到期前恢复 Active。
- 网络中断超过 lease 后，发现端先丢失实例，恢复后重新注册。
- DROP 未显式 shutdown 的句柄不会执行阻塞数据库 I/O，租约最终过期。
- 发现结果按 `service_hash` 裁剪到单分区。
- 清理只删除主键匹配的过期行，不影响其他服务或未过期实例。
- schema generation 或分区数不匹配时 `connect` fail-closed。
