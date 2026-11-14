# SQL migrations

按文件名顺序执行：

```text
000_meta.sql
001_cache.sql
002_lock.sql
003_ratelimit.sql
004_discovery.sql
005_readable_views.sql
006_coordination.sql
```

脚本默认创建 64 个 cache/rate-limit/discovery 物理分区。`schema generation` 和
`shard_count` 会写入 `pgfw_schema_meta`；修改分片数必须创建并验证新的
schema generation，不能直接修改已部署的 hash 映射。

缓存表使用 `UNLOGGED`；限频、锁、服务发现、协调会话和协调节点表使用普通日志表。
锁的 fencing sequence 必须与锁表一起迁移、备份和恢复，并禁止循环。

`pgfw_cache_namespace` 与缓存表共用恢复边界，记录 namespace `clearing` 状态和
epoch。清理流程与普通缓存写入都通过该行协调，写入事务使用 `FOR SHARE`，
清理事务使用 `UPDATE` 建立屏障。

三张业务表都把 `key_hash` 作为稳定标识和分区 key；`raw_key` 可为空，是否写入由
组件配置 `store_raw_keys` 决定。数据库对原始 key 设置 1024 字节硬上限，运行时
默认限制为 512 字节。开启原始 key 存储前必须评估备份、权限和敏感数据保留策略。

服务发现表使用 `service_hash` 分区，保存原始 service name、registration ID、
owner token、endpoint、metadata、注册 version 和 lease deadline。发现查询只返回
数据库时钟下未过期实例。

`006_coordination.sql` 增加协调会话、层级节点和顺序计数表。节点路径受规范绝对路径
约束，临时节点通过 `(namespace, session_id)` 外键绑定会话并使用 `ON DELETE CASCADE`；
删除过期会话会回收其临时节点。节点读取仍必须 JOIN 会话并过滤数据库时钟下已过期的
lease，不能仅依赖清理任务。临时节点不能作为父节点，根节点不能删除。

`005_readable_views.sql` 和 `006_coordination.sql` 共增加六个 `pgfw_*_readable` 视图。
底表继续使用 `bytea`
保存哈希、二进制 value/metadata 和 token，视图只负责展示：

- `key_hash`、`service_hash`、`registration_id`、`session_id` 显示为十六进制文本。
- 合法 UTF-8 的 key、value、metadata、节点 data 显示为文本。
- 非法 UTF-8 二进制使用 PostgreSQL escape 编码显示。
- `owner_token` 不进入视图。

可读视图会扫描父表；查询时必须自行设置 `LIMIT` 或主键条件。它们用于受控排障和
管理查询，不能作为业务请求热路径。启用 `store_raw_keys` 后才能在视图中看到原始 key。
视图权限必须只授予受控运维角色，不能把服务发现、协调节点和含原始 key 的视图授权给
`PUBLIC`。

真实 PostgreSQL smoke test：

```bash
PGFRAMEWORK_TEST_DATABASE_URL='postgresql://...' \
  cargo test -p pg-framework-integration-tests \
  --test postgres_smoke -- --ignored
```

测试会执行上述迁移脚本，因此必须使用专用测试数据库。
