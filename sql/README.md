# SQL migrations

按文件名顺序执行：

```text
000_meta.sql
001_cache.sql
002_lock.sql
003_ratelimit.sql
```

脚本默认创建 64 个 cache/rate-limit 物理分区。`schema generation` 和
`shard_count` 会写入 `pgfw_schema_meta`；修改分片数必须创建并验证新的
schema generation，不能直接修改已部署的 hash 映射。

缓存表使用 `UNLOGGED`；限频和锁表使用普通日志表。锁的 fencing sequence
必须与锁表一起迁移、备份和恢复，并禁止循环。

`pgfw_cache_namespace` 与缓存表共用恢复边界，记录 namespace `clearing` 状态和
epoch。清理流程与普通缓存写入都通过该行协调，写入事务使用 `FOR SHARE`，
清理事务使用 `UPDATE` 建立屏障。

三张业务表都把 `key_hash` 作为稳定标识和分区 key；`raw_key` 可为空，是否写入由
组件配置 `store_raw_keys` 决定。数据库对原始 key 设置 1024 字节硬上限，运行时
默认限制为 512 字节。开启原始 key 存储前必须评估备份、权限和敏感数据保留策略。

真实 PostgreSQL smoke test：

```bash
PGFRAMEWORK_TEST_DATABASE_URL='postgresql://...' \
  cargo test -p pg-framework-integration-tests \
  --test postgres_smoke -- --ignored
```

测试会执行上述迁移脚本，因此必须使用专用测试数据库。
