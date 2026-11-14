use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use deadpool_postgres::{Pool, Transaction};
use pg_framework_core::KeyId;
use tokio_postgres::IsolationLevel;

use crate::config::CoordinationConfig;
use crate::error::CoordinationError;
use crate::path::NodePath;
use crate::session::CoordinationSession;
use crate::types::{CoordinationNode, CoordinationSnapshot, CreateMode, NodeKind, NodeVersion};

const SELECT_NODE: &str = "
    SELECT
        node.path,
        node.data,
        node.version,
        node.node_type,
        node.created_at,
        node.updated_at
    FROM pgfw_coordination_node AS node
    LEFT JOIN pgfw_coordination_session AS session
      ON session.namespace = node.namespace
     AND session.session_id = node.session_id
    WHERE node.namespace = $1
      AND node.path = $2
      AND (
          node.node_type = 'persistent'
          OR session.lease_expires_at > clock_timestamp()
      )";

pub(crate) struct CoordinationInner {
    pub(crate) pool: Pool,
    pub(crate) config: CoordinationConfig,
    notification_id: String,
}

#[derive(Clone, Copy)]
pub(crate) struct SessionCredentials<'a> {
    pub(crate) session_id: &'a [u8; 16],
    pub(crate) owner_token: &'a [u8; 32],
}

/// 基于 PostgreSQL 表、会话 lease 和通知的协调节点客户端。
#[derive(Clone)]
pub struct CoordinationClient {
    pub(crate) inner: Arc<CoordinationInner>,
}

impl CoordinationClient {
    /// 基于共享 PostgreSQL 连接池创建客户端。
    ///
    /// # 错误
    ///
    /// 配置非法时返回错误。
    pub fn new(pool: Pool, config: CoordinationConfig) -> Result<Self, CoordinationError> {
        config.validate()?;
        let notification_id =
            KeyId::derive("pgfw_coordination_notify_v1", config.namespace.as_bytes()).to_hex();
        Ok(Self {
            inner: Arc::new(CoordinationInner {
                pool,
                config,
                notification_id,
            }),
        })
    }

    /// 创建客户端并校验 schema。
    ///
    /// # 错误
    ///
    /// 配置非法、schema 缺失或不匹配时返回错误。
    pub async fn connect(
        pool: Pool,
        config: CoordinationConfig,
    ) -> Result<Self, CoordinationError> {
        let client = Self::new(pool, config)?;
        client.verify_schema().await?;
        Ok(client)
    }

    /// 校验协调表、顺序计数表和 schema generation。
    ///
    /// # 错误
    ///
    /// schema 缺失或 generation 不匹配时返回错误。
    pub async fn verify_schema(&self) -> Result<(), CoordinationError> {
        self.timed("schema", async {
            pg_framework_core::verify_schema(
                &self.inner.pool,
                "coordination",
                &self.inner.config.schema_generation,
                0,
            )
            .await?;
            let client = self.inner.pool.get().await?;
            let row = client
                .query_one(
                    "SELECT
                        to_regclass('pgfw_coordination_node') IS NOT NULL,
                        to_regclass('pgfw_coordination_session') IS NOT NULL,
                        to_regclass('pgfw_coordination_sequence') IS NOT NULL",
                    &[],
                )
                .await?;
            if !row.get::<_, bool>(0) || !row.get::<_, bool>(1) || !row.get::<_, bool>(2) {
                return Err(CoordinationError::InvalidConfig(
                    "coordination tables are incomplete",
                ));
            }
            Ok(())
        })
        .await
    }

    /// 创建一个带后台心跳的协调会话。
    ///
    /// `lease_ttl` 为 `None` 时使用 [`CoordinationConfig::lease_ttl`]。
    ///
    /// # 错误
    ///
    /// lease 非法、随机数生成失败或 PostgreSQL 失败时返回错误。
    pub async fn start_session(
        &self,
        lease_ttl: Option<Duration>,
    ) -> Result<CoordinationSession, CoordinationError> {
        CoordinationSession::start(self.clone(), lease_ttl).await
    }

    /// 创建普通持久节点。父节点必须已经存在。
    ///
    /// # 错误
    ///
    /// 路径或数据非法、父节点不存在、父节点为临时节点或路径已存在时返回错误。
    pub async fn create(
        &self,
        path: &NodePath,
        data: impl Into<Vec<u8>>,
    ) -> Result<CoordinationNode, CoordinationError> {
        self.create_internal(path, data.into(), CreateMode::Persistent, None)
            .await
    }

    /// 在指定父路径下创建顺序持久节点。
    ///
    /// `prefix` 是完整非根路径前缀，例如 `/jobs/worker-`。返回值中的实际
    /// 路径带有零填充的十进制后缀。
    ///
    /// # 错误
    ///
    /// 前缀或数据非法、父节点不存在或 PostgreSQL 失败时返回错误。
    pub async fn create_sequential(
        &self,
        prefix: &NodePath,
        data: impl Into<Vec<u8>>,
    ) -> Result<CoordinationNode, CoordinationError> {
        self.create_internal(prefix, data.into(), CreateMode::PersistentSequential, None)
            .await
    }

    /// 幂等创建永久路径及其所有缺失的持久祖先节点。
    ///
    /// 已存在的最终持久节点不会被覆盖；任一已存在的路径段是临时节点时返回错误。
    ///
    /// # 错误
    ///
    /// 路径或数据非法、已有路径段为临时节点或 PostgreSQL 失败时返回错误。
    pub async fn ensure_path(
        &self,
        path: &NodePath,
        final_data: impl Into<Vec<u8>>,
    ) -> Result<CoordinationNode, CoordinationError> {
        self.validate_path(path)?;
        let final_data = final_data.into();
        self.validate_data(&final_data)?;
        self.timed("ensure path", self.execute_ensure_path(path, final_data))
            .await
    }

    /// 读取节点；临时会话失效后对应临时节点返回 `None`。
    ///
    /// # 错误
    ///
    /// 路径非法或 PostgreSQL 失败时返回错误。
    pub async fn get(
        &self,
        path: &NodePath,
    ) -> Result<Option<CoordinationNode>, CoordinationError> {
        self.validate_path(path)?;
        self.timed("get", async {
            let client = self.inner.pool.get().await?;
            let row = client
                .query_opt(SELECT_NODE, &[&self.inner.config.namespace, &path.as_str()])
                .await?;
            Ok(row
                .as_ref()
                .map(row_to_node)
                .transpose()?
                .or_else(|| path.is_root().then(|| synthetic_root(path.clone()))))
        })
        .await
    }

    /// 判断节点当前是否存在。
    ///
    /// # 错误
    ///
    /// 路径非法或 PostgreSQL 失败时返回错误。
    pub async fn exists(&self, path: &NodePath) -> Result<bool, CoordinationError> {
        Ok(self.get(path).await?.is_some())
    }

    /// 按名称排序列出直接子节点。
    ///
    /// # 错误
    ///
    /// 父节点不存在、结果超过配置上限或 PostgreSQL 失败时返回错误。
    pub async fn children(&self, path: &NodePath) -> Result<Vec<String>, CoordinationError> {
        self.validate_path(path)?;
        self.timed("children", async {
            let (node, children) = self.read_snapshot(path).await?;
            if node.is_none() {
                return Err(CoordinationError::NodeMissing);
            }
            Ok(children.to_vec())
        })
        .await
    }

    /// 使用可选版本条件更新节点数据。
    ///
    /// # 错误
    ///
    /// 路径或数据非法、节点不存在、版本冲突或 PostgreSQL 失败时返回错误。
    pub async fn set_data(
        &self,
        path: &NodePath,
        data: impl Into<Vec<u8>>,
        expected_version: Option<NodeVersion>,
    ) -> Result<CoordinationNode, CoordinationError> {
        self.validate_path(path)?;
        let data = data.into();
        self.validate_data(&data)?;
        self.timed(
            "set data",
            self.execute_set_data(path, data, expected_version),
        )
        .await
    }

    /// 删除一个没有子节点的节点。
    ///
    /// 删除临时节点后，同一会话仍然有效，但该节点不会重新出现。
    ///
    /// # 错误
    ///
    /// 根节点、节点不存在、仍有子节点、版本冲突或 PostgreSQL 失败时返回错误。
    pub async fn delete(
        &self,
        path: &NodePath,
        expected_version: Option<NodeVersion>,
    ) -> Result<(), CoordinationError> {
        self.validate_path(path)?;
        if path.is_root() {
            return Err(CoordinationError::RootCannotBeDeleted);
        }
        self.timed("delete", self.execute_delete(path, expected_version))
            .await
    }

    /// 物理清理过期会话及其临时节点。
    ///
    /// 有效性不依赖该任务：读取查询本身会过滤过期会话。返回值是清理的会话数。
    ///
    /// # 错误
    ///
    /// batch size 为 0 或 PostgreSQL 失败时返回错误。
    pub async fn cleanup_expired(&self, batch_size: usize) -> Result<u64, CoordinationError> {
        if batch_size == 0 {
            return Err(CoordinationError::InvalidConfig(
                "cleanup batch size must be positive",
            ));
        }
        let limit = i64::try_from(batch_size)
            .map_err(|_| CoordinationError::InvalidConfig("cleanup batch is too large"))?;
        let mut total = 0u64;
        loop {
            let deleted = self.timed("cleanup", self.cleanup_batch(limit)).await?;
            total = total.saturating_add(deleted);
            if deleted < u64::try_from(limit).unwrap_or(u64::MAX) {
                break;
            }
        }
        Ok(total)
    }

    pub(crate) fn validate_path(&self, path: &NodePath) -> Result<(), CoordinationError> {
        if path.byte_len() > self.inner.config.max_path_bytes {
            return Err(CoordinationError::PathTooLarge {
                max: self.inner.config.max_path_bytes,
            });
        }
        Ok(())
    }

    fn validate_data(&self, data: &[u8]) -> Result<(), CoordinationError> {
        if data.len() > self.inner.config.max_data_bytes {
            return Err(CoordinationError::DataTooLarge {
                max: self.inner.config.max_data_bytes,
            });
        }
        Ok(())
    }

    pub(crate) async fn create_internal(
        &self,
        path: &NodePath,
        data: Vec<u8>,
        mode: CreateMode,
        credentials: Option<SessionCredentials<'_>>,
    ) -> Result<CoordinationNode, CoordinationError> {
        self.validate_path(path)?;
        self.validate_data(&data)?;
        if mode.is_ephemeral() != credentials.is_some() {
            return Err(CoordinationError::InvalidConfig(
                "ephemeral creation requires a session and persistent creation does not",
            ));
        }
        self.timed("create", self.execute_create(path, data, mode, credentials))
            .await
    }

    pub(crate) async fn read_snapshot(
        &self,
        path: &NodePath,
    ) -> Result<(Option<CoordinationNode>, Arc<[String]>), CoordinationError> {
        let mut client = self.inner.pool.get().await?;
        let transaction = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;
        let node = transaction
            .query_opt(SELECT_NODE, &[&self.inner.config.namespace, &path.as_str()])
            .await?
            .as_ref()
            .map(row_to_node)
            .transpose()?
            .or_else(|| path.is_root().then(|| synthetic_root(path.clone())));

        let children = if node.is_some() {
            let limit = i64::try_from(self.inner.config.max_children)
                .map_err(|_| CoordinationError::InvalidConfig("children limit is too large"))?
                .saturating_add(1);
            let rows = transaction
                .query(
                    "SELECT child.name
                     FROM pgfw_coordination_node AS child
                     LEFT JOIN pgfw_coordination_session AS session
                       ON session.namespace = child.namespace
                      AND session.session_id = child.session_id
                     WHERE child.namespace = $1
                       AND child.parent_path = $2
                       AND (
                           child.node_type = 'persistent'
                           OR session.lease_expires_at > clock_timestamp()
                       )
                     ORDER BY child.name
                     LIMIT $3",
                    &[&self.inner.config.namespace, &path.as_str(), &limit],
                )
                .await?;
            if rows.len() > self.inner.config.max_children {
                return Err(CoordinationError::ChildrenLimitExceeded {
                    max: self.inner.config.max_children,
                });
            }
            rows.into_iter()
                .map(|row| row.try_get::<_, String>(0).map_err(CoordinationError::from))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };
        transaction.commit().await?;
        Ok((node, Arc::from(children)))
    }

    pub(crate) fn notification_id(&self) -> &str {
        &self.inner.notification_id
    }

    pub(crate) async fn timed<T, F>(
        &self,
        operation: &'static str,
        future: F,
    ) -> Result<T, CoordinationError>
    where
        F: Future<Output = Result<T, CoordinationError>>,
    {
        tokio::time::timeout(self.inner.config.operation_timeout, future)
            .await
            .map_err(|_| CoordinationError::Timeout(operation))?
    }

    async fn execute_create(
        &self,
        requested_path: &NodePath,
        data: Vec<u8>,
        mode: CreateMode,
        credentials: Option<SessionCredentials<'_>>,
    ) -> Result<CoordinationNode, CoordinationError> {
        let mut client = self.inner.pool.get().await?;
        let transaction = client.transaction().await?;
        ensure_root(&transaction, &self.inner.config.namespace).await?;

        if requested_path.is_root() {
            return Err(CoordinationError::NodeExists);
        }

        let parent = requested_path
            .parent()
            .ok_or(CoordinationError::InvalidPath("node has no parent"))?;
        self.purge_expired_at_parent(&transaction, &parent).await?;
        self.lock_parent(&transaction, &parent).await?;
        if let Some(credentials) = credentials {
            lock_active_session(&transaction, &self.inner.config.namespace, credentials).await?;
        }

        let path = if mode.is_sequential() {
            self.next_sequential_path(&transaction, requested_path)
                .await?
        } else {
            requested_path.clone()
        };
        self.validate_path(&path)?;
        let kind = mode.kind();
        let session_id = credentials.map(|credentials| credentials.session_id.as_slice());
        let row = transaction
            .query_opt(
                "INSERT INTO pgfw_coordination_node (
                    namespace,
                    path,
                    parent_path,
                    name,
                    data,
                    version,
                    node_type,
                    session_id,
                    created_at,
                    updated_at
                 )
                 VALUES (
                    $1,
                    $2,
                    $3,
                    $4,
                    $5,
                    0,
                    $6,
                    $7,
                    clock_timestamp(),
                    clock_timestamp()
                 )
                 ON CONFLICT (namespace, path) DO NOTHING
                 RETURNING
                    path,
                    data,
                    version,
                    node_type,
                    created_at,
                    updated_at",
                &[
                    &self.inner.config.namespace,
                    &path.as_str(),
                    &parent.as_str(),
                    &path.name(),
                    &data,
                    &kind_name(kind),
                    &session_id,
                ],
            )
            .await?;
        let Some(row) = row else {
            return Err(CoordinationError::NodeExists);
        };
        let node = row_to_node(&row)?;
        notify_namespace(
            &transaction,
            &self.inner.config.notification_channel,
            &self.inner.notification_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(node)
    }

    async fn execute_ensure_path(
        &self,
        path: &NodePath,
        final_data: Vec<u8>,
    ) -> Result<CoordinationNode, CoordinationError> {
        let mut client = self.inner.pool.get().await?;
        let transaction = client.transaction().await?;
        ensure_root(&transaction, &self.inner.config.namespace).await?;
        let mut changed = false;
        let mut current = NodePath::parse("/")
            .map_err(|_| CoordinationError::InvalidPath("root path is invalid"))?;

        if path.is_root() {
            transaction.commit().await?;
            return self.get(path).await?.ok_or(CoordinationError::NodeMissing);
        }

        for segment in path.as_str().trim_start_matches('/').split('/') {
            current = current
                .join(segment)
                .map_err(|_| CoordinationError::InvalidPath("path segment is invalid"))?;
            let data = if &current == path {
                final_data.as_slice()
            } else {
                &[]
            };
            let row = transaction
                .query_opt(
                    "INSERT INTO pgfw_coordination_node (
                        namespace,
                        path,
                        parent_path,
                        name,
                        data,
                        version,
                        node_type,
                        session_id,
                        created_at,
                        updated_at
                     )
                     VALUES (
                        $1,
                        $2,
                        $3,
                        $4,
                        $5,
                        0,
                        'persistent',
                        NULL,
                        clock_timestamp(),
                        clock_timestamp()
                     )
                     ON CONFLICT (namespace, path) DO NOTHING
                     RETURNING
                        path,
                        data,
                        version,
                        node_type,
                        created_at,
                        updated_at",
                    &[
                        &self.inner.config.namespace,
                        &current.as_str(),
                        &current
                            .parent()
                            .as_ref()
                            .map(NodePath::as_str)
                            .unwrap_or("/"),
                        &current.name(),
                        &data,
                    ],
                )
                .await?;
            if row.is_some() {
                changed = true;
                continue;
            }
            let existing_kind = transaction
                .query_one(
                    "SELECT node_type
                     FROM pgfw_coordination_node
                     WHERE namespace = $1
                       AND path = $2",
                    &[&self.inner.config.namespace, &current.as_str()],
                )
                .await?
                .get::<_, String>(0);
            if existing_kind != "persistent" {
                return Err(CoordinationError::EphemeralParent);
            }
        }

        if changed {
            notify_namespace(
                &transaction,
                &self.inner.config.notification_channel,
                &self.inner.notification_id,
            )
            .await?;
        }
        transaction.commit().await?;
        self.get(path).await?.ok_or(CoordinationError::NodeMissing)
    }

    async fn execute_set_data(
        &self,
        path: &NodePath,
        data: Vec<u8>,
        expected_version: Option<NodeVersion>,
    ) -> Result<CoordinationNode, CoordinationError> {
        let mut client = self.inner.pool.get().await?;
        let transaction = client.transaction().await?;
        if path.is_root() {
            ensure_root(&transaction, &self.inner.config.namespace).await?;
        }
        let current = transaction
            .query_opt(
                &format!("{SELECT_NODE} FOR UPDATE OF node"),
                &[&self.inner.config.namespace, &path.as_str()],
            )
            .await?
            .as_ref()
            .map(row_to_node)
            .transpose()?
            .ok_or(CoordinationError::NodeMissing)?;
        check_expected_version(&current, expected_version)?;
        let row = transaction
            .query_one(
                "UPDATE pgfw_coordination_node
                 SET data = $3,
                     version = version + 1,
                     updated_at = clock_timestamp()
                 WHERE namespace = $1
                   AND path = $2
                 RETURNING
                    path,
                    data,
                    version,
                    node_type,
                    created_at,
                    updated_at",
                &[&self.inner.config.namespace, &path.as_str(), &data],
            )
            .await?;
        let node = row_to_node(&row)?;
        notify_namespace(
            &transaction,
            &self.inner.config.notification_channel,
            &self.inner.notification_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(node)
    }

    async fn execute_delete(
        &self,
        path: &NodePath,
        expected_version: Option<NodeVersion>,
    ) -> Result<(), CoordinationError> {
        let mut client = self.inner.pool.get().await?;
        let transaction = client.transaction().await?;
        self.purge_expired_at_parent(&transaction, path).await?;
        let current = transaction
            .query_opt(
                &format!("{SELECT_NODE} FOR UPDATE OF node"),
                &[&self.inner.config.namespace, &path.as_str()],
            )
            .await?
            .as_ref()
            .map(row_to_node)
            .transpose()?
            .ok_or(CoordinationError::NodeMissing)?;
        check_expected_version(&current, expected_version)?;
        let child = transaction
            .query_opt(
                "SELECT 1
                 FROM pgfw_coordination_node
                 WHERE namespace = $1
                   AND parent_path = $2
                 LIMIT 1",
                &[&self.inner.config.namespace, &path.as_str()],
            )
            .await?;
        if child.is_some() {
            return Err(CoordinationError::NodeNotEmpty);
        }
        transaction
            .execute(
                "DELETE FROM pgfw_coordination_node
                 WHERE namespace = $1
                   AND path = $2",
                &[&self.inner.config.namespace, &path.as_str()],
            )
            .await?;
        notify_namespace(
            &transaction,
            &self.inner.config.notification_channel,
            &self.inner.notification_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn cleanup_batch(&self, limit: i64) -> Result<u64, CoordinationError> {
        let mut client = self.inner.pool.get().await?;
        let transaction = client.transaction().await?;
        let deleted = transaction
            .execute(
                "WITH expired AS (
                    SELECT session_id
                    FROM pgfw_coordination_session
                    WHERE namespace = $1
                      AND lease_expires_at <= clock_timestamp()
                    ORDER BY lease_expires_at
                    LIMIT $2
                    FOR UPDATE SKIP LOCKED
                 )
                 DELETE FROM pgfw_coordination_session AS session
                 USING expired
                 WHERE session.namespace = $1
                   AND session.session_id = expired.session_id",
                &[&self.inner.config.namespace, &limit],
            )
            .await?;
        if deleted > 0 {
            notify_namespace(
                &transaction,
                &self.inner.config.notification_channel,
                &self.inner.notification_id,
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(deleted)
    }

    async fn lock_parent(
        &self,
        transaction: &Transaction<'_>,
        parent: &NodePath,
    ) -> Result<(), CoordinationError> {
        let row = transaction
            .query_opt(
                "SELECT node.node_type
                 FROM pgfw_coordination_node AS node
                 LEFT JOIN pgfw_coordination_session AS session
                   ON session.namespace = node.namespace
                  AND session.session_id = node.session_id
                 WHERE node.namespace = $1
                   AND node.path = $2
                   AND (
                       node.node_type = 'persistent'
                       OR session.lease_expires_at > clock_timestamp()
                   )
                 FOR SHARE OF node",
                &[&self.inner.config.namespace, &parent.as_str()],
            )
            .await?;
        let Some(row) = row else {
            return Err(CoordinationError::NodeMissing);
        };
        if row.get::<_, String>(0) != "persistent" {
            return Err(CoordinationError::EphemeralParent);
        }
        Ok(())
    }

    async fn next_sequential_path(
        &self,
        transaction: &Transaction<'_>,
        prefix: &NodePath,
    ) -> Result<NodePath, CoordinationError> {
        if prefix.is_root() {
            return Err(CoordinationError::InvalidPath(
                "sequential create needs a non-root path prefix",
            ));
        }
        let parent = prefix.parent().ok_or(CoordinationError::InvalidPath(
            "sequential path has no parent",
        ))?;
        let sequence = transaction
            .query_opt(
                "INSERT INTO pgfw_coordination_sequence (
                    namespace,
                    parent_path,
                    next_sequence
                 )
                 VALUES ($1, $2, 1)
                 ON CONFLICT (namespace, parent_path)
                 DO UPDATE SET
                    next_sequence = pgfw_coordination_sequence.next_sequence + 1
                 WHERE pgfw_coordination_sequence.next_sequence < 9999999999
                 RETURNING next_sequence",
                &[&self.inner.config.namespace, &parent.as_str()],
            )
            .await?
            .ok_or(CoordinationError::SequenceExhausted)?
            .get::<_, i64>(0);
        NodePath::parse(format!("{}{sequence:010}", prefix.as_str()))
    }

    async fn purge_expired_at_parent(
        &self,
        transaction: &Transaction<'_>,
        parent: &NodePath,
    ) -> Result<(), CoordinationError> {
        transaction
            .execute(
                "DELETE FROM pgfw_coordination_session AS session
                 WHERE session.namespace = $1
                   AND session.lease_expires_at <= clock_timestamp()
                   AND EXISTS (
                       SELECT 1
                       FROM pgfw_coordination_node AS node
                       WHERE node.namespace = session.namespace
                         AND node.session_id = session.session_id
                         AND (
                             node.path = $2
                             OR node.parent_path = $2
                         )
                   )",
                &[&self.inner.config.namespace, &parent.as_str()],
            )
            .await?;
        Ok(())
    }
}

async fn ensure_root(
    transaction: &Transaction<'_>,
    namespace: &str,
) -> Result<(), CoordinationError> {
    transaction
        .execute(
            "INSERT INTO pgfw_coordination_node (
                namespace,
                path,
                parent_path,
                name,
                data,
                version,
                node_type,
                session_id,
                created_at,
                updated_at
             )
             VALUES (
                $1,
                '/',
                NULL,
                '',
                ''::bytea,
                0,
                'persistent',
                NULL,
                clock_timestamp(),
                clock_timestamp()
             )
             ON CONFLICT (namespace, path) DO NOTHING",
            &[&namespace],
        )
        .await?;
    Ok(())
}

async fn lock_active_session(
    transaction: &Transaction<'_>,
    namespace: &str,
    credentials: SessionCredentials<'_>,
) -> Result<(), CoordinationError> {
    let row = transaction
        .query_opt(
            "SELECT 1
             FROM pgfw_coordination_session
             WHERE namespace = $1
               AND session_id = $2
               AND owner_token = $3
               AND lease_expires_at > clock_timestamp()
             FOR SHARE",
            &[
                &namespace,
                &credentials.session_id.as_slice(),
                &credentials.owner_token.as_slice(),
            ],
        )
        .await?;
    if row.is_none() {
        return Err(CoordinationError::SessionLost);
    }
    Ok(())
}

pub(crate) async fn notify_namespace(
    transaction: &Transaction<'_>,
    channel: &str,
    notification_id: &str,
) -> Result<(), CoordinationError> {
    transaction
        .query_one("SELECT pg_notify($1, $2)", &[&channel, &notification_id])
        .await?;
    Ok(())
}

fn row_to_node(row: &tokio_postgres::Row) -> Result<CoordinationNode, CoordinationError> {
    let node_type = row.try_get::<_, String>(3)?;
    let kind = match node_type.as_str() {
        "persistent" => NodeKind::Persistent,
        "ephemeral" => NodeKind::Ephemeral,
        _ => {
            return Err(CoordinationError::InvalidConfig(
                "database contains an unknown coordination node type",
            ));
        }
    };
    let path = NodePath::parse(row.try_get::<_, String>(0)?)?;
    let data: Vec<u8> = row.try_get(1)?;
    Ok(CoordinationNode {
        path,
        data: Arc::from(data),
        version: NodeVersion::new(row.try_get(2)?),
        kind,
        created_at: row.try_get(4)?,
        updated_at: row.try_get(5)?,
    })
}

fn synthetic_root(path: NodePath) -> CoordinationNode {
    CoordinationNode {
        path,
        data: Arc::from(Vec::<u8>::new()),
        version: NodeVersion::new(0),
        kind: NodeKind::Persistent,
        created_at: UNIX_EPOCH,
        updated_at: UNIX_EPOCH,
    }
}

fn check_expected_version(
    node: &CoordinationNode,
    expected: Option<NodeVersion>,
) -> Result<(), CoordinationError> {
    if let Some(expected) = expected
        && expected != node.version
    {
        return Err(CoordinationError::VersionConflict {
            expected,
            actual: node.version,
        });
    }
    Ok(())
}

pub(crate) fn kind_name(kind: NodeKind) -> String {
    match kind {
        NodeKind::Persistent => "persistent",
        NodeKind::Ephemeral => "ephemeral",
    }
    .to_owned()
}

pub(crate) fn empty_snapshot(path: NodePath) -> CoordinationSnapshot {
    CoordinationSnapshot {
        path,
        node: None,
        children: Arc::from(Vec::<String>::new()),
        revision: 0,
        stale: true,
    }
}
