use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::Pool;
use pg_framework_core::{KeyId, duration_millis, validate_key_size};

use crate::config::RegistryConfig;
use crate::error::DiscoveryError;
use crate::registration::{RegistrationSpec, ServiceRegistration};
use crate::types::{RegistrationId, ServiceInstance};

pub(crate) struct RegistryInner {
    pub(crate) pool: Pool,
    pub(crate) config: RegistryConfig,
}

/// 基于 PostgreSQL 的服务注册与发现客户端。
#[derive(Clone)]
pub struct ServiceRegistry {
    pub(crate) inner: Arc<RegistryInner>,
}

impl ServiceRegistry {
    /// 基于共享连接池创建客户端。
    ///
    /// # 错误
    ///
    /// 配置非法时返回错误。
    pub fn new(pool: Pool, config: RegistryConfig) -> Result<Self, DiscoveryError> {
        config.validate()?;
        Ok(Self {
            inner: Arc::new(RegistryInner { pool, config }),
        })
    }

    /// 创建客户端并校验数据库 schema。
    ///
    /// # 错误
    ///
    /// 配置非法、schema 缺失或不匹配时返回错误。
    pub async fn connect(pool: Pool, config: RegistryConfig) -> Result<Self, DiscoveryError> {
        let registry = Self::new(pool, config)?;
        registry.verify_schema().await?;
        Ok(registry)
    }

    /// 校验 schema generation 和物理分区数。
    ///
    /// # 错误
    ///
    /// schema 缺失或配置不匹配时返回错误。
    pub async fn verify_schema(&self) -> Result<(), DiscoveryError> {
        self.timed("schema", async {
            pg_framework_core::verify_schema(
                &self.inner.pool,
                "discovery",
                &self.inner.config.schema_generation,
                self.inner.config.physical_partition_count,
            )
            .await
            .map_err(DiscoveryError::from)
        })
        .await?;
        Ok(())
    }

    /// 注册一个服务实例并启动后台心跳。
    ///
    /// # 错误
    ///
    /// 输入非法、TTL 与心跳配置不匹配或 PostgreSQL 失败时返回错误。
    pub async fn register(
        &self,
        service: &str,
        endpoint: impl Into<String>,
        metadata: impl Into<Vec<u8>>,
        lease_ttl: Option<Duration>,
    ) -> Result<ServiceRegistration, DiscoveryError> {
        let service_hash = self.service_hash(service)?;
        let endpoint = endpoint.into();
        let metadata = metadata.into();
        if endpoint.is_empty() || endpoint.len() > self.inner.config.max_endpoint_bytes {
            return Err(DiscoveryError::EndpointTooLarge);
        }
        if metadata.len() > self.inner.config.max_metadata_bytes {
            return Err(DiscoveryError::MetadataTooLarge);
        }
        let lease_ttl = lease_ttl.unwrap_or(self.inner.config.lease_ttl);
        if lease_ttl <= self.inner.config.heartbeat_interval {
            return Err(DiscoveryError::InvalidConfig(
                "lease TTL must exceed heartbeat interval",
            ));
        }
        let _ = duration_millis(lease_ttl)?;

        ServiceRegistration::start(
            self.clone(),
            RegistrationSpec {
                service_name: service.to_owned(),
                service_hash,
                registration_id: RegistrationId::from_bytes(random_bytes()?),
                owner_token: random_bytes()?,
                endpoint,
                metadata,
                lease_ttl,
            },
        )
        .await
    }

    /// 发现指定服务的健康实例。
    ///
    /// # 错误
    ///
    /// 服务标识非法或 PostgreSQL 失败时返回错误。
    pub async fn discover(
        &self,
        service: &str,
        limit: usize,
    ) -> Result<Vec<ServiceInstance>, DiscoveryError> {
        if limit == 0 {
            return Err(DiscoveryError::InvalidConfig(
                "discovery limit must be positive",
            ));
        }
        let service_hash = self.service_hash(service)?;
        self.discover_by_hash(service_hash, limit).await
    }

    pub(crate) async fn discover_by_hash(
        &self,
        service_hash: KeyId,
        limit: usize,
    ) -> Result<Vec<ServiceInstance>, DiscoveryError> {
        if limit == 0 {
            return Err(DiscoveryError::InvalidConfig(
                "discovery limit must be positive",
            ));
        }
        let limit = i64::try_from(limit.min(10_000))
            .map_err(|_| DiscoveryError::InvalidConfig("discovery limit is too large"))?;
        self.timed("discover", async {
            let client = self.inner.pool.get().await?;
            let rows = client
                .query(
                    "SELECT
                        registration_id,
                        endpoint,
                        metadata,
                        version,
                        GREATEST(
                            0,
                            CEIL(EXTRACT(EPOCH FROM
                                (lease_expires_at - clock_timestamp())) * 1000)
                        )::bigint
                     FROM pgfw_service_instance
                     WHERE namespace = $1
                       AND service_hash = $2
                       AND lease_expires_at > clock_timestamp()
                     ORDER BY updated_at DESC, registration_id
                     LIMIT $3",
                    &[
                        &self.inner.config.namespace,
                        &service_hash.as_bytes().as_slice(),
                        &limit,
                    ],
                )
                .await?;
            let mut instances = Vec::with_capacity(rows.len());
            for row in rows {
                let id: Vec<u8> = row.get(0);
                let Ok(id) = <[u8; 16]>::try_from(id) else {
                    continue;
                };
                let remaining_ms: i64 = row.get(4);
                instances.push(ServiceInstance {
                    registration_id: RegistrationId::from_bytes(id),
                    endpoint: row.get(1),
                    metadata: row.get(2),
                    version: row.get(3),
                    remaining: Duration::from_millis(
                        u64::try_from(remaining_ms).unwrap_or_default(),
                    ),
                });
            }
            Ok(instances)
        })
        .await
    }

    /// 按主键批次清理过期注册。
    ///
    /// # 错误
    ///
    /// batch size 为 0 或 PostgreSQL 失败时返回错误。
    pub async fn cleanup_expired(&self, batch_size: usize) -> Result<u64, DiscoveryError> {
        if batch_size == 0 {
            return Err(DiscoveryError::InvalidConfig("batch size must be positive"));
        }
        let mut total = 0u64;
        loop {
            let deleted = self
                .timed("cleanup", async {
                    let client = self.inner.pool.get().await?;
                    let deleted = client
                        .execute(
                            "WITH expired AS (
                                SELECT service_hash, registration_id
                                FROM pgfw_service_instance
                                WHERE lease_expires_at <= clock_timestamp()
                                ORDER BY lease_expires_at
                                LIMIT $1
                                FOR UPDATE SKIP LOCKED
                             )
                             DELETE FROM pgfw_service_instance AS instance
                             USING expired
                             WHERE instance.service_hash = expired.service_hash
                               AND instance.registration_id = expired.registration_id
                               AND instance.lease_expires_at <= clock_timestamp()",
                            &[&i64::try_from(batch_size).unwrap_or(i64::MAX)],
                        )
                        .await?;
                    Ok(deleted)
                })
                .await?;
            total = total.saturating_add(deleted);
            if deleted < u64::try_from(batch_size).unwrap_or(u64::MAX) {
                break;
            }
        }
        Ok(total)
    }

    pub(crate) fn service_hash(&self, service: &str) -> Result<KeyId, DiscoveryError> {
        validate_key_size(service.len(), self.inner.config.max_service_bytes)?;
        Ok(self
            .inner
            .config
            .key_hasher
            .derive(&self.inner.config.namespace, service.as_bytes()))
    }

    pub(crate) async fn timed<T, F>(
        &self,
        operation: &'static str,
        future: F,
    ) -> Result<T, DiscoveryError>
    where
        F: Future<Output = Result<T, DiscoveryError>>,
    {
        tokio::time::timeout(self.inner.config.operation_timeout, future)
            .await
            .map_err(|_| DiscoveryError::Timeout(operation))?
    }
}

pub(crate) fn random_bytes<const N: usize>() -> Result<[u8; N], DiscoveryError> {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes).map_err(|error| DiscoveryError::Random(error.to_string()))?;
    Ok(bytes)
}
