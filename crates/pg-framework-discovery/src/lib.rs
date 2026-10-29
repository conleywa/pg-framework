//! 基于 PostgreSQL 租约的服务注册与发现。

mod config;
mod error;
mod notify;
mod registration;
mod registry;
mod types;
mod watch;

pub use config::{
    DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_MAX_ENDPOINT_BYTES, DEFAULT_MAX_METADATA_BYTES,
    DEFAULT_SERVICE_LEASE, HARD_MAX_ENDPOINT_BYTES, HARD_MAX_METADATA_BYTES, RegistryConfig,
};
pub use error::DiscoveryError;
pub use registration::ServiceRegistration;
pub use registry::ServiceRegistry;
pub use types::{
    RegistrationId, RegistrationSnapshot, RegistrationStatus, ServiceInstance, ServiceSnapshot,
};
pub use watch::ServiceWatch;

pub use pg_framework_core::{DEFAULT_POOL_MAX_SIZE, KeyHasher, build_default_pool, build_pool};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_id_hex_is_fixed_size() {
        let id = RegistrationId::from_bytes([0xab; 16]);
        assert_eq!(id.to_hex().len(), 32);
    }

    #[test]
    fn heartbeat_must_be_shorter_than_lease() {
        let mut config = RegistryConfig::new("test");
        config.heartbeat_interval = config.lease_ttl;
        assert!(matches!(
            config.validate(),
            Err(DiscoveryError::InvalidConfig(_))
        ));
    }

    #[test]
    fn default_schema_includes_readable_service_name() {
        assert_eq!(RegistryConfig::new("test").schema_generation, "v2");
    }
}
