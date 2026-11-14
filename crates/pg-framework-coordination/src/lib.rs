//! 基于 PostgreSQL 的 ZooKeeper 风格协调节点。
//!
//! 组件提供层级路径、持久节点、会话租约绑定的临时节点、顺序节点、
//! 版本化 CAS 和路径 watch。临时节点是否可见由数据库时钟下的会话 lease
//! 决定；后台清理只负责回收存储，不参与有效性判断。

mod client;
mod config;
mod error;
mod path;
mod session;
mod types;
mod watch;

pub use client::CoordinationClient;
pub use config::{
    CoordinationConfig, DEFAULT_COORDINATION_LEASE, DEFAULT_COORDINATION_SAFETY_MARGIN,
    DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_MAX_COORDINATION_CHILDREN,
    DEFAULT_MAX_COORDINATION_DATA_BYTES, DEFAULT_MAX_COORDINATION_PATH_BYTES,
    DEFAULT_WATCH_REFRESH, HARD_MAX_COORDINATION_DATA_BYTES, HARD_MAX_COORDINATION_PATH_BYTES,
};
pub use error::CoordinationError;
pub use path::NodePath;
pub use session::CoordinationSession;
pub use types::{
    CoordinationNode, CoordinationSnapshot, NodeKind, NodeVersion, SessionId, SessionSnapshot,
    SessionStatus,
};
pub use watch::CoordinationWatch;

pub use pg_framework_core::{DEFAULT_POOL_MAX_SIZE, build_default_pool, build_pool};
