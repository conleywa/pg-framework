use std::fmt;
use std::str::FromStr;

use crate::{CoordinationError, HARD_MAX_COORDINATION_PATH_BYTES};

/// 已校验的规范化协调节点绝对路径。
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodePath(String);

impl NodePath {
    /// 解析并校验绝对路径。
    ///
    /// 根路径为 `/`。其他路径不能以 `/` 结尾，不能包含空段、`.`、`..`
    /// 或 ASCII 控制字符。
    ///
    /// # 错误
    ///
    /// 路径格式非法或超过 SQL 硬上限时返回错误。
    pub fn parse(path: impl AsRef<str>) -> Result<Self, CoordinationError> {
        let path = path.as_ref();
        if path.is_empty() {
            return Err(CoordinationError::InvalidPath("path is empty"));
        }
        if path.len() > HARD_MAX_COORDINATION_PATH_BYTES {
            return Err(CoordinationError::InvalidPath(
                "path exceeds the schema hard limit",
            ));
        }
        if !path.starts_with('/') {
            return Err(CoordinationError::InvalidPath(
                "path must be absolute and start with /",
            ));
        }
        if path.bytes().any(|byte| byte < 0x20 || byte == 0x7f) {
            return Err(CoordinationError::InvalidPath(
                "path contains an ASCII control character",
            ));
        }
        if path == "/" {
            return Ok(Self(path.to_owned()));
        }
        if path.ends_with('/') || path.contains("//") {
            return Err(CoordinationError::InvalidPath(
                "path contains an empty segment",
            ));
        }
        for segment in path[1..].split('/') {
            if segment.is_empty() || segment == "." || segment == ".." {
                return Err(CoordinationError::InvalidPath(
                    "path contains a non-canonical segment",
                ));
            }
        }
        Ok(Self(path.to_owned()))
    }

    /// 返回规范化路径文本。
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// 返回路径字节长度。
    #[must_use]
    pub fn byte_len(&self) -> usize {
        self.0.len()
    }

    /// 返回是否为根路径。
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.0 == "/"
    }

    /// 返回最后一个路径段；根路径返回空字符串。
    #[must_use]
    pub fn name(&self) -> &str {
        if self.is_root() {
            ""
        } else {
            self.0.rsplit('/').next().unwrap_or_default()
        }
    }

    /// 返回父路径；根路径返回 `None`。
    #[must_use]
    pub fn parent(&self) -> Option<Self> {
        if self.is_root() {
            return None;
        }
        let separator = self.0.rfind('/')?;
        let parent = if separator == 0 {
            "/"
        } else {
            &self.0[..separator]
        };
        Some(Self(parent.to_owned()))
    }

    /// 追加一个路径段。
    ///
    /// # 错误
    ///
    /// 路径段为空、包含 `/` 或使最终路径超过 SQL 硬上限时返回错误。
    pub fn join(&self, name: &str) -> Result<Self, CoordinationError> {
        if name.is_empty() || name.contains('/') {
            return Err(CoordinationError::InvalidPath(
                "path segment must be non-empty and contain no /",
            ));
        }
        if name == "." || name == ".." {
            return Err(CoordinationError::InvalidPath(
                "path segment is not canonical",
            ));
        }
        if self.is_root() {
            Self::parse(format!("/{name}"))
        } else {
            Self::parse(format!("{}/{name}", self.0))
        }
    }
}

impl fmt::Display for NodePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl fmt::Debug for NodePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("NodePath").field(&self.0).finish()
    }
}

impl FromStr for NodePath {
    type Err = CoordinationError;

    fn from_str(path: &str) -> Result<Self, Self::Err> {
        Self::parse(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_canonical_paths_and_parents() {
        let path = NodePath::parse("/services/orders/worker-1").expect("path");

        assert_eq!(path.name(), "worker-1");
        assert_eq!(
            path.parent().as_ref().map(NodePath::as_str),
            Some("/services/orders")
        );
        assert_eq!(
            NodePath::parse("/services")
                .expect("path")
                .parent()
                .as_ref()
                .map(NodePath::as_str),
            Some("/")
        );
        assert!(NodePath::parse("/").expect("root").is_root());
    }

    #[test]
    fn rejects_non_canonical_paths() {
        for path in [
            "",
            "services",
            "/services/",
            "//services",
            "/a/./b",
            "/a/../b",
        ] {
            assert!(NodePath::parse(path).is_err(), "path should fail: {path}");
        }
    }

    #[test]
    fn sequential_suffix_keeps_path_validated() {
        let prefix = NodePath::parse("/services/election-").expect("path");
        let child = NodePath::parse(format!("{}{:010}", prefix, 1)).expect("child");

        assert_eq!(child.as_str(), "/services/election-0000000001");
        assert_eq!(
            child.parent().as_ref().map(NodePath::as_str),
            Some("/services")
        );
    }
}
