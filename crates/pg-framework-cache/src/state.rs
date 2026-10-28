use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use pg_framework_core::KeyId;

#[derive(Clone)]
pub(crate) enum Mutation {
    Set {
        value: Arc<[u8]>,
        ttl_ms: Option<i64>,
        local_deadline: Option<Instant>,
        generation: u64,
    },
    Delete {
        generation: u64,
    },
    Expire {
        ttl_ms: i64,
        local_deadline: Instant,
        generation: u64,
    },
    Persist {
        generation: u64,
    },
}

impl Mutation {
    pub(crate) const fn generation(&self) -> u64 {
        match self {
            Self::Set { generation, .. }
            | Self::Delete { generation }
            | Self::Expire { generation, .. }
            | Self::Persist { generation } => *generation,
        }
    }

    pub(crate) fn value_bytes(&self) -> usize {
        match self {
            Self::Set { value, .. } => value.len(),
            Self::Delete { .. } | Self::Expire { .. } | Self::Persist { .. } => 0,
        }
    }

    pub(crate) fn merged_with(self, incoming: Self) -> Self {
        match (self, incoming) {
            (_, incoming @ (Self::Set { .. } | Self::Delete { .. })) => incoming,
            (
                Self::Set { value, .. },
                Self::Expire {
                    ttl_ms,
                    local_deadline,
                    generation,
                },
            ) => Self::Set {
                value,
                ttl_ms: Some(ttl_ms),
                local_deadline: Some(local_deadline),
                generation,
            },
            (Self::Set { value, .. }, Self::Persist { generation }) => Self::Set {
                value,
                ttl_ms: None,
                local_deadline: None,
                generation,
            },
            (
                Self::Delete { .. },
                Self::Expire { generation, .. } | Self::Persist { generation },
            ) => Self::Delete { generation },
            (_, incoming @ (Self::Expire { .. } | Self::Persist { .. })) => incoming,
        }
    }
}

#[derive(Clone)]
pub(crate) struct PendingEntry {
    pub(crate) raw_key: Option<Arc<[u8]>>,
    pub(crate) mutation: Mutation,
}

impl PendingEntry {
    pub(crate) fn generation(&self) -> u64 {
        self.mutation.generation()
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.raw_key.as_ref().map_or(0, |key| key.len()) + self.mutation.value_bytes()
    }
}

pub(crate) struct L1Entry {
    pub(crate) value: Arc<[u8]>,
    pub(crate) version: i64,
    pub(crate) expires_at: Option<Instant>,
    pub(crate) inserted_at: Instant,
}

#[derive(Default)]
pub(crate) struct L1Shard {
    pub(crate) entries: HashMap<KeyId, L1Entry>,
    pub(crate) epoch: u64,
    pub(crate) bytes: usize,
}

impl L1Shard {
    pub(crate) fn get(&self, key: KeyId) -> Option<Arc<[u8]>> {
        let entry = self.entries.get(&key)?;
        if entry
            .expires_at
            .is_some_and(|deadline| deadline <= Instant::now())
        {
            return None;
        }
        Some(Arc::clone(&entry.value))
    }

    pub(crate) fn insert(
        &mut self,
        key: KeyId,
        entry: L1Entry,
        capacity: usize,
        byte_capacity: usize,
    ) {
        if self
            .entries
            .get(&key)
            .is_some_and(|current| current.version > entry.version)
        {
            return;
        }
        if let Some(previous) = self.entries.remove(&key) {
            self.bytes = self.bytes.saturating_sub(previous.value.len());
        }
        let entry_bytes = entry.value.len();
        while !self.entries.is_empty()
            && (self.entries.len() >= capacity
                || self.bytes.saturating_add(entry_bytes) > byte_capacity)
        {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.inserted_at)
                .map(|(key, _)| *key)
            else {
                break;
            };
            if let Some(removed) = self.entries.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(removed.value.len());
            }
        }
        self.entries.insert(key, entry);
        self.bytes = self.bytes.saturating_add(entry_bytes);
    }

    pub(crate) fn invalidate(&mut self, key: KeyId) {
        if let Some(removed) = self.entries.remove(&key) {
            self.bytes = self.bytes.saturating_sub(removed.value.len());
        }
        self.epoch = self.epoch.wrapping_add(1);
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
        self.epoch = self.epoch.wrapping_add(1);
    }
}

pub(crate) struct L2Entry {
    pub(crate) value: Arc<[u8]>,
    pub(crate) version: i64,
    pub(crate) ttl_ms: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(value: u8, generation: u64) -> Mutation {
        Mutation::Set {
            value: Arc::from(vec![value]),
            ttl_ms: Some(1_000),
            local_deadline: None,
            generation,
        }
    }

    #[test]
    fn set_then_expire_keeps_new_value() {
        let merged = set(1, 1).merged_with(Mutation::Expire {
            ttl_ms: 2_000,
            local_deadline: Instant::now(),
            generation: 2,
        });

        assert!(matches!(
            merged,
            Mutation::Set {
                value,
                ttl_ms: Some(2_000),
                generation: 2,
                ..
            } if value.as_ref() == [1]
        ));
    }

    #[test]
    fn set_then_persist_keeps_new_value_without_ttl() {
        let merged = set(1, 1).merged_with(Mutation::Persist { generation: 2 });

        assert!(matches!(
            merged,
            Mutation::Set {
                value,
                ttl_ms: None,
                local_deadline: None,
                generation: 2,
            } if value.as_ref() == [1]
        ));
    }

    #[test]
    fn delete_then_persist_keeps_tombstone() {
        let merged =
            Mutation::Delete { generation: 1 }.merged_with(Mutation::Persist { generation: 2 });

        assert!(matches!(merged, Mutation::Delete { generation: 2 }));
    }

    #[test]
    fn set_replaces_previous_delete() {
        let merged = Mutation::Delete { generation: 1 }.merged_with(set(2, 2));

        assert!(matches!(merged, Mutation::Set { generation: 2, .. }));
    }

    #[test]
    fn l1_evicts_when_byte_limit_is_exceeded() {
        let mut shard = L1Shard::default();
        shard.insert(
            KeyId::derive("test", b"first"),
            L1Entry {
                value: Arc::from(vec![1, 2, 3]),
                version: 1,
                expires_at: None,
                inserted_at: Instant::now(),
            },
            10,
            4,
        );
        shard.insert(
            KeyId::derive("test", b"second"),
            L1Entry {
                value: Arc::from(vec![4, 5, 6]),
                version: 2,
                expires_at: None,
                inserted_at: Instant::now(),
            },
            10,
            4,
        );

        assert_eq!(shard.entries.len(), 1);
        assert_eq!(shard.bytes, 3);
    }

    #[test]
    fn older_version_cannot_overwrite_newer_l1_entry() {
        let key = KeyId::derive("test", b"versioned");
        let mut shard = L1Shard::default();
        shard.insert(
            key,
            L1Entry {
                value: Arc::from(vec![2]),
                version: 2,
                expires_at: None,
                inserted_at: Instant::now(),
            },
            10,
            1024,
        );
        shard.insert(
            key,
            L1Entry {
                value: Arc::from(vec![1]),
                version: 1,
                expires_at: None,
                inserted_at: Instant::now(),
            },
            10,
            1024,
        );

        assert_eq!(shard.get(key).as_deref(), Some([2].as_slice()));
    }
}
