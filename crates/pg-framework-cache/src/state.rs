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
    pub(crate) expires_at: Option<Instant>,
    pub(crate) inserted_at: Instant,
}

#[derive(Default)]
pub(crate) struct L1Shard {
    pub(crate) entries: HashMap<KeyId, L1Entry>,
    pub(crate) epoch: u64,
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

    pub(crate) fn insert(&mut self, key: KeyId, entry: L1Entry, capacity: usize) {
        if self.entries.len() >= capacity
            && !self.entries.contains_key(&key)
            && let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.inserted_at)
                .map(|(key, _)| *key)
        {
            self.entries.remove(&oldest);
        }
        self.entries.insert(key, entry);
    }

    pub(crate) fn invalidate(&mut self, key: KeyId) {
        self.entries.remove(&key);
        self.epoch = self.epoch.wrapping_add(1);
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.epoch = self.epoch.wrapping_add(1);
    }
}

pub(crate) struct L2Entry {
    pub(crate) value: Arc<[u8]>,
    pub(crate) ttl_ms: Option<i64>,
}
