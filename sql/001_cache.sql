CREATE UNLOGGED TABLE IF NOT EXISTS pgfw_cache_entry (
    namespace   text        NOT NULL,
    raw_key     bytea,
    key_hash    bytea       NOT NULL CHECK (octet_length(key_hash) = 32),
    value       bytea       NOT NULL,
    version     bigint      NOT NULL CHECK (version > 0),
    expires_at  timestamptz,
    updated_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace, key_hash),
    CHECK (raw_key IS NULL OR octet_length(raw_key) BETWEEN 1 AND 1024)
) PARTITION BY HASH (key_hash);

CREATE UNLOGGED TABLE IF NOT EXISTS pgfw_cache_namespace (
    namespace   text        NOT NULL PRIMARY KEY,
    clearing    boolean     NOT NULL DEFAULT false,
    epoch       bigint      NOT NULL DEFAULT 1 CHECK (epoch > 0),
    updated_at  timestamptz NOT NULL DEFAULT clock_timestamp()
);

DO $$
BEGIN
    FOR partition_number IN 0..63 LOOP
        EXECUTE format(
            'CREATE UNLOGGED TABLE IF NOT EXISTS %I PARTITION OF pgfw_cache_entry FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            format('pgfw_cache_entry_p%s', lpad(partition_number::text, 2, '0')),
            partition_number
        );
    END LOOP;
END
$$;

CREATE INDEX IF NOT EXISTS pgfw_cache_entry_expires_idx
    ON pgfw_cache_entry (expires_at)
    WHERE expires_at IS NOT NULL;
