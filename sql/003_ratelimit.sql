CREATE TABLE IF NOT EXISTS pgfw_rate_limit_counter (
    namespace     text        NOT NULL,
    raw_key       bytea,
    key_hash      bytea       NOT NULL CHECK (octet_length(key_hash) = 32),
    window_start  timestamptz NOT NULL,
    used          bigint      NOT NULL CHECK (used >= 0),
    expires_at    timestamptz NOT NULL,
    PRIMARY KEY (namespace, key_hash, window_start),
    CHECK (raw_key IS NULL OR octet_length(raw_key) BETWEEN 1 AND 1024)
) PARTITION BY HASH (key_hash);

DO $$
BEGIN
    FOR partition_number IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF pgfw_rate_limit_counter FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            format('pgfw_rate_limit_counter_p%s', lpad(partition_number::text, 2, '0')),
            partition_number
        );
    END LOOP;
END
$$;

CREATE INDEX IF NOT EXISTS pgfw_rate_limit_counter_expires_idx
    ON pgfw_rate_limit_counter (expires_at);
