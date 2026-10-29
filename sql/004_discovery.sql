CREATE TABLE IF NOT EXISTS pgfw_service_instance (
    namespace         text        NOT NULL,
    service_name      text        CONSTRAINT pgfw_service_instance_service_name_check CHECK (
        service_name IS NULL
        OR octet_length(service_name) BETWEEN 1 AND 1024
    ),
    service_hash      bytea       NOT NULL CHECK (octet_length(service_hash) = 32),
    registration_id   bytea       NOT NULL CHECK (octet_length(registration_id) = 16),
    owner_token       bytea       NOT NULL CHECK (octet_length(owner_token) = 32),
    endpoint          text        NOT NULL CHECK (octet_length(endpoint) BETWEEN 1 AND 2048),
    metadata          bytea       NOT NULL CHECK (octet_length(metadata) <= 8192),
    version           bigint      NOT NULL CHECK (version > 0),
    registered_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_expires_at  timestamptz NOT NULL,
    updated_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (service_hash, registration_id)
) PARTITION BY HASH (service_hash);

DO $$
BEGIN
    FOR partition_number IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF pgfw_service_instance FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            format('pgfw_service_instance_p%s', lpad(partition_number::text, 2, '0')),
            partition_number
        );
    END LOOP;
END
$$;

CREATE INDEX IF NOT EXISTS pgfw_service_instance_expires_idx
    ON pgfw_service_instance (lease_expires_at);
