CREATE SEQUENCE IF NOT EXISTS pgfw_lock_fencing_seq
    AS bigint
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    NO CYCLE
    CACHE 1;

CREATE TABLE IF NOT EXISTS pgfw_lock (
    namespace         text        NOT NULL,
    raw_key           bytea,
    key_hash          bytea       NOT NULL CHECK (octet_length(key_hash) = 32),
    owner_token       bytea       NOT NULL CHECK (octet_length(owner_token) = 32),
    fencing_token     bigint      NOT NULL CHECK (fencing_token > 0),
    lease_expires_at  timestamptz NOT NULL,
    acquired_at       timestamptz NOT NULL,
    PRIMARY KEY (namespace, key_hash),
    CHECK (raw_key IS NULL OR octet_length(raw_key) BETWEEN 1 AND 1024)
);

CREATE INDEX IF NOT EXISTS pgfw_lock_expires_idx
    ON pgfw_lock (lease_expires_at);
