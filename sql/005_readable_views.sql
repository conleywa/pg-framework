ALTER TABLE pgfw_service_instance
    ADD COLUMN IF NOT EXISTS service_name text;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'pgfw_service_instance_service_name_check'
          AND conrelid = 'pgfw_service_instance'::regclass
    ) THEN
        ALTER TABLE pgfw_service_instance
            ADD CONSTRAINT pgfw_service_instance_service_name_check
            CHECK (
                service_name IS NULL
                OR octet_length(service_name) BETWEEN 1 AND 1024
            );
    END IF;
END
$$;

COMMENT ON COLUMN pgfw_service_instance.service_name
    IS '注册时的原始 service 名称，仅供排障和运维查询';

UPDATE pgfw_schema_meta
SET schema_generation = 'v2',
    updated_at = clock_timestamp()
WHERE component = 'discovery'
  AND schema_generation = 'v1';

CREATE OR REPLACE FUNCTION pgfw_display_text(payload bytea)
RETURNS text
LANGUAGE plpgsql
STABLE
STRICT
PARALLEL SAFE
AS $$
BEGIN
    RETURN convert_from(payload, 'UTF8');
EXCEPTION
    WHEN SQLSTATE '22021' THEN
        RETURN encode(payload, 'escape');
END
$$;

COMMENT ON FUNCTION pgfw_display_text(bytea)
    IS '将 UTF-8 bytea 转为文本；非法 UTF-8 使用 PostgreSQL escape 编码显示';

CREATE OR REPLACE VIEW pgfw_cache_entry_readable AS
SELECT
    namespace,
    pgfw_display_text(raw_key) AS raw_key_text,
    encode(key_hash, 'hex') AS key_hash_hex,
    pgfw_display_text(value) AS value_text,
    octet_length(value) AS value_bytes,
    version,
    expires_at,
    updated_at
FROM pgfw_cache_entry;

COMMENT ON VIEW pgfw_cache_entry_readable
    IS '缓存表可读视图，仅供受控排障和管理查询，不用于请求热路径';

CREATE OR REPLACE VIEW pgfw_lock_readable AS
SELECT
    namespace,
    pgfw_display_text(raw_key) AS raw_key_text,
    encode(key_hash, 'hex') AS key_hash_hex,
    fencing_token,
    lease_expires_at,
    acquired_at
FROM pgfw_lock;

COMMENT ON VIEW pgfw_lock_readable
    IS '锁表可读视图，隐藏 owner token，仅供受控排障和管理查询';

CREATE OR REPLACE VIEW pgfw_rate_limit_counter_readable AS
SELECT
    namespace,
    pgfw_display_text(raw_key) AS raw_key_text,
    encode(key_hash, 'hex') AS key_hash_hex,
    window_start,
    used,
    expires_at
FROM pgfw_rate_limit_counter;

COMMENT ON VIEW pgfw_rate_limit_counter_readable
    IS '限频表可读视图，仅供受控排障和管理查询，不用于请求热路径';

CREATE OR REPLACE VIEW pgfw_service_instance_readable AS
SELECT
    namespace,
    service_name,
    encode(service_hash, 'hex') AS service_hash_hex,
    encode(registration_id, 'hex') AS registration_id_hex,
    endpoint,
    pgfw_display_text(metadata) AS metadata_text,
    octet_length(metadata) AS metadata_bytes,
    version,
    registered_at,
    lease_expires_at,
    updated_at
FROM pgfw_service_instance;

COMMENT ON VIEW pgfw_service_instance_readable
    IS '服务注册表可读视图，隐藏 owner token，仅供受控排障和管理查询';
