INSERT INTO pgfw_schema_meta (component, schema_generation, shard_count)
VALUES ('coordination', 'v1', 0)
ON CONFLICT (component) DO NOTHING;

CREATE TABLE IF NOT EXISTS pgfw_coordination_session (
    namespace         text        NOT NULL CHECK (
        octet_length(namespace) BETWEEN 1 AND 1024
    ),
    session_id        bytea       NOT NULL CHECK (octet_length(session_id) = 16),
    owner_token       bytea       NOT NULL CHECK (octet_length(owner_token) = 32),
    version           bigint      NOT NULL CHECK (version > 0),
    lease_expires_at  timestamptz NOT NULL,
    created_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace, session_id)
);

CREATE INDEX IF NOT EXISTS pgfw_coordination_session_expires_idx
    ON pgfw_coordination_session (namespace, lease_expires_at);

CREATE OR REPLACE FUNCTION pgfw_coordination_valid_path(node_path text)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    path_segment text;
BEGIN
    IF node_path = '/' THEN
        RETURN true;
    END IF;
    IF octet_length(node_path) > 4096
       OR left(node_path, 1) <> '/'
       OR right(node_path, 1) = '/'
       OR position('//' IN node_path) > 0 THEN
        RETURN false;
    END IF;
    FOREACH path_segment IN ARRAY string_to_array(substr(node_path, 2), '/')
    LOOP
        IF path_segment = ''
           OR path_segment IN ('.', '..')
           OR path_segment ~ '[[:cntrl:]]' THEN
            RETURN false;
        END IF;
    END LOOP;
    RETURN true;
END
$$;

COMMENT ON FUNCTION pgfw_coordination_valid_path(text)
    IS '校验协调节点路径是否为规范的绝对路径';

CREATE OR REPLACE FUNCTION pgfw_coordination_parent_path(node_path text)
RETURNS text
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN node_path = '/' THEN NULL
        ELSE COALESCE(
            NULLIF(regexp_replace(node_path, '/[^/]+$', ''), ''),
            '/'
        )
    END
$$;

COMMENT ON FUNCTION pgfw_coordination_parent_path(text)
    IS '返回协调节点路径的父路径，根节点返回 NULL';

CREATE OR REPLACE FUNCTION pgfw_coordination_node_name(node_path text)
RETURNS text
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN node_path = '/' THEN ''
        ELSE regexp_replace(node_path, '^.*/', '')
    END
$$;

COMMENT ON FUNCTION pgfw_coordination_node_name(text)
    IS '返回协调节点路径的最后一段，根节点返回空文本';

CREATE TABLE IF NOT EXISTS pgfw_coordination_node (
    namespace     text        NOT NULL CHECK (
        octet_length(namespace) BETWEEN 1 AND 1024
    ),
    path          text        NOT NULL CHECK (
        octet_length(path) BETWEEN 1 AND 4096
        AND pgfw_coordination_valid_path(path)
    ),
    parent_path   text,
    name          text        NOT NULL CHECK (
        octet_length(name) <= 4096
    ),
    data          bytea       NOT NULL CHECK (octet_length(data) <= 1048576),
    version       bigint      NOT NULL CHECK (version >= 0),
    node_type     text        NOT NULL CHECK (
        node_type IN ('persistent', 'ephemeral')
    ),
    session_id    bytea       CHECK (
        session_id IS NULL
        OR octet_length(session_id) = 16
    ),
    created_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace, path),
    CONSTRAINT pgfw_coordination_node_parent_path_check CHECK (
        parent_path IS NOT DISTINCT FROM pgfw_coordination_parent_path(path)
    ),
    CONSTRAINT pgfw_coordination_node_name_check CHECK (
        name = pgfw_coordination_node_name(path)
    ),
    CONSTRAINT pgfw_coordination_node_lifetime_check CHECK (
        (node_type = 'persistent' AND session_id IS NULL)
        OR (node_type = 'ephemeral' AND session_id IS NOT NULL)
    ),
    CONSTRAINT pgfw_coordination_node_root_check CHECK (
        path <> '/' OR node_type = 'persistent'
    ),
    CONSTRAINT pgfw_coordination_node_parent_fk
        FOREIGN KEY (namespace, parent_path)
        REFERENCES pgfw_coordination_node (namespace, path)
        ON DELETE RESTRICT,
    CONSTRAINT pgfw_coordination_node_session_fk
        FOREIGN KEY (namespace, session_id)
        REFERENCES pgfw_coordination_session (namespace, session_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS pgfw_coordination_node_children_idx
    ON pgfw_coordination_node (namespace, parent_path, name);

CREATE INDEX IF NOT EXISTS pgfw_coordination_node_session_idx
    ON pgfw_coordination_node (namespace, session_id)
    WHERE session_id IS NOT NULL;

CREATE OR REPLACE FUNCTION pgfw_coordination_validate_parent()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    parent_type text;
BEGIN
    IF NEW.node_type = 'ephemeral' AND EXISTS (
        SELECT 1
        FROM pgfw_coordination_node AS child
        WHERE child.namespace = NEW.namespace
          AND child.parent_path = NEW.path
    ) THEN
        RAISE EXCEPTION 'coordination nodes with children cannot become ephemeral'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.parent_path IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT node_type
    INTO parent_type
    FROM pgfw_coordination_node
    WHERE namespace = NEW.namespace
      AND path = NEW.parent_path;
    IF FOUND AND parent_type <> 'persistent' THEN
        RAISE EXCEPTION 'ephemeral coordination nodes cannot have children'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS pgfw_coordination_node_parent_guard
    ON pgfw_coordination_node;

CREATE TRIGGER pgfw_coordination_node_parent_guard
BEFORE INSERT OR UPDATE OF parent_path, node_type
ON pgfw_coordination_node
FOR EACH ROW
EXECUTE FUNCTION pgfw_coordination_validate_parent();

COMMENT ON TRIGGER pgfw_coordination_node_parent_guard ON pgfw_coordination_node
    IS '拒绝以临时节点作为父节点的协调节点';

CREATE TABLE IF NOT EXISTS pgfw_coordination_sequence (
    namespace     text   NOT NULL,
    parent_path   text   NOT NULL,
    next_sequence bigint NOT NULL CHECK (
        next_sequence BETWEEN 1 AND 9999999999
    ),
    PRIMARY KEY (namespace, parent_path)
);

COMMENT ON TABLE pgfw_coordination_session
    IS '协调会话 lease；删除会话会级联删除其临时节点';

COMMENT ON TABLE pgfw_coordination_node
    IS '持久和临时层级协调节点，临时节点可见性由会话 lease 决定';

COMMENT ON TABLE pgfw_coordination_sequence
    IS '按 namespace 和父路径分配的永久递增顺序节点编号';

CREATE OR REPLACE VIEW pgfw_coordination_session_readable AS
SELECT
    namespace,
    encode(session_id, 'hex') AS session_id_hex,
    version,
    lease_expires_at,
    created_at,
    updated_at
FROM pgfw_coordination_session;

COMMENT ON VIEW pgfw_coordination_session_readable
    IS '协调会话可读视图，隐藏 owner token，仅供受控排障';

CREATE OR REPLACE VIEW pgfw_coordination_node_readable AS
SELECT
    namespace,
    path,
    parent_path,
    name,
    node_type,
    encode(session_id, 'hex') AS session_id_hex,
    pgfw_display_text(data) AS data_text,
    octet_length(data) AS data_bytes,
    version,
    created_at,
    updated_at
FROM pgfw_coordination_node;

COMMENT ON VIEW pgfw_coordination_node_readable
    IS '协调节点可读视图，仅供受控排障，不用于服务间 watch 热路径';

UPDATE pgfw_schema_meta
SET schema_generation = 'v1',
    shard_count = 0,
    updated_at = clock_timestamp()
WHERE component = 'coordination';
