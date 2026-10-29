CREATE TABLE IF NOT EXISTS pgfw_schema_meta (
    component          text        NOT NULL PRIMARY KEY,
    schema_generation  text        NOT NULL,
    shard_count        integer     NOT NULL CHECK (shard_count >= 0),
    updated_at         timestamptz NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO pgfw_schema_meta (component, schema_generation, shard_count)
VALUES
    ('cache', 'v1', 64),
    ('discovery', 'v2', 64),
    ('ratelimit', 'v1', 64),
    ('lock', 'v1', 0)
ON CONFLICT (component) DO NOTHING;
