-- Keycloak doesn't support full range of RFC 5322, namely quotes or uppercase
-- letters. It also normalizes emails, lowercasing the address.
CREATE TABLE IF NOT EXISTS instances (
    id serial PRIMARY KEY,
    name text NOT NULL,
    region text,
    host text NOT NULL,
    port smallint NOT NULL DEFAULT 5432,
    username text NOT NULL,
    password text NOT NULL,
    database text NOT NULL,

    -- Security attributes.
    owner text NOT NULL, -- Owner login (email).
    enabled bool NOT NULL DEFAULT true, -- Can be accessed at all (set internally during admin operations).
    shadow_admins text[] NOT NULL DEFAULT '{}', -- Additional users with full privileges to the database itself (but not instance owners).

    -- TODO: Move to instance attributes.
    hidden bool NOT NULL DEFAULT false, -- Visible in the instances list or the admin panel.
    is_template bool NOT NULL DEFAULT false, -- Listed as a global template (expects `public_can_read`).

    published bool NOT NULL DEFAULT true, -- Can be accessed by users other than the owner.
    anyone_can_read bool NOT NULL DEFAULT false, -- Make open for reads by anyone (access control still applies).
    disable_security bool NOT NULL DEFAULT false, -- Full access for anyone to all data (access control disabled). Deprecated.

    -- Quotas.
    max_size int, -- In MiB
    max_request_time interval,
    max_users int,
    -- Array of objects (time in seconds):
    -- [{"period": 1, "limit": 2}, {"period": 15, "limit": 100}]
    read_rate_limits_per_user jsonb,
    write_rate_limits_per_user jsonb,
    max_js_heap_size int, -- In MiB
    max_js_stack_size int, -- In MiB

    -- Timestamps.
    created_at timestamptz NOT NULL DEFAULT now(),
    accessed_at timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS lower_name ON instances ((lower(name)));
CREATE INDEX IF NOT EXISTS lower_owner ON instances ((lower(owner)));
