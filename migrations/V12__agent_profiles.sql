-- V12: Agent profiles and credentials for swarm-mode provisioning.
--
-- Establishes the "agent is a row, not a pod" model. Agent identity,
-- configuration, and credentials live in Postgres. Pool workers load
-- profiles on demand rather than reading from env vars.
--
-- See also: swarm_patterns_exploration.md, provision_ironclaw.py

-- Agent identity and configuration
CREATE TABLE IF NOT EXISTS agent_profiles (
    id              TEXT PRIMARY KEY,                    -- 'mr-pink' (matches AGENT_ID env var)
    display_name    TEXT NOT NULL,                       -- 'Mr. Pink'
    model           TEXT NOT NULL DEFAULT 'claude-sonnet',
    system_prompt   TEXT,                                -- personality / role instructions
    tools           JSONB NOT NULL DEFAULT '[]'::jsonb,  -- enabled tool names
    memory_config   JSONB NOT NULL DEFAULT '{}'::jsonb,  -- compaction window, etc.
    nats_subjects   JSONB NOT NULL DEFAULT '[]'::jsonb,  -- auto-generated routing subjects
    port            INT,                                 -- HTTP port (for dedicated-pod mode)
    status          TEXT NOT NULL DEFAULT 'active'       -- active | paused | archived
                    CHECK (status IN ('active', 'paused', 'archived')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Per-agent API credentials (provider-keyed)
-- Values should be encrypted at the application layer before INSERT.
CREATE TABLE IF NOT EXISTS agent_credentials (
    agent_id        TEXT NOT NULL REFERENCES agent_profiles(id) ON DELETE CASCADE,
    provider        TEXT NOT NULL,                       -- 'github', 'slack', 'jira'
    credentials     JSONB NOT NULL DEFAULT '{}'::jsonb,  -- encrypted blob
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (agent_id, provider)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_agent_profiles_status ON agent_profiles(status);
CREATE INDEX IF NOT EXISTS idx_agent_credentials_agent ON agent_credentials(agent_id);

-- Reuse the update_updated_at_column() trigger function from V1
CREATE TRIGGER agent_profiles_updated_at
    BEFORE UPDATE ON agent_profiles
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER agent_credentials_updated_at
    BEFORE UPDATE ON agent_credentials
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
