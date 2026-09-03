package audit

import "fmt"

// DDL returns the CREATE TABLE IF NOT EXISTS statement (plus its
// supporting indexes) for this Store's table. It is a template for a
// service's own migration file, not something this package executes
// itself — audit has no migration runner of its own, and services
// already have one (see README.md's adoption flow for a paste-in
// example). Because s.table already passed NewStore's identifier
// guard, this method cannot itself be an injection vector; it's still
// on the caller to review the pasted SQL like any other migration.
//
// Column choices unify the shapes observed across cm_access_audit
// (Auth), file_access_audit (File), and tenant_audit (Tenant) — see
// doc.go — plus adds before_state/after_state, which none of those
// three tables have.
func (s *Store) DDL() string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id      UUID NOT NULL,
    actor_id       TEXT NOT NULL DEFAULT '',
    actor_role     TEXT NOT NULL DEFAULT '',
    actor_email    TEXT NOT NULL DEFAULT '',
    action         TEXT NOT NULL,
    resource_type  TEXT NOT NULL,
    resource_id    TEXT NOT NULL DEFAULT '',
    decision       TEXT NOT NULL DEFAULT '',
    reason         TEXT NOT NULL DEFAULT '',
    before_state   JSONB,
    after_state    JSONB,
    request_id     TEXT NOT NULL DEFAULT '',
    ip             TEXT NOT NULL DEFAULT '',
    user_agent     TEXT NOT NULL DEFAULT '',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS %[1]s_tenant_created_idx ON %[1]s (tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS %[1]s_actor_created_idx ON %[1]s (actor_id, created_at DESC);
CREATE INDEX IF NOT EXISTS %[1]s_resource_idx ON %[1]s (resource_type, resource_id);
CREATE INDEX IF NOT EXISTS %[1]s_action_idx ON %[1]s (action);
`, s.table)
}
