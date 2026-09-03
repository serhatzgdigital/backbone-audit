// Package audit is a shared row schema + retention pattern for
// audit trails across backbone services, following the same
// "shared Go module, not a shared service" shape as backbone-events
// and backbone-queue.
//
// # Why a library, not an Audit Service
//
// A 2026-09-03 inventory (see
// .claude/decisions/AUDIT_SERVICE_KARAR.md at the repo root) found
// ~20 audit-shaped tables scattered across services — cm_access_audit
// (Auth), file_access_audit (File), tenant_audit (Tenant),
// license_events (License), community_audit_log (Community), and
// more — each with its own actor/action/decision vocabulary, and
// exactly one (cm_access_audit) with a real retention policy. The
// tempting fix, a central Audit Service that every write goes
// through, was rejected: the platform's only genuinely durable event
// mechanism at decision time was a two-service outbox
// (stock_catalog_outbox), and the general-purpose event bus
// (backbone-events) is fire-and-forget Redis pub/sub by design — its
// own doc comment calls a dropped message "acceptable". Building a
// central audit consumer on top of that foundation would defeat the
// entire point of an audit trail (silent, unrecoverable gaps) and
// would touch the write path of 11+ services to get there.
//
// What was actually missing wasn't centralization, it was
// standardization: one schema, one retention pattern, adopted
// gradually by services that are opening a new audit table or
// reworking an old one. That's this package. It sits entirely on the
// read/write side of a service's own database — there is no network
// hop, no new service, no dependency on any event bus being up.
//
// # Atomic writes via the caller's transaction
//
// Store.Record takes an Execer, not a *sql.DB directly — the same
// shape backbone-queue/outbox uses for Store.Insert. A caller in the
// middle of its own domain transaction passes its *sql.Tx, so the
// audit row commits or rolls back exactly together with the write it
// describes. This is the main upgrade over today's typical pattern
// (e.g. Tenant Service's tenant_audit writes, which happen as a
// separate best-effort statement after the domain write, with no
// atomicity guarantee): either both rows land, or neither does. A
// caller that only has a *sql.DB handy may still pass that directly —
// Execer is satisfied by both — accepting the small window between
// the domain write and the audit write that implies.
//
// # Why before/after JSONB
//
// None of the existing audit tables captured a before/after diff
// (cm_access_audit records only the RBAC decision; tenant_audit
// records only an unstructured metadata blob). Entry.Before/After
// are nullable json.RawMessage columns precisely so a caller CAN
// attach a structured diff when it has one ("what changed"), without
// being forced to when it doesn't (a pure access-decision log like
// cm_access_audit's use case never needs them and leaves them nil,
// which maps to SQL NULL, not an empty-but-present {}).
//
// # Retention is opt-in and defaults off
//
// Retention generalizes Auth Service's CM-8 audit_retention.go
// pattern (a ticker-based sweeper doing batched CTE deletes so a
// large purge never holds one long table lock) into something any
// adopter can wire up. It ships disabled by default — an adopter is
// expected to gate Retention.Run behind its own
// `<SVC>_AUDIT_RETENTION_ENABLED` environment flag, the same
// convention backbone-queue/outbox uses for its Relay/Cleanup — so
// simply importing this package never starts silently deleting rows.
//
// # What is deliberately NOT included
//
// Cross-service querying — "show me everything tenant X did across
// Auth/File/Tenant/Billing in the last 90 days" — is out of scope
// for this package on purpose. That's a read-side fan-out concern
// (AUDIT-3 in the roadmap above): a gateway endpoint that queries
// each adopting service's own Store.Query and merges the results,
// the same shape Backbone Services' routes_cm_audit.go already uses
// for a single source. This package only standardizes what one
// service's own table looks like and how it's written/retained/
// queried in isolation.
//
// # Retro-migration is explicitly out of scope
//
// Existing tables (cm_access_audit, file_access_audit, tenant_audit,
// ...) are NOT migrated onto this schema. Only newly-opened or
// substantially reworked audit tables adopt it going forward — see
// the decision record's §9 for why (retro-migrating ~20 tables has
// high churn and zero evidentiary upside).
package audit
