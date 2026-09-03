# backbone-audit

A shared row schema, atomic-write helper, keyset-paginated query, and
retention worker for audit trails across backbone services. Sibling
to `backbone-events` (fire-and-forget Redis pub/sub) and
`backbone-queue` (durable RabbitMQ + its `outbox` subpackage) — same
"shared Go module, not a shared service" shape, stdlib only, zero
dependencies.

## Why this exists (and what it deliberately isn't)

A 2026-09-03 platform audit found ~20 audit-shaped tables scattered
across services, each with its own actor/action/decision vocabulary,
and exactly one (Auth Service's `cm_access_audit`, its CM-8 pattern)
with a real retention policy. A central Audit Service that every
write goes through was considered and rejected — see
`.claude/decisions/AUDIT_SERVICE_KARAR.md` at the repo root — because
the platform's only genuinely durable event mechanism at the time was
a two-service outbox, and the general-purpose event bus
(`backbone-events`) is fire-and-forget by design. Building a central
audit consumer on that foundation would defeat the entire point of an
audit trail.

What was missing wasn't centralization, it was standardization. This
package is that: one schema, one retention pattern, adopted gradually
by services opening a new audit table or reworking an old one.
Existing tables (`cm_access_audit`, `file_access_audit`,
`tenant_audit`, ...) are **not** retro-migrated onto this schema —
only new/reworked ones adopt it. See `doc.go` for the full design
rationale.

**Not included:** cross-service querying ("show me everything tenant
X did across Auth/File/Tenant/Billing"). That's a read-side gateway
concern (fan out to each adopting service's `Store.Query` and merge),
out of scope for this package, which only standardizes one service's
own table.

## When to use it vs. writing your own table

Use `backbone-audit` when you're opening a **new** audit-shaped table,
or substantially reworking an existing one, and want:

- an actor/action/resource/decision/before-after schema that doesn't
  need to be reinvented per service,
- the audit row to commit atomically with the domain write it
  describes (via your own `*sql.Tx`), instead of a separate
  best-effort write after the fact,
- an optional, off-by-default retention sweeper instead of an
  unbounded table.

Keep writing your own table if your "audit" data doesn't fit the
actor-did-action-to-resource shape at all (e.g. a pure event-payload
log like `license_events`, or a durable outbox like
`stock_catalog_outbox` — use `backbone-queue/outbox` for that case
instead).

## Adoption flow

### 1. Paste the DDL into your service's own migration

```go
store, err := audit.NewStore("tenant_audit") // must match ^[a-z_][a-z0-9_]*$
fmt.Println(store.DDL())                     // paste the printed SQL into your next migration file
```

`DDL()` returns a `CREATE TABLE IF NOT EXISTS` plus four supporting
indexes (`(tenant_id, created_at DESC)`, `(actor_id, created_at DESC)`,
`(resource_type, resource_id)`, `(action)`), all `IF NOT EXISTS` and
named after your table. This package has no migration runner of its
own — every service already has one.

### 2. Record inside your own transaction

```go
tx, err := db.BeginTx(ctx, nil)
// ... write the domain row(s) ...
err = store.Record(ctx, tx, audit.Entry{
	TenantID:     tenant.ID,
	ActorID:      actor.UserID,   // or ActorEmail — at least one is required
	ActorRole:    actor.Role,
	Action:       "tenant.update",
	ResourceType: "tenant",
	ResourceID:   tenant.ID,
	Before:       beforeJSON, // json.RawMessage; nil → SQL NULL, not "{}"
	After:        afterJSON,
	RequestID:    reqID,
})
// ... tx.Commit() — the audit row and the domain write commit together
```

`ID` and `CreatedAt` default (fresh UUIDv4, `time.Now().UTC()`) if
left zero. `Decision` is optional but validated when set: `""`,
`"allow"`, `"deny"`, or `"error"` (see `audit.DecisionAllow` /
`DecisionDeny` / `DecisionError`). A caller that only has a `*sql.DB`
handy may pass that directly instead of a `*sql.Tx` — `Execer` is
satisfied by both — accepting the small window between the domain
write and the audit write that implies.

### 3. Optionally wire a Retention worker in your service's main

```go
ret := audit.NewRetention(db, store, audit.RetentionConfig{
	Enabled:       os.Getenv("TENANT_AUDIT_RETENTION_ENABLED") == "true", // default OFF
	DryRun:        os.Getenv("TENANT_AUDIT_RETENTION_DRY_RUN") == "true",
	RetentionDays: 365, // pick per your compliance needs; default 30 if left 0
})
go ret.Run(ctx) // ticks every Interval (default 24h) until ctx is done

// Expose ret.Snapshot() on a health/admin endpoint if you want visibility
// into LastRun / TotalRuns / TotalRowsDeleted / NextRunAt, the same
// shape Auth Service's CM-8 AuditScheduler exposes today.
```

🔴 **Rule: retention defaults OFF.** Simply importing this package
and calling `NewRetention` never starts deleting rows — `Enabled`
defaults to `false`. Gate it behind your own
`<SVC>_AUDIT_RETENTION_ENABLED` flag, the same convention
`backbone-queue/outbox`'s `Relay`/`Cleanup` use.

`Retention.Tick` runs one sweep (call it directly for a manual/cron
trigger, or let `Run` call it on a ticker): `DryRun` counts what
*would* be deleted without deleting anything; otherwise it loops
batched `DELETE`s (`BatchSize`, default 5000) until the backlog is
drained or `MaxRowsPerRun` (default 100000) is hit, sleeping
`BatchPause` (default 50ms) between batches so a large purge never
holds one long table lock — generalized from Auth Service's CM-8
`PurgeOldAudit`. A panic inside a tick is recovered, logged, and
recorded as a failed result; it never crashes the host process.

### Querying

```go
entries, nextCursor, err := store.Query(ctx, db, audit.Filter{
	TenantID:     tenantID,
	ResourceType: "tenant",
	Limit:        50, // default 100, capped at 1000
})
// ... render entries ...
if nextCursor != "" {
	more, next2, err := store.Query(ctx, db, audit.Filter{TenantID: tenantID, ResourceType: "tenant", Cursor: nextCursor})
}
```

Results are newest-first (`ORDER BY created_at DESC, id DESC`). Every
`Filter` field is optional and additive — only non-zero fields
contribute a `WHERE` predicate. `Cursor` is an opaque, base64-encoded
keyset position (`created_at|id`) returned as `nextCursor` whenever
more rows exist beyond the page; treat it as opaque and pass it back
verbatim — its internal shape may change in a later version.

## Versioning

`v0.1.0` is the initial release: `Entry`/`Store`/`Filter`/`Retention`
as documented above. After the operator creates the GitHub repo
(`serhatzgdigital/backbone-audit`) and pushes this module, tag it:

```sh
git tag v0.1.0 && git push --tags
```

Future additions follow semver: a new exported method/field is a
minor bump (`v0.1.0` → `v0.2.0`), a breaking signature change is
major.
