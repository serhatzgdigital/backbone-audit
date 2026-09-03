package audit

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Decision values Entry.Decision is validated against when non-empty.
// An empty Decision is also valid — not every audited action is a
// yes/no access check (e.g. "tenant.update" isn't a decision, it's a
// fact).
const (
	DecisionAllow = "allow"
	DecisionDeny  = "deny"
	DecisionError = "error"
)

// validDecisions is the set Entry.Validate checks Decision against
// when it is non-empty.
var validDecisions = map[string]bool{
	DecisionAllow: true,
	DecisionDeny:  true,
	DecisionError: true,
}

// Entry is one audit row to be written via Store.Record or read back
// via Store.Query. Construct it as a struct literal; ID and CreatedAt
// are defaulted by Record if left zero (see withDefaults), the rest
// are checked by Validate.
type Entry struct {
	// ID is the row's primary key. Left empty, Record generates a
	// fresh UUIDv4 (crypto/rand, RFC 4122 — same construction as
	// backbone-queue/outbox's Event.ID).
	ID string

	// TenantID scopes the row to a tenant. Required — every backbone
	// domain row is tenant-owned, and an audit row is no exception.
	TenantID string

	// ActorID identifies who (or what) performed the action, e.g. a
	// user UUID or a service principal name. Either ActorID or
	// ActorEmail is required — some callers only have one of the two
	// on hand at the point they're recording the entry.
	ActorID string

	// ActorRole is the actor's role at the time of the action (e.g.
	// "owner", "admin"). Optional — stored as-is, not validated
	// against any role registry, since that varies per adopting
	// service.
	ActorRole string

	// ActorEmail is the actor's email at the time of the action.
	// Either ActorID or ActorEmail is required; see ActorID.
	ActorEmail string

	// Action names what happened, e.g. "tenant.update",
	// "file.download". Required. Convention is
	// "<resource_type>.<verb>", but this package does not enforce a
	// format beyond non-empty.
	Action string

	// ResourceType names the kind of thing acted on, e.g. "tenant",
	// "file". Required.
	ResourceType string

	// ResourceID identifies the specific resource instance. Optional —
	// some actions (e.g. a bulk export) don't have a single resource
	// ID.
	ResourceID string

	// Decision is "" (not applicable), "allow", "deny", or "error".
	// Validate rejects any other non-empty value.
	Decision string

	// Reason is a free-text explanation, e.g. why a decision was
	// "deny". Optional.
	Reason string

	// Before is the resource state prior to Action, as raw JSON. Nil
	// (the zero value) is written as SQL NULL, not an empty object —
	// callers that never capture a diff (e.g. a pure access-decision
	// log) leave this nil rather than being forced to fabricate one.
	Before json.RawMessage

	// After is the resource state following Action, as raw JSON. Same
	// nil-means-NULL rule as Before.
	After json.RawMessage

	// RequestID correlates this entry with the request/trace that
	// produced it. Optional.
	RequestID string

	// IP is the actor's network address as seen by the caller.
	// Optional.
	IP string

	// UserAgent is the actor's client user agent. Optional.
	UserAgent string

	// CreatedAt is when the action happened. Left zero, Record
	// defaults it to time.Now().UTC(). Query orders newest-first on
	// this column, so backdating it changes ordering.
	CreatedAt time.Time
}

// withDefaults returns a copy of e with ID / CreatedAt filled in
// where the caller left them zero. Called by Store.Record before
// Validate, so Validate only ever has to check fields with no sane
// default. Before/After are deliberately left untouched here — unlike
// backbone-queue/outbox's Payload, a nil diff has a real, distinct
// meaning ("no diff captured") and must reach Record as nil so it
// becomes SQL NULL, not "{}"json.
func (e Entry) withDefaults() Entry {
	if e.ID == "" {
		e.ID = newUUIDv4()
	}
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now().UTC()
	}
	return e
}

// Validate reports every problem with e at once (not just the
// first), so a caller fixing a hand-built Entry doesn't have to
// re-run it field by field.
func (e Entry) Validate() error {
	var problems []string
	if e.TenantID == "" {
		problems = append(problems, "TenantID is required")
	}
	if e.ActorID == "" && e.ActorEmail == "" {
		problems = append(problems, "ActorID or ActorEmail is required")
	}
	if e.Action == "" {
		problems = append(problems, "Action is required")
	}
	if e.ResourceType == "" {
		problems = append(problems, "ResourceType is required")
	}
	if e.Decision != "" && !validDecisions[e.Decision] {
		problems = append(problems, fmt.Sprintf("Decision %q is invalid (must be \"\", %q, %q, or %q)", e.Decision, DecisionAllow, DecisionDeny, DecisionError))
	}
	if len(problems) > 0 {
		return fmt.Errorf("audit: invalid entry: %s", strings.Join(problems, "; "))
	}
	return nil
}

// newUUIDv4 generates an RFC 4122 version-4 UUID using crypto/rand.
// Implemented locally (rather than pulling in google/uuid) to keep
// this module's dependency surface at zero — see go.mod.
func newUUIDv4() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand.Read reading from the OS's CSPRNG does not fail
		// on any platform this module targets. If it ever does, every
		// other security-sensitive operation in the process is already
		// compromised — panicking here is louder and safer than
		// silently minting a low-entropy or all-zero ID that could
		// collide with another row's ID.
		panic("audit: crypto/rand read failed: " + err.Error())
	}
	// Version 4: set the 4 most significant bits of the 7th byte to 0100.
	b[6] = (b[6] & 0x0f) | 0x40
	// Variant 10xx: set the 2 most significant bits of the 9th byte to 10.
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
