package audit

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// defaultQueryLimit and maxQueryLimit bound Filter.Limit — see Query.
const (
	defaultQueryLimit = 100
	maxQueryLimit     = 1000
)

// Filter narrows a Query call. Every field is optional; only non-zero
// fields contribute a WHERE predicate — an empty Filter{} returns the
// newest rows in the table, unfiltered.
type Filter struct {
	TenantID     string
	ActorID      string
	Action       string
	ResourceType string
	ResourceID   string
	Decision     string

	// From/To bound created_at (inclusive on both ends). Zero means
	// unbounded on that side.
	From, To time.Time

	// Limit caps how many entries Query returns. <= 0 defaults to 100;
	// values above 1000 are capped at 1000.
	Limit int

	// Cursor resumes a previous Query call: pass the nextCursor a
	// prior call returned to get the next page. Empty starts from the
	// newest row.
	Cursor string
}

// Query returns entries matching f, newest first (ORDER BY created_at
// DESC, id DESC — id is a tiebreaker for rows sharing a timestamp).
// When more rows exist beyond the page, nextCursor is non-empty; pass
// it back as the next call's Filter.Cursor to continue. An empty
// nextCursor means this was the last page.
func (s *Store) Query(ctx context.Context, q Querier, f Filter) ([]Entry, string, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = defaultQueryLimit
	} else if limit > maxQueryLimit {
		limit = maxQueryLimit
	}

	var conds []string
	var args []any
	add := func(cond string, val any) {
		args = append(args, val)
		conds = append(conds, fmt.Sprintf(cond, len(args)))
	}

	if f.TenantID != "" {
		add("tenant_id = $%d", f.TenantID)
	}
	if f.ActorID != "" {
		add("actor_id = $%d", f.ActorID)
	}
	if f.Action != "" {
		add("action = $%d", f.Action)
	}
	if f.ResourceType != "" {
		add("resource_type = $%d", f.ResourceType)
	}
	if f.ResourceID != "" {
		add("resource_id = $%d", f.ResourceID)
	}
	if f.Decision != "" {
		add("decision = $%d", f.Decision)
	}
	if !f.From.IsZero() {
		add("created_at >= $%d", f.From)
	}
	if !f.To.IsZero() {
		add("created_at <= $%d", f.To)
	}
	if f.Cursor != "" {
		key, err := decodeCursor(f.Cursor)
		if err != nil {
			return nil, "", err
		}
		args = append(args, key.createdAt, key.id)
		ts, id := len(args)-1, len(args)
		conds = append(conds, fmt.Sprintf("(created_at < $%d OR (created_at = $%d AND id < $%d))", ts, ts, id))
	}

	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	args = append(args, limit+1) // +1 so we can tell whether another page follows
	query := fmt.Sprintf(`SELECT
		id, tenant_id, actor_id, actor_role, actor_email, action, resource_type, resource_id,
		decision, reason, before_state, after_state, request_id, ip, user_agent, created_at
	FROM %s
	%s
	ORDER BY created_at DESC, id DESC
	LIMIT $%d`, s.table, where, len(args))

	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("audit: query %s: %w", s.table, err)
	}
	defer rows.Close()

	var out []Entry
	for rows.Next() {
		var e Entry
		var before, after []byte
		if err := rows.Scan(
			&e.ID, &e.TenantID, &e.ActorID, &e.ActorRole, &e.ActorEmail, &e.Action, &e.ResourceType, &e.ResourceID,
			&e.Decision, &e.Reason, &before, &after, &e.RequestID, &e.IP, &e.UserAgent, &e.CreatedAt,
		); err != nil {
			return nil, "", fmt.Errorf("audit: scan row from %s: %w", s.table, err)
		}
		if len(before) > 0 {
			e.Before = json.RawMessage(before)
		}
		if len(after) > 0 {
			e.After = json.RawMessage(after)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("audit: iterate rows from %s: %w", s.table, err)
	}

	nextCursor := ""
	if len(out) > limit {
		last := out[limit-1]
		nextCursor = encodeCursor(last.CreatedAt, last.ID)
		out = out[:limit]
	}
	return out, nextCursor, nil
}

// cursorKey is the decoded form of an opaque Filter.Cursor value.
type cursorKey struct {
	createdAt time.Time
	id        string
}

// encodeCursor packs a keyset-pagination position into the opaque
// string Query hands back as nextCursor: base64(RawURL) of
// "<created_at RFC3339Nano>|<id>". Callers must treat the result as
// opaque — its shape may change in a later version.
func encodeCursor(createdAt time.Time, id string) string {
	raw := createdAt.UTC().Format(time.RFC3339Nano) + "|" + id
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeCursor reverses encodeCursor, rejecting anything that isn't a
// well-formed cursor this package could have produced.
func decodeCursor(cursor string) (cursorKey, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return cursorKey{}, fmt.Errorf("audit: invalid cursor: %w", err)
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 || parts[1] == "" {
		return cursorKey{}, fmt.Errorf("audit: invalid cursor: malformed payload")
	}
	ts, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return cursorKey{}, fmt.Errorf("audit: invalid cursor: bad timestamp: %w", err)
	}
	return cursorKey{createdAt: ts, id: parts[1]}, nil
}
