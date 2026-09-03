package audit

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"time"
)

// Execer is the write half of database/sql that Store needs. Both
// *sql.DB and *sql.Tx satisfy it, which is the whole point: callers
// pass their own in-flight *sql.Tx to Record so the audit row commits
// atomically with the domain write that produced it (see doc.go).
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Querier is the read half. Both *sql.DB and *sql.Tx satisfy it too,
// though in practice Query/CountOlderThan are called against a plain
// *sql.DB by a read endpoint or the Retention worker, not inside a
// producer's write transaction.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// DB is the combination Retention depends on: a long-lived handle it
// can both Exec (DeleteOlderThanBatch) and Query (CountOlderThan)
// against across many ticks. *sql.DB is the expected implementation;
// *sql.Tx also satisfies it but handing Retention a single
// transaction would hold it open for the process lifetime, which is
// almost certainly not what a caller wants.
type DB interface {
	Execer
	Querier
}

// identifierRe guards every table name this package is asked to
// operate on. It is deliberately conservative (lowercase snake_case,
// must start with a letter or underscore) rather than merely "no
// semicolons": table names here are interpolated directly into SQL
// text (see the query builders below) because $n placeholders cannot
// parameterize an identifier, so this is the only thing standing
// between a misused constructor and SQL injection.
var identifierRe = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// Store is a typed accessor over one service's audit table. It holds
// no connection — every method takes the Execer/Querier/DB to run
// against — so a single Store can be shared across a producer's write
// path (via Execer = *sql.Tx) and a read endpoint / Retention worker
// (via DB = *sql.DB).
type Store struct {
	table string
}

// NewStore validates table as a bare SQL identifier and returns a
// Store bound to it. Use the same table name that DDL() was pasted in
// with (see ddl.go).
func NewStore(table string) (*Store, error) {
	if !identifierRe.MatchString(table) {
		return nil, fmt.Errorf("audit: invalid table name %q: must match %s", table, identifierRe.String())
	}
	return &Store{table: table}, nil
}

// Record defaults and validates e (see Entry.withDefaults /
// Entry.Validate), then writes it as one row. ex is normally the
// caller's own in-flight *sql.Tx so the audit row commits atomically
// with the domain write it describes (see doc.go) — the main upgrade
// over a service's own best-effort, non-transactional audit write.
func (s *Store) Record(ctx context.Context, ex Execer, e Entry) error {
	e = e.withDefaults()
	if err := e.Validate(); err != nil {
		return err
	}

	query := fmt.Sprintf(`INSERT INTO %s (
		id, tenant_id, actor_id, actor_role, actor_email, action, resource_type, resource_id,
		decision, reason, before_state, after_state, request_id, ip, user_agent, created_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12::jsonb, $13, $14, $15, $16)`, s.table)

	_, err := ex.ExecContext(ctx, query,
		e.ID, e.TenantID, e.ActorID, e.ActorRole, e.ActorEmail, e.Action, e.ResourceType, e.ResourceID,
		e.Decision, e.Reason, jsonArg(e.Before), jsonArg(e.After), e.RequestID, e.IP, e.UserAgent, e.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("audit: record into %s: %w", s.table, err)
	}
	return nil
}

// jsonArg converts a possibly-nil json.RawMessage into a driver arg:
// nil/empty becomes a real SQL NULL (via the $n::jsonb cast in
// Record's query, which accepts a NULL argument fine), a non-empty
// value becomes its string form. This is the mechanism behind
// Entry.Before/After's "nil means NULL, not {}" contract — see
// doc.go and entry.go.
func jsonArg(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	return string(raw)
}

// CountOlderThan returns the number of rows strictly older than
// cutoff. Used by Retention in DryRun mode (see retention.go) to
// report how many rows a real run would delete, without deleting
// anything.
func (s *Store) CountOlderThan(ctx context.Context, q Querier, cutoff time.Time) (int64, error) {
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE created_at < $1`, s.table)
	rows, err := q.QueryContext(ctx, query, cutoff)
	if err != nil {
		return 0, fmt.Errorf("audit: count older than in %s: %w", s.table, err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("audit: count older than in %s: %w", s.table, err)
		}
		return 0, fmt.Errorf("audit: count older than in %s: query returned no rows", s.table)
	}
	var n int64
	if err := rows.Scan(&n); err != nil {
		return 0, fmt.Errorf("audit: scan count from %s: %w", s.table, err)
	}
	return n, nil
}

// DeleteOlderThanBatch deletes at most batch rows older than cutoff,
// in a single statement (batch <= 0 is treated as 5000). The CTE
// narrows the working set to exactly the rows to delete, so Postgres
// avoids both a table-wide lock and the long planning a naive
// `DELETE ... WHERE created_at < $1 LIMIT N` (which Postgres does not
// even accept) would trigger — the same pattern Auth Service's CM-8
// PurgeOldAudit uses against cm_access_audit. Call this repeatedly
// (as Retention.Tick does) until the returned count is 0 to fully
// drain a large backlog without one giant delete.
func (s *Store) DeleteOlderThanBatch(ctx context.Context, ex Execer, cutoff time.Time, batch int) (int64, error) {
	if batch <= 0 {
		batch = 5000
	}

	query := fmt.Sprintf(`WITH victims AS (
		SELECT id FROM %s
		WHERE created_at < $1
		ORDER BY created_at
		LIMIT $2
	)
	DELETE FROM %s
	WHERE id IN (SELECT id FROM victims)`, s.table, s.table)

	res, err := ex.ExecContext(ctx, query, cutoff, batch)
	if err != nil {
		return 0, fmt.Errorf("audit: delete older than in %s: %w", s.table, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("audit: rows affected deleting from %s: %w", s.table, err)
	}
	return n, nil
}
