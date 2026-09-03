package audit

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
)

// ── Fake database/sql driver ──────────────────────────────────────
//
// Store/Retention methods take an Execer/Querier/DB (satisfied by
// *sql.DB and *sql.Tx), not a concrete Postgres connection, so they
// can be exercised end to end without a real database: this registers
// a minimal driver.Driver (stdlib interfaces only, no new module)
// that records every ExecContext/QueryContext call (query text +
// args) and returns canned results — the same pattern used by
// backbone-queue/outbox's store_test.go.

var fakeDriverSeq int64

type fakeCall struct {
	query string
	args  []any
}

// fakeState is the shared, in-memory backing for one test's *sql.DB.
// It records every call made against it and can be primed with
// canned query results / errors.
type fakeState struct {
	mu sync.Mutex

	execCalls  []fakeCall
	queryCalls []fakeCall

	execErr  error
	queryErr error

	rowsAffected int64

	// queryCols/queryRows back every QueryContext call made while this
	// state is active. Each row is a slice of driver.Value in column
	// order. Tests that don't care about query results leave these
	// nil (QueryContext then returns zero rows). queryRowsSeq, if set,
	// overrides queryRows per successive QueryContext call (index i
	// for the i-th call, clamped to the last entry) — used by
	// retention tests that need different results across loop
	// iterations.
	queryCols    []string
	queryRows    [][]driver.Value
	queryRowsSeq [][][]driver.Value

	// execRowsAffectedSeq, if set, overrides rowsAffected per
	// successive ExecContext call the same way queryRowsSeq does.
	execRowsAffectedSeq []int64
}

func (s *fakeState) recordExec(query string, args []driver.NamedValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.execCalls = append(s.execCalls, fakeCall{query: query, args: namedValueArgs(args)})
}

func (s *fakeState) recordQuery(query string, args []driver.NamedValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queryCalls = append(s.queryCalls, fakeCall{query: query, args: namedValueArgs(args)})
}

func namedValueArgs(args []driver.NamedValue) []any {
	out := make([]any, len(args))
	for i, a := range args {
		out[i] = a.Value
	}
	return out
}

func (s *fakeState) lastExec(t *testing.T) fakeCall {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.execCalls) == 0 {
		t.Fatal("expected at least one ExecContext call, got none")
	}
	return s.execCalls[len(s.execCalls)-1]
}

func (s *fakeState) lastQuery(t *testing.T) fakeCall {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queryCalls) == 0 {
		t.Fatal("expected at least one QueryContext call, got none")
	}
	return s.queryCalls[len(s.queryCalls)-1]
}

func (s *fakeState) execCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.execCalls)
}

func (s *fakeState) queryCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queryCalls)
}

// openFakeDB registers a fresh, uniquely-named fake driver backed by
// state and returns a *sql.DB using it. Each test gets its own driver
// name so parallel/sequential test runs never collide on sql.Register.
func openFakeDB(t *testing.T, state *fakeState) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("fakeaudit-%d", atomic.AddInt64(&fakeDriverSeq, 1))
	sql.Register(name, &fakeDriver{state: state})
	db, err := sql.Open(name, "fake")
	if err != nil {
		t.Fatalf("sql.Open(%s): %v", name, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type fakeDriver struct{ state *fakeState }

func (d *fakeDriver) Open(string) (driver.Conn, error) {
	return &fakeConn{state: d.state}, nil
}

type fakeConn struct{ state *fakeState }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fakeConn: Prepare not supported (Store never calls it)")
}
func (c *fakeConn) Close() error { return nil }
func (c *fakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("fakeConn: Begin not supported (Store never calls it)")
}

func (c *fakeConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.state.recordExec(query, args)
	if c.state.execErr != nil {
		return nil, c.state.execErr
	}
	c.state.mu.Lock()
	n := c.state.rowsAffected
	if len(c.state.execRowsAffectedSeq) > 0 {
		idx := len(c.state.execCalls) - 1
		if idx >= len(c.state.execRowsAffectedSeq) {
			idx = len(c.state.execRowsAffectedSeq) - 1
		}
		n = c.state.execRowsAffectedSeq[idx]
	}
	c.state.mu.Unlock()
	return driver.RowsAffected(n), nil
}

func (c *fakeConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.state.recordQuery(query, args)
	if c.state.queryErr != nil {
		return nil, c.state.queryErr
	}
	c.state.mu.Lock()
	rowSrc := c.state.queryRows
	if len(c.state.queryRowsSeq) > 0 {
		idx := len(c.state.queryCalls) - 1
		if idx >= len(c.state.queryRowsSeq) {
			idx = len(c.state.queryRowsSeq) - 1
		}
		rowSrc = c.state.queryRowsSeq[idx]
	}
	cols := c.state.queryCols
	c.state.mu.Unlock()

	rows := make([][]driver.Value, len(rowSrc))
	copy(rows, rowSrc)
	return &fakeRows{cols: cols, rows: rows}, nil
}

type fakeRows struct {
	cols []string
	rows [][]driver.Value
	pos  int
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.pos])
	r.pos++
	return nil
}
