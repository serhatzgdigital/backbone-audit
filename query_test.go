package audit

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func sampleQueryRow(id string, createdAt time.Time) []driver.Value {
	return []driver.Value{
		id, "tenant-1", "actor-1", "owner", "owner@example.com", "tenant.update", "tenant", "tenant-1",
		"allow", "reason", []byte(`{"a":1}`), nil, "req-1", "127.0.0.1", "ua", createdAt,
	}
}

func queryCols() []string {
	return []string{
		"id", "tenant_id", "actor_id", "actor_role", "actor_email", "action", "resource_type", "resource_id",
		"decision", "reason", "before_state", "after_state", "request_id", "ip", "user_agent", "created_at",
	}
}

func TestStore_Query_NoFilters_NoWhereClause(t *testing.T) {
	state := &fakeState{
		queryCols: queryCols(),
		queryRows: nil,
	}
	db := openFakeDB(t, state)
	s, _ := NewStore("tenant_audit")

	entries, next, err := s.Query(context.Background(), db, Filter{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(entries) != 0 || next != "" {
		t.Errorf("Query() = %v, %q, want empty", entries, next)
	}
	call := state.lastQuery(t)
	if strings.Contains(call.query, "WHERE") {
		t.Errorf("query = %q, want no WHERE clause for an empty Filter", call.query)
	}
	if !strings.Contains(call.query, "ORDER BY created_at DESC, id DESC") {
		t.Errorf("query = %q, want ORDER BY created_at DESC, id DESC", call.query)
	}
	// Limit defaults to 100, so the LIMIT arg (and placeholder) should be 101.
	if len(call.args) != 1 {
		t.Fatalf("args = %v, want exactly [limit+1]", call.args)
	}
	if got := fmt.Sprintf("%v", call.args[0]); got != "101" {
		t.Errorf("limit arg = %v, want 101 (default 100 + 1)", got)
	}
}

func TestStore_Query_BuildsWhereOnlyForSetFilters(t *testing.T) {
	state := &fakeState{queryCols: queryCols()}
	db := openFakeDB(t, state)
	s, _ := NewStore("tenant_audit")

	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f := Filter{
		TenantID: "tenant-1",
		Action:   "tenant.update",
		From:     from,
	}
	if _, _, err := s.Query(context.Background(), db, f); err != nil {
		t.Fatalf("Query: %v", err)
	}

	call := state.lastQuery(t)
	if !strings.Contains(call.query, "tenant_id = $1") {
		t.Errorf("query = %q, want tenant_id = $1", call.query)
	}
	if !strings.Contains(call.query, "action = $2") {
		t.Errorf("query = %q, want action = $2", call.query)
	}
	if !strings.Contains(call.query, "created_at >= $3") {
		t.Errorf("query = %q, want created_at >= $3", call.query)
	}
	// Filters not set (ActorID, ResourceType, ResourceID, Decision, To)
	// must not appear at all.
	for _, absent := range []string{"actor_id =", "resource_type =", "resource_id =", "decision =", "created_at <="} {
		if strings.Contains(call.query, absent) {
			t.Errorf("query = %q, must not contain unset filter %q", call.query, absent)
		}
	}
	// LIMIT placeholder should be $4 (3 filters + limit).
	if !strings.Contains(call.query, "LIMIT $4") {
		t.Errorf("query = %q, want LIMIT $4", call.query)
	}
	if len(call.args) != 4 {
		t.Fatalf("args = %v, want 4 args (tenant_id, action, from, limit+1)", call.args)
	}
}

func TestStore_Query_LimitDefaultAndCap(t *testing.T) {
	cases := []struct {
		name      string
		limit     int
		wantLimit int
	}{
		{"zero defaults to 100", 0, 100},
		{"negative defaults to 100", -5, 100},
		{"within range kept as-is", 250, 250},
		{"exactly max kept as-is", 1000, 1000},
		{"above max capped to 1000", 5000, 1000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := &fakeState{queryCols: queryCols()}
			db := openFakeDB(t, state)
			s, _ := NewStore("tenant_audit")

			if _, _, err := s.Query(context.Background(), db, Filter{Limit: tc.limit}); err != nil {
				t.Fatalf("Query: %v", err)
			}
			call := state.lastQuery(t)
			want := tc.wantLimit + 1
			got := fmt.Sprintf("%v", call.args[len(call.args)-1])
			if got != fmt.Sprintf("%v", want) {
				t.Errorf("limit arg = %v, want %v", got, want)
			}
		})
	}
}

func TestStore_Query_ReturnsNextCursorWhenMoreRowsExist(t *testing.T) {
	t1 := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 9, 3, 9, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 9, 3, 8, 0, 0, 0, time.UTC)
	state := &fakeState{
		queryCols: queryCols(),
		queryRows: [][]driver.Value{
			sampleQueryRow("row-1", t1),
			sampleQueryRow("row-2", t2),
			sampleQueryRow("row-3", t3),
		},
	}
	db := openFakeDB(t, state)
	s, _ := NewStore("tenant_audit")

	entries, next, err := s.Query(context.Background(), db, Filter{Limit: 2})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("len(entries) = %d, want 2 (capped at Limit)", len(entries))
	}
	if entries[0].ID != "row-1" || entries[1].ID != "row-2" {
		t.Errorf("entries = %+v, unexpected ids", entries)
	}
	if next == "" {
		t.Fatal("next cursor = \"\", want non-empty since a 3rd row exists")
	}

	key, err := decodeCursor(next)
	if err != nil {
		t.Fatalf("decodeCursor(%q): %v", next, err)
	}
	if key.id != "row-2" || !key.createdAt.Equal(t2) {
		t.Errorf("decoded cursor = %+v, want row-2 / %v", key, t2)
	}
}

func TestStore_Query_NoNextCursorWhenExactlyLimitRows(t *testing.T) {
	t1 := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	state := &fakeState{
		queryCols: queryCols(),
		queryRows: [][]driver.Value{sampleQueryRow("row-1", t1)},
	}
	db := openFakeDB(t, state)
	s, _ := NewStore("tenant_audit")

	entries, next, err := s.Query(context.Background(), db, Filter{Limit: 5})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(entries))
	}
	if next != "" {
		t.Errorf("next cursor = %q, want empty (no more rows)", next)
	}
}

func TestStore_Query_DecodesCursorIntoExtraPredicate(t *testing.T) {
	state := &fakeState{queryCols: queryCols()}
	db := openFakeDB(t, state)
	s, _ := NewStore("tenant_audit")

	createdAt := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	cursor := encodeCursor(createdAt, "row-2")

	if _, _, err := s.Query(context.Background(), db, Filter{TenantID: "tenant-1", Cursor: cursor}); err != nil {
		t.Fatalf("Query: %v", err)
	}
	call := state.lastQuery(t)
	// tenant_id is $1, cursor predicate uses $2 (created_at) and $3 (id).
	if !strings.Contains(call.query, "(created_at < $2 OR (created_at = $2 AND id < $3))") {
		t.Errorf("query = %q, want the keyset cursor predicate", call.query)
	}
	if len(call.args) != 4 { // tenant_id, cursor created_at, cursor id, limit+1
		t.Fatalf("args = %v, want 4 args", call.args)
	}
	if gt, ok := call.args[1].(time.Time); !ok || !gt.Equal(createdAt) {
		t.Errorf("args[1] = %v, want cursor createdAt %v", call.args[1], createdAt)
	}
	if call.args[2] != "row-2" {
		t.Errorf("args[2] = %v, want cursor id row-2", call.args[2])
	}
}

func TestStore_Query_InvalidCursorReturnsErrorWithoutQuerying(t *testing.T) {
	state := &fakeState{queryCols: queryCols()}
	db := openFakeDB(t, state)
	s, _ := NewStore("tenant_audit")

	_, _, err := s.Query(context.Background(), db, Filter{Cursor: "not-valid-base64!!!"})
	if err == nil {
		t.Fatal("Query with an invalid cursor = nil error, want a rejection")
	}
	if state.queryCount() != 0 {
		t.Errorf("Query executed SQL despite an invalid cursor: %d call(s)", state.queryCount())
	}
}

func TestStore_Query_ScansBeforeAfterNullAsNilRawMessage(t *testing.T) {
	t1 := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	state := &fakeState{
		queryCols: queryCols(),
		queryRows: [][]driver.Value{sampleQueryRow("row-1", t1)}, // after_state is nil in sampleQueryRow
	}
	db := openFakeDB(t, state)
	s, _ := NewStore("tenant_audit")

	entries, _, err := s.Query(context.Background(), db, Filter{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(entries))
	}
	if string(entries[0].Before) != `{"a":1}` {
		t.Errorf("Before = %q, want {\"a\":1}", string(entries[0].Before))
	}
	if entries[0].After != nil {
		t.Errorf("After = %v, want nil for a NULL after_state column", entries[0].After)
	}
}

func TestStore_Query_WrapsQueryError(t *testing.T) {
	wantErr := errors.New("connection reset")
	state := &fakeState{queryErr: wantErr}
	db := openFakeDB(t, state)
	s, _ := NewStore("tenant_audit")

	_, _, err := s.Query(context.Background(), db, Filter{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Query error = %v, want it to wrap %v", err, wantErr)
	}
}

func TestCursor_RoundTrip(t *testing.T) {
	createdAt := time.Date(2026, 9, 3, 10, 30, 0, 123456789, time.UTC)
	encoded := encodeCursor(createdAt, "abc-123")
	key, err := decodeCursor(encoded)
	if err != nil {
		t.Fatalf("decodeCursor: %v", err)
	}
	if !key.createdAt.Equal(createdAt) || key.id != "abc-123" {
		t.Errorf("round trip = %+v, want createdAt=%v id=abc-123", key, createdAt)
	}
}

func TestDecodeCursor_RejectsMalformed(t *testing.T) {
	for _, bad := range []string{"", "not base64!!", "aGVsbG8", "MjAyNi0wOS0wM3wxMjM="} {
		if _, err := decodeCursor(bad); err == nil {
			t.Errorf("decodeCursor(%q) = nil error, want a rejection", bad)
		}
	}
}
