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

func TestNewStore_AcceptsValidIdentifiers(t *testing.T) {
	for _, name := range []string{"tenant_audit", "audit", "_audit", "a1_b2"} {
		if _, err := NewStore(name); err != nil {
			t.Errorf("NewStore(%q) = %v, want nil error", name, err)
		}
	}
}

func TestNewStore_RejectsInvalidIdentifiers(t *testing.T) {
	for _, name := range []string{
		"audit; drop table",
		"Audit",        // uppercase
		"1audit",       // leading digit
		"tenant-audit", // hyphen
		"tenant audit", // space
		"",
	} {
		if _, err := NewStore(name); err == nil {
			t.Errorf("NewStore(%q) = nil error, want a rejection", name)
		}
	}
}

func TestStore_DDL_ContainsTableAndIndexes(t *testing.T) {
	s, err := NewStore("tenant_audit")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	ddl := s.DDL()

	if !strings.Contains(ddl, "CREATE TABLE IF NOT EXISTS tenant_audit") {
		t.Errorf("DDL missing CREATE TABLE for tenant_audit:\n%s", ddl)
	}
	if !strings.Contains(ddl, "before_state   JSONB") && !strings.Contains(ddl, "before_state JSONB") {
		t.Errorf("DDL missing before_state JSONB column:\n%s", ddl)
	}
	if !strings.Contains(ddl, "after_state") {
		t.Errorf("DDL missing after_state column:\n%s", ddl)
	}
	for _, idx := range []string{
		"tenant_audit_tenant_created_idx",
		"tenant_audit_actor_created_idx",
		"tenant_audit_resource_idx",
		"tenant_audit_action_idx",
	} {
		if !strings.Contains(ddl, idx) {
			t.Errorf("DDL missing index %q:\n%s", idx, ddl)
		}
		if !strings.Contains(ddl, "CREATE INDEX IF NOT EXISTS "+idx) {
			t.Errorf("DDL index %q not declared IF NOT EXISTS:\n%s", idx, ddl)
		}
	}
}

func TestStore_Record_SQLAndArgOrder(t *testing.T) {
	state := &fakeState{rowsAffected: 1}
	db := openFakeDB(t, state)
	s, err := NewStore("tenant_audit")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	createdAt := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	e := Entry{
		ID:           "id-1",
		TenantID:     "tenant-1",
		ActorID:      "actor-1",
		ActorRole:    "owner",
		ActorEmail:   "owner@example.com",
		Action:       "tenant.update",
		ResourceType: "tenant",
		ResourceID:   "tenant-1",
		Decision:     DecisionAllow,
		Reason:       "self-service update",
		Before:       []byte(`{"name":"old"}`),
		After:        []byte(`{"name":"new"}`),
		RequestID:    "req-1",
		IP:           "127.0.0.1",
		UserAgent:    "test-agent",
		CreatedAt:    createdAt,
	}

	if err := s.Record(context.Background(), db, e); err != nil {
		t.Fatalf("Record: %v", err)
	}

	call := state.lastExec(t)
	if !strings.Contains(call.query, "INSERT INTO tenant_audit") {
		t.Errorf("query = %q, want it to insert into tenant_audit", call.query)
	}
	wantArgs := []any{
		e.ID, e.TenantID, e.ActorID, e.ActorRole, e.ActorEmail, e.Action, e.ResourceType, e.ResourceID,
		e.Decision, e.Reason, string(e.Before), string(e.After), e.RequestID, e.IP, e.UserAgent, e.CreatedAt,
	}
	if len(call.args) != len(wantArgs) {
		t.Fatalf("args = %v, want %d args got %d", call.args, len(wantArgs), len(call.args))
	}
	for i, want := range wantArgs {
		got := call.args[i]
		if gt, ok := got.(time.Time); ok {
			if wt, ok := want.(time.Time); !ok || !gt.Equal(wt) {
				t.Errorf("arg[%d] = %v, want %v", i, got, want)
			}
			continue
		}
		if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", want) {
			t.Errorf("arg[%d] = %v, want %v", i, got, want)
		}
	}
}

func TestStore_Record_NilBeforeAfterBecomeNullArgs(t *testing.T) {
	state := &fakeState{rowsAffected: 1}
	db := openFakeDB(t, state)
	s, _ := NewStore("tenant_audit")

	e := Entry{
		TenantID:     "tenant-1",
		ActorID:      "actor-1",
		Action:       "tenant.update",
		ResourceType: "tenant",
	}
	if err := s.Record(context.Background(), db, e); err != nil {
		t.Fatalf("Record: %v", err)
	}

	call := state.lastExec(t)
	// Before is arg index 10, After is arg index 11 (0-based), matching
	// the (id, tenant_id, actor_id, actor_role, actor_email, action,
	// resource_type, resource_id, decision, reason, before_state,
	// after_state, ...) column order.
	if call.args[10] != nil {
		t.Errorf("Before arg = %v, want nil (SQL NULL)", call.args[10])
	}
	if call.args[11] != nil {
		t.Errorf("After arg = %v, want nil (SQL NULL)", call.args[11])
	}
}

func TestStore_Record_DefaultsIDAndCreatedAt(t *testing.T) {
	state := &fakeState{rowsAffected: 1}
	db := openFakeDB(t, state)
	s, _ := NewStore("tenant_audit")

	e := Entry{
		TenantID:     "tenant-1",
		ActorID:      "actor-1",
		Action:       "tenant.update",
		ResourceType: "tenant",
	}
	if err := s.Record(context.Background(), db, e); err != nil {
		t.Fatalf("Record: %v", err)
	}

	call := state.lastExec(t)
	id, _ := call.args[0].(string)
	if id == "" {
		t.Error("Record did not default an empty ID")
	}
	createdAt, ok := call.args[15].(time.Time)
	if !ok || createdAt.IsZero() {
		t.Errorf("Record did not default CreatedAt, got %v", call.args[15])
	}
}

func TestStore_Record_ValidateErrorPreventsExec(t *testing.T) {
	state := &fakeState{rowsAffected: 1}
	db := openFakeDB(t, state)
	s, _ := NewStore("tenant_audit")

	err := s.Record(context.Background(), db, Entry{}) // missing every required field
	if err == nil {
		t.Fatal("Record with an invalid Entry = nil error, want validation error")
	}
	if state.execCount() != 0 {
		t.Errorf("Record executed SQL despite a validation error: %d exec call(s)", state.execCount())
	}
}

func TestStore_Record_WrapsExecError(t *testing.T) {
	wantErr := errors.New("connection reset")
	state := &fakeState{execErr: wantErr}
	db := openFakeDB(t, state)
	s, _ := NewStore("tenant_audit")

	e := Entry{
		TenantID:     "tenant-1",
		ActorID:      "actor-1",
		Action:       "tenant.update",
		ResourceType: "tenant",
	}
	err := s.Record(context.Background(), db, e)
	if !errors.Is(err, wantErr) {
		t.Fatalf("Record error = %v, want it to wrap %v", err, wantErr)
	}
}

func TestStore_CountOlderThan(t *testing.T) {
	state := &fakeState{
		queryCols: []string{"count"},
		queryRows: [][]driver.Value{{int64(7)}},
	}
	db := openFakeDB(t, state)
	s, _ := NewStore("tenant_audit")

	cutoff := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	n, err := s.CountOlderThan(context.Background(), db, cutoff)
	if err != nil {
		t.Fatalf("CountOlderThan: %v", err)
	}
	if n != 7 {
		t.Errorf("CountOlderThan = %d, want 7", n)
	}
	call := state.lastQuery(t)
	if !strings.Contains(call.query, "COUNT(*)") || !strings.Contains(call.query, "created_at < $1") {
		t.Errorf("query = %q, want COUNT(*) ... created_at < $1", call.query)
	}
	if len(call.args) != 1 {
		t.Fatalf("args = %v, want [cutoff]", call.args)
	}
	if gt, ok := call.args[0].(time.Time); !ok || !gt.Equal(cutoff) {
		t.Errorf("args[0] = %v, want cutoff %v", call.args[0], cutoff)
	}
}

func TestStore_CountOlderThan_WrapsQueryError(t *testing.T) {
	wantErr := errors.New("connection reset")
	state := &fakeState{queryErr: wantErr}
	db := openFakeDB(t, state)
	s, _ := NewStore("tenant_audit")

	_, err := s.CountOlderThan(context.Background(), db, time.Now())
	if !errors.Is(err, wantErr) {
		t.Fatalf("CountOlderThan error = %v, want it to wrap %v", err, wantErr)
	}
}

func TestStore_DeleteOlderThanBatch_SQLContainsCTEAndLimit(t *testing.T) {
	state := &fakeState{rowsAffected: 42}
	db := openFakeDB(t, state)
	s, _ := NewStore("tenant_audit")

	cutoff := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	n, err := s.DeleteOlderThanBatch(context.Background(), db, cutoff, 500)
	if err != nil {
		t.Fatalf("DeleteOlderThanBatch: %v", err)
	}
	if n != 42 {
		t.Errorf("deleted = %d, want 42", n)
	}
	call := state.lastExec(t)
	if !strings.Contains(call.query, "WITH victims AS") {
		t.Errorf("query = %q, want a WITH victims CTE", call.query)
	}
	if !strings.Contains(call.query, "LIMIT $2") {
		t.Errorf("query = %q, want LIMIT $2", call.query)
	}
	if !strings.Contains(call.query, "DELETE FROM tenant_audit") {
		t.Errorf("query = %q, want DELETE FROM tenant_audit", call.query)
	}
	if len(call.args) != 2 {
		t.Fatalf("args = %v, want cutoff + batch", call.args)
	}
	if gt, ok := call.args[0].(time.Time); !ok || !gt.Equal(cutoff) {
		t.Errorf("args[0] = %v, want cutoff %v", call.args[0], cutoff)
	}
	if fmt.Sprintf("%v", call.args[1]) != "500" {
		t.Errorf("args[1] = %v, want batch 500", call.args[1])
	}
}

func TestStore_DeleteOlderThanBatch_DefaultsBatch(t *testing.T) {
	state := &fakeState{rowsAffected: 0}
	db := openFakeDB(t, state)
	s, _ := NewStore("tenant_audit")

	if _, err := s.DeleteOlderThanBatch(context.Background(), db, time.Now(), 0); err != nil {
		t.Fatalf("DeleteOlderThanBatch: %v", err)
	}
	call := state.lastExec(t)
	if fmt.Sprintf("%v", call.args[1]) != "5000" {
		t.Errorf("args[1] = %v, want default batch 5000", call.args[1])
	}
}

func TestStore_DeleteOlderThanBatch_WrapsExecError(t *testing.T) {
	wantErr := errors.New("deadlock detected")
	state := &fakeState{execErr: wantErr}
	db := openFakeDB(t, state)
	s, _ := NewStore("tenant_audit")

	_, err := s.DeleteOlderThanBatch(context.Background(), db, time.Now(), 100)
	if !errors.Is(err, wantErr) {
		t.Fatalf("DeleteOlderThanBatch error = %v, want it to wrap %v", err, wantErr)
	}
}
