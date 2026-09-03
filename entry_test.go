package audit

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestEntry_Validate(t *testing.T) {
	base := func() Entry {
		return Entry{
			TenantID:     "tenant-1",
			ActorID:      "actor-1",
			Action:       "tenant.update",
			ResourceType: "tenant",
		}
	}

	cases := []struct {
		name    string
		modify  func(e Entry) Entry
		wantErr bool
	}{
		{"ok with ActorID", func(e Entry) Entry { return e }, false},
		{"ok with ActorEmail only", func(e Entry) Entry {
			e.ActorID = ""
			e.ActorEmail = "actor@example.com"
			return e
		}, false},
		{"ok with empty Decision", func(e Entry) Entry { e.Decision = ""; return e }, false},
		{"ok with allow", func(e Entry) Entry { e.Decision = DecisionAllow; return e }, false},
		{"ok with deny", func(e Entry) Entry { e.Decision = DecisionDeny; return e }, false},
		{"ok with error", func(e Entry) Entry { e.Decision = DecisionError; return e }, false},
		{"missing TenantID", func(e Entry) Entry { e.TenantID = ""; return e }, true},
		{"missing Action", func(e Entry) Entry { e.Action = ""; return e }, true},
		{"missing ResourceType", func(e Entry) Entry { e.ResourceType = ""; return e }, true},
		{"missing ActorID and ActorEmail", func(e Entry) Entry { e.ActorID = ""; e.ActorEmail = ""; return e }, true},
		{"invalid Decision", func(e Entry) Entry { e.Decision = "maybe"; return e }, true},
		{"everything missing", func(e Entry) Entry { return Entry{} }, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.modify(base()).Validate()
			if tc.wantErr && err == nil {
				t.Errorf("Validate() = nil error, want an error")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("Validate() = %v, want nil error", err)
			}
		})
	}
}

func TestEntry_Validate_ReportsAllMissingFields(t *testing.T) {
	err := Entry{}.Validate()
	if err == nil {
		t.Fatal("Validate() = nil error, want an error for an entirely empty Entry")
	}
	msg := err.Error()
	for _, want := range []string{"TenantID", "ActorID or ActorEmail", "Action", "ResourceType"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q does not mention %q", msg, want)
		}
	}
}

func TestEntry_WithDefaults_FillsIDAndCreatedAt(t *testing.T) {
	e := Entry{
		TenantID:     "tenant-1",
		ActorID:      "actor-1",
		Action:       "tenant.update",
		ResourceType: "tenant",
	}
	d := e.withDefaults()
	if d.ID == "" {
		t.Error("withDefaults() did not fill ID")
	}
	if d.CreatedAt.IsZero() {
		t.Error("withDefaults() did not fill CreatedAt")
	}
	if d.CreatedAt.Location() != time.UTC {
		t.Errorf("withDefaults() CreatedAt location = %v, want UTC", d.CreatedAt.Location())
	}
}

func TestEntry_WithDefaults_PreservesExplicitIDAndCreatedAt(t *testing.T) {
	explicitTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	e := Entry{ID: "custom-id", CreatedAt: explicitTime}
	d := e.withDefaults()
	if d.ID != "custom-id" {
		t.Errorf("withDefaults() overwrote explicit ID: got %q", d.ID)
	}
	if !d.CreatedAt.Equal(explicitTime) {
		t.Errorf("withDefaults() overwrote explicit CreatedAt: got %v", d.CreatedAt)
	}
}

func TestEntry_WithDefaults_LeavesBeforeAfterNil(t *testing.T) {
	e := Entry{}
	d := e.withDefaults()
	if d.Before != nil {
		t.Errorf("withDefaults() set Before = %v, want nil (nil means SQL NULL)", d.Before)
	}
	if d.After != nil {
		t.Errorf("withDefaults() set After = %v, want nil (nil means SQL NULL)", d.After)
	}
}

func TestNewUUIDv4_LooksLikeAUUID(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id := newUUIDv4()
		if len(id) != 36 {
			t.Fatalf("newUUIDv4() = %q, want length 36", id)
		}
		if id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
			t.Fatalf("newUUIDv4() = %q, dashes in unexpected positions", id)
		}
		if id[14] != '4' {
			t.Fatalf("newUUIDv4() = %q, version nibble != 4", id)
		}
		if seen[id] {
			t.Fatalf("newUUIDv4() produced a duplicate: %q", id)
		}
		seen[id] = true
	}
}

func TestJSONArg_NilAndEmptyBecomeNil(t *testing.T) {
	if v := jsonArg(nil); v != nil {
		t.Errorf("jsonArg(nil) = %v, want nil", v)
	}
	if v := jsonArg(json.RawMessage("")); v != nil {
		t.Errorf("jsonArg(empty) = %v, want nil", v)
	}
}

func TestJSONArg_NonEmptyBecomesString(t *testing.T) {
	v := jsonArg(json.RawMessage(`{"a":1}`))
	s, ok := v.(string)
	if !ok || s != `{"a":1}` {
		t.Errorf("jsonArg(...) = %v (%T), want string {\"a\":1}", v, v)
	}
}
