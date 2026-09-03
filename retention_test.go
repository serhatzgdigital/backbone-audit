package audit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeRetentionStore is an in-memory stand-in for *Store's two
// retention-relevant methods, so Retention's own logic (batching
// loop, MaxRowsPerRun, DryRun, panic recovery, snapshot bookkeeping)
// can be exercised without a real database or even the fake SQL
// driver used by store_test.go/query_test.go.
type fakeRetentionStore struct {
	mu sync.Mutex

	countCalls  int
	deleteCalls int

	countResult int64
	countErr    error

	// deleteSeq is consumed one entry per DeleteOlderThanBatch call
	// (clamped to the last entry once exhausted), letting a test
	// script "N rows, then N rows, then 0" to end a batching loop.
	deleteSeq []int64
	deleteErr error

	panicOnDelete bool
}

func (f *fakeRetentionStore) CountOlderThan(ctx context.Context, q Querier, cutoff time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.countCalls++
	return f.countResult, f.countErr
}

func (f *fakeRetentionStore) DeleteOlderThanBatch(ctx context.Context, ex Execer, cutoff time.Time, batch int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.panicOnDelete {
		panic("boom: simulated store panic")
	}
	idx := f.deleteCalls
	f.deleteCalls++
	if f.deleteErr != nil {
		return 0, f.deleteErr
	}
	if len(f.deleteSeq) == 0 {
		return 0, nil
	}
	if idx >= len(f.deleteSeq) {
		idx = len(f.deleteSeq) - 1
	}
	return f.deleteSeq[idx], nil
}

func (f *fakeRetentionStore) calls() (count, del int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.countCalls, f.deleteCalls
}

// fakeRetentionStore satisfies retentionStore (declared in
// retention.go, alongside *Store), so it can substitute for the real
// database-backed Store below.
var _ retentionStore = (*fakeRetentionStore)(nil)

// newTestRetention builds a *Retention wired directly to a fake
// store, bypassing NewRetention's defaulting so tests get exact
// control over cfg (including intentionally-invalid values, e.g.
// RetentionDays <= 0). Since retention_test.go lives in package
// audit, it can set the unexported store/cfg fields directly. db is
// left nil — fakeRetentionStore ignores the Querier/Execer it's
// handed, so Retention never dereferences it.
func newTestRetention(cfg RetentionConfig, store retentionStore) *Retention {
	return &Retention{cfg: cfg, store: store}
}

func TestRetention_Tick_Disabled_ReturnsSkippedResult(t *testing.T) {
	fs := &fakeRetentionStore{}
	r := newTestRetention(RetentionConfig{Enabled: false, RetentionDays: 30, BatchSize: 10}, fs)

	result, err := r.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if result.RowsDeleted != 0 || result.Iterations != 0 {
		t.Errorf("Tick(disabled) = %+v, want a zero-ish skipped result", result)
	}
	cnt, del := fs.calls()
	if cnt != 0 || del != 0 {
		t.Errorf("Tick(disabled) touched the store: count=%d delete=%d, want 0/0", cnt, del)
	}
	snap := r.Snapshot()
	if snap.LastRun != nil {
		t.Errorf("Snapshot().LastRun = %+v, want nil after a disabled Tick", snap.LastRun)
	}
	if snap.TotalRuns != 0 {
		t.Errorf("Snapshot().TotalRuns = %d, want 0 after a disabled Tick", snap.TotalRuns)
	}
}

func TestRetention_Tick_RetentionDaysNotPositive_ReturnsError(t *testing.T) {
	for _, days := range []int{0, -1, -30} {
		t.Run(fmt.Sprintf("days=%d", days), func(t *testing.T) {
			fs := &fakeRetentionStore{}
			r := newTestRetention(RetentionConfig{Enabled: true, RetentionDays: days, BatchSize: 10}, fs)

			_, err := r.Tick(context.Background())
			if err == nil {
				t.Fatal("Tick with RetentionDays <= 0 = nil error, want a rejection")
			}
			cnt, del := fs.calls()
			if cnt != 0 || del != 0 {
				t.Errorf("Tick touched the store despite invalid RetentionDays: count=%d delete=%d", cnt, del)
			}
		})
	}
}

func TestRetention_Tick_DryRun_CountsOnlyNoDelete(t *testing.T) {
	fs := &fakeRetentionStore{countResult: 1234}
	r := newTestRetention(RetentionConfig{Enabled: true, DryRun: true, RetentionDays: 30, BatchSize: 500}, fs)

	result, err := r.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if !result.DryRun {
		t.Error("result.DryRun = false, want true")
	}
	if result.RowsDeleted != 1234 {
		t.Errorf("result.RowsDeleted = %d, want 1234 (the dry-run count)", result.RowsDeleted)
	}
	cnt, del := fs.calls()
	if cnt != 1 {
		t.Errorf("CountOlderThan called %d times, want 1", cnt)
	}
	if del != 0 {
		t.Errorf("DeleteOlderThanBatch called %d times, want 0 (dry run must never delete)", del)
	}

	snap := r.Snapshot()
	if snap.TotalRuns != 1 {
		t.Errorf("Snapshot().TotalRuns = %d, want 1", snap.TotalRuns)
	}
	if snap.TotalRowsDeleted != 0 {
		t.Errorf("Snapshot().TotalRowsDeleted = %d, want 0 (dry run never counts toward deleted total)", snap.TotalRowsDeleted)
	}
}

func TestRetention_Tick_DeletesInBatchesUntilDrained(t *testing.T) {
	fs := &fakeRetentionStore{deleteSeq: []int64{500, 500, 137, 0}}
	r := newTestRetention(RetentionConfig{
		Enabled:       true,
		RetentionDays: 30,
		BatchSize:     500,
		MaxRowsPerRun: 1_000_000,
		BatchPause:    0, // skip real sleeping in the test
	}, fs)

	result, err := r.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if result.RowsDeleted != 1137 {
		t.Errorf("result.RowsDeleted = %d, want 1137", result.RowsDeleted)
	}
	if result.Iterations != 4 {
		t.Errorf("result.Iterations = %d, want 4 (three non-empty batches + the draining 0)", result.Iterations)
	}
	if result.HitMaxRows {
		t.Error("result.HitMaxRows = true, want false (backlog drained before the cap)")
	}
	_, del := fs.calls()
	if del != 4 {
		t.Errorf("DeleteOlderThanBatch called %d times, want 4", del)
	}

	snap := r.Snapshot()
	if snap.TotalRowsDeleted != 1137 {
		t.Errorf("Snapshot().TotalRowsDeleted = %d, want 1137", snap.TotalRowsDeleted)
	}
}

func TestRetention_Tick_HitsMaxRowsPerRun(t *testing.T) {
	fs := &fakeRetentionStore{deleteSeq: []int64{500, 500, 500, 500, 500}}
	r := newTestRetention(RetentionConfig{
		Enabled:       true,
		RetentionDays: 30,
		BatchSize:     500,
		MaxRowsPerRun: 1000,
		BatchPause:    0,
	}, fs)

	result, err := r.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if !result.HitMaxRows {
		t.Error("result.HitMaxRows = false, want true")
	}
	if result.RowsDeleted < 1000 {
		t.Errorf("result.RowsDeleted = %d, want >= MaxRowsPerRun (1000)", result.RowsDeleted)
	}
	_, del := fs.calls()
	if del >= 5 {
		t.Errorf("DeleteOlderThanBatch called %d times, want it to stop once MaxRowsPerRun is hit (< 5)", del)
	}
}

func TestRetention_Tick_StoreErrorIsSurfacedAndRecorded(t *testing.T) {
	wantErr := errors.New("connection reset")
	fs := &fakeRetentionStore{deleteErr: wantErr}
	r := newTestRetention(RetentionConfig{Enabled: true, RetentionDays: 30, BatchSize: 500}, fs)

	_, err := r.Tick(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("Tick error = %v, want it to wrap %v", err, wantErr)
	}
	snap := r.Snapshot()
	if snap.LastRun == nil || snap.LastRun.Err == nil {
		t.Fatalf("Snapshot().LastRun = %+v, want a recorded error", snap.LastRun)
	}
	if snap.TotalRuns != 0 {
		t.Errorf("Snapshot().TotalRuns = %d, want 0 (a failed run must not count as successful)", snap.TotalRuns)
	}
}

func TestRetention_Run_PanicInStoreIsRecovered(t *testing.T) {
	fs := &fakeRetentionStore{panicOnDelete: true}
	r := newTestRetention(RetentionConfig{Enabled: true, RetentionDays: 30, BatchSize: 500}, fs)

	// Exercise the same dispatch path Run uses (single-flight +
	// safeTick's panic recovery) without waiting on a real ticker.
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.dispatchTick(context.Background())
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatchTick did not return — panic was not recovered")
	}

	snap := r.Snapshot()
	if snap.LastRun == nil || snap.LastRun.Err == nil {
		t.Fatalf("Snapshot().LastRun = %+v, want a recorded panic error", snap.LastRun)
	}
}

func TestRetention_Snapshot_TotalsAccumulateAcrossTicks(t *testing.T) {
	fs := &fakeRetentionStore{deleteSeq: []int64{10, 0}}
	r := newTestRetention(RetentionConfig{Enabled: true, RetentionDays: 30, BatchSize: 500}, fs)

	if _, err := r.Tick(context.Background()); err != nil {
		t.Fatalf("Tick #1: %v", err)
	}
	fs.mu.Lock()
	fs.deleteCalls = 0
	fs.deleteSeq = []int64{5, 0}
	fs.mu.Unlock()
	if _, err := r.Tick(context.Background()); err != nil {
		t.Fatalf("Tick #2: %v", err)
	}

	snap := r.Snapshot()
	if snap.TotalRuns != 2 {
		t.Errorf("Snapshot().TotalRuns = %d, want 2", snap.TotalRuns)
	}
	if snap.TotalRowsDeleted != 15 {
		t.Errorf("Snapshot().TotalRowsDeleted = %d, want 15", snap.TotalRowsDeleted)
	}
}

func TestRetention_Run_ExitsOnContextCancel(t *testing.T) {
	fs := &fakeRetentionStore{}
	r := newTestRetention(RetentionConfig{Enabled: false, Interval: time.Hour}, fs)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run() = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func TestRetention_UpdateConfig_SwapsValues(t *testing.T) {
	fs := &fakeRetentionStore{}
	r := newTestRetention(RetentionConfig{Enabled: false, RetentionDays: 30, BatchSize: 500}, fs)

	if snap := r.Snapshot(); snap.Enabled {
		t.Fatal("expected Enabled=false before UpdateConfig")
	}

	r.UpdateConfig(RetentionConfig{Enabled: true, RetentionDays: 7, BatchSize: 250, MaxRowsPerRun: 1000})

	snap := r.Snapshot()
	if !snap.Enabled {
		t.Error("Snapshot().Enabled = false after UpdateConfig(Enabled: true), want true")
	}

	// Confirm the swap actually reached Tick's behavior, not just the
	// snapshot's Enabled mirror: a RetentionDays <= 0 update, applied
	// the same way, must make the very next Tick fail — proving
	// UpdateConfig does not silently re-default it back to 30.
	r.UpdateConfig(RetentionConfig{Enabled: true, RetentionDays: -1, BatchSize: 250})
	if _, err := r.Tick(context.Background()); err == nil {
		t.Error("Tick after UpdateConfig(RetentionDays: -1) = nil error, want a rejection")
	}
}

func TestNewRetention_AppliesDefaults(t *testing.T) {
	store, err := NewStore("tenant_audit")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	r := NewRetention(nil, store, RetentionConfig{})

	snap := r.Snapshot()
	if snap.Enabled {
		t.Error("NewRetention default Enabled = true, want false")
	}
	if r.cfg.RetentionDays != 30 {
		t.Errorf("NewRetention default RetentionDays = %d, want 30", r.cfg.RetentionDays)
	}
	if r.cfg.BatchSize != 5000 {
		t.Errorf("NewRetention default BatchSize = %d, want 5000", r.cfg.BatchSize)
	}
	if r.cfg.MaxRowsPerRun != 100000 {
		t.Errorf("NewRetention default MaxRowsPerRun = %d, want 100000", r.cfg.MaxRowsPerRun)
	}
	if r.cfg.Interval != 24*time.Hour {
		t.Errorf("NewRetention default Interval = %v, want 24h", r.cfg.Interval)
	}
	if r.cfg.BatchPause != 50*time.Millisecond {
		t.Errorf("NewRetention default BatchPause = %v, want 50ms", r.cfg.BatchPause)
	}
	if r.cfg.Logger == nil {
		t.Error("NewRetention default Logger = nil, want slog.Default()")
	}
}

func TestNewRetention_NegativeRetentionDaysIsNotSilentlyDefaulted(t *testing.T) {
	store, _ := NewStore("tenant_audit")
	r := NewRetention(nil, store, RetentionConfig{Enabled: true, RetentionDays: -5})
	if r.cfg.RetentionDays != -5 {
		t.Errorf("NewRetention overwrote an explicit negative RetentionDays: got %d", r.cfg.RetentionDays)
	}
	_, err := r.Tick(context.Background())
	if err == nil {
		t.Error("Tick with a constructor-supplied negative RetentionDays = nil error, want a rejection")
	}
}
