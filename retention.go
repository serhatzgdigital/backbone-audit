package audit

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// RetentionConfig configures a Retention worker. Passed to
// NewRetention, which fills in zero fields with the defaults
// documented per-field below. UpdateConfig, by contrast, is a pure
// hot-swap: it replaces the live config verbatim, with no defaulting
// — see UpdateConfig's doc for why.
type RetentionConfig struct {
	// Enabled gates whether Tick actually does anything. Default
	// false — see doc.go's "Retention is opt-in and defaults off".
	Enabled bool

	// DryRun, when true, makes Tick call CountOlderThan instead of
	// deleting anything. RowsDeleted on the result is the count that
	// a real run would have deleted.
	DryRun bool

	// RetentionDays is how old (by CreatedAt) a row must be to become
	// eligible for deletion. NewRetention defaults an unset (zero)
	// value to 30; a negative value is left as-is by NewRetention and
	// rejected by Tick with an error the next time it runs — this
	// distinguishes "caller didn't set it" (silently defaulted) from
	// "caller explicitly set an invalid value" (surfaced loudly,
	// mirroring Auth Service's CM-8 PurgeOldAudit, which rejects
	// RetentionDays <= 0 rather than guessing).
	RetentionDays int

	// BatchSize bounds how many rows one DeleteOlderThanBatch call
	// removes. Default 5000, mirroring CM-8's default. <= 0 is also
	// defaulted at the Store layer independently (see
	// Store.DeleteOlderThanBatch), so this is a belt-and-suspenders
	// default.
	BatchSize int

	// MaxRowsPerRun caps how many rows a single Tick call deletes
	// across all its batches, so one very large backlog can't turn a
	// tick into an unbounded delete storm. Default 100000. When hit,
	// RetentionResult.HitMaxRows is true and the rest of the backlog
	// is picked up on the next tick.
	MaxRowsPerRun int

	// Interval is how often Run ticks. Default 24h, matching CM-8.
	// Changing Interval via UpdateConfig while Run is already looping
	// does not reschedule the running ticker — see Run's doc.
	Interval time.Duration

	// BatchPause is how long Tick sleeps between delete batches, to
	// give the table room to accept concurrent writes rather than
	// hammering it with back-to-back deletes. Default 50ms, matching
	// CM-8. 0 means no pause (this is what tests should set to run
	// fast — see the field's use in Tick).
	BatchPause time.Duration

	// Logger receives operational logs. Falls back to slog.Default()
	// at call time if nil, so a zero-value RetentionConfig (or an
	// UpdateConfig that omits it) never panics on a nil logger.
	Logger *slog.Logger
}

// RetentionResult is the outcome of one Retention.Tick call.
type RetentionResult struct {
	StartedAt   time.Time
	CompletedAt time.Time
	Cutoff      time.Time
	RowsDeleted int64
	Iterations  int
	HitMaxRows  bool
	DryRun      bool
	Err         error
}

// RetentionSnapshot is what an adopter's health/admin endpoint reads
// from a running Retention — the same shape Auth Service's CM-8
// AuditScheduler.Snapshot exposes to its AdminPanel card.
type RetentionSnapshot struct {
	// LastRun is the most recently completed Tick's result, or nil if
	// Tick has never run (including "disabled the whole time").
	LastRun *RetentionResult
	// TotalRuns counts successful Tick calls (Err == nil) since this
	// Retention was constructed.
	TotalRuns int64
	// TotalRowsDeleted accumulates RowsDeleted across successful,
	// non-DryRun runs.
	TotalRowsDeleted int64
	// NextRunAt estimates when Run will next attempt a Tick, based on
	// the last completed run plus Interval (or now plus Interval if
	// there's no LastRun yet). Zero when Enabled is false.
	NextRunAt time.Time
	// Enabled mirrors the live config at the moment Snapshot was
	// called.
	Enabled bool
}

// retentionStore is the subset of *Store that Retention depends on.
// Unexported and satisfied by *Store (see the compile-time assertion
// below) so retention_test.go can substitute an in-memory fake,
// mirroring backbone-queue/outbox's cleanupStore pattern.
type retentionStore interface {
	CountOlderThan(ctx context.Context, q Querier, cutoff time.Time) (int64, error)
	DeleteOlderThanBatch(ctx context.Context, ex Execer, cutoff time.Time, batch int) (int64, error)
}

var _ retentionStore = (*Store)(nil)

// Retention periodically purges rows older than RetentionConfig.
// RetentionDays from a Store's table, generalizing Auth Service's
// CM-8 audit_retention.go pattern (batched CTE deletes so a large
// purge never holds one long table lock, panic recovery so a bad tick
// never takes the host process down, single-flight so overlapping
// ticks can't race each other).
type Retention struct {
	db    DB
	store retentionStore

	mu               sync.RWMutex
	cfg              RetentionConfig
	lastRun          *RetentionResult
	totalRuns        int64
	totalRowsDeleted int64

	inFlight       int32 // atomic; single-flight guard for dispatchTick
	disabledLogged int32 // atomic; log "disabled, skipping" once, not every tick
}

// NewRetention builds a Retention bound to db/store, defaulting any
// zero field of cfg: Enabled=false (bool zero value, no code needed),
// RetentionDays=30 (only when exactly 0 — see RetentionConfig.
// RetentionDays' doc), BatchSize=5000, MaxRowsPerRun=100000,
// Interval=24h, BatchPause=50ms, Logger=slog.Default().
func NewRetention(db DB, store *Store, cfg RetentionConfig) *Retention {
	if cfg.RetentionDays == 0 {
		cfg.RetentionDays = 30
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 5000
	}
	if cfg.MaxRowsPerRun <= 0 {
		cfg.MaxRowsPerRun = 100000
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 24 * time.Hour
	}
	if cfg.BatchPause <= 0 {
		cfg.BatchPause = 50 * time.Millisecond
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Retention{db: db, store: store, cfg: cfg}
}

// logger returns the configured logger, falling back to
// slog.Default() if the live config's Logger is nil (which UpdateConfig
// can produce, since it does not re-apply NewRetention's defaulting).
func (r *Retention) logger() *slog.Logger {
	r.mu.RLock()
	l := r.cfg.Logger
	r.mu.RUnlock()
	if l == nil {
		return slog.Default()
	}
	return l
}

// Tick runs one retention sweep and returns its result.
//
//   - Disabled (Enabled=false): returns a zero RetentionResult, nil
//     error, and does not touch the store or the running totals —
//     this is the "skipped" result direct callers see.
//   - RetentionDays <= 0: returns an error without touching the
//     store.
//   - DryRun: calls Store.CountOlderThan once; RowsDeleted is the
//     count a real run would delete, nothing is removed.
//   - Otherwise: loops Store.DeleteOlderThanBatch(BatchSize) until a
//     batch returns 0 (backlog drained) or cumulative RowsDeleted
//     reaches MaxRowsPerRun (HitMaxRows=true, remainder picked up
//     next Tick), sleeping BatchPause between batches (BatchPause=0,
//     as tests should set, skips the sleep entirely).
//
// Every Tick that actually runs (Enabled=true) updates the snapshot
// Snapshot() reads, whether it succeeds or fails.
func (r *Retention) Tick(ctx context.Context) (RetentionResult, error) {
	r.mu.RLock()
	cfg := r.cfg
	r.mu.RUnlock()

	if !cfg.Enabled {
		return RetentionResult{}, nil
	}

	result := RetentionResult{StartedAt: time.Now().UTC(), DryRun: cfg.DryRun}

	if cfg.RetentionDays <= 0 {
		result.CompletedAt = time.Now().UTC()
		result.Err = fmt.Errorf("audit: retention: RetentionDays must be > 0, got %d", cfg.RetentionDays)
		r.recordResult(result)
		return result, result.Err
	}

	cutoff := time.Now().UTC().AddDate(0, 0, -cfg.RetentionDays)
	result.Cutoff = cutoff

	if cfg.DryRun {
		n, err := r.store.CountOlderThan(ctx, r.db, cutoff)
		result.CompletedAt = time.Now().UTC()
		if err != nil {
			result.Err = fmt.Errorf("audit: retention: dry-run count: %w", err)
			r.recordResult(result)
			return result, result.Err
		}
		result.RowsDeleted = n
		r.recordResult(result)
		return result, nil
	}

	for {
		n, err := r.store.DeleteOlderThanBatch(ctx, r.db, cutoff, cfg.BatchSize)
		if err != nil {
			result.CompletedAt = time.Now().UTC()
			result.Err = fmt.Errorf("audit: retention: delete batch: %w", err)
			r.recordResult(result)
			return result, result.Err
		}
		result.Iterations++
		result.RowsDeleted += n
		if n == 0 {
			break
		}
		if cfg.MaxRowsPerRun > 0 && result.RowsDeleted >= int64(cfg.MaxRowsPerRun) {
			result.HitMaxRows = true
			break
		}
		if cfg.BatchPause > 0 {
			timer := time.NewTimer(cfg.BatchPause)
			select {
			case <-ctx.Done():
				timer.Stop()
				result.CompletedAt = time.Now().UTC()
				result.Err = ctx.Err()
				r.recordResult(result)
				return result, result.Err
			case <-timer.C:
			}
		}
	}
	result.CompletedAt = time.Now().UTC()
	r.recordResult(result)
	return result, nil
}

// recordResult stores result as the latest LastRun and, on success
// (Err == nil), accumulates TotalRuns/TotalRowsDeleted (skipping the
// latter for DryRun results, since nothing was actually deleted).
func (r *Retention) recordResult(result RetentionResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rc := result
	r.lastRun = &rc
	if result.Err == nil {
		r.totalRuns++
		if !result.DryRun {
			r.totalRowsDeleted += result.RowsDeleted
		}
	}
}

// Snapshot returns the current state for an adopter's health/admin
// endpoint. Safe for concurrent use.
func (r *Retention) Snapshot() RetentionSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var next time.Time
	if r.cfg.Enabled {
		base := time.Now().UTC()
		if r.lastRun != nil && !r.lastRun.CompletedAt.IsZero() {
			base = r.lastRun.CompletedAt
		}
		next = base.Add(r.cfg.Interval)
	}
	return RetentionSnapshot{
		LastRun:          r.lastRun,
		TotalRuns:        r.totalRuns,
		TotalRowsDeleted: r.totalRowsDeleted,
		NextRunAt:        next,
		Enabled:          r.cfg.Enabled,
	}
}

// UpdateConfig hot-swaps the live config under the same mutex that
// guards Snapshot, mirroring Auth Service's CM-8 AuditScheduler,
// which offers the same runtime control surface. Unlike
// AuditScheduler.UpdateConfig (which merges, keeping old values for
// any zero field the caller omits), this is a plain replace: cfg
// becomes the new live config verbatim, including any zero or
// negative field. This is deliberate — it's what lets a caller (or a
// test) put Retention into an exact state, like RetentionDays <= 0,
// that NewRetention's defaulting would otherwise never produce, so
// Tick's own validation actually gets exercised in production
// wiring, not just at construction time.
//
// The currently-running Tick, if any, is unaffected — it already
// captured its own cfg snapshot at the top of Tick and keeps running
// with the old values.
func (r *Retention) UpdateConfig(cfg RetentionConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cfg = cfg
}

// Run ticks on the Interval that was in effect when Run started,
// until ctx is done, at which point it returns ctx.Err(). A later
// UpdateConfig that changes Interval does not reschedule this running
// ticker (same documented limitation as CM-8's AuditScheduler) —
// restart the process/goroutine to pick up a new Interval.
//
// While cfg.Enabled is false, Run keeps looping (so ctx cancellation
// and restart behavior are uniform whether or not retention is
// currently turned on) but skips calling Tick, logging that it is
// skipping exactly once rather than on every tick. Snapshot remains
// readable throughout — a caller can always tell whether Retention is
// enabled without needing a Tick to have run.
func (r *Retention) Run(ctx context.Context) error {
	r.mu.RLock()
	interval := r.cfg.Interval
	r.mu.RUnlock()
	if interval <= 0 {
		interval = 24 * time.Hour
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r.dispatchTick(ctx)
		}
	}
}

// dispatchTick runs one Tick with single-flight (a still-running
// previous tick makes this call a no-op) and, via safeTick, panic
// recovery. Also owns the "log disabled-skip once" behavior described
// on Run.
func (r *Retention) dispatchTick(ctx context.Context) {
	if !atomic.CompareAndSwapInt32(&r.inFlight, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&r.inFlight, 0)

	r.mu.RLock()
	enabled := r.cfg.Enabled
	r.mu.RUnlock()

	if !enabled {
		if atomic.CompareAndSwapInt32(&r.disabledLogged, 0, 1) {
			r.logger().Info("audit: retention disabled, skipping ticks", "component", "audit_retention", "event", "skip", "reason", "disabled")
		}
		return
	}
	atomic.StoreInt32(&r.disabledLogged, 0)
	r.safeTick(ctx)
}

// safeTick wraps Tick with panic recovery so one bad tick (e.g. the
// underlying store panicking on a malformed connection) cannot take
// down the process Retention.Run is running in. A recovered panic is
// recorded as a failed RetentionResult, same as any other Tick error.
func (r *Retention) safeTick(ctx context.Context) {
	defer func() {
		if rec := recover(); rec != nil {
			r.logger().Error("audit: retention tick panic recovered", "component", "audit_retention", "panic", rec)
			now := time.Now().UTC()
			r.recordResult(RetentionResult{
				StartedAt:   now,
				CompletedAt: now,
				Err:         fmt.Errorf("audit: retention: tick panic: %v", rec),
			})
		}
	}()
	_, _ = r.Tick(ctx)
}
