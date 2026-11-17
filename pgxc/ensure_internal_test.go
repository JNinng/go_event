package pgxc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/postgresschema"
)

// fakeManager 是 schemaManager 的替身：记录调用次数并返回注入结果。
type fakeManager struct {
	migrateCalls atomic.Int64
	verifyCalls  atomic.Int64
	migrateErr   error
	verifyReport event.VerifyReport
	verifyErr    error
}

func (f *fakeManager) migrate(context.Context) error {
	f.migrateCalls.Add(1)
	return f.migrateErr
}

func (f *fakeManager) verify(context.Context) (event.VerifyReport, error) {
	f.verifyCalls.Add(1)
	return f.verifyReport, f.verifyErr
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func testDriver(mode event.MigrateMode, mgr schemaManager, clock *fakeClock) *Driver {
	return &Driver{mode: mode, manager: mgr, now: clock.now}
}

func TestEnsureCreateMode(t *testing.T) {
	t.Run("Success_FirstEnsure_CallsMigrateOnceThenCaches", func(t *testing.T) {
		mgr := &fakeManager{}
		d := testDriver(event.MigrateCreate, mgr, newFakeClock())
		ctx := context.Background()
		if err := d.ensure(ctx); err != nil {
			t.Fatalf("ensure = %v, want nil", err)
		}
		if err := d.ensure(ctx); err != nil {
			t.Fatalf("ensure second = %v, want nil", err)
		}
		if got := mgr.migrateCalls.Load(); got != 1 {
			t.Fatalf("migrate calls = %d, want 1", got)
		}
	})
	t.Run("TransientError_WithinBackoff_ReturnsSameErrorWithoutRetry", func(t *testing.T) {
		mgr := &fakeManager{migrateErr: errors.New("db down")}
		clock := newFakeClock()
		d := testDriver(event.MigrateCreate, mgr, clock)
		ctx := context.Background()
		first := d.ensure(ctx)
		if first == nil {
			t.Fatal("ensure = nil, want transient error")
		}
		clock.advance(50 * time.Millisecond) // 窗口为 100ms，仍在窗口内
		second := d.ensure(ctx)
		if !errors.Is(second, mgr.migrateErr) {
			t.Fatalf("ensure = %v, want same error", second)
		}
		if got := mgr.migrateCalls.Load(); got != 1 {
			t.Fatalf("migrate calls = %d, want 1 (no retry inside window)", got)
		}
	})
	t.Run("TransientError_AfterBackoffWindow_RetriesExponentially", func(t *testing.T) {
		mgr := &fakeManager{migrateErr: errors.New("db down")}
		clock := newFakeClock()
		d := testDriver(event.MigrateCreate, mgr, clock)
		ctx := context.Background()
		_ = d.ensure(ctx) // 第 1 次，backoff=100ms，重试点 t+100ms
		clock.advance(110 * time.Millisecond)
		_ = d.ensure(ctx) // 第 2 次，backoff=200ms
		if got := mgr.migrateCalls.Load(); got != 2 {
			t.Fatalf("migrate calls = %d, want 2", got)
		}
		clock.advance(150 * time.Millisecond) // 距第 2 次失败 150ms < 200ms
		_ = d.ensure(ctx)
		if got := mgr.migrateCalls.Load(); got != 2 {
			t.Fatalf("migrate calls = %d, want 2 inside second window", got)
		}
		clock.advance(60 * time.Millisecond) // 累计 210ms >= 200ms
		_ = d.ensure(ctx)
		if got := mgr.migrateCalls.Load(); got != 3 {
			t.Fatalf("migrate calls = %d, want 3 after second window", got)
		}
	})
	t.Run("TransientThenSuccess_AfterWindow_RecoversAndCaches", func(t *testing.T) {
		mgr := &fakeManager{migrateErr: errors.New("db down")}
		clock := newFakeClock()
		d := testDriver(event.MigrateCreate, mgr, clock)
		ctx := context.Background()
		if err := d.ensure(ctx); err == nil {
			t.Fatal("ensure = nil, want transient error")
		}
		mgr.migrateErr = nil
		clock.advance(100 * time.Millisecond)
		if err := d.ensure(ctx); err != nil {
			t.Fatalf("ensure = %v, want nil after recovery", err)
		}
		if err := d.ensure(ctx); err != nil {
			t.Fatalf("ensure third = %v, want nil", err)
		}
		if got := mgr.migrateCalls.Load(); got != 2 {
			t.Fatalf("migrate calls = %d, want 2", got)
		}
	})
	t.Run("VerifyError_Permanent_FailFastNoRetry", func(t *testing.T) {
		mgr := &fakeManager{migrateErr: &event.VerifyError{Report: event.VerifyReport{
			Issues: []event.VerifyIssue{{Object: "public.event", Kind: event.KindTable,
				Problem: event.ProblemOutdated, Action: "align"}},
		}}}
		clock := newFakeClock()
		d := testDriver(event.MigrateCreate, mgr, clock)
		ctx := context.Background()
		if err := d.ensure(ctx); !event.IsPermanentMigrationError(err) {
			t.Fatalf("ensure = %v, want permanent VerifyError", err)
		}
		clock.advance(24 * time.Hour)
		if err := d.ensure(ctx); !event.IsPermanentMigrationError(err) {
			t.Fatalf("ensure = %v, want same permanent error", err)
		}
		if got := mgr.migrateCalls.Load(); got != 1 {
			t.Fatalf("migrate calls = %d, want 1 (fail-fast)", got)
		}
	})
	t.Run("PermissionDenied_Permanent_FailFastNoRetry", func(t *testing.T) {
		mgr := &fakeManager{migrateErr: fmt.Errorf("ddl: %w", event.ErrMigrationPermissionDenied)}
		clock := newFakeClock()
		d := testDriver(event.MigrateCreate, mgr, clock)
		ctx := context.Background()
		if err := d.ensure(ctx); !errors.Is(err, event.ErrMigrationPermissionDenied) {
			t.Fatalf("ensure = %v, want permission sentinel", err)
		}
		clock.advance(24 * time.Hour)
		_ = d.ensure(ctx)
		if got := mgr.migrateCalls.Load(); got != 1 {
			t.Fatalf("migrate calls = %d, want 1 (fail-fast)", got)
		}
	})
}

func TestEnsureVerifyMode(t *testing.T) {
	t.Run("OKReport_Ensure_SucceedsAndCaches", func(t *testing.T) {
		mgr := &fakeManager{}
		d := testDriver(event.MigrateVerify, mgr, newFakeClock())
		ctx := context.Background()
		if err := d.ensure(ctx); err != nil {
			t.Fatalf("ensure = %v, want nil", err)
		}
		if err := d.ensure(ctx); err != nil {
			t.Fatalf("ensure second = %v, want nil", err)
		}
		if got := mgr.verifyCalls.Load(); got != 1 {
			t.Fatalf("verify calls = %d, want 1", got)
		}
		if got := mgr.migrateCalls.Load(); got != 0 {
			t.Fatalf("migrate calls = %d, want 0", got)
		}
	})
	t.Run("ProblemReport_Ensure_ReturnsVerifyErrorPermanent", func(t *testing.T) {
		mgr := &fakeManager{verifyReport: event.VerifyReport{
			Issues: []event.VerifyIssue{{Object: "public.event", Kind: event.KindTable,
				Problem: event.ProblemMissing, Action: "create"}},
		}}
		clock := newFakeClock()
		d := testDriver(event.MigrateVerify, mgr, clock)
		ctx := context.Background()
		err := d.ensure(ctx)
		var verifyErr *event.VerifyError
		if !errors.As(err, &verifyErr) {
			t.Fatalf("ensure = %v, want *VerifyError", err)
		}
		if len(verifyErr.Report.Issues) != 1 {
			t.Fatalf("report issues = %d, want 1", len(verifyErr.Report.Issues))
		}
		clock.advance(24 * time.Hour)
		_ = d.ensure(ctx)
		if got := mgr.verifyCalls.Load(); got != 1 {
			t.Fatalf("verify calls = %d, want 1 (fail-fast)", got)
		}
		if got := mgr.migrateCalls.Load(); got != 0 {
			t.Fatalf("migrate calls = %d, want 0", got)
		}
	})
	t.Run("QueryError_Ensure_RetriesAfterBackoff", func(t *testing.T) {
		queryErr := errors.New("connection refused")
		mgr := &fakeManager{verifyErr: queryErr}
		clock := newFakeClock()
		d := testDriver(event.MigrateVerify, mgr, clock)
		ctx := context.Background()
		if err := d.ensure(ctx); !errors.Is(err, queryErr) {
			t.Fatalf("ensure = %v, want query error", err)
		}
		clock.advance(100 * time.Millisecond)
		_ = d.ensure(ctx)
		if got := mgr.verifyCalls.Load(); got != 2 {
			t.Fatalf("verify calls = %d, want 2", got)
		}
	})
}

func TestEnsureNoneAndInvalidModes(t *testing.T) {
	t.Run("NoneMode_Ensure_SkipsManager", func(t *testing.T) {
		mgr := &fakeManager{}
		d := testDriver(event.MigrateNone, mgr, newFakeClock())
		if err := d.ensure(context.Background()); err != nil {
			t.Fatalf("ensure = %v, want nil", err)
		}
		if mgr.verifyCalls.Load() != 0 || mgr.migrateCalls.Load() != 0 {
			t.Fatal("None mode must not consult the manager")
		}
	})
	t.Run("UnknownMode_Ensure_ReturnsMigrateModeUnsupported", func(t *testing.T) {
		mgr := &fakeManager{}
		d := testDriver(event.MigrateMode(99), mgr, newFakeClock())
		err := d.ensure(context.Background())
		if !errors.Is(err, event.ErrMigrateModeUnsupported) {
			t.Fatalf("ensure = %v, want ErrMigrateModeUnsupported", err)
		}
		if mgr.migrateCalls.Load() != 0 || mgr.verifyCalls.Load() != 0 {
			t.Fatal("invalid mode must not consult the manager")
		}
	})
}

func TestEnsureConcurrent(t *testing.T) {
	t.Run("ConcurrentFirstUse_Migrate_CalledOnce", func(t *testing.T) {
		mgr := &fakeManager{}
		d := testDriver(event.MigrateCreate, mgr, newFakeClock())
		var wg sync.WaitGroup
		errs := make([]error, 16)
		for i := range errs {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				errs[i] = d.ensure(context.Background())
			}(i)
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("goroutine %d ensure = %v, want nil", i, err)
			}
		}
		if got := mgr.migrateCalls.Load(); got != 1 {
			t.Fatalf("migrate calls = %d, want 1", got)
		}
	})
}

// 错误分类：重复对象按差异错误、权限与锁等待映射到对应哨兵；
// 23505 区分 pg_type 元数据竞态（瞬时）与真实唯一冲突（差异错误）。
func TestClassifyMigrationError(t *testing.T) {
	const constraintObject = "public.event_pk"
	tests := []struct {
		name           string
		code           string
		constraintName string
		wantIs         error
		wantVer        bool
		perm           bool
		wantObject     string
		wantKind       event.ObjectKind
	}{
		{
			name: "DuplicateType_MapsVerifyError", code: "42710", wantVer: true, perm: true,
			wantObject: constraintObject, wantKind: event.KindConstraint,
		},
		{
			name: "DuplicateTable_MapsVerifyError", code: "42P07", wantVer: true, perm: true,
			wantObject: constraintObject, wantKind: event.KindConstraint,
		},
		{
			name: "DuplicateColumn_MapsVerifyError", code: "42701", wantVer: true, perm: true,
			wantObject: constraintObject, wantKind: event.KindConstraint,
		},
		{
			name: "PermissionDenied_MapsSentinel", code: "42501",
			wantIs: event.ErrMigrationPermissionDenied, perm: true,
		},
		{
			name: "LockNotAvailable_MapsTimeout", code: "55P03",
			wantIs: event.ErrMigrationLockTimeout,
		},
		{
			name: "PgTypeMetadataRace_MapsTransient", code: "23505",
			constraintName: "pg_type_typname_nsp_index", perm: false,
		},
		{
			name: "UniqueViolation_MapsVerifyError", code: "23505",
			constraintName: "event_pk", wantVer: true, perm: true,
			wantObject: constraintObject, wantKind: event.KindConstraint,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 与生产同形：配对期望对象，错误报告使用 Object/Kind。
			stmt := postgresschema.PlannedStatement{
				Object: postgresschema.Object{
					Name: constraintObject,
					Kind: event.KindConstraint,
				},
			}
			err := classifyMigrationError("pgxc", stmt, &pgconn.PgError{
				Code: tt.code, ConstraintName: tt.constraintName, Message: "test",
			})
			if tt.wantIs != nil && !errors.Is(err, tt.wantIs) {
				t.Fatalf("err = %v, want errors.Is(%v)", err, tt.wantIs)
			}
			var verifyErr *event.VerifyError
			if gotVer := errors.As(err, &verifyErr); gotVer != tt.wantVer {
				t.Fatalf("errors.As(*VerifyError) = %v, want %v (err=%v)", gotVer, tt.wantVer, err)
			}
			if got := event.IsPermanentMigrationError(err); got != tt.perm {
				t.Fatalf("IsPermanentMigrationError = %v, want %v", got, tt.perm)
			}
			if tt.wantVer {
				if len(verifyErr.Report.Issues) != 1 {
					t.Fatalf("issues = %d, want 1", len(verifyErr.Report.Issues))
				}
				issue := verifyErr.Report.Issues[0]
				if issue.Object != tt.wantObject {
					t.Fatalf("issue object = %q, want %q", issue.Object, tt.wantObject)
				}
				if issue.Kind != tt.wantKind {
					t.Fatalf("issue kind = %q, want %q", issue.Kind, tt.wantKind)
				}
				if issue.Problem != event.ProblemOutdated {
					t.Fatalf("issue problem = %q, want outdated", issue.Problem)
				}
				if issue.Action == "" {
					t.Fatal("issue action is empty")
				}
			}
		})
	}
	t.Run("UnknownError_WrappedNotPermanent", func(t *testing.T) {
		cause := errors.New("boom")
		err := classifyMigrationError("pgxc", postgresschema.PlannedStatement{}, cause)
		if !errors.Is(err, cause) {
			t.Fatalf("err = %v, want wrapped cause", err)
		}
		if event.IsPermanentMigrationError(err) {
			t.Fatal("unknown error must not be classified permanent")
		}
	})
}

// 调用方取消/超时不是迁移结论：ctx 已取消时不写负缓存、不推进退避，
// 下一次立即重试（即使底层错误被驱动映射为普通错误）。
func TestEnsureContextCancellationNotCached(t *testing.T) {
	tests := []struct {
		name   string
		newCtx func() (context.Context, context.CancelFunc)
	}{
		{
			name: "Canceled_Ensure_NotCachedAndRetriesImmediately",
			newCtx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
		},
		{
			name: "DeadlineExceeded_Ensure_NotCachedAndRetriesImmediately",
			newCtx: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 驱动在取消后常返回普通错误（如 57014/连接关闭），不是 ctx 错误。
			mgr := &fakeManager{migrateErr: errors.New("driver: query canceled")}
			clock := newFakeClock()
			d := testDriver(event.MigrateCreate, mgr, clock)
			ctx, cancel := tt.newCtx()
			defer cancel()
			if err := d.ensure(ctx); err == nil {
				t.Fatal("ensure = nil, want error from canceled context")
			}
			mgr.migrateErr = nil
			// 不推进时钟：若取消被负缓存，会直接返回旧错误而不调用 manager。
			if err := d.ensure(context.Background()); err != nil {
				t.Fatalf("ensure after cancellation = %v, want nil", err)
			}
			if got := mgr.migrateCalls.Load(); got != 2 {
				t.Fatalf("migrate calls = %d, want 2 (no cached cancellation)", got)
			}
		})
	}
}
