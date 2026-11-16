package eventtest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/postgresschema"
)

// MigrationScene 是迁移契约测试的场景（空库 / 完整基线 / 部分存量 / 定义冲突）。
type MigrationScene int

const (
	// MigrationSceneEmpty 空库（无 schema、无表）。
	MigrationSceneEmpty MigrationScene = iota
	// MigrationSceneComplete 完整基线（对象集合与定义全部一致）。
	MigrationSceneComplete
	// MigrationScenePartial 部分存量：已有对象定义与基线一致，仅缺对象。
	MigrationScenePartial
	// MigrationSceneConflict 已有对象定义与基线冲突（不自动修复）。
	MigrationSceneConflict
)

func (s MigrationScene) String() string {
	switch s {
	case MigrationSceneEmpty:
		return "empty"
	case MigrationSceneComplete:
		return "complete"
	case MigrationScenePartial:
		return "partial"
	case MigrationSceneConflict:
		return "conflict"
	default:
		return "unknown"
	}
}

// MigrationBackend 是迁移契约测试的后端适配接口，由各后端测试提供实现。
// suite 本身不读环境变量：门控（如 TEST_POSTGRES_DSN）在 backend 测试内完成。
//
// 每个子测试都会获得独立环境：suite 先调用 Reset 清空到初始状态，再调用
// Prepare 构造场景，并在子测试结束时调用 Cleanup 释放资源。
type MigrationBackend interface {
	// NewDriver 按迁移模式构造 Driver，与 Migrate/Verify 共享同一持久结构。
	NewDriver(t *testing.T, mode event.MigrateMode) event.Driver
	// Migrate 执行基线迁移（幂等）。
	Migrate(ctx context.Context) error
	// Verify 只读校验基线对象，发现问题返回 *event.VerifyError。
	Verify(ctx context.Context) (event.VerifyReport, error)
	// Statements 返回基线语句（不含迁移锁与账号授权）。
	Statements() ([]string, error)
	// SQL 返回手工脚本（BEGIN 后设 lock_timeout 并取迁移锁，随后是基线语句）。
	SQL() (string, error)
	// Capabilities 返回后端迁移能力声明。
	Capabilities() event.MigrationCapabilities
	// Prepare 构造指定场景（空 / 完整 / 部分 / 冲突）。
	Prepare(t *testing.T, scene MigrationScene)
	// Reset 清空到初始状态。
	Reset(t *testing.T)
	// ForcePath 强制执行路径；不支持时返回 ErrUnsupportedByBackend。
	ForcePath(path event.MigrationPath) error
	// InjectFault 在指定边界注入错误（err 为 nil 表示清除）；不支持时返回
	// ErrUnsupportedByBackend。
	InjectFault(point event.FaultPoint, err error) error
	// Cleanup 释放 Reset/Prepare 占用的资源（如真实 schema）。
	Cleanup(t *testing.T)
}

// ErrUnsupportedByBackend 表示后端不支持某项可选能力（强制路径 / 故障注入）。
var ErrUnsupportedByBackend = errors.New("eventtest: backend does not support")

// ProbeObserver 是可选观察接口：统计 step 路径的对象探测次数。真实后端
// 不实现；模拟 backend 实现，用于断言 fast 路径不逐对象探测。
type ProbeObserver interface {
	ProbeCount() int
	ResetProbeCount()
}

// ApplyObserver 是可选观察接口：统计每个对象被成功应用的次数（按限定
// 标识）。模拟 backend 实现，用于断言 step 路径探测跳过已应用对象。
type ApplyObserver interface {
	ApplyCount(object string) int
	ResetApplyCounts()
}

// FaultObserver 是可选观察接口：报告本次 Migrate 已尝试的步骤数（失败或
// 回滚后仍保留）。用于证明故障注入确实发生在推进之后，而不是入口即失败。
type FaultObserver interface {
	AttemptedSteps() int
	ResetAttempts()
}

// LockObserver 是可选观察接口：统计迁移锁争用次数。配合 EntryHooker 使用
// 才能确定两个 goroutine 都已进入 Migrate，从而稳定观察到争用。
type LockObserver interface {
	LockContentions() int
	ResetLockStats()
}

// EntryHooker 是可选接口：在 Migrate 入口安装钩子。suite 用它让两个并发
// goroutine 都进入 Migrate 后再同时放行，避免因调度顺序漏掉锁争用。
type EntryHooker interface {
	SetEntryHook(func())
}

// LockNoneProvider 是可选接口：返回一个声明 Lock=none 的等价 backend。
// 真实后端不实现；模拟 backend 实现，用于验证「无锁能力面不支持自动迁移」。
type LockNoneProvider interface {
	LockNoneBackend(t *testing.T) MigrationBackend
}

// ObjectDescriber 是可选接口：返回基线期望对象的限定名列表。冲突用例用它
// 断言「原本缺失的对象仍缺失」。
type ObjectDescriber interface {
	ExpectedObjects() []string
}

// RelationChecker 是可选接口：报告限定标识对应的关系是否存在。冲突用例用
// 它直接断言未被自动补齐的对象在库中确实不存在。
type RelationChecker interface {
	RelationExists(object string) bool
}

// SchemaNamer 是可选接口：返回 backend 当前使用的 schema 名。suite 用它
// 构造具体对象的精确断言；未实现时退化为按对象类型覆盖断言。
type SchemaNamer interface {
	Schema() string
}

// SchemaExporter 是可选接口：按显式 schema 导出语句，供「schema 名非法
// 时报错」的用例使用（Statements/SQL 本身使用 backend 当前 schema）。
type SchemaExporter interface {
	StatementsForSchema(schema string) ([]string, error)
	SQLForSchema(schema string) (string, error)
}

// RunMigrationSuite 运行迁移契约测试。覆盖空库/完整/部分/冲突场景的
// Migrate 与 Verify、三种迁移模式的 Driver 首次操作、导出物、并发首启、
// 锁释放、Lock=none 变体，以及 fast / step 两条执行路径（含故障注入）。
//
// 用例命名遵循三段式「条件_动作_预期结果」；不依赖 sleep，全部使用
// barrier 或确定性计数。
func RunMigrationSuite(t *testing.T, b MigrationBackend) {
	t.Helper()

	t.Run("EmptyDatabase_Migrate_SucceedsAndVerifyOK", func(t *testing.T) {
		testMigrationEmptyCreate(t, b)
	})
	t.Run("CompleteBaseline_Migrate_IdempotentAndVerifyOK", func(t *testing.T) {
		testMigrationCompleteIdempotent(t, b)
	})
	t.Run("PartialBaseline_Migrate_ConvergesToBaseline", func(t *testing.T) {
		testMigrationPartialConverges(t, b)
	})
	t.Run("ConflictingDefinition_Migrate_ReturnsVerifyErrorWithoutRepair", func(t *testing.T) {
		testMigrationConflictNoRepair(t, b)
	})
	t.Run("EmptyDatabase_Verify_ReportsMissingObjects", func(t *testing.T) {
		testMigrationEmptyVerify(t, b)
	})
	t.Run("CompleteBaseline_Verify_ReportsOK", func(t *testing.T) {
		testMigrationCompleteVerify(t, b)
	})
	t.Run("EmptyDatabase_CreateDriverFirstAppend_Succeeds", func(t *testing.T) {
		testMigrationCreateDriver(t, b)
	})
	t.Run("EmptyDatabase_VerifyDriverFirstAppend_FailsWithoutDDL", func(t *testing.T) {
		testMigrationVerifyDriver(t, b)
	})
	t.Run("EmptyDatabase_NoneDriverFirstAppend_FailsWithoutDDL", func(t *testing.T) {
		testMigrationNoneDriver(t, b)
	})
	t.Run("Statements_Export_NonEmptyWithoutLockOrGrants", func(t *testing.T) {
		testMigrationStatements(t, b)
	})
	t.Run("SQL_Export_ContainsStatementsAfterLock", func(t *testing.T) {
		testMigrationSQL(t, b)
	})
	t.Run("InvalidSchema_Export_ReturnsErrInvalidSchema", func(t *testing.T) {
		testMigrationInvalidSchemaExport(t, b)
	})
	t.Run("ConcurrentFirstStart_TwoDrivers_BothSucceedSingleApply", func(t *testing.T) {
		testMigrationConcurrentFirstStart(t, b)
	})
	t.Run("ConsecutiveMigrate_RepeatedCalls_LockReleased", func(t *testing.T) {
		testMigrationLockRelease(t, b)
	})
	t.Run("LockNoneVariant_Migrate_UnsupportedVerifyReadable", func(t *testing.T) {
		testMigrationLockNone(t, b)
	})
	t.Run("FastPath_FaultInjection_RollsBackAllObjects", func(t *testing.T) {
		testMigrationFastFaults(t, b)
	})
	t.Run("FastPath_EmptyDatabase_SkipsObjectProbe", func(t *testing.T) {
		testMigrationFastNoProbe(t, b)
	})
	t.Run("TransactionalBackend_DefaultMigrate_NoObjectProbe", func(t *testing.T) {
		testMigrationDefaultFast(t, b)
	})
	t.Run("StepPath_EmptyDatabase_MigrateSucceeds", func(t *testing.T) {
		testMigrationStepSuccess(t, b)
	})
	t.Run("StepPath_FaultInjection_RetryConvergesWithoutReapply", func(t *testing.T) {
		testMigrationStepFaults(t, b)
	})
	t.Run("Capabilities_LockDeclaration_MatchesVariantBehavior", func(t *testing.T) {
		testMigrationLockCapability(t, b)
	})
	t.Run("Capabilities_LockRelease_MatchesRepeatedMigrate", func(t *testing.T) {
		testMigrationLockReleaseCapability(t, b)
	})
}

// ---- 场景用例 ----

func testMigrationEmptyCreate(t *testing.T, b MigrationBackend) {
	resetTo(t, b, MigrationSceneEmpty)
	if err := b.Migrate(t.Context()); err != nil {
		t.Fatalf("migrate on empty database: %v", err)
	}
	requireMigrationVerifyOK(t, b)
}

func testMigrationCompleteIdempotent(t *testing.T, b MigrationBackend) {
	resetTo(t, b, MigrationSceneComplete)
	if err := b.Migrate(t.Context()); err != nil {
		t.Fatalf("first migrate over complete baseline: %v", err)
	}
	if err := b.Migrate(t.Context()); err != nil {
		t.Fatalf("second migrate over complete baseline: %v", err)
	}
	requireMigrationVerifyOK(t, b)
}

func testMigrationPartialConverges(t *testing.T, b MigrationBackend) {
	resetTo(t, b, MigrationScenePartial)
	if err := b.Migrate(t.Context()); err != nil {
		t.Fatalf("migrate over partial baseline: %v", err)
	}
	requireMigrationVerifyOK(t, b)
}

func testMigrationConflictNoRepair(t *testing.T, b MigrationBackend) {
	resetTo(t, b, MigrationSceneConflict)
	if _, ok := b.(ObjectDescriber); !ok {
		if _, ok := b.(SchemaNamer); !ok {
			t.Skip("backend implements neither ObjectDescriber nor SchemaNamer; " +
				"conflict preservation cannot be asserted")
		}
	}

	// Migrate 前先记录所有缺失对象：冲突场景下这些对象若被自动补齐，说明
	// 基线在定义冲突时仍推进了 DDL，属于违规修复。
	before, _ := requireMigrationVerifyError(t, b)
	missing := map[string]event.ObjectKind{}
	for _, issue := range before.Issues {
		if issue.Problem == event.ProblemMissing {
			missing[issue.Object] = issue.Kind
		}
	}
	if len(missing) == 0 {
		t.Fatalf("conflict scene Verify reports no missing objects: %+v", before.Issues)
	}

	err := b.Migrate(t.Context())
	var verifyErr *event.VerifyError
	if !errors.As(err, &verifyErr) {
		t.Fatalf("migrate over conflicting definition = %v, want *event.VerifyError", err)
	}
	if len(verifyErr.Report.Issues) == 0 {
		t.Fatal("conflict report has no issues")
	}
	if !hasMigrationProblem(verifyErr.Report, event.ProblemOutdated) {
		t.Fatalf("conflict report = %+v, want a ProblemOutdated issue", verifyErr.Report.Issues)
	}

	// 不自动修复：Migrate 前缺失的每一项在失败后仍必须缺失。
	report, _ := requireMigrationVerifyError(t, b)
	for object, kind := range missing {
		requireMigrationIssue(t, report, object, kind, event.ProblemMissing)
	}
	// 已有冲突对象未被修改：event.payload 仍是 outdated。
	payload := ""
	for _, issue := range report.Issues {
		if strings.HasSuffix(issue.Object, ".event.payload") &&
			issue.Problem == event.ProblemOutdated {
			payload = issue.Object
			break
		}
	}
	if payload == "" {
		t.Fatalf("payload outdated issue not found in %+v", report.Issues)
	}
	requireMigrationIssue(t, report, payload, event.KindColumn, event.ProblemOutdated)

	// 直接断言缺失的表/索引关系在库中不存在（而非仅报告缺失）。
	if checker, ok := b.(RelationChecker); ok {
		for object, kind := range missing {
			switch kind {
			case event.KindTable, event.KindIndex:
			default:
				continue
			}
			if checker.RelationExists(object) {
				t.Fatalf("relation %s exists after conflicting migrate, want absent", object)
			}
		}
	}
}

func testMigrationEmptyVerify(t *testing.T, b MigrationBackend) {
	resetTo(t, b, MigrationSceneEmpty)
	report, _ := requireMigrationVerifyError(t, b)
	for _, issue := range report.Issues {
		if issue.Action == "" {
			t.Fatalf("issue %+v has empty Action", issue)
		}
	}
	seen := map[event.ObjectKind]bool{}
	for _, issue := range report.Issues {
		seen[issue.Kind] = true
	}
	for _, kind := range []event.ObjectKind{
		event.KindSchema, event.KindTable, event.KindIndex, event.KindConstraint,
	} {
		if !seen[kind] {
			t.Fatalf("verify report does not cover object kind %s: %+v", kind, report.Issues)
		}
	}
	if objs := migrationExpectedObjects(b); objs != nil {
		for _, obj := range objs {
			requireMigrationIssue(t, report, obj.Name, obj.Kind, event.ProblemMissing)
		}
	}
}

func testMigrationCompleteVerify(t *testing.T, b MigrationBackend) {
	resetTo(t, b, MigrationSceneComplete)
	requireMigrationVerifyOK(t, b)
}

func testMigrationCreateDriver(t *testing.T, b MigrationBackend) {
	resetTo(t, b, MigrationSceneEmpty)
	drv := b.NewDriver(t, event.MigrateCreate)
	if err := drv.Append(t.Context(), migrationEnvelope("create-first")); err != nil {
		t.Fatalf("create-mode first append: %v", err)
	}
	requireMigrationVerifyOK(t, b)
}

func testMigrationVerifyDriver(t *testing.T, b MigrationBackend) {
	resetTo(t, b, MigrationSceneEmpty)
	drv := b.NewDriver(t, event.MigrateVerify)
	err := drv.Append(t.Context(), migrationEnvelope("verify-first"))
	var verifyErr *event.VerifyError
	if !errors.As(err, &verifyErr) {
		t.Fatalf("verify-mode first append = %v, want *event.VerifyError", err)
	}
	if len(verifyErr.Report.Issues) == 0 {
		t.Fatal("verify-mode error report has no issues")
	}
	// 无 DDL：Verify 仍报缺失。
	report, _ := requireMigrationVerifyError(t, b)
	if !hasMigrationProblem(report, event.ProblemMissing) {
		t.Fatalf("verify after verify-mode append = %+v, want ProblemMissing", report.Issues)
	}
}

func testMigrationNoneDriver(t *testing.T, b MigrationBackend) {
	resetTo(t, b, MigrationSceneEmpty)
	drv := b.NewDriver(t, event.MigrateNone)
	err := drv.Append(t.Context(), migrationEnvelope("none-first"))
	if err == nil {
		t.Fatal("none-mode first append = nil, want table-missing error")
	}
	// None 不校验，因此错误不能是 schema 校验错误。
	var verifyErr *event.VerifyError
	if errors.As(err, &verifyErr) {
		t.Fatalf("none-mode first append = %v, want plain missing-table error", err)
	}
	// 不建表也不校验：Verify 仍报 schema / event 缺失。
	report, _ := requireMigrationVerifyError(t, b)
	names, kinds := migrationExpectedNames(b)
	if names != nil {
		requireNamedIssue(t, report, names, kinds, "event", event.KindTable, event.ProblemMissing)
	}
	if !hasMissingKind(report, event.KindSchema) {
		t.Fatalf("verify after none-mode append = %+v, want missing schema", report.Issues)
	}
	if !hasMissingKind(report, event.KindTable) {
		t.Fatalf("verify after none-mode append = %+v, want missing table", report.Issues)
	}
}

func testMigrationStatements(t *testing.T, b MigrationBackend) {
	resetTo(t, b, MigrationSceneEmpty)
	stmts, err := b.Statements()
	if err != nil {
		t.Fatalf("statements: %v", err)
	}
	if len(stmts) == 0 {
		t.Fatal("statements = empty, want baseline DDL")
	}
	for _, stmt := range stmts {
		assertNoLockOrGrant(t, stmt)
	}
}

func testMigrationSQL(t *testing.T, b MigrationBackend) {
	resetTo(t, b, MigrationSceneEmpty)
	stmts, err := b.Statements()
	if err != nil {
		t.Fatalf("statements: %v", err)
	}
	script, err := b.SQL()
	if err != nil {
		t.Fatalf("sql: %v", err)
	}
	if len(stmts) == 0 {
		t.Fatal("statements = empty")
	}
	for i, stmt := range stmts {
		if !strings.Contains(script, stmt) {
			t.Fatalf("SQL does not contain statement %d: %s", i, stmt)
		}
	}
	lockIdx := strings.Index(script, "pg_advisory")
	firstIdx := strings.Index(script, stmts[0])
	if lockIdx < 0 {
		t.Fatal("SQL does not take the migration lock")
	}
	if firstIdx < 0 {
		t.Fatal("SQL does not contain the first DDL statement")
	}
	if lockIdx >= firstIdx {
		t.Fatalf("migration lock (index %d) is not before the first DDL (index %d)",
			lockIdx, firstIdx)
	}
	assertNoGrant(t, script)
}

func testMigrationInvalidSchemaExport(t *testing.T, b MigrationBackend) {
	exporter, ok := b.(SchemaExporter)
	if !ok {
		t.Skip("backend does not implement SchemaExporter; invalid-schema export not covered")
	}
	if _, err := exporter.StatementsForSchema("bad-schema"); !errors.Is(err, event.ErrInvalidSchema) {
		t.Fatalf("StatementsForSchema(invalid) = %v, want ErrInvalidSchema", err)
	}
	if _, err := exporter.SQLForSchema("bad-schema"); !errors.Is(err, event.ErrInvalidSchema) {
		t.Fatalf("SQLForSchema(invalid) = %v, want ErrInvalidSchema", err)
	}
}

func testMigrationConcurrentFirstStart(t *testing.T, b MigrationBackend) {
	resetTo(t, b, MigrationSceneEmpty)
	drivers := []event.Driver{
		b.NewDriver(t, event.MigrateCreate),
		b.NewDriver(t, event.MigrateCreate),
	}

	// 若 backend 支持入口钩子，让两个 goroutine 都进入 Migrate 后再放行，
	// 从而确定性地观察到锁争用；否则仍执行正向断言。
	hooker, hasHook := b.(EntryHooker)
	var arrived atomic.Int32
	release := make(chan struct{})
	if hasHook {
		hooker.SetEntryHook(func() {
			if arrived.Add(1) == int32(len(drivers)) {
				close(release)
			}
			<-release
		})
		t.Cleanup(func() { hooker.SetEntryHook(nil) })
	}

	start := make(chan struct{})
	errs := make([]error, len(drivers))
	ctx := t.Context()
	var wg sync.WaitGroup
	for i := range drivers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = drivers[i].Append(ctx,
				migrationEnvelope("concurrent-"+string(rune('a'+i))))
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("driver %d concurrent first append: %v", i, err)
		}
	}
	requireMigrationVerifyOK(t, b)
	if observer, ok := b.(ApplyObserver); ok {
		objs := migrationExpectedObjects(b)
		if objs == nil {
			t.Fatal("ApplyObserver without SchemaNamer cannot resolve expected objects")
		}
		for _, obj := range objs {
			if got := observer.ApplyCount(obj.Name); got != 1 {
				t.Fatalf("object %s applied %d times under concurrent first start, want 1",
					obj.Name, got)
			}
		}
	}

	lockObserver, hasLockObserver := b.(LockObserver)
	if !hasLockObserver {
		t.Log("backend does not implement LockObserver; lock contention not observable")
		return
	}
	if !hasHook {
		t.Log("backend does not implement EntryHooker; " +
			"lock contention sub-assertion skipped (not deterministic)")
		return
	}
	if got := lockObserver.LockContentions(); got < 1 {
		t.Fatalf("lock contentions = %d under concurrent first start, want >= 1", got)
	}
}

func testMigrationLockRelease(t *testing.T, b MigrationBackend) {
	resetTo(t, b, MigrationSceneEmpty)
	if err := b.Migrate(t.Context()); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if err := b.Migrate(t.Context()); err != nil {
		t.Fatalf("second migrate after lock release: %v", err)
	}
	requireMigrationVerifyOK(t, b)
}

func testMigrationLockNone(t *testing.T, b MigrationBackend) {
	provider, ok := b.(LockNoneProvider)
	if !ok {
		t.Skip("backend does not provide a Lock=none variant")
	}
	none := provider.LockNoneBackend(t)
	none.Reset(t)
	t.Cleanup(func() { none.Cleanup(t) })
	if got := none.Capabilities().Lock; got != event.LockNone {
		t.Fatalf("Lock=none variant Capabilities().Lock = %v, want LockNone", got)
	}
	if err := none.Migrate(t.Context()); !errors.Is(err, event.ErrMigrationUnsupported) {
		t.Fatalf("Lock=none variant migrate = %v, want ErrMigrationUnsupported", err)
	}
	// Driver 层同语义：Create 模式首次 Append 返回明确不支持错误。
	none.Reset(t)
	none.Prepare(t, MigrationSceneEmpty)
	drv := none.NewDriver(t, event.MigrateCreate)
	err := drv.Append(t.Context(), migrationEnvelope("lock-none-first"))
	if !errors.Is(err, event.ErrMigrationUnsupported) {
		t.Fatalf("Lock=none create-mode first append = %v, want ErrMigrationUnsupported", err)
	}
	// Verify 仍可用（只读）。
	none.Reset(t)
	none.Prepare(t, MigrationSceneComplete)
	report, err := none.Verify(t.Context())
	if err != nil {
		t.Fatalf("Lock=none variant verify: %v", err)
	}
	if !report.OK() {
		t.Fatalf("Lock=none variant verify issues: %+v", report.Issues)
	}
}

// ---- fast 路径 ----

func testMigrationFastFaults(t *testing.T, b MigrationBackend) {
	if b.Capabilities().Atomicity != event.DDLAtomicTransactional {
		t.Skip("backend does not support transactional DDL; fast path not applicable")
	}
	if !forceMigrationPath(t, b, event.MigrationPathFast) {
		t.Skip("backend does not support forcing the fast path")
	}
	stmts, err := b.Statements()
	if err != nil {
		t.Fatalf("statements: %v", err)
	}
	for _, point := range []event.FaultPoint{
		event.FaultAfterStep, event.FaultBeforeCommit,
	} {
		t.Run("FastPath_"+string(point)+"Fault_RollsBackAllObjects", func(t *testing.T) {
			resetTo(t, b, MigrationSceneEmpty)
			if !forceMigrationPath(t, b, event.MigrationPathFast) {
				t.Fatal("backend stopped supporting the fast path")
			}
			injected := errors.New("injected migration fault")
			if !changeFault(t, b, point, injected) {
				return
			}
			if err := b.Migrate(t.Context()); err == nil {
				t.Fatalf("migrate with fault at %s = nil, want error", point)
			}
			// 证明故障发生在推进之后：AfterStep 至少推进 1 步，
			// BeforeCommit 已尝试全部步骤。
			if observer, ok := b.(FaultObserver); ok {
				got := observer.AttemptedSteps()
				switch point {
				case event.FaultAfterStep:
					if got < 1 {
						t.Fatalf("attempted steps = %d after %s fault, want >= 1", got, point)
					}
				case event.FaultBeforeCommit:
					if got != len(stmts) {
						t.Fatalf("attempted steps = %d before commit fault, want %d",
							got, len(stmts))
					}
				}
			}
			report, _ := requireMigrationVerifyError(t, b)
			if objs := migrationExpectedObjects(b); objs != nil {
				for _, obj := range objs {
					requireMigrationIssue(t, report, obj.Name, obj.Kind, event.ProblemMissing)
				}
			}
			// 清除故障后重跑：覆盖回滚后的锁释放与整体重放。
			if !changeFault(t, b, point, nil) {
				return
			}
			if err := b.Migrate(t.Context()); err != nil {
				t.Fatalf("migrate after clearing fault at %s: %v", point, err)
			}
			requireMigrationVerifyOK(t, b)
		})
	}
}

func testMigrationFastNoProbe(t *testing.T, b MigrationBackend) {
	if b.Capabilities().Atomicity != event.DDLAtomicTransactional {
		t.Skip("backend does not support transactional DDL; fast path not applicable")
	}
	if !forceMigrationPath(t, b, event.MigrationPathFast) {
		t.Skip("backend does not support forcing the fast path")
	}
	resetTo(t, b, MigrationSceneEmpty)
	if !forceMigrationPath(t, b, event.MigrationPathFast) {
		t.Fatal("backend stopped supporting the fast path")
	}
	observer, ok := b.(ProbeObserver)
	if !ok {
		t.Skip("backend does not implement ProbeObserver; probe behavior not observable")
	}
	observer.ResetProbeCount()
	if err := b.Migrate(t.Context()); err != nil {
		t.Fatalf("fast migrate on empty database: %v", err)
	}
	if got := observer.ProbeCount(); got != 0 {
		t.Fatalf("fast path probed %d objects, want 0", got)
	}
	requireMigrationVerifyOK(t, b)
}

// testMigrationDefaultFast 断言事务档后端在不调用 ForcePath 时仍走 fast：
// 空库 Migrate 不做逐对象探测（不得静默降级为 step）。
func testMigrationDefaultFast(t *testing.T, b MigrationBackend) {
	if b.Capabilities().Atomicity != event.DDLAtomicTransactional {
		t.Skip("backend is not transactional; default fast path not applicable")
	}
	observer, ok := b.(ProbeObserver)
	if !ok {
		t.Skip("backend does not implement ProbeObserver; cannot observe default path")
	}
	resetTo(t, b, MigrationSceneEmpty)
	observer.ResetProbeCount()
	if err := b.Migrate(t.Context()); err != nil {
		t.Fatalf("default migrate on empty database: %v", err)
	}
	if got := observer.ProbeCount(); got != 0 {
		t.Fatalf("default transactional migrate probed %d objects, want 0", got)
	}
	requireMigrationVerifyOK(t, b)
}

// ---- step 路径 ----

func testMigrationStepSuccess(t *testing.T, b MigrationBackend) {
	if !forceMigrationPath(t, b, event.MigrationPathStep) {
		t.Skip("backend does not support step path (real Postgres runs fast; step is covered by the simulated backend)")
	}
	resetTo(t, b, MigrationSceneEmpty)
	if !forceMigrationPath(t, b, event.MigrationPathStep) {
		t.Fatal("backend stopped supporting the step path")
	}
	if err := b.Migrate(t.Context()); err != nil {
		t.Fatalf("step migrate on empty database: %v", err)
	}
	requireMigrationVerifyOK(t, b)
}

func testMigrationStepFaults(t *testing.T, b MigrationBackend) {
	if !forceMigrationPath(t, b, event.MigrationPathStep) {
		t.Skip("backend does not support step path (real Postgres runs fast; step is covered by the simulated backend)")
	}
	stmts, err := b.Statements()
	if err != nil {
		t.Fatalf("statements: %v", err)
	}
	points := []event.FaultPoint{
		event.FaultAfterLock, event.FaultBeforeStep,
		event.FaultAfterStep, event.FaultBeforeCommit,
	}
	for _, point := range points {
		t.Run("StepPath_"+string(point)+"Fault_RetryConvergesWithoutReapply", func(t *testing.T) {
			resetTo(t, b, MigrationSceneEmpty)
			if !forceMigrationPath(t, b, event.MigrationPathStep) {
				t.Fatal("backend stopped supporting the step path")
			}
			injected := errors.New("injected migration fault")
			if !changeFault(t, b, point, injected) {
				return
			}
			if err := b.Migrate(t.Context()); err == nil {
				t.Fatalf("migrate with fault at %s = nil, want error", point)
			}
			// 故障边界后的进度证据（失败/残留仍可观察）。
			if observer, ok := b.(ApplyObserver); ok {
				objs := migrationExpectedObjects(b)
				if objs == nil {
					t.Fatal("ApplyObserver without SchemaNamer cannot resolve expected objects")
				}
				switch point {
				case event.FaultBeforeStep:
					for _, obj := range objs {
						if got := observer.ApplyCount(obj.Name); got != 0 {
							t.Fatalf("object %s applied %d times before-step fault, want 0",
								obj.Name, got)
						}
					}
				case event.FaultAfterStep:
					applied := 0
					for _, obj := range objs {
						if observer.ApplyCount(obj.Name) > 0 {
							applied++
						}
					}
					if applied < 1 {
						t.Fatal("after-step fault left no applied object, want >= 1")
					}
				case event.FaultBeforeCommit:
					for _, obj := range objs {
						if got := observer.ApplyCount(obj.Name); got != 1 {
							t.Fatalf("object %s applied %d times before-commit fault, want 1",
								obj.Name, got)
						}
					}
				}
			}
			if observer, ok := b.(FaultObserver); ok && point == event.FaultBeforeCommit {
				if got := observer.AttemptedSteps(); got != len(stmts) {
					t.Fatalf("attempted steps = %d before commit fault, want %d",
						got, len(stmts))
				}
			}
			if !changeFault(t, b, point, nil) {
				return
			}
			if err := b.Migrate(t.Context()); err != nil {
				t.Fatalf("retry after fault at %s: %v", point, err)
			}
			requireMigrationVerifyOK(t, b)
			observer, ok := b.(ApplyObserver)
			if !ok {
				return
			}
			objs := migrationExpectedObjects(b)
			if objs == nil {
				t.Fatal("ApplyObserver without SchemaNamer cannot resolve expected objects")
			}
			for _, obj := range objs {
				if got := observer.ApplyCount(obj.Name); got != 1 {
					t.Fatalf("object %s applied %d times after retry, want 1", obj.Name, got)
				}
			}
		})
	}
}

// ---- 能力面一致性 ----

func testMigrationLockCapability(t *testing.T, b MigrationBackend) {
	cap := b.Capabilities()
	if cap.Lock == event.LockNone {
		t.Fatal("backend supports automatic migration but declares Lock=none")
	}
	wantRelease, ok := lockReleaseFor(cap.Lock)
	if !ok {
		t.Fatalf("backend declares unknown lock primitive %v", cap.Lock)
	}
	if cap.LockRelease != wantRelease {
		t.Fatalf("LockRelease = %v for Lock %v, want %v",
			cap.LockRelease, cap.Lock, wantRelease)
	}
	if provider, ok := b.(LockNoneProvider); ok {
		none := provider.LockNoneBackend(t)
		if got := none.Capabilities().Lock; got != event.LockNone {
			t.Fatalf("Lock=none variant Capabilities().Lock = %v, want LockNone", got)
		}
	}
}

func lockReleaseFor(lock event.LockPrimitive) (event.LockRelease, bool) {
	switch lock {
	case event.LockTransaction:
		return event.LockReleaseTransaction, true
	case event.LockSession:
		return event.LockReleaseSession, true
	case event.LockRowLease:
		return event.LockReleaseLease, true
	default:
		return 0, false
	}
}

func testMigrationLockReleaseCapability(t *testing.T, b MigrationBackend) {
	if b.Capabilities().LockRelease != event.LockReleaseTransaction {
		t.Skip("backend does not release locks on transaction commit/rollback")
	}
	resetTo(t, b, MigrationSceneEmpty)
	if err := b.Migrate(t.Context()); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if err := b.Migrate(t.Context()); err != nil {
		t.Fatalf("second migrate after transactional lock release: %v", err)
	}
}

// ---- 测试设施 ----

func resetTo(t *testing.T, b MigrationBackend, scene MigrationScene) {
	t.Helper()
	b.Reset(t)
	b.Prepare(t, scene)
	t.Cleanup(func() { b.Cleanup(t) })
}

func requireMigrationVerifyOK(t *testing.T, b MigrationBackend) event.VerifyReport {
	t.Helper()
	report, err := b.Verify(t.Context())
	if err != nil {
		t.Fatalf("verify = %v, want nil error", err)
	}
	if !report.OK() {
		t.Fatalf("verify issues: %+v", report.Issues)
	}
	return report
}

func requireMigrationVerifyError(t *testing.T, b MigrationBackend) (event.VerifyReport, *event.VerifyError) {
	t.Helper()
	report, err := b.Verify(t.Context())
	var verifyErr *event.VerifyError
	if !errors.As(err, &verifyErr) {
		t.Fatalf("verify = %v, want *event.VerifyError", err)
	}
	if len(report.Issues) == 0 {
		t.Fatal("verify returned *VerifyError with an empty report")
	}
	return report, verifyErr
}

func requireMigrationIssue(t *testing.T, report event.VerifyReport, object string,
	kind event.ObjectKind, problem event.VerifyProblem) {
	t.Helper()
	for _, issue := range report.Issues {
		if issue.Object != object {
			continue
		}
		if issue.Kind != kind {
			t.Fatalf("issue %s kind = %q, want %q", object, issue.Kind, kind)
		}
		if issue.Problem != problem {
			t.Fatalf("issue %s problem = %q, want %q", object, issue.Problem, problem)
		}
		if issue.Action == "" {
			t.Fatalf("issue %s action is empty", object)
		}
		return
	}
	t.Fatalf("issue %s not found in %+v", object, report.Issues)
}

func hasMigrationProblem(report event.VerifyReport, problem event.VerifyProblem) bool {
	for _, issue := range report.Issues {
		if issue.Problem == problem {
			return true
		}
	}
	return false
}

func hasMissingKind(report event.VerifyReport, kind event.ObjectKind) bool {
	for _, issue := range report.Issues {
		if issue.Kind == kind && issue.Problem == event.ProblemMissing {
			return true
		}
	}
	return false
}

// requireNamedIssue 按限定名后缀查找对象并断言其问题。
func requireNamedIssue(t *testing.T, report event.VerifyReport, names []string,
	kinds map[string]event.ObjectKind, suffix string, kind event.ObjectKind,
	problem event.VerifyProblem) {
	t.Helper()
	name, ok := qualifiedName(names, suffix)
	if !ok {
		t.Fatalf("expected object %s not found in %v", suffix, names)
	}
	if got := kinds[name]; got != "" && got != kind {
		t.Fatalf("expected object %s kind = %q, want %q", name, got, kind)
	}
	requireMigrationIssue(t, report, name, kind, problem)
}

// migrationExpectedObjects 返回当前 schema 的期望对象；backend 未实现
// SchemaNamer 时返回 nil（调用方退化为类型级断言）。
func migrationExpectedObjects(b MigrationBackend) []postgresschema.Object {
	namer, ok := b.(SchemaNamer)
	if !ok {
		return nil
	}
	return postgresschema.ExpectedObjects(namer.Schema())
}

// migrationExpectedNames 返回限定名列表与名称到类型的映射：优先使用
// ObjectDescriber（对象名唯一事实源），否则从 SchemaNamer + postgresschema
// 推导；两者皆无时返回 nil。
func migrationExpectedNames(b MigrationBackend) ([]string, map[string]event.ObjectKind) {
	var names []string
	if describer, ok := b.(ObjectDescriber); ok {
		names = describer.ExpectedObjects()
	}
	objs := migrationExpectedObjects(b)
	if objs != nil {
		if len(names) == 0 {
			names = make([]string, 0, len(objs))
			for _, obj := range objs {
				names = append(names, obj.Name)
			}
		}
		kinds := make(map[string]event.ObjectKind, len(objs))
		for _, obj := range objs {
			kinds[obj.Name] = obj.Kind
		}
		return names, kinds
	}
	if len(names) == 0 {
		return nil, nil
	}
	kinds := map[string]event.ObjectKind{}
	for _, name := range names {
		kinds[name] = kindFromSuffix(name)
	}
	return names, kinds
}

func kindFromSuffix(name string) event.ObjectKind {
	switch {
	case strings.HasSuffix(name, ".event_claim_eligible_idx"),
		strings.HasSuffix(name, ".event_type_position_idx"):
		return event.KindIndex
	case strings.HasSuffix(name, ".event_claim_pk"),
		strings.HasSuffix(name, ".event_pk"),
		strings.HasSuffix(name, ".event_type_id_uniq"),
		strings.HasSuffix(name, ".event_claim_event_id_fkey"):
		return event.KindConstraint
	case strings.HasSuffix(name, ".event"), strings.HasSuffix(name, ".event_claim"):
		return event.KindTable
	default:
		return event.KindSchema
	}
}

// qualifiedName 在限定名列表中按完整名或「.后缀」查找。
func qualifiedName(names []string, suffix string) (string, bool) {
	for _, name := range names {
		if name == suffix || strings.HasSuffix(name, "."+suffix) {
			return name, true
		}
	}
	return "", false
}

func forceMigrationPath(t *testing.T, b MigrationBackend, path event.MigrationPath) bool {
	t.Helper()
	err := b.ForcePath(path)
	if errors.Is(err, ErrUnsupportedByBackend) {
		return false
	}
	if err != nil {
		t.Fatalf("force migration path %v: %v", path, err)
	}
	return true
}

// changeFault 注入或清除故障；点不受支持时跳过（不是失败）。
func changeFault(t *testing.T, b MigrationBackend, point event.FaultPoint, err error) bool {
	t.Helper()
	action := "inject"
	if err == nil {
		action = "clear"
	}
	if ferr := b.InjectFault(point, err); ferr != nil {
		if errors.Is(ferr, ErrUnsupportedByBackend) {
			t.Skipf("backend does not support %s fault at %s", action, point)
			return false
		}
		t.Fatalf("%s fault at %s: %v", action, point, ferr)
	}
	return true
}

func assertNoLockOrGrant(t *testing.T, stmt string) {
	t.Helper()
	lower := strings.ToLower(stmt)
	if strings.Contains(lower, "advisory") || strings.Contains(lower, "lock table") {
		t.Fatalf("statements must not take the migration lock: %s", stmt)
	}
	assertNoGrant(t, stmt)
}

func assertNoGrant(t *testing.T, text string) {
	t.Helper()
	lower := strings.ToLower(text)
	for _, forbidden := range []string{"grant ", "revoke ", "create role", "alter default privileges"} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("exports must not contain account grants (%q): %s", forbidden, text)
		}
	}
}

func migrationEnvelope(id string) event.RawEnvelope {
	return event.RawEnvelope{
		ID:         id,
		Type:       "migration.suite",
		Payload:    []byte(`{}`),
		OccurredAt: time.Now(),
	}
}
