// Package simmigrate 是迁移契约测试的进程内模拟 backend：用内存 catalog
// 模拟 Postgres 的 schema/表/列/约束/索引，始终运行、可强制 step 路径并
// 在固定边界注入故障。
//
// 步骤与期望对象直接消费 postgresschema（单一事实源），模拟器不复制 DDL
// 文本；判定（缺失 / 定义冲突）复用 postgresschema.Diff 与 Repairable，
// 保证与真实后端同一套语义。
package simmigrate

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/eventtest"
	"github.com/jninng/go_event/postgresschema"
)

// schema 是模拟 backend 使用的固定 schema 名（内存中无命名空间冲突）。
const schema = "simmigrate"

// New 构造一个空 catalog 的模拟 backend。
func New() *Backend {
	return &Backend{
		catalog:   newCatalog(),
		faults:    map[event.FaultPoint]error{},
		applied:   map[string]int{},
		contended: make(chan struct{}, 1),
	}
}

// Backend 是 eventtest.MigrationBackend 的进程内模拟实现。同一 backend
// 上的多个 Driver 与 Lock=none 变体共享同一 catalog。
//
// statsMu 保护路径、故障表与全部观察计数（探测/应用/尝试/锁争用/入口钩子），
// 供 suite 在并发用例或未来的 t.Parallel 下安全读取。
type Backend struct {
	catalog  *catalog
	lockNone bool

	statsMu     sync.Mutex
	path        event.MigrationPath
	faults      map[event.FaultPoint]error
	probes      int
	applied     map[string]int
	attempts    int
	contentions int
	entryHook   func()
	// contended 由「try-lock 失败」的迁移者写入；持锁者在 EntryHook 场景下
	// 等待该信号后才继续，确保并发用例确定性地观察到锁争用。
	contended      chan struct{}
	contentionSeen bool
}

// catalog 是共享的持久结构状态与迁移锁（内存所有权 + 有界等待）。
type catalog struct {
	sem   chan struct{}
	mu    sync.Mutex
	state *state
}

func newCatalog() *catalog {
	return &catalog{sem: make(chan struct{}, 1), state: newState()}
}

// acquireLock 取得迁移锁：先 try-lock，失败记一次争用后按有界等待阻塞；
// 超时按瞬时错误返回（与真实后端一致）。
func (b *Backend) acquireLock(ctx context.Context) error {
	select {
	case b.catalog.sem <- struct{}{}:
		return nil
	default:
		b.statsMu.Lock()
		b.contentions++
		if !b.contentionSeen {
			b.contentionSeen = true
			select {
			case b.contended <- struct{}{}:
			default:
			}
		}
		b.statsMu.Unlock()
	}
	timer := time.NewTimer(postgresschema.LockWaitTimeout)
	defer timer.Stop()
	select {
	case b.catalog.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("simmigrate: lock wait: %w", event.ErrMigrationLockTimeout)
	}
}

func (b *Backend) releaseLock() {
	<-b.catalog.sem
}

func (c *catalog) baselineReady() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	report := postgresschema.Diff(postgresschema.ExpectedObjects(schema), c.state.snapshot())
	return report.OK()
}

// ---- eventtest.MigrationBackend ----

// NewDriver 按迁移模式构造 Driver：Create 首次操作时执行 Migrate，Verify
// 首检失败即返回 *VerifyError，None 不建也不校验（baseline 缺失时 DML 报错）。
func (b *Backend) NewDriver(_ *testing.T, mode event.MigrateMode) event.Driver {
	return newDriver(b, mode)
}

// Migrate 执行基线迁移。默认 fast：所有缺失对象先在 staging 上应用，全部
// 成功后原子提交；任一故障丢弃 staging，库内对象保持不变（无部分对象）。
// ForcePath(Step) 切换为逐对象探测/应用；step 失败时已应用的对象保留，
// 由重跑时的探测收敛（不重复应用）。
func (b *Backend) Migrate(ctx context.Context) error {
	if b.lockNone {
		return event.ErrMigrationUnsupported
	}
	b.ResetAttempts()
	b.runEntryHook()
	steps, err := postgresschema.Baseline(schema)
	if err != nil {
		return fmt.Errorf("simmigrate: migrate: %w", err)
	}
	expected := postgresschema.ExpectedObjects(schema)
	if err := b.acquireLock(ctx); err != nil {
		return fmt.Errorf("simmigrate: migrate: %w", err)
	}
	defer b.releaseLock()
	b.awaitContenderIfHooked()

	b.catalog.mu.Lock()
	defer b.catalog.mu.Unlock()

	if err := b.injected(event.FaultAfterLock); err != nil {
		return fmt.Errorf("simmigrate: migrate: %w", err)
	}
	if b.pathValue() == event.MigrationPathStep {
		return b.migrateStep(steps)
	}
	return b.migrateFast(expected, steps)
}

func (b *Backend) migrateFast(expected []postgresschema.Object,
	steps []postgresschema.Step) error {
	// 基线接管校验：一次全局 Diff 判定已有一致对象与缺失对象；定义冲突
	// 直接返回差异错误。fast 路径不逐对象探测，只按该结果批应用缺失对象。
	report := postgresschema.Diff(expected, b.catalog.state.snapshot())
	if !postgresschema.Repairable(report) {
		return &event.VerifyError{Report: report}
	}
	missing := make(map[string]bool, len(report.Issues))
	for _, issue := range report.Issues {
		missing[issue.Object] = true
	}
	staging := b.catalog.state.clone()
	applied := map[string]int{}
	for _, step := range steps {
		if !missing[step.Object] {
			continue // 已存在且一致：不重复应用
		}
		b.addAttempt()
		if err := b.injected(event.FaultBeforeStep); err != nil {
			return fmt.Errorf("simmigrate: migrate: %w", err)
		}
		obj, ok := expectedObject(step.Object)
		if !ok {
			return fmt.Errorf("simmigrate: migrate: unknown step object %q", step.Object)
		}
		created, err := staging.applyObject(obj)
		if err != nil {
			return err
		}
		if created {
			applied[step.Object]++
		}
		if err := b.injected(event.FaultAfterStep); err != nil {
			return fmt.Errorf("simmigrate: migrate: %w", err)
		}
	}
	if err := b.injected(event.FaultBeforeCommit); err != nil {
		return fmt.Errorf("simmigrate: migrate: %w", err)
	}
	// 原子提交：staging 生效后的对象与应用计数一并合入。
	b.catalog.state = staging
	b.mergeApplied(applied)
	return nil
}

func (b *Backend) migrateStep(steps []postgresschema.Step) error {
	for _, step := range steps {
		b.addAttempt()
		b.addProbe()
		obj, ok := expectedObject(step.Object)
		if !ok {
			return fmt.Errorf("simmigrate: migrate: unknown step object %q", step.Object)
		}
		applied, conflict := b.catalog.state.probeObject(obj)
		if conflict != nil {
			return conflict
		}
		if applied {
			continue // 已存在且一致：探测跳过，不重复应用
		}
		if err := b.injected(event.FaultBeforeStep); err != nil {
			return fmt.Errorf("simmigrate: migrate: %w", err)
		}
		created, err := b.catalog.state.applyObject(obj)
		if err != nil {
			return err
		}
		if created {
			b.recordApplied(step.Object)
		}
		if err := b.injected(event.FaultAfterStep); err != nil {
			return fmt.Errorf("simmigrate: migrate: %w", err)
		}
	}
	if err := b.injected(event.FaultBeforeCommit); err != nil {
		return fmt.Errorf("simmigrate: migrate: %w", err)
	}
	return nil
}

// Verify 只读校验：稳定顺序（期望对象顺序）列出缺失与定义冲突。
func (b *Backend) Verify(_ context.Context) (event.VerifyReport, error) {
	b.catalog.mu.Lock()
	defer b.catalog.mu.Unlock()
	report := postgresschema.Diff(
		postgresschema.ExpectedObjects(schema), b.catalog.state.snapshot())
	if !report.OK() {
		return report, &event.VerifyError{Report: report}
	}
	return report, nil
}

// Statements 返回基线语句（不含迁移锁）。
func (b *Backend) Statements() ([]string, error) {
	return postgresschema.Statements(schema)
}

// SQL 返回手工脚本（BEGIN 后设 lock_timeout 并取迁移锁，随后是基线语句）。
func (b *Backend) SQL() (string, error) {
	return postgresschema.SQL(schema)
}

// StatementsForSchema 按显式 schema 导出语句（非法 schema 包装
// ErrInvalidSchema）。
func (b *Backend) StatementsForSchema(s string) ([]string, error) {
	stmts, err := postgresschema.Statements(s)
	if err != nil {
		return nil, fmt.Errorf("simmigrate: statements: %w: %v", event.ErrInvalidSchema, err)
	}
	return stmts, nil
}

// SQLForSchema 按显式 schema 导出脚本（非法 schema 包装 ErrInvalidSchema）。
func (b *Backend) SQLForSchema(s string) (string, error) {
	script, err := postgresschema.SQL(s)
	if err != nil {
		return "", fmt.Errorf("simmigrate: sql: %w: %v", event.ErrInvalidSchema, err)
	}
	return script, nil
}

// Schema 返回模拟 backend 使用的 schema 名。
func (b *Backend) Schema() string {
	return schema
}

// ExpectedObjects 返回基线期望对象的限定名列表。
func (b *Backend) ExpectedObjects() []string {
	objs := postgresschema.ExpectedObjects(schema)
	names := make([]string, 0, len(objs))
	for _, obj := range objs {
		names = append(names, obj.Name)
	}
	return names
}

// RelationExists 报告限定标识对应的表/约束/索引/schema 是否存在。
func (b *Backend) RelationExists(object string) bool {
	b.catalog.mu.Lock()
	defer b.catalog.mu.Unlock()
	name := unqualified(object)
	if name == schema {
		return b.catalog.state.schemaExists
	}
	if _, ok := b.catalog.state.tables[name]; ok {
		return true
	}
	if _, ok := b.catalog.state.constraints[name]; ok {
		return true
	}
	_, ok := b.catalog.state.indexes[name]
	return ok
}

// Capabilities 声明模拟 backend 的能力：Postgres 事务档 + 事务级锁。
func (b *Backend) Capabilities() event.MigrationCapabilities {
	cap := event.MigrationCapabilities{
		Atomicity: event.DDLAtomicTransactional,
		IfNotExists: map[event.ObjectKind]event.NativeSupport{
			event.KindSchema:     event.SupportNative,
			event.KindTable:      event.SupportNative,
			event.KindColumn:     event.SupportNative,
			event.KindIndex:      event.SupportNative,
			event.KindConstraint: event.SupportNone,
		},
		Probe: map[event.ObjectKind]event.ProbeSupport{
			event.KindSchema:     event.ProbeExistence,
			event.KindTable:      event.ProbeDefinition,
			event.KindColumn:     event.ProbeDefinition,
			event.KindIndex:      event.ProbeDefinition,
			event.KindConstraint: event.ProbeDefinition,
		},
		Errors:                event.ErrorClasses{Exists: true, Permission: true, Transient: true},
		Lock:                  event.LockTransaction,
		LockRelease:           event.LockReleaseTransaction,
		Completion:            event.DDLSync,
		ImplicitCommit:        false,
		NonTransactionalSteps: true,
	}
	if b.lockNone {
		cap.Lock = event.LockNone
	}
	return cap
}

// Prepare 构造场景。Partial 应用基线前 5 个步骤（schema、event 表、
// event_pk、event_type_id_uniq、event_type_position_idx）：已有对象定义与
// 基线一致，仅缺 event_claim 相关对象。Conflict 用 payload text（基线为
// bytea）构造定义冲突。
func (b *Backend) Prepare(t *testing.T, scene eventtest.MigrationScene) {
	t.Helper()
	b.catalog.mu.Lock()
	defer b.catalog.mu.Unlock()
	st := newState()
	switch scene {
	case eventtest.MigrationSceneEmpty:
		// 空库：不构造任何对象。
	case eventtest.MigrationSceneComplete:
		if err := st.applyBaseline(0); err != nil {
			t.Fatalf("prepare complete scene: %v", err)
		}
	case eventtest.MigrationScenePartial:
		if err := st.applyBaseline(5); err != nil {
			t.Fatalf("prepare partial scene: %v", err)
		}
	case eventtest.MigrationSceneConflict:
		st.buildConflict()
	default:
		t.Fatalf("unknown migration scene %d", int(scene))
	}
	b.catalog.state = st
}

// Reset 清空 catalog 到初始状态（空库）并复位路径、故障、统计与钩子。
func (b *Backend) Reset(_ *testing.T) {
	b.catalog.mu.Lock()
	defer b.catalog.mu.Unlock()
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	b.catalog.state = newState()
	b.path = event.MigrationPathFast
	b.faults = map[event.FaultPoint]error{}
	b.probes = 0
	b.applied = map[string]int{}
	b.attempts = 0
	b.contentions = 0
	b.entryHook = nil
	b.contentionSeen = false
	b.contended = make(chan struct{}, 1)
}

// ForcePath 切换执行路径（模拟 backend 支持 fast 与 step）。
func (b *Backend) ForcePath(path event.MigrationPath) error {
	switch path {
	case event.MigrationPathFast, event.MigrationPathStep:
		b.statsMu.Lock()
		b.path = path
		b.statsMu.Unlock()
		return nil
	default:
		return fmt.Errorf("simmigrate: unknown migration path %d", int(path))
	}
}

// InjectFault 在指定边界注入错误（nil 清除）。模拟 backend 支持全部
// FaultPoint。
func (b *Backend) InjectFault(point event.FaultPoint, err error) error {
	switch point {
	case event.FaultAfterLock, event.FaultBeforeStep,
		event.FaultAfterStep, event.FaultBeforeCommit:
	default:
		return fmt.Errorf("simmigrate: unknown fault point %q", point)
	}
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	if err == nil {
		delete(b.faults, point)
		return nil
	}
	b.faults[point] = err
	return nil
}

// Cleanup 释放资源：内存 backend 无需清理。
func (b *Backend) Cleanup(_ *testing.T) {}

// ProbeCount 返回 step 路径的探测次数。
func (b *Backend) ProbeCount() int {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	return b.probes
}

// ResetProbeCount 清零探测计数。
func (b *Backend) ResetProbeCount() {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	b.probes = 0
}

// ApplyCount 返回对象被成功应用的次数（按限定标识）。
func (b *Backend) ApplyCount(object string) int {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	return b.applied[object]
}

// ResetApplyCounts 清零应用计数。
func (b *Backend) ResetApplyCounts() {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	b.applied = map[string]int{}
}

// AttemptedSteps 返回本次 Migrate 已尝试的步骤数（失败/回滚后仍保留）。
func (b *Backend) AttemptedSteps() int {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	return b.attempts
}

// ResetAttempts 清零尝试计数（Migrate 入口也会重置）。
func (b *Backend) ResetAttempts() {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	b.attempts = 0
}

// LockContentions 返回迁移锁争用次数。
func (b *Backend) LockContentions() int {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	return b.contentions
}

// ResetLockStats 清零锁争用计数。
func (b *Backend) ResetLockStats() {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	b.contentions = 0
}

// SetEntryHook 安装 Migrate 入口钩子（nil 清除），供并发用例确定性同步。
func (b *Backend) SetEntryHook(fn func()) {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	b.entryHook = fn
	if fn != nil {
		b.contended = make(chan struct{}, 1)
		b.contentionSeen = false
	}
}

// LockNoneBackend 返回声明 Lock=none 的等价 backend（共享 catalog）。
func (b *Backend) LockNoneBackend(_ *testing.T) eventtest.MigrationBackend {
	return &Backend{
		catalog:   b.catalog,
		lockNone:  true,
		path:      event.MigrationPathFast,
		faults:    map[event.FaultPoint]error{},
		applied:   map[string]int{},
		contended: make(chan struct{}, 1),
	}
}

func (b *Backend) injected(point event.FaultPoint) error {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	return b.faults[point]
}

func (b *Backend) pathValue() event.MigrationPath {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	return b.path
}

func (b *Backend) addAttempt() {
	b.statsMu.Lock()
	b.attempts++
	b.statsMu.Unlock()
}

func (b *Backend) addProbe() {
	b.statsMu.Lock()
	b.probes++
	b.statsMu.Unlock()
}

func (b *Backend) recordApplied(object string) {
	b.statsMu.Lock()
	b.applied[object]++
	b.statsMu.Unlock()
}

func (b *Backend) mergeApplied(counts map[string]int) {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	for object, n := range counts {
		b.applied[object] += n
	}
}

// awaitContenderIfHooked 在 EntryHook 场景下等待另一个迁移者尝试取锁。
// 持锁者先等待、竞争者 try-lock 失败后发信号，从而避免调度顺序让锁争用
// 不可观察；未安装钩子时立即返回。
func (b *Backend) awaitContenderIfHooked() {
	b.statsMu.Lock()
	if b.entryHook == nil || b.contentionSeen {
		b.statsMu.Unlock()
		return
	}
	contended := b.contended
	b.statsMu.Unlock()
	timer := time.NewTimer(postgresschema.LockWaitTimeout)
	defer timer.Stop()
	select {
	case <-contended:
		b.statsMu.Lock()
		b.contentionSeen = true
		b.statsMu.Unlock()
	case <-timer.C:
	}
}

func (b *Backend) runEntryHook() {
	b.statsMu.Lock()
	hook := b.entryHook
	b.statsMu.Unlock()
	if hook != nil {
		hook()
	}
}

func expectedObject(name string) (postgresschema.Object, bool) {
	for _, obj := range postgresschema.ExpectedObjects(schema) {
		if obj.Name == name {
			return obj, true
		}
	}
	return postgresschema.Object{}, false
}

func unqualified(name string) string {
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		return name[i+1:]
	}
	return name
}
