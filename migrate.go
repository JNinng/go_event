package event

import (
	"errors"
	"fmt"
)

// MigrateMode 是运行面（Driver 构造期）的迁移模式（design.md §8）。
// 模式只对 NewDriver 有意义：显式传给 Migrate 返回 ErrMigrateModeUnsupported。
type MigrateMode int

const (
	// MigrateCreate 缺省模式：首次使用时执行基线（持迁移锁）。
	// 只建基线，不升级存量表；需要 DDL 权限。
	MigrateCreate MigrateMode = iota

	// MigrateVerify 只读校验：不执行 DDL，只校验对象是否与基线一致。
	// 表须由部署流程创建；运行账号可只授 DML。
	MigrateVerify

	// MigrateNone 不建表也不校验，完全交给调用方（部署流程与权限自担）。
	MigrateNone
)

func (m MigrateMode) String() string {
	switch m {
	case MigrateCreate:
		return "create"
	case MigrateVerify:
		return "verify"
	case MigrateNone:
		return "none"
	default:
		return fmt.Sprintf("MigrateMode(%d)", int(m))
	}
}

// ObjectKind 迁移对象的类型。
type ObjectKind string

const (
	KindSchema     ObjectKind = "schema"
	KindTable      ObjectKind = "table"
	KindColumn     ObjectKind = "column"
	KindIndex      ObjectKind = "index"
	KindConstraint ObjectKind = "constraint"
)

// VerifyProblem 是只读校验发现的问题类别。
type VerifyProblem string

const (
	// ProblemMissing 对象缺失（可由基线补齐）。
	ProblemMissing VerifyProblem = "missing"
	// ProblemOutdated 对象已存在但定义与基线不一致（基线不自动修复）。
	ProblemOutdated VerifyProblem = "outdated"
	// ProblemTooNew 预留 P1：库内版本高于二进制要求。
	ProblemTooNew VerifyProblem = "too_new"
	// ProblemFailed 预留 P1：版本处于 failed 状态。
	ProblemFailed VerifyProblem = "failed"
)

// VerifyIssue 是校验报告中的一项。Object 是稳定标识（如 "public"、
// "public.event"、"public.event.id"），契约测试只断言字段、不断言 Action
// 文本。
type VerifyIssue struct {
	Object  string
	Kind    ObjectKind
	Problem VerifyProblem
	Action  string
}

// VerifyReport 是只读校验报告；Issues 为空表示与基线一致。
type VerifyReport struct {
	Issues []VerifyIssue
}

// OK 报告是否没有发现问题。
func (r VerifyReport) OK() bool {
	return len(r.Issues) == 0
}

// VerifyError 在 Verify 发现问题时返回，携带结构化报告；
// errors.As 可取回 *VerifyError 后读取 Report。
type VerifyError struct {
	Report VerifyReport
}

func (e *VerifyError) Error() string {
	if e == nil {
		return "event: schema verification failed"
	}
	return fmt.Sprintf("event: schema verification failed with %d issue(s)", len(e.Report.Issues))
}

// DDLAtomicity 声明后端的 DDL 原子性档位（决定迁移执行路径）。
type DDLAtomicity int

const (
	// DDLAtomicNone 无 DDL 原子性：不支持自动迁移（只能 Verify/None）。
	DDLAtomicNone DDLAtomicity = iota
	// DDLAtomicSingle 只有单语句原子性。
	DDLAtomicSingle
	// DDLAtomicTransactional 支持跨语句事务。
	DDLAtomicTransactional
)

// NativeSupport 声明某对象类型对 IF NOT EXISTS 语法的原生支持程度。
type NativeSupport int

const (
	SupportNone NativeSupport = iota
	// SupportWarnOnly 接受语法但会告警/有限支持。
	SupportWarnOnly
	SupportNative
)

// ProbeSupport 声明某对象类型的探测能力。
type ProbeSupport int

const (
	ProbeNone ProbeSupport = iota
	// ProbeExistence 只能探测存在性。
	ProbeExistence
	// ProbeDefinition 可探测存在性并比对定义。
	ProbeDefinition
)

// LockPrimitive 声明迁移锁的取得方式。
type LockPrimitive int

const (
	LockNone LockPrimitive = iota
	// LockRowLease 锁行租约（需 TTL、续约与过期回收）。
	LockRowLease
	// LockSession 原生会话锁（固定连接、断连释放）。
	LockSession
	// LockTransaction 事务级锁（随提交/回滚释放）。
	LockTransaction
)

// LockRelease 声明迁移锁的释放语义。注意零值是 LockReleaseTransaction。
type LockRelease int

const (
	LockReleaseTransaction LockRelease = iota
	LockReleaseSession
	LockReleaseLease
)

// DDLCompletion 声明 DDL 的完成语义。
type DDLCompletion int

const (
	// DDLSync DDL 返回即完成。
	DDLSync DDLCompletion = iota
	// DDLAsync DDL 可能返回异步 job。
	DDLAsync
)

// ErrorClasses 声明后端可识别的错误类别。
type ErrorClasses struct {
	Exists     bool // 对象已存在/重复定义
	Permission bool // 权限不足
	Transient  bool // 瞬时错误（可退避重试）
}

// MigrationCapabilities 是后端迁移能力声明，供契约测试与文档判定；P1
// 执行器按声明选择路径。锁能力为无（LockNone）时 Create 返回
// ErrMigrationUnsupported；DDL 原子性为无（DDLAtomicNone）时同样返回该错误
// （P0 无此档后端）。
type MigrationCapabilities struct {
	Atomicity   DDLAtomicity
	IfNotExists map[ObjectKind]NativeSupport
	Probe       map[ObjectKind]ProbeSupport
	Errors      ErrorClasses
	Lock        LockPrimitive
	LockRelease LockRelease
	Completion  DDLCompletion
	// ImplicitCommit 表示 DDL 是否隐式提交（把语句放进事务也无法整体回滚）。
	ImplicitCommit bool
	// NonTransactionalSteps 表示迁移计划可声明必须在事务外执行的步骤
	// （如并发建索引）；含此类步骤的版本不得走 fast 路径。
	NonTransactionalSteps bool
}

// MigrationPath 是单版本迁移的执行路径；P0 由迁移契约测试使用，P1 台账/
// step 状态机复用。
type MigrationPath int

const (
	// MigrationPathFast 单事务：锁、基线、版本 DDL、台账同事务。
	MigrationPathFast MigrationPath = iota
	// MigrationPathStep 逐步探测/执行（无跨语句事务或含不可事务化步骤）。
	MigrationPathStep
)

// FaultPoint 是迁移执行器可注入故障的边界点；供迁移契约测试使用，P1
// 状态机复用。
type FaultPoint string

const (
	FaultAfterLock    FaultPoint = "after_lock"
	FaultBeforeStep   FaultPoint = "before_step"
	FaultAfterStep    FaultPoint = "after_step"
	FaultBeforeCommit FaultPoint = "before_commit"
)

// IsPermanentMigrationError 报告迁移失败是否属于永久配置错误：此类错误
// 应 fail-fast，不进入重试窗口。瞬时错误（数据库不可用、锁等待超时）
// 返回 false。
func IsPermanentMigrationError(err error) bool {
	if err == nil {
		return false
	}
	var verifyErr *VerifyError
	if errors.As(err, &verifyErr) {
		return true
	}
	return errors.Is(err, ErrMigrationUnsupported) ||
		errors.Is(err, ErrMigrateModeUnsupported) ||
		errors.Is(err, ErrMigrationPermissionDenied) ||
		errors.Is(err, ErrInvalidSchema)
}
