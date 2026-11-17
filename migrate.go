package event

import (
	"errors"
	"fmt"
)

// MigrateMode 是运行面（Driver 构造期）的迁移模式（design.md §8）。
// 模式只对 NewDriver 有意义：显式传给 Migrate 返回 ErrMigrateModeUnsupported。
type MigrateMode int

const (
	// MigrateCreate 缺省模式：首次使用时持迁移锁幂等应用版本计划全量
	// 语句（基线与各版本；升级随部署隐式发生，无独立 Upgrade 模式）。
	// 需要 DDL 权限。
	MigrateCreate MigrateMode = iota

	// MigrateVerify 只读校验：不执行 DDL，只校验对象是否与计划期望一致。
	// 表须由部署流程创建（或手工脚本）；运行账号可只授 DML。
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
	// ProblemMissing 对象缺失（可由版本计划补齐；对后续版本对象表示
	// 库落后于二进制，处置为执行迁移）。
	ProblemMissing VerifyProblem = "missing"
	// ProblemOutdated 对象已存在但定义与计划期望不一致（不自动修复）。
	ProblemOutdated VerifyProblem = "outdated"
	// ProblemTooNew 预留：库内对象超出计划期望且需判定为「库版本高于
	// 二进制」时使用；无台账模型（对象集合即事实）当前不产生该问题，
	// 保留枚举以稳定 API。
	ProblemTooNew VerifyProblem = "too_new"
	// ProblemFailed 预留：迁移台账被否决（见 ADR 0007），当前模型不产生
	// 该问题，保留枚举以稳定 API。
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

// MigrationCapabilities 是后端迁移能力声明，供契约测试与文档判定。锁能力
// 为无（LockNone）时 Create 返回 ErrMigrationUnsupported；DDL 原子性为无
// （DDLAtomicNone）时同样返回该错误。
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
	// NonTransactionalSteps 表示计划可包含必须在事务外执行的语句
	// （如并发建索引）；Postgres 单一执行路径当前只执行可事务化语句，
	// 含此类语句的版本引入时按当时能力扩展执行器。
	NonTransactionalSteps bool
}

// FaultPoint 是迁移执行器可注入故障的边界点（语句边界：单条语句执行前后
// 与事务提交前）；供迁移契约测试使用。
type FaultPoint string

const (
	FaultAfterLock       FaultPoint = "after_lock"
	FaultBeforeStatement FaultPoint = "before_statement"
	FaultAfterStatement  FaultPoint = "after_statement"
	FaultBeforeCommit    FaultPoint = "before_commit"
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
