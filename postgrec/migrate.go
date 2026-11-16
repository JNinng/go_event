package postgrec

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/postgresschema"
)

// Migrate 执行基线迁移：在单事务内取迁移锁、校验既有对象、执行幂等
// 基线 DDL（schema、event / event_claim 两表、主键/唯一/外键约束与两个
// 索引），任何一步失败整体回滚。可重复执行、不破坏既有数据与状态。
//
// 需要 DDL 权限；运行账号只有 DML 权限时应使用 Verify 模式或 None 模式，
// 由部署流程建表。WithMigrateMode 只对 NewDriver 有意义，显式传给本函数
// 返回 ErrMigrateModeUnsupported。
func Migrate(ctx context.Context, db *sql.DB, opts ...Option) error {
	cfg := newConfig(opts)
	if cfg.modeSet {
		return fmt.Errorf("postgrec: migrate: %w", event.ErrMigrateModeUnsupported)
	}
	if err := postgresschema.ValidateSchema(cfg.schema); err != nil {
		return fmt.Errorf("postgrec: migrate: %w: %v", event.ErrInvalidSchema, err)
	}
	return migrateFast(ctx, db, cfg.schema)
}

// Verify 只读校验：不执行任何 DDL，只比对 schema/表/列/主键/唯一/索引/
// 外键是否与基线一致。发现问题时返回报告与 *event.VerifyError；查询本身
// 失败时返回错误。只依赖表已由部署流程创建，运行账号可只授 DML 权限。
func Verify(ctx context.Context, db *sql.DB, opts ...Option) (event.VerifyReport, error) {
	cfg := newConfig(opts)
	if err := postgresschema.ValidateSchema(cfg.schema); err != nil {
		return event.VerifyReport{}, fmt.Errorf("postgrec: verify: %w: %v", event.ErrInvalidSchema, err)
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return event.VerifyReport{}, fmt.Errorf("postgrec: verify: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	snap, err := postgresschema.ReadSnapshot(ctx, sqlQueryer{q: tx}, cfg.schema)
	if err != nil {
		return event.VerifyReport{}, fmt.Errorf("postgrec: verify: %w", err)
	}
	report := postgresschema.Diff(postgresschema.ExpectedObjects(cfg.schema), snap)
	if !report.OK() {
		return report, &event.VerifyError{Report: report}
	}
	return report, nil
}

// Statements 返回基线语句（不含迁移锁），按执行顺序排列。
func Statements(opts ...Option) ([]string, error) {
	cfg := newConfig(opts)
	stmts, err := postgresschema.Statements(cfg.schema)
	if err != nil {
		return nil, fmt.Errorf("postgrec: statements: %w: %v", event.ErrInvalidSchema, err)
	}
	return stmts, nil
}

// SQL 返回手工可执行的迁移脚本：以 BEGIN 开始，随后设 lock_timeout 并取
// 迁移锁（锁键含 schema，锁语句位于全部基线 DDL 之前），末尾 COMMIT。
// 整段必须在同一事务运行。
func SQL(opts ...Option) (string, error) {
	cfg := newConfig(opts)
	script, err := postgresschema.SQL(cfg.schema)
	if err != nil {
		return "", fmt.Errorf("postgrec: sql: %w: %v", event.ErrInvalidSchema, err)
	}
	return script, nil
}

// Capabilities 返回本后端的迁移能力声明（供契约测试与文档矩阵读取）。
func Capabilities() event.MigrationCapabilities {
	return event.MigrationCapabilities{
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
		ImplicitCommit:        false, // 事务内 DDL 可整体回滚
		NonTransactionalSteps: true,  // 计划可声明事务外步骤（如并发建索引）
	}
}

// testStepHook 仅供测试在步骤执行前注入失败（生产路径为 nil）。
var testStepHook func(postgresschema.Step) error

// lockWaitTimeout 是迁移锁的有界等待上限；测试可通过 export_test.go 缩短。
var lockWaitTimeout = postgresschema.LockWaitTimeout

// migrateFast 是 fast 路径执行器：锁、基线接管校验与基线语句同事务，
// 全成或全败；失败回滚即恢复，不逐对象探测（接管校验除外）。
func migrateFast(ctx context.Context, db *sql.DB, schema string) error {
	steps, err := postgresschema.Baseline(schema)
	if err != nil {
		return fmt.Errorf("postgrec: migrate: %w", err)
	}
	// 显式 READ COMMITTED：避免连接串默认隔离级别（如 repeatable read）
	// 让锁释放后仍用旧快照做接管判定。
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("postgrec: migrate: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 有界等待：try-lock + 退避（总上限 LockWaitTimeout）；DDL 自身的
	// 锁等待由 lock_timeout 限定并映射为瞬时错误。
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'",
		lockWaitTimeout.Milliseconds())); err != nil {
		return classifyMigrationError("postgrec", postgresschema.Step{}, err)
	}
	if err := acquireLock(ctx, tx, schema); err != nil {
		return err
	}

	// 基线接管校验：已有对象与基线一致且仅缺对象 → 补齐；任一已有对象
	// 定义冲突或无法判定 → 回滚并返回差异错误，不自动修复。
	snap, err := postgresschema.ReadSnapshot(ctx, sqlQueryer{q: tx}, schema)
	if err != nil {
		return fmt.Errorf("postgrec: migrate: verify existing objects: %w", err)
	}
	report := postgresschema.Diff(postgresschema.ExpectedObjects(schema), snap)
	if !postgresschema.Repairable(report) {
		return &event.VerifyError{Report: report}
	}

	for _, step := range steps {
		if testStepHook != nil {
			if err := testStepHook(step); err != nil {
				return fmt.Errorf("postgrec: migrate: %w", err)
			}
		}
		if _, err := tx.ExecContext(ctx, step.Statement); err != nil {
			return classifyMigrationError("postgrec", step, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("postgrec: migrate: commit: %w", err)
	}
	return nil
}

func acquireLock(ctx context.Context, tx *sql.Tx, schema string) error {
	deadline := time.Now().Add(lockWaitTimeout)
	backoff := 10 * time.Millisecond
	for {
		var ok bool
		if err := tx.QueryRowContext(ctx, postgresschema.TryLockStatement,
			postgresschema.LockKey(schema)).Scan(&ok); err != nil {
			return classifyMigrationError("postgrec", postgresschema.Step{}, err)
		}
		if ok {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("postgrec: migrate: %w", event.ErrMigrationLockTimeout)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("postgrec: migrate: %w", ctx.Err())
		case <-time.After(backoff):
		}
		if backoff < 100*time.Millisecond {
			backoff *= 2
			if backoff > 100*time.Millisecond {
				backoff = 100 * time.Millisecond
			}
		}
	}
}

// classifyMigrationError 用 pgconn.PgError 识别重复对象、权限不足与瞬时
// 错误并包装为对应哨兵；未知错误原样包装，不吞掉。
func classifyMigrationError(prefix string, step postgresschema.Step, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42710", "42P07", "42701":
			// 对象已存在/重复定义：接管校验视为可补齐却仍冲突，说明存在
			// 无法判定的同名对象。按差异错误返回，不自动修复。
			return &event.VerifyError{Report: event.VerifyReport{Issues: []event.VerifyIssue{{
				Object:  step.Object,
				Kind:    step.Kind,
				Problem: event.ProblemOutdated,
				Action:  "inspect and align the conflicting object manually",
			}}}}
		case "42501":
			return fmt.Errorf("%s: migrate: %w: %w", prefix, event.ErrMigrationPermissionDenied, err)
		case "55P03":
			return fmt.Errorf("%s: migrate: %w: %w", prefix, event.ErrMigrationLockTimeout, err)
		case "23505":
			// pg_type 元数据竞态：并发建表时唯一索引冲突，属瞬时错误。
			if pgErr.ConstraintName == "pg_type_typname_nsp_index" {
				return fmt.Errorf("%s: migrate: transient catalog conflict: %w", prefix, err)
			}
			return &event.VerifyError{Report: event.VerifyReport{Issues: []event.VerifyIssue{{
				Object:  step.Object,
				Kind:    step.Kind,
				Problem: event.ProblemOutdated,
				Action:  "clean up conflicting duplicate data before retrying the migration",
			}}}}
		}
	}
	return fmt.Errorf("%s: migrate: %w", prefix, err)
}

// sqlQueryer 把 *sql.DB / *sql.Tx 适配为 postgresschema.Queryer。
type sqlQueryer struct {
	q interface {
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	}
}

func (a sqlQueryer) QueryContext(ctx context.Context, query string, args ...any) (postgresschema.Rows, error) {
	return a.q.QueryContext(ctx, query, args...)
}
