package pgxc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/postgresschema"
)

// Migrate 幂等应用版本计划（Create 语义）：在单事务内取迁移锁、做接管
// 校验（仅缺对象放行）、执行计划内全部语句（基线与各版本，全部幂等），
// 失败整体回滚、中断重跑即收敛；升级随部署隐式发生。
//
// 需要 DDL 权限，仅 DML 账号应改用 Verify/None（部署流程建表）；
// WithMigrateMode 对 NewDriver / NewStore（内部 Driver 构造期）生效，
// 显式传入本函数返回 ErrMigrateModeUnsupported。
func Migrate(ctx context.Context, pool *pgxpool.Pool, opts ...Option) error {
	cfg := newConfig(opts)
	if cfg.modeSet {
		return fmt.Errorf("pgxc: migrate: %w", event.ErrMigrateModeUnsupported)
	}
	if err := postgresschema.ValidateSchema(cfg.schema); err != nil {
		return fmt.Errorf("pgxc: migrate: %w: %v", event.ErrInvalidSchema, err)
	}
	mm := newMigrationMetrics(cfg.mp, cfg.scope)
	start := time.Now()
	err := postgresschema.Migrate(ctx, pgxMigrator{pool: pool}, cfg.schema,
		postgresschema.WithMigrationLockWait(lockWaitTimeout),
		postgresschema.WithMigrationClassify(func(s postgresschema.PlannedStatement, err error) error {
			return classifyMigrationError("pgxc", s, err)
		}),
		postgresschema.WithMigrationHook(func(s postgresschema.PlannedStatement) error {
			if testStatementHook == nil {
				return nil
			}
			return testStatementHook(s)
		}),
	)
	mm.record(ctx, cfg.scope, cfg.schema, err, time.Since(start))
	if err != nil {
		return err
	}
	cfg.logger.Info("pgxc: migration applied",
		"schema", cfg.schema, "mode", cfg.mode.String(),
		"duration_seconds", time.Since(start).Seconds())
	return nil
}

// Verify 只读校验：不执行任何 DDL，只比对 schema/表/列/主键/唯一/索引/
// 外键是否与计划期望（全量版本）一致。缺失表示待升级（处置为执行迁移）；
// 定义冲突返回 *event.VerifyError；查询本身失败时返回错误。只依赖表已由
// 部署流程创建，运行账号可只授 DML 权限。
func Verify(ctx context.Context, pool *pgxpool.Pool, opts ...Option) (event.VerifyReport, error) {
	cfg := newConfig(opts)
	if err := postgresschema.ValidateSchema(cfg.schema); err != nil {
		return event.VerifyReport{}, fmt.Errorf("pgxc: verify: %w: %v", event.ErrInvalidSchema, err)
	}
	mm := newMigrationMetrics(cfg.mp, cfg.scope)
	start := time.Now()
	report, err := verifySnapshot(ctx, pool, cfg.schema)
	if err != nil {
		mm.record(ctx, cfg.scope, cfg.schema, err, time.Since(start))
		return event.VerifyReport{}, err
	}
	if !report.OK() {
		mm.record(ctx, cfg.scope, cfg.schema, &event.VerifyError{Report: report}, time.Since(start))
		return report, &event.VerifyError{Report: report}
	}
	mm.record(ctx, cfg.scope, cfg.schema, nil, time.Since(start))
	return report, nil
}

// verifySnapshot 在只读事务内读取系统目录快照并比对计划期望。
func verifySnapshot(ctx context.Context, pool *pgxpool.Pool,
	schema string) (event.VerifyReport, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return event.VerifyReport{}, fmt.Errorf("pgxc: verify: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	snap, err := postgresschema.ReadSnapshot(ctx, pgxQueryer{q: tx}, schema)
	if err != nil {
		return event.VerifyReport{}, fmt.Errorf("pgxc: verify: %w", err)
	}
	return postgresschema.Diff(postgresschema.ExpectedObjects(schema), snap), nil
}

// Statements 返回计划内全部语句（不含迁移锁），按执行顺序排列；与执行器
// 共用同一事实源（postgresschema.Plan）。
func Statements(opts ...Option) ([]string, error) {
	cfg := newConfig(opts)
	stmts, err := postgresschema.Statements(cfg.schema)
	if err != nil {
		return nil, fmt.Errorf("pgxc: statements: %w: %v", event.ErrInvalidSchema, err)
	}
	return stmts, nil
}

// SQL 返回手工可执行的迁移脚本：以 BEGIN 开始，随后设 lock_timeout 并取
// 迁移锁（锁键含 schema，锁语句位于全部 DDL 之前），随后依次输出计划内
// 全部语句（基线 + 各版本，全部幂等），末尾 COMMIT。整段必须在同一事务
// 运行、整段可重放；人工执行不得拆分跳过。
func SQL(opts ...Option) (string, error) {
	cfg := newConfig(opts)
	script, err := postgresschema.SQL(cfg.schema)
	if err != nil {
		return "", fmt.Errorf("pgxc: sql: %w: %v", event.ErrInvalidSchema, err)
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
		NonTransactionalSteps: true,  // 计划可包含事务外语句（如并发建索引）
	}
}

// testStatementHook 仅供测试在语句执行前注入失败（生产路径为 nil）。
var testStatementHook func(postgresschema.PlannedStatement) error

// lockWaitTimeout 是迁移锁的有界等待上限；测试可通过 export_test.go 缩短。
var lockWaitTimeout = postgresschema.LockWaitTimeout

// pgxMigrator 把 *pgxpool.Pool 适配为 postgresschema.MigrationConnector。
type pgxMigrator struct {
	pool *pgxpool.Pool
}

// BeginMigration 以 Read Committed 开始迁移事务：避免连接串默认隔离级别
// （如 repeatable read）让锁释放后仍用旧快照做接管判定。
func (m pgxMigrator) BeginMigration(ctx context.Context) (postgresschema.MigrationTx, error) {
	tx, err := m.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	return pgxMigrationTx{tx: tx}, nil
}

type pgxMigrationTx struct {
	tx pgx.Tx
}

func (t pgxMigrationTx) ExecContext(ctx context.Context, query string, args ...any) error {
	_, err := t.tx.Exec(ctx, query, args...)
	return err
}

func (t pgxMigrationTx) QueryContext(ctx context.Context, query string, args ...any) (postgresschema.Rows, error) {
	return pgxQueryer{q: t.tx}.QueryContext(ctx, query, args...)
}

func (t pgxMigrationTx) QueryRowContext(ctx context.Context, query string, args ...any) postgresschema.MigrationRow {
	return t.tx.QueryRow(ctx, query, args...)
}

func (t pgxMigrationTx) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

func (t pgxMigrationTx) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

// classifyMigrationError 用 pgconn.PgError 识别重复对象、权限不足与瞬时
// 错误并包装为对应哨兵；未知错误原样包装，不吞掉。
func classifyMigrationError(prefix string, stmt postgresschema.PlannedStatement, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42710", "42P07", "42701":
			// 对象已存在/重复定义：接管校验视为可补齐却仍冲突，说明存在
			// 无法判定的同名对象。按差异错误返回，不自动修复。
			return &event.VerifyError{Report: event.VerifyReport{Issues: []event.VerifyIssue{{
				Object:  stmt.Object.Name,
				Kind:    stmt.Object.Kind,
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
				Object:  stmt.Object.Name,
				Kind:    stmt.Object.Kind,
				Problem: event.ProblemOutdated,
				Action:  "clean up conflicting duplicate data before retrying the migration",
			}}}}
		}
	}
	return fmt.Errorf("%s: migrate: %w", prefix, err)
}

// pgxQueryer 把 *pgxpool.Pool / pgx.Tx 适配为 postgresschema.Queryer。
type pgxQueryer struct {
	q interface {
		Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	}
}

func (a pgxQueryer) QueryContext(ctx context.Context, query string, args ...any) (postgresschema.Rows, error) {
	rows, err := a.q.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return pgxRows{Rows: rows}, nil
}

// pgxRows 把 pgx.Rows.Close()（无返回值）适配为 postgresschema.Rows。
type pgxRows struct {
	pgx.Rows
}

func (r pgxRows) Close() error {
	r.Rows.Close()
	return nil
}
