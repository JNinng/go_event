package postgresschema

import (
	"context"
	"fmt"
	"time"

	"github.com/jninng/go_event"
)

// MigrationRow 是单行查询结果的最小视图（pgx.Row / *sql.Row 直接满足）。
type MigrationRow interface {
	Scan(dest ...any) error
}

// MigrationTx 是迁移事务的最小能力（后端把 pgx.Tx / *sql.Tx 适配进来）：
// DDL 执行、系统目录查询、try-lock 单行查询与提交/回滚。事务须以
// Read Committed 开始，避免锁释放后仍用旧快照做接管判定。
type MigrationTx interface {
	ExecContext(ctx context.Context, query string, args ...any) error
	QueryContext(ctx context.Context, query string, args ...any) (Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) MigrationRow
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// MigrationConnector 提供迁移事务；实现方决定连接载体（连接池 / *sql.DB）。
type MigrationConnector interface {
	BeginMigration(ctx context.Context) (MigrationTx, error)
}

// MigrationOption 是共享执行器的选项。
type MigrationOption func(*migrationConfig)

type migrationConfig struct {
	lockWait time.Duration
	// classify 把语句执行错误映射为迁移错误（哨兵或 *VerifyError）；
	// nil 表示原样返回。锁相关语句以零值 PlannedStatement 调用。
	classify func(PlannedStatement, error) error
	// hook 在每条计划语句执行前调用（返回错误即中断）；生产路径为 nil，
	// 供迁移契约测试注入语句边界故障。
	hook func(PlannedStatement) error
}

// WithMigrationLockWait 覆盖迁移锁的有界等待上限（测试缩短用）。
func WithMigrationLockWait(d time.Duration) MigrationOption {
	return func(c *migrationConfig) { c.lockWait = d }
}

// WithMigrationClassify 注入后端错误分类器（如 pgconn.PgError 到哨兵的
// 映射）；执行器对锁语句与计划语句的执行错误调用它。
func WithMigrationClassify(fn func(PlannedStatement, error) error) MigrationOption {
	return func(c *migrationConfig) { c.classify = fn }
}

// WithMigrationHook 注入语句边界钩子（测试故障注入用；生产为 nil）。
func WithMigrationHook(fn func(PlannedStatement) error) MigrationOption {
	return func(c *migrationConfig) { c.hook = fn }
}

// Migrate 是共享迁移执行器（单一执行路径）：取锁（有界等待）→ 接管校验
// （对象集合与计划期望比对：仅缺对象放行执行；任一定义冲突 → 差异错误，
// 不修复）→ 整版单事务依次执行计划内全部语句，任何失败整体回滚；中断后
// 重跑即收敛。不做逐对象探测、不写执行状态、无台账（对象集合即事实）。
func Migrate(ctx context.Context, conn MigrationConnector, schema string, opts ...MigrationOption) error {
	if err := ValidateSchema(schema); err != nil {
		return fmt.Errorf("postgresschema: migrate: %w: %v", event.ErrInvalidSchema, err)
	}
	cfg := migrationConfig{lockWait: LockWaitTimeout}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	plan, err := PlannedStatements(schema)
	if err != nil {
		return fmt.Errorf("postgresschema: migrate: %w", err)
	}

	tx, err := conn.BeginMigration(ctx)
	if err != nil {
		return fmt.Errorf("postgresschema: migrate: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// DDL 自身的锁等待由 lock_timeout 限定并映射为瞬时错误（由后端分类器
	// 处理 55P03）。
	if err := tx.ExecContext(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'",
		cfg.lockWait.Milliseconds())); err != nil {
		return cfg.classifyError(PlannedStatement{}, err)
	}
	if err := acquireMigrationLock(ctx, tx, schema, cfg); err != nil {
		return err
	}

	// 接管校验：已有对象与计划期望一致且仅缺对象 → 放行执行；任一定义
	// 冲突或无法判定 → 回滚并返回差异错误，不自动修复。
	snap, err := ReadSnapshot(ctx, tx, schema)
	if err != nil {
		return fmt.Errorf("postgresschema: migrate: read snapshot: %w", err)
	}
	report := Diff(ExpectedObjects(schema), snap)
	if !Repairable(report) {
		return &event.VerifyError{Report: report}
	}

	for _, ps := range plan {
		if cfg.hook != nil {
			if err := cfg.hook(ps); err != nil {
				return fmt.Errorf("postgresschema: migrate: %w", err)
			}
		}
		if err := tx.ExecContext(ctx, ps.SQL); err != nil {
			return cfg.classifyError(ps, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgresschema: migrate: commit: %w", err)
	}
	return nil
}

func (c *migrationConfig) classifyError(ps PlannedStatement, err error) error {
	if c.classify == nil {
		return err
	}
	return c.classify(ps, err)
}

// acquireMigrationLock 有界等待取事务级迁移锁：try-lock + 退避，总上限
// lockWait；超时映射为瞬时错误 ErrMigrationLockTimeout。
func acquireMigrationLock(ctx context.Context, tx MigrationTx, schema string, cfg migrationConfig) error {
	deadline := time.Now().Add(cfg.lockWait)
	backoff := 10 * time.Millisecond
	for {
		var ok bool
		if err := tx.QueryRowContext(ctx, TryLockStatement, LockKey(schema)).Scan(&ok); err != nil {
			return cfg.classifyError(PlannedStatement{}, err)
		}
		if ok {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("postgresschema: migrate: %w", event.ErrMigrationLockTimeout)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("postgresschema: migrate: %w", ctx.Err())
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
