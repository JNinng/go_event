package postgrec

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/postgresschema"
)

// adminLockWait 是管理写互斥锁的有界等待上限（与迁移锁同一把锁，语义见
// postgresschema.AcquireExclusiveLock）；测试可通过 export_test.go 缩短。
var adminLockWait = postgresschema.LockWaitTimeout

// admin 管理面写操作载体（独立类型，不挂在 Driver 上）：复用 Driver 的
// 连接、命名空间与迁移模式语义（ensure），管理写路径不经 Driver 类型
// 暴露（design.md §6 三面分离）。
type admin struct {
	d *Driver

	// 观测注入（操作来源记录，ADR 0010）：指标走 otel、日志走 slog、
	// 类型化 Observer 经 ObserverV3 分发，均为旁路。
	logger *slog.Logger
	am     *adminMetrics
	obs    event.Observer
	schema string
}

// NewAdmin 构造管理面 event.Admin：与 NewDriver 共享连接与命名空间选项
// （同一 schema、同一迁移模式语义——首次操作按模式处理持久结构）。管理
// 写操作不经运行面类型获得——经本构造或组合对象 Store 聚合（同一内部
// 构造路径）；Driver 类型上不存在管理写路径；鉴权由使用方负责。
func NewAdmin(db *sql.DB, opts ...Option) event.Admin {
	return newAdminOn(newDriver(db, opts...), newConfig(opts))
}

// newAdminOn 以给定 Driver 与配置构造管理面载体：NewStore 复用其唯一
// Driver 实例（共享 ensure 状态与失败负缓存），NewAdmin 自建实例。
func newAdminOn(d *Driver, cfg *config) *admin {
	return &admin{
		d:      d,
		logger: cfg.logger,
		am:     newAdminMetrics(cfg.mp, cfg.scope),
		obs:    cfg.obs,
		schema: cfg.schema,
	}
}

// ReplayFailed 把（事件 × 消费者身份）的 failed 终态认领行重新置为可认领
// （手工重放，语义见 event.Admin.ReplayFailed）。
//
// 执行次序：ensure（迁移模式语义）→ 事务内取命名空间互斥锁（与迁移执行
// 同一把 advisory 锁，键含 schema——写与迁移互斥、不同 schema 互不阻塞，
// 有界等待）→ 单条条件更新（CAS，归属校验：事件 × 身份主键 + status=
// 'failed'）→ 提交（锁随事务释放）。重放置 retrying、claim_count=0、清
// lease_token、lease_until / next_eligible_at 置为 now、failed_at 清空、
// last_error 保留（ADR 0009）。命中 0 行即行不存在或非 failed 终态
// （done / active / retrying）：ok=false、无错误，判别方式统一——需要区分
// 原因时先经 Inspector.ClaimState 查询。
//
// 错误可判别：锁等待超时映射 ErrAdminLockTimeout（瞬时）；迁移面哨兵经
// ensure 原样上抛；连接 / 驱动故障原样包装。执行故障不得折叠为 ok=false。
func (a *admin) ReplayFailed(ctx context.Context, eventID, consumerType string, now time.Time) (bool, error) {
	ok, err := a.replayFailed(ctx, eventID, consumerType, now)
	a.record(ctx, eventID, consumerType, ok, err)
	return ok, err
}

func (a *admin) replayFailed(ctx context.Context, eventID, consumerType string, now time.Time) (bool, error) {
	// 调用方取消：不发起写。pre-canceled ctx 在连接池空闲快速路径下可能
	// 不被驱动观察到，显式预检保证取消语义确定（canceled 结果判别）。
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := a.d.ensure(ctx); err != nil {
		return false, err
	}
	// 重放事务以 Read Committed 开始（比照迁移事务先例）：避免连接串默认
	// 隔离级别（如 repeatable read）下 CAS 以旧快照判定、迟到重放以 40001
	// 串行化冲突浮出——契约承诺非 failed 行统一 ok=false。
	tx, err := a.d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, fmt.Errorf("postgrec: admin replay: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 命名空间互斥锁：与迁移执行共用同一把 advisory 锁（迁移在整版事务
	// 内持锁执行 DDL），管理写必须等迁移提交/回滚后才改行——DDL 加列与
	// 行更新不再竞争表锁，双方等待均有界。
	if err := postgresschema.AcquireExclusiveLock(ctx, sqlMigrationTx{tx: tx}, a.d.schema, adminLockWait); err != nil {
		if errors.Is(err, event.ErrMigrationLockTimeout) {
			return false, fmt.Errorf("postgrec: admin replay: %w", event.ErrAdminLockTimeout)
		}
		return false, fmt.Errorf("postgrec: admin replay: %w", err)
	}

	tag, err := tx.ExecContext(ctx, fmt.Sprintf(`
UPDATE %s.event_claim
SET status = 'retrying', claim_count = 0, lease_token = '', lease_until = $3,
	next_eligible_at = $3, failed_at = NULL
WHERE event_id = $1 AND consumer_type = $2 AND status = 'failed'`, a.d.schema),
		eventID, consumerType, now)
	if err != nil {
		return false, fmt.Errorf("postgrec: admin replay: %w", err)
	}
	rows, err := tag.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("postgrec: admin replay rows: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return false, fmt.Errorf("postgrec: admin replay: commit: %w", err)
	}
	return rows > 0, nil
}

// record 记录操作来源（观测均为旁路，失败不影响业务结果）：otel 计数 +
// slog 审计行（操作、事件 × 身份、结果；不含事件 payload 与 last_error
// 摘要，失败结果附 error.type / error 分类属性）+ 类型化 Observer 事件
// （ObserverV3 分发）。
func (a *admin) record(ctx context.Context, eventID, consumerType string, ok bool, err error) {
	res := adminResult(ok, err)
	a.am.record(ctx, a.schema, ok, err)

	attrs := []any{
		"op", "replay_failed",
		"schema", a.schema,
		"event_id", eventID,
		"consumer_type", consumerType,
		"result", res,
	}
	switch res {
	case event.AdminReplayCanceled:
		a.logger.InfoContext(ctx, "postgrec: admin replay canceled", attrs...)
	case event.AdminReplayLockTimeout, event.AdminReplayError:
		a.logger.ErrorContext(ctx, "postgrec: admin replay failed",
			append(attrs, logErrAttrs(err)...)...)
	default:
		a.logger.InfoContext(ctx, "postgrec: admin replay", attrs...)
	}

	observeAdminReplay(a.obs, event.AdminReplayEvent{
		EventID:      eventID,
		ConsumerType: consumerType,
		Result:       res,
		Err:          err,
	})
}
