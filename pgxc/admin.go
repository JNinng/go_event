package pgxc

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jninng/go_event"
)

// admin 管理面写操作载体（独立类型，不挂在 Driver 上）：复用 Driver 的
// 连接、命名空间与迁移模式语义（ensure），管理写路径不经 Driver 类型
// 暴露（design.md §6 三面分离）。
type admin struct {
	d *Driver
}

// NewAdmin 构造管理面 event.Admin：与 NewDriver 共享连接与命名空间选项
// （同一 schema、同一迁移模式语义——首次操作按模式处理持久结构）。管理
// 写操作只经本构造获得，Driver 类型上不存在管理写路径；鉴权由使用方负责。
func NewAdmin(pool *pgxpool.Pool, opts ...Option) event.Admin {
	return &admin{d: newDriver(pool, opts...)}
}

// ReplayFailed 把（事件 × 消费者身份）的 failed 终态认领行重新置为可认领
// （手工重放，语义见 event.Admin.ReplayFailed）。单条条件更新（CAS，
// WHERE status='failed'）保证并发重放 / 认领 / Fail 竞态下不改写非
// failed 行：重放置 retrying、claim_count=0、清 lease_token、lease_until
// 置为 now、next_eligible_at=now、failed_at 清空、last_error 保留
// （ADR 0009）。命中 0 行即行不存在或非 failed 终态（done / active /
// retrying）：ok=false、无错误，判别方式统一——需要区分原因时先经
// Inspector.ClaimState 查询。
func (a *admin) ReplayFailed(ctx context.Context, eventID, consumerType string, now time.Time) (bool, error) {
	if err := a.d.ensure(ctx); err != nil {
		return false, err
	}
	tag, err := a.d.pool.Exec(ctx, fmt.Sprintf(`
UPDATE %s.event_claim
SET status = 'retrying', claim_count = 0, lease_token = '', lease_until = $3,
	next_eligible_at = $3, failed_at = NULL
WHERE event_id = $1 AND consumer_type = $2 AND status = 'failed'`, a.d.schema),
		eventID, consumerType, now)
	if err != nil {
		return false, fmt.Errorf("pgxc: admin replay failed: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}
