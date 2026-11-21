package pgxc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jninng/go_event"
)

// NewInspector 构造只读查询面 event.Inspector：与 NewDriver 共享连接与
// 命名空间选项（同一 schema、同一迁移模式语义——首次查询按模式处理持久
// 结构）。探测方式：调用方也可对 NewDriver 返回值做 event.Inspector
// 类型断言（本后端 Driver 同时实现该接口），见 docs/api.md §3。
func NewInspector(pool *pgxpool.Pool, opts ...Option) event.Inspector {
	return newDriver(pool, opts...)
}

// inspectStatus 把落库 status 映射为查询面状态：active（持有租约）对应
// processing，其余取值一致。
func inspectStatus(s string) event.ClaimStatus {
	if s == "active" {
		return event.ClaimProcessing
	}
	return event.ClaimStatus(s)
}

// ClaimState 查询（事件 × 消费者身份）认领行的落库事实；无认领行时
// ok 为 false（「待处理」态，含事件不存在与该身份尚未认领）。只读，
// 不做租约过期推断。
func (d *Driver) ClaimState(ctx context.Context, eventID, consumerType string) (event.ClaimState, bool, error) {
	if err := d.ensure(ctx); err != nil {
		return event.ClaimState{}, false, err
	}
	var status string
	var count int
	err := d.pool.QueryRow(ctx, fmt.Sprintf(`
SELECT status, claim_count FROM %s.event_claim
WHERE event_id = $1 AND consumer_type = $2`, d.schema),
		eventID, consumerType).Scan(&status, &count)
	if errors.Is(err, pgx.ErrNoRows) {
		return event.ClaimState{}, false, nil
	}
	if err != nil {
		return event.ClaimState{}, false, fmt.Errorf("pgxc: inspect claim state: %w", err)
	}
	return event.ClaimState{Status: inspectStatus(status), Attempts: count}, true, nil
}

// CountStates 按消费者身份对事件类型做状态分档计数。单条语句内完成：
// LEFT JOIN 认领行，无认领行（c.event_id IS NULL）按 superseded 属性细分
// ——未取代计入 Pending、已被合并取代计入 Superseded（事件行属性档，
// ADR 0014），其余按 status 列分档（口径见 event.Inspector.CountStates）。
func (d *Driver) CountStates(ctx context.Context, typ, consumerType string) (event.StateCounts, error) {
	if err := d.ensure(ctx); err != nil {
		return event.StateCounts{}, err
	}
	var c event.StateCounts
	err := d.pool.QueryRow(ctx, fmt.Sprintf(`
SELECT
	COUNT(*) FILTER (WHERE c.event_id IS NULL AND e.superseded_at IS NULL)     AS pending,
	COUNT(*) FILTER (WHERE c.event_id IS NULL AND e.superseded_at IS NOT NULL) AS superseded,
	COUNT(*) FILTER (WHERE c.status = 'retrying') AS retrying,
	COUNT(*) FILTER (WHERE c.status = 'active')   AS processing,
	COUNT(*) FILTER (WHERE c.status = 'failed')   AS failed,
	COUNT(*) FILTER (WHERE c.status = 'done')     AS done
FROM %s.event e
LEFT JOIN %s.event_claim c
       ON c.event_id = e.id AND c.consumer_type = $2
WHERE e.type = $1`, d.schema, d.schema), typ, consumerType).
		Scan(&c.Pending, &c.Superseded, &c.Retrying, &c.Processing, &c.Failed, &c.Done)
	if err != nil {
		return event.StateCounts{}, fmt.Errorf("pgxc: count states: %w", err)
	}
	return c, nil
}

// FailedEvents 返回 failed 终态事件的偏移分页列表：failed_at 降序（最新
// 在前）、同刻按事件 ID 升序；Limit<=0 返回空且不查询，Offset<0 视为 0，
// 越界返回空（非错误）。last_error 为可空列（接管终态与未提供摘要落
// NULL），映射为空串；failed_at 为可空列——不变式「非空 ⟺ failed」仅对
// v2 加列后的写路径成立，升级前的遗留 failed 行落 NULL（加列无回填），
// 映射为零值 time.Time（升级边界，docs/api.md §2.10）。
func (d *Driver) FailedEvents(ctx context.Context, q event.FailedQuery) ([]event.FailedEvent, error) {
	if err := d.ensure(ctx); err != nil {
		return nil, err
	}
	if q.Limit <= 0 {
		return nil, nil
	}
	offset := max(q.Offset, 0)
	rows, err := d.pool.Query(ctx, fmt.Sprintf(`
SELECT e.id, e.type, c.consumer_type, c.last_error, c.failed_at
FROM %s.event_claim c
JOIN %s.event e ON e.id = c.event_id
WHERE c.status = 'failed'
  AND ($1 = '' OR e.type = $1)
  AND ($2 = '' OR c.consumer_type = $2)
ORDER BY c.failed_at DESC, e.id ASC
OFFSET $3 LIMIT $4`, d.schema, d.schema),
		q.Type, q.ConsumerType, offset, q.Limit)
	if err != nil {
		return nil, fmt.Errorf("pgxc: failed events: %w", err)
	}
	defer rows.Close()
	var out []event.FailedEvent
	for rows.Next() {
		var f event.FailedEvent
		var lastErr *string
		var failedAt *time.Time // v2 升级前的遗留 failed 行落 NULL，映射零值
		if err := rows.Scan(&f.EventID, &f.Type, &f.ConsumerType, &lastErr, &failedAt); err != nil {
			return nil, fmt.Errorf("pgxc: scan failed event: %w", err)
		}
		if lastErr != nil {
			f.LastError = *lastErr
		}
		if failedAt != nil {
			f.FailedAt = *failedAt
		}
		out = append(out, f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgxc: scan failed events: %w", err)
	}
	return out, nil
}
