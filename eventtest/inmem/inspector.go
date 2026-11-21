package inmem

import (
	"context"
	"sort"

	"github.com/jninng/go_event"
)

// NewInspector 构造进程内只读查询面 event.Inspector：与 New 共享同一
// 实现（*Driver 同时实现 Inspector），供契约套件与本地测试按可选能力
// 探测使用（ADR 0005）。查询口径与 Postgres 后端一致：返回落库事实，
// 不做租约过期推断。
func NewInspector(opts ...Option) event.Inspector {
	return newDriver(opts...)
}

// inspectStatus 把落库 status 映射为查询面状态：active（持有租约）对应
// processing，其余取值一致。
func inspectStatus(s string) event.ClaimStatus {
	if s == string(statusActive) {
		return event.ClaimProcessing
	}
	return event.ClaimStatus(s)
}

// ClaimState 返回（事件 × 消费者身份）认领行的当前状态与重试计数；
// 无认领行时 ok 为 false（「待处理」态，含事件不存在与该身份尚未认领）。
func (d *Driver) ClaimState(_ context.Context, eventID, consumerType string) (event.ClaimState, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	sc, ok := d.claims[claimKey{eventID, consumerType}]
	if !ok {
		return event.ClaimState{}, false, nil
	}
	return event.ClaimState{
		Status:   inspectStatus(string(sc.status)),
		Attempts: sc.claimCount,
	}, true, nil
}

// CountStates 按消费者身份对事件类型做状态分档计数：无认领行按 superseded
// 属性细分——未取代计入 Pending（「待处理」= 该身份无认领行且未被取代）、
// 已被合并取代计入 Superseded（事件行属性档，ADR 0014），其余按认领行
// status 分档。
func (d *Driver) CountStates(_ context.Context, typ, consumerType string) (event.StateCounts, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var c event.StateCounts
	for _, ev := range d.events {
		if ev.env.Type != typ {
			continue
		}
		sc, ok := d.claims[claimKey{ev.env.ID, consumerType}]
		if !ok {
			if ev.supersededAt.IsZero() {
				c.Pending++
			} else {
				c.Superseded++
			}
			continue
		}
		switch sc.status {
		case statusActive:
			c.Processing++
		case statusRetrying:
			c.Retrying++
		case statusFailed:
			c.Failed++
		case statusDone:
			c.Done++
		}
	}
	return c, nil
}

// FailedEvents 返回 failed 终态事件的偏移分页列表：failed_at 降序、
// 同刻按事件 ID 升序；Offset<0 视为 0，Limit<=0 返回空，越界返回空。
func (d *Driver) FailedEvents(_ context.Context, q event.FailedQuery) ([]event.FailedEvent, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if q.Limit <= 0 {
		return nil, nil
	}
	var failed []event.FailedEvent
	for key, sc := range d.claims {
		if sc.status != statusFailed {
			continue
		}
		if q.ConsumerType != "" && key.consumerType != q.ConsumerType {
			continue
		}
		ev, ok := d.events[key.eventID]
		if !ok {
			continue // 认领行随事件清理，无孤儿；防御性跳过
		}
		if q.Type != "" && ev.env.Type != q.Type {
			continue
		}
		failed = append(failed, event.FailedEvent{
			EventID:      key.eventID,
			Type:         ev.env.Type,
			ConsumerType: key.consumerType,
			LastError:    sc.lastError,
			FailedAt:     sc.failedAt,
		})
	}
	sort.Slice(failed, func(i, j int) bool {
		if !failed[i].FailedAt.Equal(failed[j].FailedAt) {
			return failed[i].FailedAt.After(failed[j].FailedAt)
		}
		return failed[i].EventID < failed[j].EventID
	})
	start := max(q.Offset, 0)
	if start >= len(failed) {
		return nil, nil
	}
	end := min(start+q.Limit, len(failed))
	return failed[start:end], nil
}
