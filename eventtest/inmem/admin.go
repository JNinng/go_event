package inmem

import (
	"context"
	"time"

	"github.com/jninng/go_event"
)

// Admin 管理面写操作载体（独立类型，不挂在 Driver 上）：与传入的 Driver
// 共享同一份内存状态。形态与 Postgres 后端一致——管理写路径由 Admin 承载，
// Driver 类型上不暴露写操作（design.md §6 三面分离）。
type Admin struct {
	d *Driver
}

// NewAdmin 构造进程内管理面 event.Admin：与传入的 Driver（须为本包
// New 构造，与 NewInspector 共享同一底层实例）共享同一份内存状态，供
// 契约套件与本地测试重放 failed 事件使用。传入其他实现的 Driver 视为
// 装配错误（panic，与类型断言失败同义——本包 Driver 为具体类型，无
// 跨实现共享状态）。
func NewAdmin(d event.Driver) event.Admin {
	drv, ok := d.(*Driver)
	if !ok {
		panic("inmem: NewAdmin requires a driver constructed by this package")
	}
	return &Admin{d: drv}
}

// ReplayFailed 把（事件 × 消费者身份）的 failed 终态行重新置为可认领
// （语义见 event.Admin.ReplayFailed）。单次持锁的条件更新（仅改写
// status='failed' 的行，CAS）：重放置 retrying、claim_count=0、清
// lease_token、lease_until / next_eligible 置为 now、failed_at 清空、
// last_error 保留（ADR 0009）。非 failed 行（done / active / retrying）
// 与不存在的键一律 ok=false 且不改动（判别方式统一，需区分原因先经
// Inspector.ClaimState 查询）。
func (a *Admin) ReplayFailed(_ context.Context, eventID, consumerType string, now time.Time) (bool, error) {
	a.d.mu.Lock()
	defer a.d.mu.Unlock()
	sc, ok := a.d.claims[claimKey{eventID, consumerType}]
	if !ok || sc.status != statusFailed {
		return false, nil // 非 failed 终态或无认领行：无效果，不改动
	}
	sc.status = statusRetrying
	sc.claimCount = 0
	sc.leaseToken = ""
	sc.leaseUntil = now
	sc.nextEligible = now
	sc.failedAt = time.Time{}
	return true, nil
}
