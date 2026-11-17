package inmem

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jninng/go_event"
)

// adminLockWait 是管理写互斥锁的有界等待上限：Postgres 后端为迁移 advisory
// 锁的等待上限（postgresschema.LockWaitTimeout），进程内等价取同值；契约
// 套件经 WithAdminLockWait 缩短。
const adminLockWait = 5 * time.Second

// Admin 管理面写操作载体（独立类型，不挂在 Driver 上）：与传入的 Driver
// 共享同一份内存状态。形态与 Postgres 后端一致——管理写路径由 Admin 承载，
// Driver 类型上不暴露写操作（design.md §6 三面分离）。写路径持有与迁移
// 执行等价的命名空间互斥锁（进程内容量 1 信号量，对齐 simmigrate 执行器
// 与 Postgres advisory 键的进程内形态），并发射类型化观测旁路事件
// （ObserverV3 分发，操作来源记录，ADR 0010；进程内不接 otel / slog）。
type Admin struct {
	d   *Driver
	obs event.Observer

	// exclWait 是互斥锁的有界等待上限（缺省 adminLockWait）。
	exclWait time.Duration
}

// AdminOption inmem Admin 构造选项。
type AdminOption func(*Admin)

// WithAdminObserver 注入类型化观测旁路（缺省无观察者）：ReplayFailed 结束
// 时经 ObserverV3 分发 AdminReplayEvent（未实现 V3 的观察者不感知）。回调
// 同步执行、库内 recover panic，纯旁路。
func WithAdminObserver(o event.Observer) AdminOption {
	return func(a *Admin) { a.obs = o }
}

// WithAdminLockWait 覆盖互斥锁有界等待上限（契约套件缩短用）。
func WithAdminLockWait(d time.Duration) AdminOption {
	return func(a *Admin) { a.exclWait = d }
}

// NewAdmin 构造进程内管理面 event.Admin：与传入的 Driver（须为本包
// New 构造，与 NewInspector 共享同一底层实例）共享同一份内存状态，供
// 契约套件与本地测试重放 failed 事件使用。传入其他实现的 Driver 视为
// 装配错误（panic，与类型断言失败同义——本包 Driver 为具体类型，无
// 跨实现共享状态）。
func NewAdmin(d event.Driver, opts ...AdminOption) event.Admin {
	drv, ok := d.(*Driver)
	if !ok {
		panic("inmem: NewAdmin requires a driver constructed by this package")
	}
	a := &Admin{d: drv, exclWait: adminLockWait}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// ReplayFailed 把（事件 × 消费者身份）的 failed 终态行重新置为可认领
// （语义见 event.Admin.ReplayFailed）。执行次序与 Postgres 后端一致：
// 有界等待取得互斥锁（进程内无迁移执行，锁为契约等价形态）→ 单次持锁的
// 条件更新（仅改写 status='failed' 的行，CAS；归属校验即 CAS 本身）→
// 释放。重放置 retrying、claim_count=0、清 lease_token、lease_until /
// next_eligible 置为 now、failed_at 清空、last_error 保留（ADR 0009）。
// 非 failed 行（done / active / retrying）与不存在的键一律 ok=false 且不
// 改动（判别方式统一，需区分原因先经 Inspector.ClaimState 查询）。锁等待
// 超时返回 event.ErrAdminLockTimeout（瞬时），执行故障不折叠为 ok=false。
func (a *Admin) ReplayFailed(ctx context.Context, eventID, consumerType string, now time.Time) (bool, error) {
	ok, err := a.replayFailed(ctx, eventID, consumerType, now)
	a.record(eventID, consumerType, ok, err)
	return ok, err
}

func (a *Admin) replayFailed(ctx context.Context, eventID, consumerType string, now time.Time) (bool, error) {
	// 调用方取消：不发起写（三后端写路径入口统一预检，pre-canceled ctx
	// 不依赖驱动观察；canceled 口径对齐 core pollResult / 后端 adminResult）。
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := a.acquireExclusion(ctx); err != nil {
		return false, err
	}
	defer func() { <-a.d.excl }()

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

// acquireExclusion 有界等待取得互斥锁（进程内等价 postgresschema.
// AcquireExclusiveLock）：先 try，占满时有界阻塞；超时返回瞬时错误。
func (a *Admin) acquireExclusion(ctx context.Context) error {
	select {
	case a.d.excl <- struct{}{}:
		return nil
	default:
	}
	timer := time.NewTimer(a.exclWait)
	defer timer.Stop()
	select {
	case a.d.excl <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("inmem: admin replay: %w", event.ErrAdminLockTimeout)
	}
}

// record 发射类型化观测旁路事件（操作来源记录）：进程内不接 otel / slog
// （ADR 0008：inmem 不引入可观测依赖），仅 Observer 事件。
func (a *Admin) record(eventID, consumerType string, ok bool, err error) {
	if a.obs == nil {
		return
	}
	v3, okV3 := a.obs.(event.ObserverV3)
	if !okV3 {
		return
	}
	res := event.AdminReplayError
	switch {
	case err == nil && ok:
		res = event.AdminReplayOK
	case err == nil:
		res = event.AdminReplayNoEffect
	case errors.Is(err, context.Canceled):
		res = event.AdminReplayCanceled
	case errors.Is(err, event.ErrAdminLockTimeout):
		res = event.AdminReplayLockTimeout
	}
	defer func() { _ = recover() }()
	v3.OnAdminReplay(event.AdminReplayEvent{
		EventID:      eventID,
		ConsumerType: consumerType,
		Result:       res,
		Err:          err,
	})
}

// WithReplayObserver 返回挂接观测旁路的等价 Admin（原实例不变更，共享
// 同一 Driver 与互斥锁）：契约套件经 eventtest.AdminObserverSetter 断言
// 操作来源记录。
func (a *Admin) WithReplayObserver(o event.Observer) event.Admin {
	return &Admin{d: a.d, obs: o, exclWait: a.exclWait}
}

// HoldExclusionLock 从外部占用互斥锁直到 release 被调用（重复调用释放
// 函数无效果）：契约套件经 eventtest.AdminExclusionLock 断言锁占用期间的
// 互斥与有界等待。进程内无迁移执行——真实后端该锁的占用方是迁移执行。
func (a *Admin) HoldExclusionLock(t *testing.T) (release func()) {
	t.Helper()
	a.d.excl <- struct{}{}
	var once bool
	return func() {
		if once {
			return
		}
		once = true
		<-a.d.excl
	}
}
