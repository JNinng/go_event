// Package inmem 构造进程内 event.Driver：供 eventtest 契约测试与使用方
// 本地测试/示例使用。条件过滤在内存模拟，能力边界注明「模拟」档
// （ADR 0005）。同一实现提供 Inspector / Admin / TransactionalAppender /
// Store 各面：各面共享同一份内存状态与互斥锁，语义与 Postgres 后端一致，
// 逐面契约见 docs/api.md §4 与 docs/design.md §6。延迟事件（DeliverAt）
// 在内存模拟同一到期判据（deliver_at <= now，ADR 0013）；合并投递
// （CoalesceKey，同键最新一次消费）在内存模拟同一取代与下推排除口径
// （ADR 0014）；就近唤醒为同进程 LocalWake 语义（无跨实例面）。
package inmem

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/eventtest"
)

// Option inmem Driver 构造选项。
type Option func(*Driver)

// WithRetention 保留窗口（惰性清理资格：超窗且无活跃租约，无论终态与否，
// design.md §5.2）。零值表示不清理（进程内 Driver 缺省不清理）。
func WithRetention(d time.Duration) Option {
	return func(drv *Driver) { drv.retention = d }
}

// New 构造进程内 Driver。
func New(opts ...Option) event.Driver {
	return newDriver(opts...)
}

// newDriver 构造具体 Driver（*Driver 同时实现 event.Inspector，供
// NewInspector 与调用方的类型断言探测共用）。
func newDriver(opts ...Option) *Driver {
	d := &Driver{
		events: make(map[string]*storedEvent),
		claims: make(map[claimKey]*storedClaim),
		excl:   make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

type storedEvent struct {
	env          event.RawEnvelope
	position     uint64
	supersededAt time.Time // 被取代时刻；零值 = 未取代（合并投递 ADR 0014）
}

type claimStatus string

const (
	statusActive   claimStatus = "active"   // 持有租约
	statusRetrying claimStatus = "retrying" // 已失败/退避中，待重认领
	statusDone     claimStatus = "done"     // 处理完成终态
	statusFailed   claimStatus = "failed"   // 重试耗尽终态
)

type storedClaim struct {
	leaseToken   string
	leaseUntil   time.Time
	claimCount   int // 已失败次数：显式 Fail 与接管各计一次（ADR 0006）
	status       claimStatus
	nextEligible time.Time // 退避期满时刻；零值表示立即可认领
	lastError    string    // 错误摘要（清洗后文本；空串 = NULL/未提供）
	failedAt     time.Time // 进入 failed 终态的时刻；零值 = 无
}

// claimKey 认领互斥与终态的（事件 × 消费者身份）粒度键。
type claimKey struct {
	eventID      string
	consumerType string
}

// Driver 进程内 Driver 实现。所有判定时钟一律取自方法入参 Now，
// 不依赖宿主时钟（docs/api.md §2.4）。实现 eventtest.ClaimStateReader
// （ReadClaimState）：契约套件据它断言 Fail 错误摘要与终态时刻的落库
// 事实，口径与 Postgres 后端一致（ADR 0009）。
type Driver struct {
	mu          sync.Mutex
	events      map[string]*storedEvent
	claims      map[claimKey]*storedClaim
	position    uint64
	retention   time.Duration
	lastCleanup time.Time

	// excl 是管理面写路径与迁移执行的进程内互斥锁（容量 1 信号量，对齐
	// simmigrate 迁移执行器与 Postgres advisory 键的进程内等价形态）：
	// 本进程无迁移执行，契约等价锁供 Admin 写路径持锁与套件注入验证。
	excl chan struct{}
}

// Append 写入事件：同 ID 首写为准、不覆盖不重投（幂等与合并投递正交，
// 重复 Append 不触发取代）；CoalesceKey 非空时锁内原子「置同键待消费
// 前驱 superseded + 插入新行」（ADR 0014：前驱 = 同 (Type, key) 且无任何
// 认领行、未被取代的事件行；被取代行照常占 position）。运行时注入字段
// 不落库（保持零值）。
func (d *Driver) Append(_ context.Context, e event.RawEnvelope) (event.AppendResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.events[e.ID]; ok {
		return event.AppendResult{}, nil // 幂等：首写为准
	}
	now := time.Now()
	superseded := 0
	if e.CoalesceKey != "" {
		superseded = d.supersedeLocked(e.Type, e.CoalesceKey, now)
	}
	d.position++
	ev := e
	ev.Position = strconv.FormatUint(d.position, 10)
	ev.Attempts = 0
	ev.LastAttempt = false
	d.events[e.ID] = &storedEvent{env: ev, position: d.position}
	d.cleanupLocked(time.Now())
	return event.AppendResult{Superseded: superseded}, nil
}

// supersedeLocked 置同键待消费前驱为 superseded（v1 保守口径，ADR 0014）：
// 仅同 (type, key)、未被取代、且无任何认领行（任意消费者身份）的事件行；
// 处理中 / 等退避 / 租约过期待接管 / 终态行不动，新事件照常排队。
// 取代先于插入（与 SQL CTE 的 sup → ins 次序一致）；now 为内部簿记墙钟。
func (d *Driver) supersedeLocked(typ, key string, now time.Time) int {
	count := 0
	for id, ev := range d.events {
		if ev.env.Type != typ || ev.env.CoalesceKey != key {
			continue
		}
		if !ev.supersededAt.IsZero() || d.hasClaimLocked(id) {
			continue
		}
		ev.supersededAt = now
		count++
	}
	return count
}

// hasClaimLocked 报告事件是否存在任意消费者身份的认领行（取代范围判据）。
func (d *Driver) hasClaimLocked(eventID string) bool {
	for k := range d.claims {
		if k.eventID == eventID {
			return true
		}
	}
	return false
}

// Fetch 返回 position 严格大于 After 的已提交事件（升序，按 Limit 截断）；
// 延迟事件以 q.Now 判定到期，未到期行在单次读取内过滤（ADR 0013 §1）。
// Limit<=0 返回 nil。
func (d *Driver) Fetch(_ context.Context, q event.FetchQuery) ([]event.RawEnvelope, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if q.Limit <= 0 {
		return nil, nil
	}
	after := uint64(0)
	if q.After != "" {
		var err error
		if after, err = strconv.ParseUint(q.After, 10, 64); err != nil {
			return nil, fmt.Errorf("inmem: invalid position %q: %w", q.After, err)
		}
	}
	var out []event.RawEnvelope
	for _, ev := range d.events {
		if ev.env.Type != q.Type || ev.position <= after {
			continue
		}
		if !ev.supersededAt.IsZero() {
			continue // 被取代行不满足下推条件，广播水位直接越过（ADR 0014）
		}
		if !deliverableAt(ev.env, q.Now) {
			continue
		}
		out = append(out, ev.env)
	}
	sort.Slice(out, func(i, j int) bool {
		return parsePosition(out[i].Position) < parsePosition(out[j].Position)
	})
	if len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
}

// HeadPosition 广播基线锚点：该类型事件的最大 Position，无该类型事件时返回空串。
func (d *Driver) HeadPosition(_ context.Context, typ string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var head uint64
	for _, ev := range d.events {
		if ev.env.Type == typ && ev.position > head {
			head = ev.position
		}
	}
	if head == 0 {
		return "", nil
	}
	return strconv.FormatUint(head, 10), nil
}

// Claim 原子认领至多 Limit 个租约；语义同 event.Driver.Claim。接管与
// 终态计数随 ClaimResult 返回，与落库事实一致（design.md §9.2）。
func (d *Driver) Claim(_ context.Context, q event.ClaimQuery) (event.ClaimResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if q.Limit <= 0 {
		return event.ClaimResult{}, nil
	}

	// 候选：无主、租约过期、退避期满、且延迟投递已到期（过滤在认领时
	// 原子完成，ADR 0004；deliver_at <= now 判据见 ADR 0013 §1）。
	type candidate struct {
		ev    *storedEvent
		claim *storedClaim // 可为 nil（无主）
	}
	var candidates []candidate
	for _, ev := range d.events {
		if ev.env.Type != q.Type {
			continue
		}
		if !ev.supersededAt.IsZero() {
			continue // 被取代行不进拾取窗口（事件行属性，对新身份同样生效，ADR 0014）
		}
		if !deliverableAt(ev.env, q.Now) {
			continue
		}
		key := claimKey{ev.env.ID, q.ConsumerType}
		c, ok := d.claims[key]
		if !ok {
			candidates = append(candidates, candidate{ev, nil})
			continue
		}
		switch c.status {
		case statusActive: // 接管：租约过期且退避期满
			if c.leaseUntil.After(q.Now) || c.nextEligible.After(q.Now) {
				continue
			}
		case statusRetrying: // 退避期满
			if c.nextEligible.After(q.Now) {
				continue
			}
		default: // done / failed 不可认领
			continue
		}
		candidates = append(candidates, candidate{ev, c})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].ev.position < candidates[j].ev.position
	})

	// 1) 接管达重试上限的行直接置 failed 终态：不发放、不占用 Limit
	//    槽位（与 SQL 实现的「先置 failed 再 LIMIT 拾取」一致，ADR 0006/0011）。
	//    终态时刻随置入写入；接管路径没有本次处理错误，不写摘要。
	terminalFailed := 0
	for _, cd := range candidates {
		if cd.claim == nil || cd.claim.status != statusActive {
			continue
		}
		if cd.claim.claimCount+1 >= q.RetryLimit {
			cd.claim.claimCount++
			cd.claim.status = statusFailed
			cd.claim.leaseUntil = q.Now
			cd.claim.failedAt = q.Now
			terminalFailed++ // 置终态必经「过期租约接管」路径（下推条件含租约过期）
		}
	}

	// 2) 按 position 升序认领至多 Limit 条。
	token := newToken()
	var leases []event.Lease
	granted := 0
	grantedTakeovers := 0
	for _, cd := range candidates {
		if granted >= q.Limit {
			break
		}
		key := claimKey{cd.ev.env.ID, q.ConsumerType}
		if cd.claim == nil {
			// 无主：首次认领。
			d.claims[key] = &storedClaim{
				leaseToken: token,
				leaseUntil: q.Now.Add(q.Lease),
				claimCount: 0,
				status:     statusActive,
			}
		} else if cd.claim.status == statusActive {
			// 接管：前任失去租约计一次失败（ADR 0006）；达上限者已在
			// 上一步置 failed、不再进入此处。
			cd.claim.claimCount++
			cd.claim.leaseToken = token
			cd.claim.leaseUntil = q.Now.Add(q.Lease)
			cd.claim.nextEligible = time.Time{}
			grantedTakeovers++
		} else if cd.claim.status == statusRetrying { // 失败已在 Fail 时计入，此处仅重认领
			cd.claim.leaseToken = token
			cd.claim.leaseUntil = q.Now.Add(q.Lease)
			cd.claim.status = statusActive
			cd.claim.nextEligible = time.Time{}
		} else {
			continue // done / failed（含上一步刚翻转的）不可认领
		}
		granted++
		leases = append(leases, event.Lease{
			Envelope:     cd.ev.env,
			ConsumerType: q.ConsumerType,
			LeaseToken:   token,
			LeaseUntil:   q.Now.Add(q.Lease),
			Attempts:     d.claims[key].claimCount + 1, // 含本次认领
		})
	}
	return event.ClaimResult{
		Leases:         leases,
		Takeovers:      terminalFailed + grantedTakeovers, // 接管计数覆盖发放与终态两条路径
		TerminalFailed: terminalFailed,
	}, nil
}

// Heartbeat CAS 续租：仅属主凭证匹配且租约未过期时生效，不命中静默返回 nil。
func (d *Driver) Heartbeat(_ context.Context, h event.HeartbeatRequest) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	c, ok := d.claims[claimKey{h.EventID, h.ConsumerType}]
	if !ok || c.leaseToken != h.LeaseToken || c.status != statusActive ||
		!c.leaseUntil.After(h.Now) {
		return nil // 接管场景的正常结果
	}
	c.leaseUntil = h.Now.Add(h.Extend)
	return nil
}

// Complete CAS 置 done：幂等不可逆，迟到的 Complete 静默不生效。
func (d *Driver) Complete(_ context.Context, c event.CompleteRequest) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	sc, ok := d.claims[claimKey{c.EventID, c.ConsumerType}]
	if !ok || sc.leaseToken != c.LeaseToken || sc.status != statusActive {
		return nil
	}
	sc.status = statusDone
	sc.leaseUntil = c.Now
	return nil
}

// Fail 记失败：递增计数、退避与终态判定原子完成（ADR 0006）；迟到的 Fail 静默不生效。
// 仅当本次 Fail 使行进入 failed 终态时写入错误摘要与终态时刻（摘要经
// event.SanitizeErrorSummary 清洗；空串视为未提供）。
func (d *Driver) Fail(_ context.Context, f event.FailRequest) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	sc, ok := d.claims[claimKey{f.EventID, f.ConsumerType}]
	if !ok || sc.leaseToken != f.LeaseToken || sc.status != statusActive {
		return nil // 迟到的 Fail 不递增、不写摘要、不污染新属主状态
	}
	sc.claimCount++
	if sc.claimCount >= f.RetryLimit {
		sc.status = statusFailed
		sc.leaseUntil = f.Now
		sc.lastError = event.SanitizeErrorSummary(f.Error)
		sc.failedAt = f.Now
		return nil
	}
	sc.status = statusRetrying
	sc.leaseUntil = f.Now                                      // 立即过期，重认领由退避判据约束
	sc.nextEligible = f.Now.Add(f.Backoff.Next(sc.claimCount)) // 首次失败 = 1
	return nil
}

// ReadClaimState 返回认领行的落库事实（eventtest.ClaimStateReader）：
// 状态、错误摘要与终态时刻。行不存在时 ok 为 false；t 用于报告装配层
// 查询故障（内存实现只做行查找，不会失败）。
func (d *Driver) ReadClaimState(_ *testing.T, eventID, consumerType string) (eventtest.ClaimState, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	sc, ok := d.claims[claimKey{eventID, consumerType}]
	if !ok {
		return eventtest.ClaimState{}, false
	}
	return eventtest.ClaimState{
		Status:    string(sc.status),
		LastError: sc.lastError,
		FailedAt:  sc.failedAt,
	}, true
}

// cleanupLocked 惰性清理：超保留窗口且无活跃租约的行（无论终态与否）
// 均可清理；认领记录随事件一并清理。保留窗口自「可投递时刻」起算
// （DeliverAt 非零以其为起点，零值行以 OccurredAt 判定——延迟行不因
// 「等待投递」被清理，口径与 Postgres 后端一致，ADR 0013 §1）。
// 内部簿记用墙钟，不参与契约语义。
func (d *Driver) cleanupLocked(now time.Time) {
	if d.retention <= 0 {
		return
	}
	if !d.lastCleanup.IsZero() && now.Sub(d.lastCleanup) < max(d.retention/4, 50*time.Millisecond) {
		return
	}
	d.lastCleanup = now
	cutoff := now.Add(-d.retention)
	for id, ev := range d.events {
		effective := ev.env.OccurredAt
		if !ev.env.DeliverAt.IsZero() {
			effective = ev.env.DeliverAt
		}
		if !effective.Before(cutoff) {
			continue
		}
		hasActive := false
		for k, c := range d.claims {
			if k.eventID != id {
				continue
			}
			if c.status == statusActive && c.leaseUntil.After(now) {
				hasActive = true
				break
			}
		}
		if hasActive {
			continue
		}
		delete(d.events, id)
		for k := range d.claims {
			if k.eventID == id {
				delete(d.claims, k)
			}
		}
	}
}

// deliverableAt 判定信封在 now 时是否可投递：DeliverAt 零值立即；非零
// 按 deliver_at <= now（含到期时刻本身，与 SQL 下推条件一致）。
func deliverableAt(e event.RawEnvelope, now time.Time) bool {
	return e.DeliverAt.IsZero() || !e.DeliverAt.After(now)
}

func parsePosition(s string) uint64 {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func newToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
