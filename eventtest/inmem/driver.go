// Package inmem 构造进程内 event.Driver：供 eventtest 契约测试与使用方
// 本地测试/示例使用。条件过滤在内存模拟，能力边界注明「模拟」档
// （ADR 0005）。
package inmem

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/jninng/go_event"
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
	d := &Driver{
		events: make(map[string]*storedEvent),
		claims: make(map[claimKey]*storedClaim),
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

type storedEvent struct {
	env      event.RawEnvelope
	position uint64
}

type claimStatus string

const (
	statusActive   claimStatus = "active"   // 持有租约
	statusRetrying claimStatus = "retrying" // 已失败/退避中，待重认领
	statusDone     claimStatus = "done"
	statusFailed   claimStatus = "failed"
)

type storedClaim struct {
	leaseToken   string
	leaseUntil   time.Time
	claimCount   int // 已失败次数：显式 Fail 与接管各计一次（ADR 0006）
	status       claimStatus
	nextEligible time.Time // 退避期满时刻；零值表示立即可认领
}

type claimKey struct {
	eventID      string
	consumerType string
}

// Driver 进程内 Driver 实现。所有判定时钟一律取自方法入参 Now，
// 不依赖宿主时钟（docs/api.md §2.4）。
type Driver struct {
	mu          sync.Mutex
	events      map[string]*storedEvent
	claims      map[claimKey]*storedClaim
	position    uint64
	retention   time.Duration
	lastCleanup time.Time
}

func (d *Driver) Append(_ context.Context, e event.RawEnvelope) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.events[e.ID]; ok {
		return nil // 幂等：首写为准，不覆盖、不重投
	}
	d.position++
	ev := e
	ev.Position = strconv.FormatUint(d.position, 10)
	d.events[e.ID] = &storedEvent{env: ev, position: d.position}
	d.cleanupLocked(time.Now())
	return nil
}

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

// HeadPosition 广播基线锚点：该类型事件的最大 Position，无行返回空串。
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

func (d *Driver) Claim(_ context.Context, q event.ClaimQuery) ([]event.Lease, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if q.Limit <= 0 {
		return nil, nil
	}

	// 候选：无主、租约过期、或退避期满（过滤在认领时原子完成，ADR 0004）。
	type candidate struct {
		ev    *storedEvent
		claim *storedClaim // 可为 nil（无主）
	}
	var candidates []candidate
	for _, ev := range d.events {
		if ev.env.Type != q.Type {
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
	//    槽位（与 SQL 实现的「先置 failed 再 LIMIT 拾取」一致，ADR 0006）。
	for _, cd := range candidates {
		if cd.claim == nil || cd.claim.status != statusActive {
			continue
		}
		if cd.claim.claimCount+1 >= q.RetryLimit {
			cd.claim.claimCount++
			cd.claim.status = statusFailed
			cd.claim.leaseUntil = q.Now
		}
	}

	// 2) 按 position 升序认领至多 Limit 条。
	token := newToken()
	var leases []event.Lease
	granted := 0
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
	return leases, nil
}

func (d *Driver) Heartbeat(_ context.Context, h event.HeartbeatRequest) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	c, ok := d.claims[claimKey{h.EventID, h.ConsumerType}]
	if !ok || c.leaseToken != h.LeaseToken || c.status != statusActive ||
		!c.leaseUntil.After(h.Now) {
		return nil // CAS 不命中静默返回：接管场景的正常结果
	}
	c.leaseUntil = h.Now.Add(h.Extend)
	return nil
}

func (d *Driver) Complete(_ context.Context, c event.CompleteRequest) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	sc, ok := d.claims[claimKey{c.EventID, c.ConsumerType}]
	if !ok || sc.leaseToken != c.LeaseToken || sc.status != statusActive {
		return nil // 幂等、不可逆；迟到的 Complete 不生效
	}
	sc.status = statusDone
	sc.leaseUntil = c.Now
	return nil
}

func (d *Driver) Fail(_ context.Context, f event.FailRequest) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	sc, ok := d.claims[claimKey{f.EventID, f.ConsumerType}]
	if !ok || sc.leaseToken != f.LeaseToken || sc.status != statusActive {
		return nil // 迟到的 Fail 不递增、不污染新属主状态
	}
	// 递增计数、退避调度与终态判定单次原子完成（ADR 0006）。
	sc.claimCount++
	if sc.claimCount >= f.RetryLimit {
		sc.status = statusFailed
		sc.leaseUntil = f.Now
		return nil
	}
	sc.status = statusRetrying
	sc.leaseUntil = f.Now                                      // 立即过期，重认领由退避判据约束
	sc.nextEligible = f.Now.Add(f.Backoff.Next(sc.claimCount)) // 首次失败 = 1
	return nil
}

// cleanupLocked 惰性清理：超保留窗口且无活跃租约的行（无论终态与否）
// 均可清理；认领记录随事件一并清理。内部簿记用墙钟，不参与契约语义。
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
		if !ev.env.OccurredAt.Before(cutoff) {
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
