package eventtest

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jninng/go_event"
)

// 幂等 Append：重复 Append 同 ID 首写为准，不覆盖既有行、不触发重投，
// 不校验 payload 一致性（design.md §4）。
func testAppendIdempotent(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "aid-1", typ, "first", nil)
	appendEvent(t, d, "aid-1", typ, "second", nil) // 同 ID 重复 Append

	batch, err := d.Fetch(t.Context(), event.FetchQuery{Type: typ, Limit: 10})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("want 1 event after duplicate append, got %d", len(batch))
	}
	var p suitePayload
	if err := event.NewJSONCodec().Decode(batch[0].Payload, &p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if p.Value != "first" {
		t.Fatalf("first write must win, got payload %q", p.Value)
	}
	if batch[0].ID != "aid-1" || batch[0].Type != typ {
		t.Fatalf("envelope mismatch: %+v", batch[0])
	}
}

// 元数据块原样存储透传、不解释语义（design.md §4）。
func testMetadataPassthrough(t *testing.T, d event.Driver) {
	typ := t.Name()
	meta := map[string]string{"trace_id": "tr-123", "k": "v"}
	appendEvent(t, d, "mid-1", typ, "x", meta)

	batch, err := d.Fetch(t.Context(), event.FetchQuery{Type: typ, Limit: 10})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("want 1 event, got %d", len(batch))
	}
	if batch[0].Metadata["trace_id"] != "tr-123" || batch[0].Metadata["k"] != "v" {
		t.Fatalf("metadata not passed through: %+v", batch[0].Metadata)
	}
}

// HeadPosition 广播基线锚点：返回该类型已提交事件的最大 Position
// （docs/api.md §2.4、ADR 0006）。
func testHeadPositionNewest(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "hp1", typ, "1", nil)
	appendEvent(t, d, "hp2", typ, "2", nil)

	batch, err := d.Fetch(t.Context(), event.FetchQuery{Type: typ, Limit: 10})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	want := batch[len(batch)-1].Position // Fetch 升序，末位即最大

	head, err := d.HeadPosition(t.Context(), typ)
	if err != nil {
		t.Fatalf("head position: %v", err)
	}
	if head != want {
		t.Fatalf("head position must be newest position %q, got %q", want, head)
	}
}

// HeadPosition 对无事件的类型返回空串（docs/api.md §2.4）。
func testHeadPositionEmpty(t *testing.T, d event.Driver) {
	head, err := d.HeadPosition(t.Context(), "no-such-type-" + t.Name())
	if err != nil {
		t.Fatalf("head position: %v", err)
	}
	if head != "" {
		t.Fatalf("empty type must return empty position, got %q", head)
	}
}

// Fetch 类型过滤：其他类型的行不混入（docs/api.md §2.4）。
func testFetchForeignType(t *testing.T, d event.Driver) {
	typ := "ft-" + t.Name()
	appendEvent(t, d, "ft1", typ, "1", nil)
	appendEvent(t, d, "ft2", typ, "2", nil)
	appendEvent(t, d, "fu1", typ+"-other", "x", nil)

	batch, err := d.Fetch(t.Context(), event.FetchQuery{Type: typ, Limit: 10})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("want 2 events of type, got %d", len(batch))
	}
	for _, e := range batch {
		if e.Type != typ {
			t.Fatalf("foreign type must not be returned, got %+v", e)
		}
	}
}

// Fetch 水位条件：Position 严格大于 After，升序返回（docs/api.md §2.4）。
func testFetchWatermark(t *testing.T, d event.Driver) {
	typ := "fw-" + t.Name()
	appendEvent(t, d, "fw1", typ, "1", nil)
	appendEvent(t, d, "fw2", typ, "2", nil)
	appendEvent(t, d, "fw3", typ, "3", nil)

	batch, err := d.Fetch(t.Context(), event.FetchQuery{Type: typ, Limit: 10})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(batch) != 3 {
		t.Fatalf("want 3 events, got %d", len(batch))
	}
	if !slices.IsSortedFunc(batch, func(a, b event.RawEnvelope) int {
		return comparePosition(a.Position, b.Position)
	}) {
		t.Fatalf("fetch must return ascending positions, got %+v", batch)
	}

	after := batch[0].Position
	batch, err = d.Fetch(t.Context(), event.FetchQuery{Type: typ, After: after, Limit: 10})
	if err != nil {
		t.Fatalf("fetch after watermark: %v", err)
	}
	if len(batch) != 2 || batch[0].ID != "fw2" || batch[1].ID != "fw3" {
		t.Fatalf("want strictly greater [fw2 fw3], got %+v", batch)
	}
}

// Fetch 限额：至多返回 Limit 条（docs/api.md §2.4）。
func testFetchLimit(t *testing.T, d event.Driver) {
	typ := "fl-" + t.Name()
	appendEvent(t, d, "lm1", typ, "1", nil)
	appendEvent(t, d, "lm2", typ, "2", nil)
	appendEvent(t, d, "lm3", typ, "3", nil)

	batch, err := d.Fetch(t.Context(), event.FetchQuery{Type: typ, Limit: 1})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(batch) != 1 || batch[0].ID != "lm1" {
		t.Fatalf("limit 1 must cap batch at first row, got %+v", batch)
	}
}

// 无效水位：After 非空且无法解析为 Position 时返回错误（docs/api.md §2.4）。
func testFetchMalformedAfter(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "ma1", typ, "1", nil)

	if _, err := d.Fetch(t.Context(), event.FetchQuery{Type: typ, After: "not-a-position", Limit: 10}); err == nil {
		t.Fatal("malformed After must return error")
	}
}

// Fetch 只读、不产生任何簿记：重复读取结果一致（docs/api.md §2.4）。
func testFetchReadOnly(t *testing.T, d event.Driver) {
	typ := "ro-" + t.Name()
	appendEvent(t, d, "ro1", typ, "1", nil)

	q := event.FetchQuery{Type: typ, Limit: 10}
	first, err := d.Fetch(t.Context(), q)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	second, err := d.Fetch(t.Context(), q)
	if err != nil {
		t.Fatalf("refetch: %v", err)
	}
	if len(first) != 1 || len(second) != 1 || first[0].ID != second[0].ID {
		t.Fatalf("fetch must be read-only and stable: %v vs %v", first, second)
	}
}

// 认领互斥：租约活跃期间同身份不可再认领（design.md §5.3）。
func testClaimActiveLeaseRejected(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "cm1", typ, "1", nil)

	leases := claim(t, d, typ, "cm-A", time.Hour, baseNow)
	if len(leases) != 1 || leases[0].Attempts != 1 {
		t.Fatalf("first claim want 1 lease attempts=1, got %+v", leases)
	}
	if leases[0].LeaseToken == "" {
		t.Fatal("lease token must be non-empty")
	}

	leases = claim(t, d, typ, "cm-A", time.Hour, baseNow.Add(time.Minute))
	if len(leases) != 0 {
		t.Fatalf("active lease must exclude re-claim, got %+v", leases)
	}
}

// 不同消费者身份各自独立认领（design.md §5.3）。
func testClaimIndependentIdentity(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "ci1", typ, "1", nil)

	if ls := claim(t, d, typ, "ci-A", time.Hour, baseNow); len(ls) != 1 {
		t.Fatalf("consumer A must claim independently, got %+v", ls)
	}
	if ls := claim(t, d, typ, "ci-B", time.Hour, baseNow); len(ls) != 1 {
		t.Fatalf("consumer B must claim independently, got %+v", ls)
	}
}

// 并发认领原子性：多个并发认领者抢同一批事件，每个事件恰好发放一次
// 租约、无重复发放（FOR UPDATE SKIP LOCKED 的存在意义，design.md §5.3）。
func testClaimConcurrentDisjoint(t *testing.T, d event.Driver) {
	typ := t.Name()
	const total = 10
	for i := range total {
		appendEvent(t, d, "cc-"+string(rune('a'+i)), typ, "x", nil)
	}

	const claimers = 4
	var (
		mu     sync.Mutex
		seen   = map[string]int{}
		claimWg sync.WaitGroup
	)
	for range claimers {
		claimWg.Go(func() {
			for {
				ls, err := d.Claim(t.Context(), event.ClaimQuery{
					Type: typ, ConsumerType: "cc", Lease: time.Hour, Limit: 2,
					RetryLimit: 10, Now: baseNow,
				})
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				if len(ls) == 0 {
					return
				}
				mu.Lock()
				for _, l := range ls {
					seen[l.Envelope.ID]++
				}
				mu.Unlock()
			}
		})
	}
	claimWg.Wait()

	if len(seen) != total {
		t.Fatalf("all events must be claimed exactly once, got %d unique of %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("event %s granted %d leases, want 1", id, n)
		}
	}
}

// 接管：租约过期后可被再认领，且计入重试（Attempts 递增，ADR 0006）。
func testClaimTakeover(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "tk1", typ, "1", nil)

	first := claim(t, d, typ, "tk", time.Minute, baseNow)
	if len(first) != 1 || first[0].Attempts != 1 {
		t.Fatalf("first claim: %+v", first)
	}
	token1 := first[0].LeaseToken

	// 租约过期（base+2m > base+1m）→ 接管认领，Attempts 递增。
	second := claim(t, d, typ, "tk", time.Minute, baseNow.Add(2*time.Minute))
	if len(second) != 1 {
		t.Fatalf("expired lease must be reclaimable, got %d", len(second))
	}
	if second[0].Attempts != 2 {
		t.Fatalf("takeover must increment attempts, want 2 got %d", second[0].Attempts)
	}
	if second[0].LeaseToken == token1 {
		t.Fatal("takeover must issue a new lease token")
	}
	if !second[0].LeaseUntil.Equal(baseNow.Add(2 * time.Minute).Add(time.Minute)) {
		t.Fatalf("lease until = now+lease, got %v", second[0].LeaseUntil)
	}
}

// 接管计入重试 + 上限：达 RetryLimit 的接管认领不再发放租约、直接置
// failed 终态（ADR 0006「不再被认领」）。
func testClaimOverLimitTerminal(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "ov1", typ, "1", nil)
	const retryLimit = 2
	doClaim := func(now time.Time) []event.Lease {
		t.Helper()
		ls, err := d.Claim(t.Context(), event.ClaimQuery{
			Type: typ, ConsumerType: "ov", Lease: time.Minute, Limit: 10,
			RetryLimit: retryLimit, Now: now,
		})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		return ls
	}

	// 第 1 次认领（Attempts=1）。
	ls := doClaim(baseNow)
	if len(ls) != 1 {
		t.Fatalf("first claim: %+v", ls)
	}
	// 过期接管 → 计入第 2 次失败 = 尚未达 RetryLimit=2 → 发放第 2 次尝试。
	ls = doClaim(baseNow.Add(2 * time.Minute))
	if len(ls) != 1 || ls[0].Attempts != 2 {
		t.Fatalf("second attempt within limit must be handed out, got %+v", ls)
	}
	// 第 2 次尝试也失去租约 → 再接管将计入第 3 次失败，超过 RetryLimit
	// → 置 failed、不发放。
	ls = doClaim(baseNow.Add(4 * time.Minute))
	if len(ls) != 0 {
		t.Fatalf("over-limit takeover must not hand out lease, got %+v", ls)
	}
	// 终态后永不再认领。
	ls = doClaim(baseNow.Add(time.Hour))
	if len(ls) != 0 {
		t.Fatalf("failed terminal must never be claimed, got %+v", ls)
	}
}

// 超限行不占限额槽位：认领时把达 RetryLimit 的过期行置 failed 的动作
// 不消耗 Limit 名额，后续可认领行正常发放（与 SQL 实现的「先置 failed
// 再 LIMIT 拾取」一致，ADR 0006）。
func testClaimOverLimitSlot(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "os1", typ, "1", nil)
	appendEvent(t, d, "os2", typ, "2", nil)
	const retryLimit = 2
	doClaim := func(now time.Time, limit int) []event.Lease {
		t.Helper()
		ls, err := d.Claim(t.Context(), event.ClaimQuery{
			Type: typ, ConsumerType: "os", Lease: time.Minute, Limit: limit,
			RetryLimit: retryLimit, Now: now,
		})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		return ls
	}
	fail := func(lease event.Lease, now time.Time) {
		t.Helper()
		if err := d.Fail(t.Context(), event.FailRequest{
			EventID: lease.Envelope.ID, ConsumerType: "os", LeaseToken: lease.LeaseToken,
			RetryLimit: retryLimit, Backoff: linearBackoffMs(), Now: now,
		}); err != nil {
			t.Fatalf("fail %s: %v", lease.Envelope.ID, err)
		}
	}

	// 各认领一次（Lease 1 分钟），随后 os1 两次失败达 RetryLimit=2 终态。
	ls := doClaim(baseNow, 10)
	if len(ls) != 2 {
		t.Fatalf("first claim: %+v", ls)
	}
	var os1, os2 event.Lease
	for _, l := range ls {
		switch l.Envelope.ID {
		case "os1":
			os1 = l
		case "os2":
			os2 = l
		}
	}
	if os1.Envelope.ID == "" || os2.Envelope.ID == "" {
		t.Fatalf("want os1/os2 claimed, got %+v", ls)
	}
	fail(os1, baseNow) // count=1 < 2 → retrying

	// os1 退避期满重认领（Attempts=2），os2 过期被接管（Attempts=2）。
	ls = doClaim(baseNow.Add(2*time.Minute), 10)
	if len(ls) != 2 {
		t.Fatalf("second claim: %+v", ls)
	}
	for _, l := range ls {
		if l.Envelope.ID == "os1" {
			os1 = l
		}
	}
	fail(os1, baseNow.Add(2*time.Minute)) // count=2 达上限 → failed 终态

	// os1 failed；os2 活跃租约过期且接管将达上限（2+1 >= 2）→ 置 failed
	// 不占槽位；新行 os3 无主 → Limit=1 恰好发放给 os3。
	appendEvent(t, d, "os3", typ, "3", nil)
	ls = doClaim(baseNow.Add(5*time.Minute), 1)
	if len(ls) != 1 || ls[0].Envelope.ID != "os3" {
		t.Fatalf("over-limit row must not consume slot; want [os3], got %+v", ls)
	}
	// os2 已随超限翻转进入终态：永不再认领（os3 租约过期被重认领属正常）。
	ls = doClaim(baseNow.Add(6*time.Minute), 10)
	for _, l := range ls {
		if l.Envelope.ID == "os2" {
			t.Fatalf("flipped over-limit row must be terminal, got lease %+v", l)
		}
	}
}

// done 幂等不可逆、failed 终态不可认领（design.md §5.3）。
func testClaimSkipTerminal(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "dn1", typ, "1", nil)
	appendEvent(t, d, "fl1", typ, "2", nil)

	// done：完成即不可再认领。
	ls := claim(t, d, typ, "term", time.Minute, baseNow)
	if len(ls) != 2 {
		t.Fatalf("want both events claimed, got %d", len(ls))
	}
	doneLease := ls[0]
	if err := d.Complete(t.Context(), event.CompleteRequest{
		EventID: doneLease.Envelope.ID, ConsumerType: "term",
		LeaseToken: doneLease.LeaseToken, Now: baseNow,
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// failed：fail 到达 RetryLimit → 终态。
	failLease := ls[1]
	if err := d.Fail(t.Context(), event.FailRequest{
		EventID: failLease.Envelope.ID, ConsumerType: "term",
		LeaseToken: failLease.LeaseToken,
		RetryLimit: 1, Backoff: linearBackoffMs(),
		Now: baseNow,
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}

	ls = claim(t, d, typ, "term", time.Minute, baseNow.Add(time.Hour))
	if len(ls) != 0 {
		t.Fatalf("done/failed rows must not be claimable, got %+v", ls)
	}
}

// 终态是（事件 × consumer_type）粒度：新增身份重放全部窗口内历史，
// 含其他身份已 done 的事件；各身份消费彼此独立（design.md §5.3）。
func testNewConsumerReplays(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "rp1", typ, "1", nil)

	// 身份 A 认领并完成。
	lsA := claim(t, d, typ, "rp-A", time.Minute, baseNow)
	if len(lsA) != 1 {
		t.Fatalf("claim A: %+v", lsA)
	}
	if err := d.Complete(t.Context(), event.CompleteRequest{
		EventID: "rp1", ConsumerType: "rp-A",
		LeaseToken: lsA[0].LeaseToken, Now: baseNow,
	}); err != nil {
		t.Fatalf("complete A: %v", err)
	}

	// 新身份 B：重放全部窗口内历史（含 A 已 done）。
	lsB := claim(t, d, typ, "rp-B", time.Minute, baseNow.Add(time.Hour))
	if len(lsB) != 1 || lsB[0].Attempts != 1 {
		t.Fatalf("new consumer must replay history, got %+v", lsB)
	}

	// A 的终态不受影响：A 不可再认领。
	lsA2 := claim(t, d, typ, "rp-A", time.Minute, baseNow.Add(time.Hour))
	if len(lsA2) != 0 {
		t.Fatalf("A done is irreversible, got %+v", lsA2)
	}
}

// 心跳续租：属主凭证匹配时生效，延长租约有效期（docs/api.md §2.4）。
func testHeartbeatExtends(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "hb1", typ, "1", nil)

	ls := claim(t, d, typ, "hb", time.Minute, baseNow)
	if len(ls) != 1 {
		t.Fatalf("claim: %+v", ls)
	}
	// base+30s 心跳续租 1m → 租约至 base+1m30s。
	if err := d.Heartbeat(t.Context(), event.HeartbeatRequest{
		EventID: "hb1", ConsumerType: "hb", LeaseToken: ls[0].LeaseToken,
		Extend: time.Minute, Now: baseNow.Add(30 * time.Second),
	}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	// 原到期点（base+1m）仍在续租后的租约内 → 不可认领。
	ls2 := claim(t, d, typ, "hb", time.Minute, baseNow.Add(time.Minute))
	if len(ls2) != 0 {
		t.Fatalf("heartbeat must extend lease, got %+v", ls2)
	}
	// 续租后的到期点之后 → 接管。
	ls3 := claim(t, d, typ, "hb", time.Minute, baseNow.Add(2*time.Minute))
	if len(ls3) != 1 || ls3[0].Attempts != 2 {
		t.Fatalf("takeover after extended expiry: %+v", ls3)
	}
}

// 心跳 CAS：凭证不匹配静默返回 nil、不产生任何效果（docs/api.md §2.4）。
func testHeartbeatStaleToken(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "hs1", typ, "1", nil)

	ls := claim(t, d, typ, "hs", 10*time.Minute, baseNow)
	if len(ls) != 1 {
		t.Fatalf("claim: %+v", ls)
	}
	// 错误凭证心跳：不生效、不报错。
	if err := d.Heartbeat(t.Context(), event.HeartbeatRequest{
		EventID: "hs1", ConsumerType: "hs", LeaseToken: "bogus",
		Extend: time.Minute, Now: baseNow,
	}); err != nil {
		t.Fatalf("stale heartbeat must return nil, got %v", err)
	}
	// 租约未被错误心跳扰动：1 分钟后仍不可认领（租约 10m）。
	ls2 := claim(t, d, typ, "hs", time.Minute, baseNow.Add(time.Minute))
	if len(ls2) != 0 {
		t.Fatalf("stale heartbeat must not affect lease, got %+v", ls2)
	}
}

// Complete 幂等、不可逆（design.md §5.3）。
func testComplete(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "cp1", typ, "1", nil)

	ls := claim(t, d, typ, "cp", time.Minute, baseNow)
	if len(ls) != 1 {
		t.Fatalf("claim: %+v", ls)
	}
	req := event.CompleteRequest{
		EventID: "cp1", ConsumerType: "cp", LeaseToken: ls[0].LeaseToken, Now: baseNow,
	}
	if err := d.Complete(t.Context(), req); err != nil {
		t.Fatalf("complete: %v", err)
	}
	// 重复 Complete：幂等，不报错。
	if err := d.Complete(t.Context(), req); err != nil {
		t.Fatalf("re-complete must be idempotent nil, got %v", err)
	}
	// done 后不可再认领。
	ls2 := claim(t, d, typ, "cp", time.Minute, baseNow.Add(time.Hour))
	if len(ls2) != 0 {
		t.Fatalf("done must not be reclaimable, got %+v", ls2)
	}
}

// 迟到的 Complete 不生效：租约过期/被接管后，旧实例的 Complete 不置
// done、不覆盖新属主状态（ADR 0006）。
func testLateComplete(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "lc1", typ, "1", nil)

	ls1 := claim(t, d, typ, "lc", time.Minute, baseNow)
	if len(ls1) != 1 {
		t.Fatalf("claim 1: %+v", ls1)
	}
	// 接管。
	ls2 := claim(t, d, typ, "lc", time.Minute, baseNow.Add(2*time.Minute))
	if len(ls2) != 1 {
		t.Fatalf("takeover: %+v", ls2)
	}
	// 旧实例迟到的 Complete：静默不生效。
	if err := d.Complete(t.Context(), event.CompleteRequest{
		EventID: "lc1", ConsumerType: "lc", LeaseToken: ls1[0].LeaseToken,
		Now: baseNow.Add(2 * time.Minute).Add(30 * time.Second),
	}); err != nil {
		t.Fatalf("late complete must return nil, got %v", err)
	}
	// 新属主仍持有租约，事件未被旧 Complete 置 done：新属主 Fail 成功
	// （若 done 则 CAS 不命中，行会停留在活跃租约状态）。
	if err := d.Fail(t.Context(), event.FailRequest{
		EventID: "lc1", ConsumerType: "lc", LeaseToken: ls2[0].LeaseToken,
		RetryLimit: 5, Backoff: linearBackoffMs(),
		Now: baseNow.Add(2 * time.Minute).Add(30 * time.Second),
	}); err != nil {
		t.Fatalf("fail after late complete: %v", err)
	}
	// 退避期满后重新可认领 → 证明行未进入 done 终态。
	ls3 := claim(t, d, typ, "lc", time.Minute, baseNow.Add(5*time.Minute))
	if len(ls3) != 1 {
		t.Fatalf("late complete must not mark done, got %+v", ls3)
	}
}

// 迟到的 Fail 不递增、不污染新属主状态（ADR 0006）。
func testLateFail(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "lf1", typ, "1", nil)

	ls1 := claim(t, d, typ, "lf", time.Minute, baseNow)
	if len(ls1) != 1 {
		t.Fatalf("claim 1: %+v", ls1)
	}
	// 接管（计入第 1 次失败，count=1）。
	ls2 := claim(t, d, typ, "lf", time.Minute, baseNow.Add(2*time.Minute))
	if len(ls2) != 1 || ls2[0].Attempts != 2 {
		t.Fatalf("takeover: %+v", ls2)
	}
	// 旧实例迟到的 Fail：静默不生效、不递增。
	if err := d.Fail(t.Context(), event.FailRequest{
		EventID: "lf1", ConsumerType: "lf", LeaseToken: ls1[0].LeaseToken,
		RetryLimit: 5, Backoff: linearBackoffMs(),
		Now: baseNow.Add(2 * time.Minute).Add(30 * time.Second),
	}); err != nil {
		t.Fatalf("late fail must return nil, got %v", err)
	}
	// 新属主租约过期后再接管：Attempts=3（count=2+1）。若迟到 Fail
	// 生效递增，此处会是 4。
	ls3 := claim(t, d, typ, "lf", time.Minute, baseNow.Add(5*time.Minute))
	if len(ls3) != 1 || ls3[0].Attempts != 3 {
		t.Fatalf("late fail must not increment count, want attempts=3 got %+v", ls3)
	}
}

// 失败按递增退避置为再次可认领：退避期内不可认领，期满后可认领
// （ADR 0006 防热循环）。
func testFailBackoff(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "bk1", typ, "1", nil)

	ls := claim(t, d, typ, "bk", time.Minute, baseNow)
	if len(ls) != 1 {
		t.Fatalf("claim: %+v", ls)
	}
	// 首次失败（attempt=1）→ 线性退避 1h。
	if err := d.Fail(t.Context(), event.FailRequest{
		EventID: "bk1", ConsumerType: "bk", LeaseToken: ls[0].LeaseToken,
		RetryLimit: 5, Backoff: event.LinearBackoff(time.Hour, time.Hour),
		Now: baseNow,
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}
	// 退避期内不可认领。
	ls2 := claim(t, d, typ, "bk", time.Minute, baseNow.Add(30*time.Minute))
	if len(ls2) != 0 {
		t.Fatalf("backoff must delay reclaim, got %+v", ls2)
	}
	// 退避期满后可认领。
	ls3 := claim(t, d, typ, "bk", time.Minute, baseNow.Add(time.Hour).Add(time.Second))
	if len(ls3) != 1 || ls3[0].Attempts != 2 {
		t.Fatalf("after backoff reclaim: %+v", ls3)
	}
}

// claim_count 达 RetryLimit 时 Fail 原子置 failed 终态，不再被认领
// （ADR 0006）。
func testFailRetryLimit(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "rl1", typ, "1", nil)
	backoff := event.LinearBackoff(time.Millisecond, time.Hour)

	// 第 1 次尝试 → fail（count=1 < 2，退避 1ms）。
	ls := claim(t, d, typ, "rl", time.Minute, baseNow)
	if len(ls) != 1 {
		t.Fatalf("claim 1: %+v", ls)
	}
	if err := d.Fail(t.Context(), event.FailRequest{
		EventID: "rl1", ConsumerType: "rl", LeaseToken: ls[0].LeaseToken,
		RetryLimit: 2, Backoff: backoff, Now: baseNow,
	}); err != nil {
		t.Fatalf("fail 1: %v", err)
	}

	// 第 2 次尝试 → fail（count=2 达上限 → failed 终态）。
	ls2 := claim(t, d, typ, "rl", time.Minute, baseNow.Add(time.Second))
	if len(ls2) != 1 || ls2[0].Attempts != 2 {
		t.Fatalf("claim 2: %+v", ls2)
	}
	if err := d.Fail(t.Context(), event.FailRequest{
		EventID: "rl1", ConsumerType: "rl", LeaseToken: ls2[0].LeaseToken,
		RetryLimit: 2, Backoff: backoff, Now: baseNow.Add(time.Second),
	}); err != nil {
		t.Fatalf("fail 2: %v", err)
	}

	// 终态后任何时刻都不可认领。
	ls3 := claim(t, d, typ, "rl", time.Minute, baseNow.Add(time.Hour))
	if len(ls3) != 0 {
		t.Fatalf("failed terminal must never be claimed, got %+v", ls3)
	}
}

func linearBackoffMs() event.Backoff {
	return event.LinearBackoff(time.Millisecond, time.Hour)
}
