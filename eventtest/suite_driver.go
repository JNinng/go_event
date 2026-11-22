package eventtest

import (
	"context"
	"errors"
	"maps"
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
	appendEvent(t, d, "aid-1", typ, "second", nil)

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

// 同类型同显式 ID 重复发布：两次 Publish 均返回 nil，落库恰好一行，
// 且 payload、Metadata、OccurredAt 全部保留首写值（ID 全局唯一、首写
// 为准、不校验 payload，docs/api.md §2.3）。
func testDuplicateIDPublishSameType(t *testing.T, d event.Driver) {
	ctx := t.Context()
	typ := "dup-same-" + t.Name()
	id := "dup-same-id-" + t.Name()
	ev := event.Declare[suitePayload](typ)
	pub := event.NewPublisher(d)
	firstMeta := map[string]string{"k": "v1"}

	mustPublish(t, ctx, pub, ev, suitePayload{"first"},
		event.WithEventID(id), event.WithMetadata(firstMeta))

	// 先读出首写行（Publisher 的 OccurredAt 取 time.Now()，以后端落库值
	// 作为后续比较基准，避免依赖宿主时钟）。
	first, err := d.Fetch(ctx, event.FetchQuery{Type: typ, Limit: 10})
	if err != nil {
		t.Fatalf("fetch first: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("first publish must store one row, got %d", len(first))
	}
	var firstPayload suitePayload
	if err := event.NewJSONCodec().Decode(first[0].Payload, &firstPayload); err != nil {
		t.Fatalf("decode first: %v", err)
	}
	if firstPayload.Value != "first" || !maps.Equal(first[0].Metadata, firstMeta) {
		t.Fatalf("first write mismatch: payload=%q metadata=%+v",
			firstPayload.Value, first[0].Metadata)
	}

	mustPublish(t, ctx, pub, ev, suitePayload{"second"},
		event.WithEventID(id),
		event.WithMetadata(map[string]string{"k": "v2", "extra": "x"}))

	batch, err := d.Fetch(ctx, event.FetchQuery{Type: typ, Limit: 10})
	if err != nil {
		t.Fatalf("fetch after duplicate: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("duplicate publish must store one row, got %d", len(batch))
	}
	var p suitePayload
	if err := event.NewJSONCodec().Decode(batch[0].Payload, &p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if batch[0].ID != id || p.Value != firstPayload.Value {
		t.Fatalf("first payload must win, got id=%q payload=%q", batch[0].ID, p.Value)
	}
	if !maps.Equal(batch[0].Metadata, first[0].Metadata) {
		t.Fatalf("first metadata must win, got %+v", batch[0].Metadata)
	}
	if !batch[0].OccurredAt.Equal(first[0].OccurredAt) {
		t.Fatalf("first OccurredAt must win, got %v want %v",
			batch[0].OccurredAt, first[0].OccurredAt)
	}
}

// 跨类型复用同一显式 ID：两次 Publish 均返回 nil，该 ID 全局只有一行
// （落在首写类型），第二个 payload 被静默丢弃（docs/api.md §2.3）。
func testSameIDCrossTypePublish(t *testing.T, d event.Driver) {
	ctx := t.Context()
	id := "dup-cross-id-" + t.Name()
	typA := "dup-cross-a-" + t.Name()
	typB := "dup-cross-b-" + t.Name()
	evA := event.Declare[suitePayload](typA)
	evB := event.Declare[suitePayload](typB)
	pub := event.NewPublisher(d)

	mustPublish(t, ctx, pub, evA, suitePayload{"first"}, event.WithEventID(id))
	mustPublish(t, ctx, pub, evB, suitePayload{"second"}, event.WithEventID(id))

	batchA, err := d.Fetch(ctx, event.FetchQuery{Type: typA, Limit: 10})
	if err != nil {
		t.Fatalf("fetch A: %v", err)
	}
	if len(batchA) != 1 || batchA[0].ID != id {
		t.Fatalf("first type must hold the single row, got %+v", batchA)
	}
	var p suitePayload
	if err := event.NewJSONCodec().Decode(batchA[0].Payload, &p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if p.Value != "first" {
		t.Fatalf("second payload must be discarded, got %q", p.Value)
	}
	batchB, err := d.Fetch(ctx, event.FetchQuery{Type: typB, Limit: 10})
	if err != nil {
		t.Fatalf("fetch B: %v", err)
	}
	if len(batchB) != 0 {
		t.Fatalf("same ID must not create a row for the second type, got %+v", batchB)
	}
}

// 显式空串 ID 经事件侧糖发布：返回 ErrEmptyEventID 且不落行
// （docs/api.md §2.3、§2.7）。
func testEmptyEventIDEventPublish(t *testing.T, d event.Driver) {
	ctx := t.Context()
	typ := "empty-id-sugar-" + t.Name()
	ev := event.Declare[suitePayload](typ)

	err := ev.Publish(ctx, event.NewPublisher(d), suitePayload{"x"}, event.WithEventID(""))
	if !errors.Is(err, event.ErrEmptyEventID) {
		t.Fatalf("empty explicit ID must return ErrEmptyEventID, got %v", err)
	}
	batch, ferr := d.Fetch(ctx, event.FetchQuery{Type: typ, Limit: 10})
	if ferr != nil {
		t.Fatalf("fetch: %v", ferr)
	}
	if len(batch) != 0 {
		t.Fatalf("rejected publish must not store a row, got %+v", batch)
	}
}

// 显式空串 ID 经 PublishNoPayload 发布：同样返回 ErrEmptyEventID 且不落行
// （docs/api.md §2.3、§2.7）。
func testEmptyEventIDPublishNoPayload(t *testing.T, d event.Driver) {
	ctx := t.Context()
	typ := "empty-id-nopayload-" + t.Name()
	ev := event.Declare[event.NoPayload](typ)

	err := event.NewPublisher(d).PublishNoPayload(ctx, ev, event.WithEventID(""))
	if !errors.Is(err, event.ErrEmptyEventID) {
		t.Fatalf("empty explicit ID must return ErrEmptyEventID, got %v", err)
	}
	batch, ferr := d.Fetch(ctx, event.FetchQuery{Type: typ, Limit: 10})
	if ferr != nil {
		t.Fatalf("fetch: %v", ferr)
	}
	if len(batch) != 0 {
		t.Fatalf("rejected publish must not store a row, got %+v", batch)
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
	head, err := d.HeadPosition(t.Context(), "no-such-type-"+t.Name())
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

// Claim 限额：至多返回 Limit 条（docs/api.md §2.4；core 认领预取按此契约
// 计算每轮批大小）。普通发放轮的接管/终态计数为零。
func testClaimLimit(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "cl1", typ, "1", nil)
	appendEvent(t, d, "cl2", typ, "2", nil)
	appendEvent(t, d, "cl3", typ, "3", nil)

	res, err := d.Claim(t.Context(), event.ClaimQuery{
		Type: typ, ConsumerType: "cl", Lease: time.Minute, Limit: 1,
		RetryLimit: 5, Now: baseNow,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(res.Leases) != 1 {
		t.Fatalf("limit 1 must cap claim batch at 1 lease, got %d", len(res.Leases))
	}
	if res.Takeovers != 0 || res.TerminalFailed != 0 {
		t.Fatalf("fresh grant must not count takeovers/terminal, got %+v", res)
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
		mu      sync.Mutex
		seen    = map[string]int{}
		claimWg sync.WaitGroup
	)
	for range claimers {
		claimWg.Go(func() {
			for {
				res, err := d.Claim(t.Context(), event.ClaimQuery{
					Type: typ, ConsumerType: "cc", Lease: time.Hour, Limit: 2,
					RetryLimit: 10, Now: baseNow,
				})
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				if len(res.Leases) == 0 {
					return
				}
				mu.Lock()
				for _, l := range res.Leases {
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
// 接管发放计数：Takeovers=1、TerminalFailed=0（design.md §9.2）。
func testClaimTakeover(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "tk1", typ, "1", nil)

	first := claimResult(t, d, typ, "tk", time.Minute, baseNow)
	if len(first.Leases) != 1 || first.Leases[0].Attempts != 1 {
		t.Fatalf("first claim: %+v", first)
	}
	if first.Takeovers != 0 || first.TerminalFailed != 0 {
		t.Fatalf("fresh claim must not count takeover/terminal, got %+v", first)
	}
	token1 := first.Leases[0].LeaseToken

	// 租约过期（base+2m > base+1m）→ 接管认领，Attempts 递增。
	second := claimResult(t, d, typ, "tk", time.Minute, baseNow.Add(2*time.Minute))
	if len(second.Leases) != 1 {
		t.Fatalf("expired lease must be reclaimable, got %+v", second)
	}
	if second.Leases[0].Attempts != 2 {
		t.Fatalf("takeover must increment attempts, want 2 got %d", second.Leases[0].Attempts)
	}
	if second.Takeovers != 1 || second.TerminalFailed != 0 {
		t.Fatalf("takeover grant must count Takeovers=1 TerminalFailed=0, got %+v", second)
	}
	if second.Leases[0].LeaseToken == token1 {
		t.Fatal("takeover must issue a new lease token")
	}
	if !second.Leases[0].LeaseUntil.Equal(baseNow.Add(2 * time.Minute).Add(time.Minute)) {
		t.Fatalf("lease until = now+lease, got %v", second.Leases[0].LeaseUntil)
	}
}

// 接管计入重试 + 上限：达 RetryLimit 的接管认领不再发放租约、直接置
// failed 终态（ADR 0006「不再被认领」）。接管计数覆盖发放与终态两条
// 路径：接管发放 Takeovers=1/TerminalFailed=0；终态接管 Takeovers=1 且
// TerminalFailed=1（design.md §9.2）。
func testClaimOverLimitTerminal(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "ov1", typ, "1", nil)
	const retryLimit = 2
	doClaim := func(now time.Time) event.ClaimResult {
		t.Helper()
		res, err := d.Claim(t.Context(), event.ClaimQuery{
			Type: typ, ConsumerType: "ov", Lease: time.Minute, Limit: 10,
			RetryLimit: retryLimit, Now: now,
		})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		return res
	}

	// 第 1 次认领（Attempts=1，无主首认非接管）。
	res := doClaim(baseNow)
	if len(res.Leases) != 1 || res.Leases[0].Attempts != 1 {
		t.Fatalf("first claim: %+v", res)
	}
	if res.Takeovers != 0 || res.TerminalFailed != 0 {
		t.Fatalf("fresh claim must not count takeover/terminal, got %+v", res)
	}
	// 过期接管 → 计入第 2 次失败 = 尚未达 RetryLimit=2 → 发放第 2 次尝试。
	// 接管发放：Takeovers=1、TerminalFailed=0。
	res = doClaim(baseNow.Add(2 * time.Minute))
	if len(res.Leases) != 1 || res.Leases[0].Attempts != 2 {
		t.Fatalf("second attempt within limit must be handed out, got %+v", res)
	}
	if res.Takeovers != 1 || res.TerminalFailed != 0 {
		t.Fatalf("takeover grant must count Takeovers=1 TerminalFailed=0, got %+v", res)
	}
	// 第 2 次尝试也失去租约 → 再接管将计入第 3 次失败，超过 RetryLimit
	// → 置 failed、不发放。终态接管同时计入两者。
	res = doClaim(baseNow.Add(4 * time.Minute))
	if len(res.Leases) != 0 {
		t.Fatalf("over-limit takeover must not hand out lease, got %+v", res)
	}
	if res.Takeovers != 1 || res.TerminalFailed != 1 {
		t.Fatalf("terminal takeover must count Takeovers=1 TerminalFailed=1, got %+v", res)
	}
	// 终态后永不再认领，计数归零。
	res = doClaim(baseNow.Add(time.Hour))
	if len(res.Leases) != 0 || res.Takeovers != 0 || res.TerminalFailed != 0 {
		t.Fatalf("failed terminal must never be claimed, got %+v", res)
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
		res, err := d.Claim(t.Context(), event.ClaimQuery{
			Type: typ, ConsumerType: "os", Lease: time.Minute, Limit: limit,
			RetryLimit: retryLimit, Now: now,
		})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		return res.Leases
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

	// os1 failed；os2 活跃租约过期且接管将达上限（claim_count=1，本次接管
	// 1+1 >= RetryLimit=2 → 翻转置 failed）不占槽位；新行 os3 无主 →
	// Limit=1 恰好发放给 os3。
	appendEvent(t, d, "os3", typ, "3", nil)
	res, err := d.Claim(t.Context(), event.ClaimQuery{
		Type: typ, ConsumerType: "os", Lease: time.Minute, Limit: 1,
		RetryLimit: retryLimit, Now: baseNow.Add(5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(res.Leases) != 1 || res.Leases[0].Envelope.ID != "os3" {
		t.Fatalf("over-limit row must not consume slot; want [os3], got %+v", res)
	}
	// 超限翻转（os2）计入终态接管：Takeovers=1、TerminalFailed=1；
	// 无主首认（os3）不计入接管。
	if res.Takeovers != 1 || res.TerminalFailed != 1 {
		t.Fatalf("over-limit flip must count terminal takeover, got %+v", res)
	}
	// os2 已随超限翻转进入终态：永不再认领（os3 租约过期被重认领属正常）。
	ls = doClaim(baseNow.Add(6*time.Minute), 10)
	for _, l := range ls {
		if l.Envelope.ID == "os2" {
			t.Fatalf("flipped over-limit row must be terminal, got lease %+v", l)
		}
	}
}

// Fail 重试的重认领不算接管：retrying 行的租约已被 Fail 释放、退避期满
// 重认领不接管任何租约——Takeovers=0（接管专属计数不与 Fail 重试混同，
// `Attempts > 1` 不能代替接管计数，design.md §9.2）。
func testFailRetryReclaimNotTakeover(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "fr1", typ, "1", nil)

	ls := claim(t, d, typ, "fr", time.Minute, baseNow)
	if len(ls) != 1 {
		t.Fatalf("first claim: %+v", ls)
	}
	if err := d.Fail(t.Context(), event.FailRequest{
		EventID: "fr1", ConsumerType: "fr", LeaseToken: ls[0].LeaseToken,
		RetryLimit: 5, Backoff: linearBackoffMs(), Now: baseNow,
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}

	// 退避期满重认领：Attempts 递增（含本次 = 2），但不计接管。
	res := claimResult(t, d, typ, "fr", time.Minute, baseNow.Add(time.Second))
	if len(res.Leases) != 1 || res.Leases[0].Attempts != 2 {
		t.Fatalf("retry reclaim: %+v", res)
	}
	if res.Takeovers != 0 || res.TerminalFailed != 0 {
		t.Fatalf("retry reclaim must not count takeover/terminal, got %+v", res)
	}
}

// 非接管路径不产生认领侧终态：retrying 行不因 RetryLimit 下调而在认领时
// 置 failed——置终态的下推条件只针对「过期租约接管」（ADR 0006），行被
// 正常重认领发放（Attempts 超出下调后的 RetryLimit 属订阅配置差异边界），
// 落库状态与计数一致（design.md §9.2）。
func testRetryingOverLimitRowNoTerminalOnClaim(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "rn1", typ, "1", nil)

	// Fail 一次（RetryLimit=5 → count=1，retrying）。
	ls := claim(t, d, typ, "rn", time.Minute, baseNow)
	if len(ls) != 1 {
		t.Fatalf("first claim: %+v", ls)
	}
	if err := d.Fail(t.Context(), event.FailRequest{
		EventID: "rn1", ConsumerType: "rn", LeaseToken: ls[0].LeaseToken,
		RetryLimit: 5, Backoff: linearBackoffMs(), Now: baseNow,
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}

	// 以更低的 RetryLimit=1 认领：retrying 行重认领发放，不置终态。
	res := claimResult(t, d, typ, "rn", time.Minute, baseNow.Add(time.Second))
	if len(res.Leases) != 1 || res.Leases[0].Attempts != 2 {
		t.Fatalf("retrying row must be re-claimed, got %+v", res)
	}
	if res.Takeovers != 0 || res.TerminalFailed != 0 {
		t.Fatalf("retrying reclaim must not count takeover/terminal, got %+v", res)
	}
	// 对账落库事实：行为 active（非 failed），未被认领动作置终态。
	rd, ok := d.(ClaimStateReader)
	if !ok {
		t.Fatalf("driver %T must implement ClaimStateReader", d)
	}
	st := readClaimState(t, rd, "rn1", "rn")
	if st.Status != "active" {
		t.Fatalf("status = %q, want active (claim must not terminalize retrying rows)", st.Status)
	}
}

// 批量认领混合计数：无主首认 + 过期租约接管 + Fail 重试重认领同轮发放，
// Takeovers 只计接管行；Attempts 与落库状态可对账（design.md §9.2）。
func testMixedBatchClaimCounts(t *testing.T, d event.Driver) {
	typ := t.Name()
	appendEvent(t, d, "mb2", typ, "2", nil) // 将成为过期接管
	appendEvent(t, d, "mb3", typ, "3", nil) // 将成为 Fail 重试重认领

	// mb2/mb3 先认领一次（count=0）。
	ls := claim(t, d, typ, "mb", time.Minute, baseNow)
	if len(ls) != 2 {
		t.Fatalf("first claim want 2 leases, got %+v", ls)
	}
	var mb2, mb3 event.Lease
	for _, l := range ls {
		switch l.Envelope.ID {
		case "mb2":
			mb2 = l
		case "mb3":
			mb3 = l
		}
	}
	if mb2.Envelope.ID == "" || mb3.Envelope.ID == "" {
		t.Fatalf("want mb2/mb3 claimed, got %+v", ls)
	}
	// mb3 显式 Fail（retrying，count=1）；mb2 保持活跃直至过期。
	if err := d.Fail(t.Context(), event.FailRequest{
		EventID: mb3.Envelope.ID, ConsumerType: "mb", LeaseToken: mb3.LeaseToken,
		RetryLimit: 5, Backoff: linearBackoffMs(), Now: baseNow,
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}
	// mb1 在首认之后追加：混合轮中为无主首认。
	appendEvent(t, d, "mb1", typ, "1", nil)

	// 混合轮：mb1 无主首认（Takeovers 不计）、mb2 过期接管（计 1）、
	// mb3 退避期满重认领（不计）。
	res := claimResult(t, d, typ, "mb", time.Minute, baseNow.Add(2*time.Minute))
	if len(res.Leases) != 3 {
		t.Fatalf("mixed batch must grant 3 leases, got %+v", res)
	}
	if res.Takeovers != 1 || res.TerminalFailed != 0 {
		t.Fatalf("mixed batch must count exactly the takeover, got %+v", res)
	}
	attempts := map[string]int{}
	for _, l := range res.Leases {
		attempts[l.Envelope.ID] = l.Attempts
	}
	if attempts["mb1"] != 1 || attempts["mb2"] != 2 || attempts["mb3"] != 2 {
		t.Fatalf("attempts = %v, want mb1=1 (fresh) mb2=2 (takeover) mb3=2 (retry)", attempts)
	}
	// 对账落库事实：三行均为 active 租约内处理中。
	rd, ok := d.(ClaimStateReader)
	if !ok {
		t.Fatalf("driver %T must implement ClaimStateReader", d)
	}
	for _, id := range []string{"mb1", "mb2", "mb3"} {
		if st := readClaimState(t, rd, id, "mb"); st.Status != "active" {
			t.Fatalf("status of %s = %q, want active", id, st.Status)
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
	// （Fail 的 CAS 要求行仍为 active：若已被置 done，Fail 静默不命中，
	// 末尾重新认领断言即失败）。
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

// 运行时注入字段不落库、不被后端依赖：Append 带入非零 Attempts/LastAttempt
// 被忽略（Fetch 返回零值），Claim 的尝试序号从 1 起算；经 Runner 投递时
// LastAttempt 由 core 按订阅 RetryLimit 计算，不被写入值污染。
func testRuntimeFieldsNotPersisted(t *testing.T, d event.Driver) {
	typ := t.Name()
	payload, err := event.NewJSONCodec().Encode(suitePayload{Value: "runtime"})
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	injected := event.RawEnvelope{
		ID: "rf1", Type: typ, Payload: payload, OccurredAt: baseNow,
		Attempts: 99, LastAttempt: true,
	}
	if _, err := d.Append(t.Context(), injected); err != nil {
		t.Fatalf("append: %v", err)
	}

	// Fetch 返回必须为零值：运行时字段不落库。
	batch, err := d.Fetch(t.Context(), event.FetchQuery{Type: typ, Limit: 10})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(batch) != 1 || batch[0].Attempts != 0 || batch[0].LastAttempt {
		t.Fatalf("fetched envelope must carry zero runtime fields, got %+v", batch)
	}

	// 直接认领：尝试序号 = 认领后 claim_count + 1 = 1，而非写入的 99。
	ls := claim(t, d, typ, "rf-direct", time.Minute, baseNow)
	if len(ls) != 1 || ls[0].Attempts != 1 {
		t.Fatalf("claim want attempts=1 (not persisted 99), got %+v", ls)
	}

	// handler 侧 LastAttempt 由 core 计算：RetryLimit=2 时首次为 false。
	ev := event.Declare[suitePayload](typ)
	got := make(chan attemptObservation, 1)
	run := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond),
		event.WithIdentity("rf-runner"), event.WithLogger(discardLogger()))
	if err := ev.SubscribeClaim(run, func(_ context.Context, e event.Envelope[suitePayload]) error {
		recordAttempt(got, e)
		return nil
	},
		event.WithRetryLimit(2),
		event.WithLease(time.Second),
		event.WithHeartbeat(200*time.Millisecond),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(t.Context())) }()

	var obs attemptObservation
	select {
	case obs = <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting: runtime-fields delivery")
	}
	if obs.attempts != 1 || obs.lastAttempt {
		t.Fatalf("handler attempts=%d last=%v, want 1/false (core computed)", obs.attempts, obs.lastAttempt)
	}
}
