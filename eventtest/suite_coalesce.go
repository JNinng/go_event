package eventtest

import (
	"testing"
	"time"

	"github.com/jninng/go_event"
)

// RunCoalescingSuite 运行合并投递契约测试（同键最新一次消费，design.md §9.2、
// ADR 0014，契约要点见 docs/api.md §4）：latest-wins 与 Fetch / Claim 双模式
// 排除、保守取代范围、键与事件 ID 正交、AppendTx 取代与插入同事务、
// Inspector superseded 分档与保留期清理。newDriver 须每用例返回配置了保留
// 窗口（≤2s，同 RunRetentionSuite）的全新 Driver；beginTx 须与 driver 同源
// 绑定事务会话（同 RunTransactionalAppendSuite 的 TxSession），nil 即 Fatal。
func RunCoalescingSuite(t *testing.T, newDriver func(*testing.T) event.Driver,
	beginTx func(*testing.T, event.Driver) TxSession) {
	if newDriver == nil {
		t.Fatal("newDriver is nil: the coalescing suite requires a per-case driver factory")
	}
	if beginTx == nil {
		t.Fatal("beginTx is nil: the coalescing suite requires a transaction " +
			"session factory wired by the backend test assembly (AppendTx path)")
	}

	// ---- latest-wins 与双模式排除（Fetch / Claim 同一下推）----
	t.Run("NewerSameKey_Append_SupersedesPendingPredecessorBothModes", func(t *testing.T) {
		testCoalesceLatestWins(t, newDriver)
	})
	t.Run("SupersededFirstRow_Fetch_WatermarkSkipsHeadPositionAnchors", func(t *testing.T) {
		testCoalesceWatermarkSkip(t, newDriver)
	})
	t.Run("NewIdentity_Claim_SkipsSupersededRow", func(t *testing.T) {
		testCoalesceNewIdentitySkip(t, newDriver)
	})

	// ---- 保守取代范围（有认领行的前驱不取代，新事件照常排队）----
	t.Run("ActiveLeasePredecessor_Append_NotSupersededLeaseUntouched", func(t *testing.T) {
		testCoalesceActiveLeaseUntouched(t, newDriver)
	})
	t.Run("RetryingPredecessor_Append_NotSupersededReclaimable", func(t *testing.T) {
		testCoalesceRetryingNotSuperseded(t, newDriver)
	})
	t.Run("ExpiredLeasePredecessor_Append_NotSupersededTakeoverPaths", func(t *testing.T) {
		testCoalesceExpiredLeaseNotSuperseded(t, newDriver)
	})
	t.Run("TerminalFailedPredecessor_Append_QueuesNewKeepsFailedRow", func(t *testing.T) {
		testCoalesceTerminalFailedNotSuperseded(t, newDriver)
	})

	// ---- 作用域与 ID 正交 ----
	t.Run("SameKeyDifferentTypes_Append_NoCrossTypeSupersede", func(t *testing.T) {
		testCoalesceScopePerType(t, newDriver)
	})
	t.Run("EmptyCoalesceKey_Append_NoSupersede", func(t *testing.T) {
		testCoalesceEmptyKeyNoOp(t, newDriver)
	})
	t.Run("RedeliveredOldID_Append_DoesNotSupersedeNewerWinner", func(t *testing.T) {
		testCoalesceIDOrthogonal(t, newDriver)
	})
	t.Run("DuplicateID_Append_SupersededZeroSingleRow", func(t *testing.T) {
		testCoalesceDuplicateIDNoOp(t, newDriver)
	})

	// ---- 与延迟投递组合（被取代的延迟行永不到期投递）----
	t.Run("SupersededDelayedRow_DeliverAt_NeverDelivers", func(t *testing.T) {
		testCoalesceDelayedCombination(t, newDriver)
	})

	// ---- 信封透传 ----
	t.Run("CoalesceKey_Envelope_RoundTripsBothModes", func(t *testing.T) {
		testCoalesceEnvelopeRoundTrip(t, newDriver)
	})

	// ---- AppendTx 路径（取代与插入同事务，ADR 0012 边界内）----
	t.Run("UncommittedTx_AppendTx_PredecessorStillVisibleNoSupersede", func(t *testing.T) {
		testCoalesceTxUncommitted(t, newDriver, beginTx)
	})
	t.Run("CommittedTx_AppendTx_SupersedesPendingPredecessor", func(t *testing.T) {
		testCoalesceTxCommit(t, newDriver, beginTx)
	})
	t.Run("RolledBackTx_AppendTx_NoSupersedeNoInsert", func(t *testing.T) {
		testCoalesceTxRollback(t, newDriver, beginTx)
	})

	// ---- 查询面 superseded 分档 ----
	t.Run("SupersededRow_CountStates_SeparateTierPendingShrinks", func(t *testing.T) {
		testCoalesceInspectorTier(t, newDriver)
	})

	// ---- 保留期清理（superseded 行照常清理）----
	t.Run("SupersededRow_Retention_CleanedLikeOtherRows", func(t *testing.T) {
		testCoalesceRetentionCleanup(t, newDriver)
	})
}

// appendCoalesced 以合并键追加事件并返回 AppendResult（取代行数断言用）。
func appendCoalesced(t *testing.T, d event.Driver, id, typ, key string,
	occurredAt, deliverAt time.Time) event.AppendResult {
	t.Helper()
	payload, err := event.NewJSONCodec().Encode(suitePayload{Value: id})
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	e := event.RawEnvelope{
		ID: id, Type: typ, Payload: payload,
		OccurredAt: occurredAt, DeliverAt: deliverAt, CoalesceKey: key,
	}
	res, err := d.Append(t.Context(), e)
	if err != nil {
		t.Fatalf("append coalesced: %v", err)
	}
	return res
}

// latest-wins：同键第二次 Append 原子取代待消费前驱（Superseded=1），
// 广播与认领两种模式均只见最新行。
func testCoalesceLatestWins(t *testing.T, newDriver func(*testing.T) event.Driver) {
	d := newDriver(t)
	typ := t.Name()
	now := microNow()

	if res := appendCoalesced(t, d, "cw-1", typ, "kb:1", now, time.Time{}); res.Superseded != 0 {
		t.Fatalf("first append superseded = %d, want 0 (no predecessor)", res.Superseded)
	}
	res := appendCoalesced(t, d, "cw-2", typ, "kb:1", now, time.Time{})
	if res.Superseded != 1 {
		t.Fatalf("second append superseded = %d, want 1 (pending predecessor)", res.Superseded)
	}

	envs := fetchAt(t, d, typ, now)
	if len(envs) != 1 || envs[0].ID != "cw-2" {
		t.Fatalf("fetch after supersede = %+v, want exactly cw-2 (broadcast excludes superseded)", envs)
	}
	leases := claim(t, d, typ, "cw", time.Minute, now)
	if len(leases) != 1 || leases[0].Envelope.ID != "cw-2" {
		t.Fatalf("claim after supersede = %+v, want exactly cw-2 (claim excludes superseded)", leases)
	}
}

// 广播水位越过被取代行：position 顺序里被取代行不进批次，水位直接推进
// 到其后行；HeadPosition 照常锚定被取代行的 position（照常占 position，
// ADR 0014）。
func testCoalesceWatermarkSkip(t *testing.T, newDriver func(*testing.T) event.Driver) {
	d := newDriver(t)
	typ := t.Name()
	now := microNow()

	appendCoalesced(t, d, "wm-sup-1", typ, "kb:1", now, time.Time{}) // 将被取代
	appendCoalesced(t, d, "wm-plain", typ, "", now, time.Time{})     // 无键行
	res := appendCoalesced(t, d, "wm-sup-2", typ, "kb:1", now, time.Time{})
	if res.Superseded != 1 {
		t.Fatalf("superseded = %d, want 1", res.Superseded)
	}

	envs := fetchAt(t, d, typ, now)
	if len(envs) != 2 || envs[0].ID != "wm-plain" || envs[1].ID != "wm-sup-2" {
		t.Fatalf("fetch = %+v, want exactly [wm-plain, wm-sup-2] (superseded skipped)", envs)
	}
	for i := 1; i < len(envs); i++ {
		if comparePosition(envs[i].Position, envs[i-1].Position) <= 0 {
			t.Fatalf("fetch order broken at %d: %q after %q", i, envs[i].Position, envs[i-1].Position)
		}
	}
	head, err := d.HeadPosition(t.Context(), typ)
	if err != nil {
		t.Fatalf("head position: %v", err)
	}
	if head != envs[1].Position {
		t.Fatalf("head position = %q, want superseding row's position %q (superseded row still occupies position)", head, envs[1].Position)
	}
}

// superseded 是事件行属性：全新消费者身份（无任何认领行历史）同样不拾取
// 被取代行（ADR 0014）。
func testCoalesceNewIdentitySkip(t *testing.T, newDriver func(*testing.T) event.Driver) {
	d := newDriver(t)
	typ := t.Name()
	now := microNow()

	appendCoalesced(t, d, "ni-sup-1", typ, "kb:1", now, time.Time{})
	appendCoalesced(t, d, "ni-sup-2", typ, "kb:1", now, time.Time{})

	res := claimResult(t, d, typ, "brand-new-identity", time.Minute, now)
	if len(res.Leases) != 1 || res.Leases[0].Envelope.ID != "ni-sup-2" {
		t.Fatalf("claim by new identity = %+v, want exactly ni-sup-2 (superseded is an event-row attribute)", res.Leases)
	}
}

// 活跃租约前驱不取代：新事件照常排队，原租约与属主凭证不受影响。
func testCoalesceActiveLeaseUntouched(t *testing.T, newDriver func(*testing.T) event.Driver) {
	d := newDriver(t)
	typ := t.Name()
	now := microNow()

	appendCoalesced(t, d, "al-1", typ, "kb:1", now, time.Time{})
	ls := claim(t, d, typ, "c1", 10*time.Minute, now)
	if len(ls) != 1 || ls[0].Envelope.ID != "al-1" {
		t.Fatalf("claim = %+v, want exactly al-1", ls)
	}
	token := ls[0].LeaseToken

	res := appendCoalesced(t, d, "al-2", typ, "kb:1", now, time.Time{})
	if res.Superseded != 0 {
		t.Fatalf("superseded with active lease predecessor = %d, want 0 (v1 conservative scope)", res.Superseded)
	}

	// 原属主租约不受影响：属主凭证照常心跳并完成（完成即租约完好的证据）。
	if err := d.Heartbeat(t.Context(), event.HeartbeatRequest{
		EventID: "al-1", ConsumerType: "c1", LeaseToken: token,
		Extend: time.Minute, Now: now,
	}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if err := d.Complete(t.Context(), event.CompleteRequest{
		EventID: "al-1", ConsumerType: "c1", LeaseToken: token, Now: now,
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	// 新事件照常排队：原属主认领时活跃租约的 al-1（已 done）不再发放，
	// 恰好发放新行。
	leases := claim(t, d, typ, "c1", time.Minute, now)
	if len(leases) != 1 || leases[0].Envelope.ID != "al-2" {
		t.Fatalf("claim = %+v, want exactly al-2 (new event queued normally)", leases)
	}
}

// 退避中前驱不取代：退避期满后照常可认领，与新事件一并发放。
func testCoalesceRetryingNotSuperseded(t *testing.T, newDriver func(*testing.T) event.Driver) {
	d := newDriver(t)
	typ := t.Name()
	now := microNow()

	appendCoalesced(t, d, "rt-1", typ, "kb:1", now, time.Time{})
	ls := claim(t, d, typ, "c1", time.Minute, now)
	if len(ls) != 1 || ls[0].Envelope.ID != "rt-1" {
		t.Fatalf("claim = %+v, want exactly rt-1", ls)
	}
	if err := d.Fail(t.Context(), event.FailRequest{
		EventID: "rt-1", ConsumerType: "c1", LeaseToken: ls[0].LeaseToken,
		Error: "transient", RetryLimit: 5,
		Backoff: event.LinearBackoff(time.Minute, time.Hour), Now: now,
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}

	res := appendCoalesced(t, d, "rt-2", typ, "kb:1", now, time.Time{})
	if res.Superseded != 0 {
		t.Fatalf("superseded with retrying predecessor = %d, want 0 (v1 conservative scope)", res.Superseded)
	}

	// 退避期满：两行均照常可认领。
	leases := claim(t, d, typ, "c2", time.Minute, now.Add(2*time.Minute))
	if len(leases) != 2 {
		t.Fatalf("claim after backoff = %+v, want both rows", leases)
	}
}

// 租约过期待接管的前驱不取代：接管认领与新事件认领两条路径都不受影响。
func testCoalesceExpiredLeaseNotSuperseded(t *testing.T, newDriver func(*testing.T) event.Driver) {
	d := newDriver(t)
	typ := t.Name()
	now := microNow()

	appendCoalesced(t, d, "el-1", typ, "kb:1", now, time.Time{})
	ls := claim(t, d, typ, "c1", time.Nanosecond, now) // 立即过期
	if len(ls) != 1 || ls[0].Envelope.ID != "el-1" {
		t.Fatalf("claim = %+v, want exactly el-1", ls)
	}
	later := now.Add(time.Minute)

	res := appendCoalesced(t, d, "el-2", typ, "kb:1", later, time.Time{})
	if res.Superseded != 0 {
		t.Fatalf("superseded with expired-lease predecessor = %d, want 0 (v1 conservative scope)", res.Superseded)
	}
	leases := claim(t, d, typ, "c2", time.Minute, later)
	if len(leases) != 2 {
		t.Fatalf("claim = %+v, want both rows (takeover path unaffected)", leases)
	}
}

// 终态 failed 前驱不取代：新事件照常排队，failed 行保留（管理面重放
// 语义不受影响，ADR 0010）。
func testCoalesceTerminalFailedNotSuperseded(t *testing.T, newDriver func(*testing.T) event.Driver) {
	d := newDriver(t)
	typ := t.Name()
	now := microNow()

	appendCoalesced(t, d, "tf-1", typ, "kb:1", now, time.Time{})
	ls := claim(t, d, typ, "c1", time.Minute, now)
	if len(ls) != 1 || ls[0].Envelope.ID != "tf-1" {
		t.Fatalf("claim = %+v, want exactly tf-1", ls)
	}
	if err := d.Fail(t.Context(), event.FailRequest{
		EventID: "tf-1", ConsumerType: "c1", LeaseToken: ls[0].LeaseToken,
		Error: "boom", RetryLimit: 1, Backoff: event.LinearBackoff(time.Second, time.Second), Now: now,
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}

	res := appendCoalesced(t, d, "tf-2", typ, "kb:1", now, time.Time{})
	if res.Superseded != 0 {
		t.Fatalf("superseded with terminal-failed predecessor = %d, want 0 (v1 conservative scope)", res.Superseded)
	}
	// 同一身份：failed 行不再认领（终态保留，管理面重放语义不受影响，
	// ADR 0010），新事件照常发放。
	leases := claim(t, d, typ, "c1", time.Minute, now)
	if len(leases) != 1 || leases[0].Envelope.ID != "tf-2" {
		t.Fatalf("claim = %+v, want exactly tf-2 (failed row terminal, new event queued)", leases)
	}
}

// 作用域 = (type, key)：同键跨事件类型不互相取代。
func testCoalesceScopePerType(t *testing.T, newDriver func(*testing.T) event.Driver) {
	d := newDriver(t)
	now := microNow()
	typA := t.Name() + "-a"
	typB := t.Name() + "-b"

	if res := appendCoalesced(t, d, "pt-a1", typA, "kb:1", now, time.Time{}); res.Superseded != 0 {
		t.Fatalf("type a superseded = %d, want 0", res.Superseded)
	}
	res := appendCoalesced(t, d, "pt-b1", typB, "kb:1", now, time.Time{})
	if res.Superseded != 0 {
		t.Fatalf("type b superseded = %d, want 0 (scope is (type, key))", res.Superseded)
	}
	if ids := fetchIDsAt(t, d, typA, now); len(ids) != 1 {
		t.Fatalf("type a fetch = %+v, want exactly pt-a1", ids)
	}
	if ids := fetchIDsAt(t, d, typB, now); len(ids) != 1 {
		t.Fatalf("type b fetch = %+v, want exactly pt-b1", ids)
	}
}

// 空键 = 未设置：无键行不参与合并，重复发布照常各投一次。
func testCoalesceEmptyKeyNoOp(t *testing.T, newDriver func(*testing.T) event.Driver) {
	d := newDriver(t)
	typ := t.Name()
	now := microNow()

	if res := appendCoalesced(t, d, "ek-1", typ, "", now, time.Time{}); res.Superseded != 0 {
		t.Fatalf("first append superseded = %d, want 0", res.Superseded)
	}
	res := appendCoalesced(t, d, "ek-2", typ, "", now, time.Time{})
	if res.Superseded != 0 {
		t.Fatalf("append without key superseded = %d, want 0 (empty key never coalesces)", res.Superseded)
	}
	if ids := fetchIDsAt(t, d, typ, now); len(ids) != 2 {
		t.Fatalf("fetch = %+v, want both rows", ids)
	}
}

// ID 正交：重发已被取代的旧事件（同 ID）是纯 no-op——不杀死新赢家、不
// 复活自身（同 ID 幂等语义不变，ADR 0014）。
func testCoalesceIDOrthogonal(t *testing.T, newDriver func(*testing.T) event.Driver) {
	d := newDriver(t)
	typ := t.Name()
	now := microNow()

	appendCoalesced(t, d, "io-old", typ, "kb:1", now, time.Time{})
	if res := appendCoalesced(t, d, "io-new", typ, "kb:1", now, time.Time{}); res.Superseded != 1 {
		t.Fatalf("superseded = %d, want 1", res.Superseded)
	}

	res := appendCoalesced(t, d, "io-old", typ, "kb:1", now, time.Time{})
	if res.Superseded != 0 {
		t.Fatalf("duplicate append superseded = %d, want 0 (idempotent append never supersedes)", res.Superseded)
	}
	envs := fetchAt(t, d, typ, now)
	if len(envs) != 1 || envs[0].ID != "io-new" {
		t.Fatalf("fetch = %+v, want exactly io-new (old ID must not resurrect or kill the winner)", envs)
	}
	leases := claim(t, d, typ, "io", time.Minute, now)
	if len(leases) != 1 || leases[0].Envelope.ID != "io-new" {
		t.Fatalf("claim = %+v, want exactly io-new", leases)
	}
}

// 同 ID 幂等重复 Append（同键）：首写为准、无取代、单行。
func testCoalesceDuplicateIDNoOp(t *testing.T, newDriver func(*testing.T) event.Driver) {
	d := newDriver(t)
	typ := t.Name()
	now := microNow()

	if res := appendCoalesced(t, d, "dup-1", typ, "kb:1", now, time.Time{}); res.Superseded != 0 {
		t.Fatalf("first append superseded = %d, want 0", res.Superseded)
	}
	res := appendCoalesced(t, d, "dup-1", typ, "kb:1", now, time.Time{})
	if res.Superseded != 0 {
		t.Fatalf("duplicate append superseded = %d, want 0", res.Superseded)
	}
	envs := fetchAt(t, d, typ, now)
	if len(envs) != 1 || envs[0].ID != "dup-1" {
		t.Fatalf("fetch = %+v, want exactly one first-write row", envs)
	}
}

// 与延迟投递组合：被取代的延迟行永不到期投递——到期时刻后广播与认领均
// 只见最新行（去抖形态：同键 + deliver_at 每次新发布自带基线）。
func testCoalesceDelayedCombination(t *testing.T, newDriver func(*testing.T) event.Driver) {
	d := newDriver(t)
	typ := t.Name()
	now := microNow()
	due := now.Add(time.Minute)

	appendCoalesced(t, d, "dc-1", typ, "kb:1", now, due)
	res := appendCoalesced(t, d, "dc-2", typ, "kb:1", now, due)
	if res.Superseded != 1 {
		t.Fatalf("superseded = %d, want 1 (delayed pending predecessor is superseded)", res.Superseded)
	}

	if envs := fetchAt(t, d, typ, now); len(envs) != 0 {
		t.Fatalf("fetch before due = %+v, want none", envs)
	}
	envs := fetchAt(t, d, typ, due)
	if len(envs) != 1 || envs[0].ID != "dc-2" {
		t.Fatalf("fetch at due = %+v, want exactly dc-2 (superseded delayed row never delivers)", envs)
	}
	leases := claim(t, d, typ, "dc", time.Minute, due)
	if len(leases) != 1 || leases[0].Envelope.ID != "dc-2" {
		t.Fatalf("claim at due = %+v, want exactly dc-2", leases)
	}
}

// 信封透传：Fetch / Claim 返回的 CoalesceKey 与发布侧声明一致（后端原样
// 落库透传，空键往返为空串）。
func testCoalesceEnvelopeRoundTrip(t *testing.T, newDriver func(*testing.T) event.Driver) {
	d := newDriver(t)
	typ := t.Name()
	now := microNow()

	appendCoalesced(t, d, "ck-keyed", typ, "kb:doc-1", now, time.Time{})
	appendCoalesced(t, d, "ck-plain", typ, "", now, time.Time{})

	envs := fetchAt(t, d, typ, now)
	var keyed, plain *event.RawEnvelope
	for i := range envs {
		switch envs[i].ID {
		case "ck-keyed":
			keyed = &envs[i]
		case "ck-plain":
			plain = &envs[i]
		}
	}
	if keyed == nil || keyed.CoalesceKey != "kb:doc-1" {
		t.Fatalf("fetch coalesce key round trip = %+v, want kb:doc-1 on ck-keyed", envs)
	}
	if plain == nil || plain.CoalesceKey != "" {
		t.Fatalf("fetch coalesce key round trip = %+v, want empty on ck-plain", envs)
	}
	leases := claim(t, d, typ, "ck", time.Minute, now)
	var claimKeyed, claimPlain bool
	for _, l := range leases {
		switch {
		case l.Envelope.ID == "ck-keyed" && l.Envelope.CoalesceKey == "kb:doc-1":
			claimKeyed = true
		case l.Envelope.ID == "ck-plain" && l.Envelope.CoalesceKey == "":
			claimPlain = true
		}
	}
	if !claimKeyed || !claimPlain {
		t.Fatalf("claim envelope round trip = %+v, want both keys verbatim", leases)
	}
}

// 提交前对外不可见：未提交事务内的同键新行既不可见、也不取代已提交前驱
// （取代随事务可见，ADR 0012 隔离边界）。断言仅走广播读取——认领路径在
// Postgres 后端对持锁行 SKIP LOCKED，跨后端非确定，不属于本用例契约。
func testCoalesceTxUncommitted(t *testing.T, newDriver func(*testing.T) event.Driver,
	beginTx func(*testing.T, event.Driver) TxSession) {
	d := newDriver(t)
	typ := t.Name()
	now := microNow()

	appendCoalesced(t, d, "txu-1", typ, "kb:1", now, time.Time{})
	tx := beginTx(t, d)
	tx.AppendTx(t, event.RawEnvelope{ID: "txu-2", Type: typ,
		Payload: txPayload(t, "2"), OccurredAt: now, CoalesceKey: "kb:1"})
	defer tx.Rollback(t) // 用例结束丢弃，不影响其他用例

	envs := fetchAt(t, d, typ, now)
	if len(envs) != 1 || envs[0].ID != "txu-1" {
		t.Fatalf("fetch over uncommitted tx = %+v, want exactly txu-1 (supersede not visible before commit)", envs)
	}
}

// 提交后可见：同事务的取代与插入一起生效——前驱被取代、新行照常可投递
// （outbox 取代与插入同事务，ADR 0014）。
func testCoalesceTxCommit(t *testing.T, newDriver func(*testing.T) event.Driver,
	beginTx func(*testing.T, event.Driver) TxSession) {
	d := newDriver(t)
	typ := t.Name()
	now := microNow()

	appendCoalesced(t, d, "txc-1", typ, "kb:1", now, time.Time{})
	tx := beginTx(t, d)
	tx.AppendTx(t, event.RawEnvelope{ID: "txc-2", Type: typ,
		Payload: txPayload(t, "2"), OccurredAt: now, CoalesceKey: "kb:1"})
	tx.Commit(t)

	envs := fetchAt(t, d, typ, now)
	if len(envs) != 1 || envs[0].ID != "txc-2" {
		t.Fatalf("fetch after commit = %+v, want exactly txc-2 (supersede and insert committed together)", envs)
	}
	leases := claim(t, d, typ, "txc", time.Minute, now)
	if len(leases) != 1 || leases[0].Envelope.ID != "txc-2" {
		t.Fatalf("claim after commit = %+v, want exactly txc-2", leases)
	}
}

// 回滚零事实：事务回滚时取代与插入一并丢弃——前驱照常待消费、新行不存在。
func testCoalesceTxRollback(t *testing.T, newDriver func(*testing.T) event.Driver,
	beginTx func(*testing.T, event.Driver) TxSession) {
	d := newDriver(t)
	typ := t.Name()
	now := microNow()

	appendCoalesced(t, d, "txr-1", typ, "kb:1", now, time.Time{})
	tx := beginTx(t, d)
	tx.AppendTx(t, event.RawEnvelope{ID: "txr-2", Type: typ,
		Payload: txPayload(t, "2"), OccurredAt: now, CoalesceKey: "kb:1"})
	tx.Rollback(t)

	envs := fetchAt(t, d, typ, now)
	if len(envs) != 1 || envs[0].ID != "txr-1" {
		t.Fatalf("fetch after rollback = %+v, want exactly txr-1 (predecessor untouched)", envs)
	}
	leases := claim(t, d, typ, "txr", time.Minute, now)
	if len(leases) != 1 || leases[0].Envelope.ID != "txr-1" {
		t.Fatalf("claim after rollback = %+v, want exactly txr-1", leases)
	}
}

// Inspector superseded 分档：被取代行从 Pending 移入 Superseded（六档
// 互斥且完备，合计等于事件总数）。
func testCoalesceInspectorTier(t *testing.T, newDriver func(*testing.T) event.Driver) {
	d := newDriver(t)
	insp, ok := d.(event.Inspector)
	if !ok {
		t.Fatalf("driver %T must implement event.Inspector: the superseded tier "+
			"is part of the CountStates contract (ADR 0014)", d)
	}
	typ := t.Name()
	now := microNow()

	appendCoalesced(t, d, "it-plain", typ, "", now, time.Time{})  // 留 Pending
	appendCoalesced(t, d, "it-sup", typ, "kb:1", now, time.Time{}) // 将被取代
	appendCoalesced(t, d, "it-new", typ, "kb:1", now, time.Time{}) // 最新一行，Pending

	c, err := insp.CountStates(t.Context(), typ, "it")
	if err != nil {
		t.Fatalf("count states: %v", err)
	}
	if c.Pending != 2 || c.Superseded != 1 {
		t.Fatalf("count states = %+v, want Pending=2 Superseded=1", c)
	}
	if total := c.Pending + c.Superseded + c.Retrying + c.Processing + c.Failed + c.Done; total != 3 {
		t.Fatalf("tier sum = %d, want 3 (six tiers are mutually exclusive and complete)", total)
	}
}

// 保留期清理：superseded 行照常按保留窗口清理（与即时行同判据：
// COALESCE(deliver_at, occurred_at)，无活跃租约即删）。序列沿
// RunRetentionSuite：新鲜行触发首轮清理 → 超窗行被节流跳过 → 等过节流窗
// → 合并追加（先取代后清理）评估。
func testCoalesceRetentionCleanup(t *testing.T, newDriver func(*testing.T) event.Driver) {
	d := newDriver(t)
	typ := t.Name()
	now := microNow()

	appendCoalesced(t, d, "rc-fresh", typ, "", now, time.Time{}) // 首轮清理：无可清理
	appendCoalesced(t, d, "rc-old", typ, "kb:1",
		now.Add(-2*time.Hour), time.Time{}) // 超窗行，节流窗内不被清理

	retentionSettle()
	res := appendCoalesced(t, d, "rc-new", typ, "kb:1", now, time.Time{})
	if res.Superseded != 1 {
		t.Fatalf("superseded = %d, want 1 (supersede precedes cleanup)", res.Superseded)
	}

	ids := fetchIDsAt(t, d, typ, now)
	if ids["rc-old"] {
		t.Fatal("superseded row beyond the retention window must be cleaned like other rows")
	}
	if !ids["rc-new"] || !ids["rc-fresh"] {
		t.Fatalf("within-window rows must be retained, got %+v", ids)
	}
}
