package eventtest

import (
	"fmt"
	"testing"
	"time"

	"github.com/jninng/go_event"
)

// Inspector 只读查询面契约用例（design.md §6/§9.2、ADR 0005 可选能力
// 探测）：三类查询分别与 Claim/Fail/Complete 的落库事实比对。
//   - 状态查询：无认领行 = 「待处理」（ok=false）；processing/retrying/
//     done/failed 与认领行 status 严格对齐；Attempts = 已计入的重试计数。
//   - 分档计数：「待处理」= 该身份无认领行（口径写进 Inspector 契约）；
//     done 为归档档，六档互斥且完备。
//   - failed 列表：偏移分页（首页/末页/越界/零 Limit/负 Offset），按
//     failed_at 降序、同刻按事件 ID 升序；摘要与终态时刻随行（ADR 0009）。
//
// 结果仅在保留窗口内可见：超窗行被清理后查询自然不可见，Inspector 不做
// 额外过滤（清理资格已由 RunRetentionSuite 契约约束，此处不重复）。

// RunInspectorSuite 运行 Inspector 查询面契约测试。d 须实现
// event.Inspector（inmem 原生实现；Postgres 后端的 Driver 同时实现该
// 接口）；探测失败按 ClaimStateReader 先例 Fatal 终止——提供查询面的
// 后端必须全过本套件。
func RunInspectorSuite(t *testing.T, d event.Driver) {
	insp, ok := d.(event.Inspector)
	if !ok {
		t.Fatalf("driver %T must implement event.Inspector: the read-only inspection "+
			"surface is an optional capability probed by type assertion (ADR 0005)", d)
	}

	// ---- 状态查询（事件 × 消费者身份）----
	t.Run("AppendedOnlyEvent_ClaimState_NoClaimRowPending", func(t *testing.T) { testInspectorStatePending(t, d, insp) })
	t.Run("ActiveLease_ClaimState_ProcessingAttemptsZero", func(t *testing.T) { testInspectorStateProcessing(t, d, insp) })
	t.Run("NonTerminalFail_ClaimState_RetryingAttemptsOne", func(t *testing.T) { testInspectorStateRetrying(t, d, insp) })
	t.Run("OverLimitFail_ClaimState_FailedTerminal", func(t *testing.T) { testInspectorStateFailed(t, d, insp) })
	t.Run("Completed_ClaimState_DoneTerminal", func(t *testing.T) { testInspectorStateDone(t, d, insp) })
	t.Run("ExpiredLease_ClaimState_TakeoverIncrementsAttempts", func(t *testing.T) { testInspectorStateTakeover(t, d, insp) })
	t.Run("OtherIdentity_ClaimState_NoClaimRow", func(t *testing.T) { testInspectorStateForeignIdentity(t, d, insp) })
	t.Run("UnknownEventID_ClaimState_NoClaimRow", func(t *testing.T) { testInspectorStateUnknownEvent(t, d, insp) })

	// ---- 状态分档计数 ----
	t.Run("UnclaimedEvents_CountStates_AllPending", func(t *testing.T) { testInspectorCountAllPending(t, d, insp) })
	t.Run("MixedClaimFacts_CountStates_BucketedPerFacts", func(t *testing.T) { testInspectorCountMixed(t, d, insp) })
	t.Run("CompletedEvents_CountStates_DoneArchived", func(t *testing.T) { testInspectorCountDoneArchived(t, d, insp) })
	t.Run("OtherIdentity_CountStates_AllPending", func(t *testing.T) { testInspectorCountForeignIdentity(t, d, insp) })
	t.Run("UnknownType_CountStates_AllZero", func(t *testing.T) { testInspectorCountUnknownType(t, d, insp) })

	// ---- failed 分页列表 ----
	t.Run("NoTerminalFail_FailedEvents_EmptyNoError", func(t *testing.T) { testInspectorFailedEmpty(t, d, insp) })
	t.Run("TerminalFail_FailedEvents_CarriesSummaryAndFailedAt", func(t *testing.T) { testInspectorFailedCarriesFacts(t, d, insp) })
	t.Run("OverLimitTakeover_FailedEvents_EmptySummaryWithFailedAt", func(t *testing.T) { testInspectorFailedTakeoverFacts(t, d, insp) })
	t.Run("DistinctFailedAt_FailedEvents_NewestFirst", func(t *testing.T) { testInspectorFailedNewestFirst(t, d, insp) })
	t.Run("SameFailedAt_FailedEvents_TieBreakByEventID", func(t *testing.T) { testInspectorFailedTieBreak(t, d, insp) })
	t.Run("NonTerminalAndOtherStates_FailedEvents_Excluded", func(t *testing.T) { testInspectorFailedExcludesOtherStates(t, d, insp) })
	t.Run("ForeignIdentityFilter_FailedEvents_OnlyMatchedIdentity", func(t *testing.T) { testInspectorFailedIdentityFilter(t, d, insp) })
	t.Run("ForeignTypeFilter_FailedEvents_OnlyMatchedType", func(t *testing.T) { testInspectorFailedTypeFilter(t, d, insp) })
	t.Run("FirstPage_FailedEvents_ReturnsHeadInOrder", func(t *testing.T) { testInspectorFailedFirstPage(t, d, insp) })
	t.Run("LastPageRemainder_FailedEvents_ReturnsLessThanLimit", func(t *testing.T) { testInspectorFailedLastPage(t, d, insp) })
	t.Run("OffsetBeyondTotal_FailedEvents_EmptyNoError", func(t *testing.T) { testInspectorFailedOffsetBeyond(t, d, insp) })
	t.Run("NonPositiveLimit_FailedEvents_EmptyNoError", func(t *testing.T) { testInspectorFailedZeroLimit(t, d, insp) })
	t.Run("NegativeOffset_FailedEvents_TreatedAsFirstPage", func(t *testing.T) { testInspectorFailedNegativeOffset(t, d, insp) })
	t.Run("RepeatedQueriesOnActiveLease_Inspector_ReadOnlyStable", func(t *testing.T) { testInspectorReadOnly(t, d, insp) })
}

// ---- 状态查询 ----

// mustClaimState 读取状态；用例前提是认领行必须存在（查询故障或行缺失
// 均立即失败于断言）。
func mustClaimState(t *testing.T, insp event.Inspector, eventID, consumerType string) event.ClaimState {
	t.Helper()
	st, ok, err := insp.ClaimState(t.Context(), eventID, consumerType)
	if err != nil {
		t.Fatalf("claim state (%s, %s): %v", eventID, consumerType, err)
	}
	if !ok {
		t.Fatalf("claim row (%s, %s) not found: pending has no claim row", eventID, consumerType)
	}
	return st
}

// 未认领事件：该身份无认领行，状态查询报「待处理」（ok=false，零值）。
func testInspectorStatePending(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	appendEvent(t, d, "is-p1", typ, "1", nil)

	st, ok, err := insp.ClaimState(t.Context(), "is-p1", "ip")
	if err != nil {
		t.Fatalf("claim state: %v", err)
	}
	if ok {
		t.Fatalf("unclaimed event must report pending (ok=false), got %+v", st)
	}
	if st.Status != "" || st.Attempts != 0 {
		t.Fatalf("pending state must be zero value, got %+v", st)
	}
}

// 租约内的认领行：processing，重试计数为认领时初始值 0。
func testInspectorStateProcessing(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	appendEvent(t, d, "is-a1", typ, "1", nil)
	claim(t, d, typ, "ia", time.Minute, baseNow)

	st := mustClaimState(t, insp, "is-a1", "ia")
	if st.Status != event.ClaimProcessing {
		t.Fatalf("status = %q, want processing", st.Status)
	}
	if st.Attempts != 0 {
		t.Fatalf("attempts = %d, want 0 (claim_count before any fail)", st.Attempts)
	}
}

// 非终态 Fail：retrying，计数递增到 1。
func testInspectorStateRetrying(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	appendEvent(t, d, "is-r1", typ, "1", nil)
	ls := terminalClaim(t, d, typ, "ir")
	if err := d.Fail(t.Context(), event.FailRequest{
		EventID: ls.Envelope.ID, ConsumerType: ls.ConsumerType, LeaseToken: ls.LeaseToken,
		RetryLimit: 5, Backoff: linearBackoffMs(), Now: baseNow,
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}

	st := mustClaimState(t, insp, "is-r1", "ir")
	if st.Status != event.ClaimRetrying {
		t.Fatalf("status = %q, want retrying", st.Status)
	}
	if st.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (one counted fail)", st.Attempts)
	}
}

// 终态 Fail：failed。
func testInspectorStateFailed(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	appendEvent(t, d, "is-f1", typ, "1", nil)
	ls := terminalClaim(t, d, typ, "ifs")
	terminalFail(t, d, ls, "boom", baseNow)

	st := mustClaimState(t, insp, "is-f1", "ifs")
	if st.Status != event.ClaimFailed {
		t.Fatalf("status = %q, want failed", st.Status)
	}
	if st.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (terminal on first fail)", st.Attempts)
	}
}

// Complete：done。
func testInspectorStateDone(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	appendEvent(t, d, "is-d1", typ, "1", nil)
	ls := terminalClaim(t, d, typ, "id")
	if err := d.Complete(t.Context(), event.CompleteRequest{
		EventID: ls.Envelope.ID, ConsumerType: ls.ConsumerType,
		LeaseToken: ls.LeaseToken, Now: baseNow,
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	st := mustClaimState(t, insp, "is-d1", "id")
	if st.Status != event.ClaimDone {
		t.Fatalf("status = %q, want done", st.Status)
	}
	if st.Attempts != 0 {
		t.Fatalf("attempts = %d, want 0 (completed without fail)", st.Attempts)
	}
}

// 租约过期被接管：接管认领计入重试计数（ADR 0006），行回到 processing。
func testInspectorStateTakeover(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	appendEvent(t, d, "is-t1", typ, "1", nil)
	claim(t, d, typ, "it", time.Minute, baseNow)
	ls2 := claim(t, d, typ, "it", time.Minute, baseNow.Add(2*time.Minute))
	if len(ls2) != 1 {
		t.Fatalf("takeover claim: %+v", ls2)
	}

	st := mustClaimState(t, insp, "is-t1", "it")
	if st.Status != event.ClaimProcessing {
		t.Fatalf("status = %q, want processing after takeover", st.Status)
	}
	if st.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (takeover counts one fail)", st.Attempts)
	}
}

// 身份隔离：事件已被身份 A 认领，身份 B 查询仍无认领行（待处理）。
func testInspectorStateForeignIdentity(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	appendEvent(t, d, "is-x1", typ, "1", nil)
	claim(t, d, typ, "ix", time.Minute, baseNow)

	_, ok, err := insp.ClaimState(t.Context(), "is-x1", "iy")
	if err != nil {
		t.Fatalf("claim state: %v", err)
	}
	if ok {
		t.Fatal("foreign identity must see no claim row (pending)")
	}
}

// 不存在的事件：无认领行，ok=false（与待处理同口径），非错误。
func testInspectorStateUnknownEvent(t *testing.T, d event.Driver, insp event.Inspector) {
	_, ok, err := insp.ClaimState(t.Context(), "is-none", "iz")
	if err != nil {
		t.Fatalf("claim state for unknown event must not error: %v", err)
	}
	if ok {
		t.Fatal("unknown event must report no claim row")
	}
}

// ---- 状态分档计数 ----

// claimLimit 以指定限额认领（position 升序取前 limit 条）。
func claimLimit(t *testing.T, d event.Driver, typ, consumerType string, limit int, now time.Time) []event.Lease {
	t.Helper()
	res, err := d.Claim(t.Context(), event.ClaimQuery{
		Type: typ, ConsumerType: consumerType, Lease: time.Minute, Limit: limit,
		RetryLimit: 5, Now: now,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return res.Leases
}

// leaseByID 从认领批次中取出指定事件的租约（缺失即装配缺陷，立即失败）。
func leaseByID(t *testing.T, ls []event.Lease, eventID string) event.Lease {
	t.Helper()
	for _, l := range ls {
		if l.Envelope.ID == eventID {
			return l
		}
	}
	t.Fatalf("lease for %s not granted", eventID)
	return event.Lease{}
}

// mixedFixture 五档混合事实的落库装配结果。事件 ID 以类型名前缀保证
// 跨用例全局唯一（事件 ID 全局唯一，跨类型复用同 ID 会被首写吞掉）。
type mixedFixture struct {
	Processing []string // 租约内 processing（3 条）
	Retrying   string   // retrying（1 条）
	Failed     string   // failed 终态（1 条）
	Done       string   // done 终态（1 条）
	Pending    []string // 无认领行（2 条）
}

// inspectMixedFixture 构造五档齐全的事件类型（身份 im）：3 条 processing
// （租约内）、1 条 retrying、1 条 failed、1 条 done、2 条 pending（无
// 认领行）。
func inspectMixedFixture(t *testing.T, d event.Driver, typ string) mixedFixture {
	t.Helper()
	fx := mixedFixture{
		Processing: []string{typ + "-im-1", typ + "-im-2", typ + "-im-3"},
		Retrying:   typ + "-im-4",
		Failed:     typ + "-im-5",
		Done:       typ + "-im-6",
		Pending:    []string{typ + "-im-7", typ + "-im-8"},
	}
	all := append(append([]string{}, fx.Processing...), fx.Retrying, fx.Failed, fx.Done)
	all = append(all, fx.Pending...)
	for _, id := range all {
		appendEvent(t, d, id, typ, id, nil)
	}
	ls := claimLimit(t, d, typ, "im", 6, baseNow) // position 升序认领前 6 条
	if len(ls) != 6 {
		t.Fatalf("claim: got %d leases, want 6", len(ls))
	}
	lease := func(eventID string) event.Lease {
		t.Helper()
		return leaseByID(t, ls, eventID)
	}
	if err := d.Fail(t.Context(), event.FailRequest{
		EventID: fx.Retrying, ConsumerType: "im", LeaseToken: lease(fx.Retrying).LeaseToken,
		RetryLimit: 5, Backoff: linearBackoffMs(), Now: baseNow,
	}); err != nil {
		t.Fatalf("fail retrying: %v", err)
	}
	terminalFail(t, d, lease(fx.Failed), "boom", baseNow)
	if err := d.Complete(t.Context(), event.CompleteRequest{
		EventID: fx.Done, ConsumerType: "im",
		LeaseToken: lease(fx.Done).LeaseToken, Now: baseNow,
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	return fx
}

// 全部事件未认领：全档归 Pending（「待处理」= 无认领行）。
func testInspectorCountAllPending(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	for _, id := range []string{"ic-p1", "ic-p2", "ic-p3"} {
		appendEvent(t, d, id, typ, id, nil)
	}

	c, err := insp.CountStates(t.Context(), typ, "ic")
	if err != nil {
		t.Fatalf("count states: %v", err)
	}
	if want := (event.StateCounts{Pending: 3}); c != want {
		t.Fatalf("counts = %+v, want %+v", c, want)
	}
}

// 五档混合事实（superseded 档为空）与 Claim/Fail/Complete 落库事实逐档
// 一致；六档互斥且合计等于该类型事件总数（本用例 superseded=0）。
func testInspectorCountMixed(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	inspectMixedFixture(t, d, typ)

	c, err := insp.CountStates(t.Context(), typ, "im")
	if err != nil {
		t.Fatalf("count states: %v", err)
	}
	want := event.StateCounts{
		Pending: 2, Retrying: 1, Processing: 3, Failed: 1, Done: 1,
	}
	if c != want {
		t.Fatalf("counts = %+v, want %+v (mixed claim facts)", c, want)
	}
}

// 全部完成：done 单独归档，不计入其余五档（done 归档口径）。
func testInspectorCountDoneArchived(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	appendEvent(t, d, "ic-d1", typ, "1", nil)
	appendEvent(t, d, "ic-d2", typ, "2", nil)
	ls := claim(t, d, typ, "icd", time.Minute, baseNow)
	if len(ls) != 2 {
		t.Fatalf("claim: %+v", ls)
	}
	for _, l := range ls {
		if err := d.Complete(t.Context(), event.CompleteRequest{
			EventID: l.Envelope.ID, ConsumerType: l.ConsumerType,
			LeaseToken: l.LeaseToken, Now: baseNow,
		}); err != nil {
			t.Fatalf("complete: %v", err)
		}
	}

	c, err := insp.CountStates(t.Context(), typ, "icd")
	if err != nil {
		t.Fatalf("count states: %v", err)
	}
	if want := (event.StateCounts{Done: 2}); c != want {
		t.Fatalf("counts = %+v, want %+v (done archived)", c, want)
	}
}

// 身份隔离：其他身份的认领事实不影响本身份计数——未认领身份全档 Pending。
func testInspectorCountForeignIdentity(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	inspectMixedFixture(t, d, typ)

	c, err := insp.CountStates(t.Context(), typ, "foreign")
	if err != nil {
		t.Fatalf("count states: %v", err)
	}
	if want := (event.StateCounts{Pending: 8}); c != want {
		t.Fatalf("counts = %+v, want %+v (foreign identity sees all pending)", c, want)
	}
}

// 未知事件类型：零计数、无错误。
func testInspectorCountUnknownType(t *testing.T, d event.Driver, insp event.Inspector) {
	c, err := insp.CountStates(t.Context(), t.Name()+"-none", "icn")
	if err != nil {
		t.Fatalf("count states: %v", err)
	}
	if want := (event.StateCounts{}); c != want {
		t.Fatalf("counts = %+v, want zero value", c)
	}
}

// ---- failed 分页列表 ----

// failedList 按条件取列表；查询失败立即终止。
func failedList(t *testing.T, insp event.Inspector, q event.FailedQuery) []event.FailedEvent {
	t.Helper()
	out, err := insp.FailedEvents(t.Context(), q)
	if err != nil {
		t.Fatalf("failed events: %v", err)
	}
	return out
}

// 无 failed 事实：空列表、无错误。
func testInspectorFailedEmpty(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	appendEvent(t, d, "if-e1", typ, "1", nil)
	claim(t, d, typ, "ine", time.Minute, baseNow)

	out := failedList(t, insp, event.FailedQuery{Type: typ, ConsumerType: "ine", Limit: 10})
	if len(out) != 0 {
		t.Fatalf("failed events = %+v, want empty", out)
	}
}

// 终态 Fail 的摘要与终态时刻随列表返回（timestamptz 微秒精度，双侧
// 截断比较）。
func testInspectorFailedCarriesFacts(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	appendEvent(t, d, "if-1", typ, "1", nil)
	ls := terminalClaim(t, d, typ, "ifs")
	failAt := baseNow.Add(30 * time.Second)
	terminalFail(t, d, ls, "boom: order rejected", failAt)

	out := failedList(t, insp, event.FailedQuery{Type: typ, ConsumerType: "ifs", Limit: 10})
	if len(out) != 1 {
		t.Fatalf("failed events len = %d, want 1", len(out))
	}
	f := out[0]
	if f.EventID != "if-1" || f.ConsumerType != "ifs" {
		t.Fatalf("claim key = (%s, %s), want (if-1, ifs)", f.EventID, f.ConsumerType)
	}
	if f.LastError != "boom: order rejected" {
		t.Fatalf("last_error = %q, want %q", f.LastError, "boom: order rejected")
	}
	if !f.FailedAt.Truncate(time.Microsecond).Equal(failAt.Truncate(time.Microsecond)) {
		t.Fatalf("failed_at = %v, want %v", f.FailedAt, failAt)
	}
}

// 接管终态只写 failed_at、不写摘要（ADR 0009 口径随列表可见）。
func testInspectorFailedTakeoverFacts(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	appendEvent(t, d, "if-v1", typ, "1", nil)
	// 接管计数达 RetryLimit=2 的认领直接置 failed：第 1 次认领（count=0）、
	// 第 2 次接管认领（count=1，发放）、第 3 次接管认领（count=2 达上限）。
	doClaim := func(now time.Time) event.ClaimResult {
		t.Helper()
		res, err := d.Claim(t.Context(), event.ClaimQuery{
			Type: typ, ConsumerType: "iv", Lease: time.Minute, Limit: 10,
			RetryLimit: 2, Now: now,
		})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		return res
	}
	if res := doClaim(baseNow); len(res.Leases) != 1 {
		t.Fatalf("first claim: %+v", res)
	}
	if res := doClaim(baseNow.Add(2 * time.Minute)); len(res.Leases) != 1 {
		t.Fatalf("second claim (within limit): %+v", res)
	}
	terminalAt := baseNow.Add(4 * time.Minute)
	if res := doClaim(terminalAt); len(res.Leases) != 0 {
		t.Fatalf("over-limit takeover must not hand out lease, got %+v", res)
	}

	out := failedList(t, insp, event.FailedQuery{Type: typ, ConsumerType: "iv", Limit: 10})
	if len(out) != 1 {
		t.Fatalf("failed events len = %d, want 1", len(out))
	}
	if out[0].LastError != "" {
		t.Fatalf("takeover terminal has no handler error, got summary %q", out[0].LastError)
	}
	if !out[0].FailedAt.Truncate(time.Microsecond).Equal(terminalAt.Truncate(time.Microsecond)) {
		t.Fatalf("failed_at = %v, want %v (the terminal claim's clock)", out[0].FailedAt, terminalAt)
	}
}

// 不同终态时刻：按 failed_at 降序（最新在前）。
func testInspectorFailedNewestFirst(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	for i, id := range []string{"if-o1", "if-o2", "if-o3"} {
		appendEvent(t, d, id, typ, id, nil)
		ls := terminalClaim(t, d, typ, "ifo")
		terminalFail(t, d, ls, "boom "+id, baseNow.Add(time.Duration(i+1)*time.Minute))
	}

	out := failedList(t, insp, event.FailedQuery{Type: typ, ConsumerType: "ifo", Limit: 10})
	if got := failedIDs(out); got[0] != "if-o3" || got[1] != "if-o2" || got[2] != "if-o1" {
		t.Fatalf("order = %v, want [if-o3 if-o2 if-o1] (failed_at desc)", got)
	}
}

// 同一终态时刻：按事件 ID 升序保证确定性。
func testInspectorFailedTieBreak(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	for _, id := range []string{"if-t2", "if-t1"} {
		appendEvent(t, d, id, typ, id, nil)
		ls := terminalClaim(t, d, typ, "ift")
		terminalFail(t, d, ls, "boom", baseNow.Add(5*time.Minute)) // 同刻失败
	}

	out := failedList(t, insp, event.FailedQuery{Type: typ, ConsumerType: "ift", Limit: 10})
	if got := failedIDs(out); got[0] != "if-t1" || got[1] != "if-t2" {
		t.Fatalf("order = %v, want [if-t1 if-t2] (event id asc tie-break)", got)
	}
}

// 非 failed 状态（pending / processing / retrying / done）不进列表。
func testInspectorFailedExcludesOtherStates(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	fx := inspectMixedFixture(t, d, typ)

	out := failedList(t, insp, event.FailedQuery{Type: typ, ConsumerType: "im", Limit: 10})
	if ids := failedIDs(out); len(ids) != 1 || ids[0] != fx.Failed {
		t.Fatalf("failed events = %v, want [%s] only", ids, fx.Failed)
	}
}

// 身份过滤：只列出匹配身份的 failed 行。
func testInspectorFailedIdentityFilter(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	fx := inspectMixedFixture(t, d, typ)

	out := failedList(t, insp, event.FailedQuery{Type: typ, ConsumerType: "foreign", Limit: 10})
	if len(out) != 0 {
		t.Fatalf("foreign identity filter = %v, want empty", failedIDs(out))
	}
	out = failedList(t, insp, event.FailedQuery{Type: typ, ConsumerType: "im", Limit: 10})
	if ids := failedIDs(out); len(ids) != 1 || ids[0] != fx.Failed {
		t.Fatalf("matched identity filter = %v, want [%s]", ids, fx.Failed)
	}
}

// 类型过滤：只列出匹配类型的 failed 行；空类型过滤（限定本用例身份）
// 列出两类型的行，同刻按事件 ID 升序。
func testInspectorFailedTypeFilter(t *testing.T, d event.Driver, insp event.Inspector) {
	typA := t.Name() + "-a"
	typB := t.Name() + "-b"
	for _, id := range []string{"if-ta", "if-tb"} {
		typ := typA
		if id == "if-tb" {
			typ = typB
		}
		appendEvent(t, d, id, typ, id, nil)
		ls := terminalClaim(t, d, typ, "ifl")
		terminalFail(t, d, ls, "boom", baseNow.Add(5*time.Minute))
	}

	out := failedList(t, insp, event.FailedQuery{Type: typA, Limit: 10})
	if ids := failedIDs(out); len(ids) != 1 || ids[0] != "if-ta" {
		t.Fatalf("type filter = %v, want [if-ta]", ids)
	}
	out = failedList(t, insp, event.FailedQuery{ConsumerType: "ifl", Limit: 10})
	if ids := failedIDs(out); len(ids) != 2 || ids[0] != "if-ta" || ids[1] != "if-tb" {
		t.Fatalf("unfiltered by type = %v, want [if-ta if-tb]", ids)
	}
}

// failedIDs 提取列表的事件 ID 序列（排序断言用）。
func failedIDs(out []event.FailedEvent) []string {
	ids := make([]string, len(out))
	for i, f := range out {
		ids[i] = f.EventID
	}
	return ids
}

// inspectPageFixture 构造 3 条 failed 事件（身份 pg）：分别在 +1m / +2m /
// +3m 进入终态，返回 failed_at 降序的事件 ID（[最新, …, 最旧]）。事件 ID
// 以类型名前缀保证跨用例全局唯一。
func inspectPageFixture(t *testing.T, d event.Driver, typ string) []string {
	t.Helper()
	newestFirst := make([]string, 0, 3)
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("%s-pg-%d", typ, i)
		appendEvent(t, d, id, typ, id, nil)
		ls := terminalClaim(t, d, typ, "pg")
		terminalFail(t, d, ls, "boom", baseNow.Add(time.Duration(i)*time.Minute))
		newestFirst = append([]string{id}, newestFirst...) // 后失败的排前
	}
	return newestFirst
}

// 首页：Offset=0 返回排序后的前 Limit 条。
func testInspectorFailedFirstPage(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	want := inspectPageFixture(t, d, typ)

	out := failedList(t, insp, event.FailedQuery{Type: typ, ConsumerType: "pg", Offset: 0, Limit: 2})
	if ids := failedIDs(out); len(ids) != 2 || ids[0] != want[0] || ids[1] != want[1] {
		t.Fatalf("first page = %v, want head %v", ids, want[:2])
	}
}

// 末页：剩余不足 Limit 条时返回剩余条（可少于 Limit）。
func testInspectorFailedLastPage(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	want := inspectPageFixture(t, d, typ)

	out := failedList(t, insp, event.FailedQuery{Type: typ, ConsumerType: "pg", Offset: 2, Limit: 2})
	if ids := failedIDs(out); len(ids) != 1 || ids[0] != want[2] {
		t.Fatalf("last page = %v, want remainder [%s] (fewer than limit)", ids, want[2])
	}
}

// 越界：Offset 不小于结果总数时返回空列表（非错误）。
func testInspectorFailedOffsetBeyond(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	inspectPageFixture(t, d, typ)

	for _, offset := range []int{3, 100} {
		out := failedList(t, insp, event.FailedQuery{Type: typ, ConsumerType: "pg", Offset: offset, Limit: 2})
		if len(out) != 0 {
			t.Fatalf("offset %d beyond total = %v, want empty", offset, failedIDs(out))
		}
	}
}

// Limit <= 0：返回空列表且不产生错误。
func testInspectorFailedZeroLimit(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	inspectPageFixture(t, d, typ)

	for _, limit := range []int{0, -1} {
		out := failedList(t, insp, event.FailedQuery{Type: typ, ConsumerType: "pg", Limit: limit})
		if len(out) != 0 {
			t.Fatalf("limit %d = %v, want empty", limit, failedIDs(out))
		}
	}
}

// 负 Offset 视为 0（首页）。
func testInspectorFailedNegativeOffset(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	want := inspectPageFixture(t, d, typ)

	out := failedList(t, insp, event.FailedQuery{Type: typ, ConsumerType: "pg", Offset: -5, Limit: 2})
	if ids := failedIDs(out); len(ids) != 2 || ids[0] != want[0] || ids[1] != want[1] {
		t.Fatalf("negative offset = %v, want first page %v", ids, want[:2])
	}
}

// 只读边界：三类查询对活跃租约行不产生任何簿记——重复查询后落库事实
// 不变（Inspector 不影响 Driver 行为）。
func testInspectorReadOnly(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	appendEvent(t, d, "ir-1", typ, "1", nil)
	claim(t, d, typ, "iro", time.Minute, baseNow)

	// 连续执行三类查询各一次。
	if _, _, err := insp.ClaimState(t.Context(), "ir-1", "iro"); err != nil {
		t.Fatalf("claim state: %v", err)
	}
	if _, err := insp.CountStates(t.Context(), typ, "iro"); err != nil {
		t.Fatalf("count states: %v", err)
	}
	if _, err := insp.FailedEvents(t.Context(), event.FailedQuery{Type: typ, ConsumerType: "iro", Limit: 10}); err != nil {
		t.Fatalf("failed events: %v", err)
	}

	// 查询后落库事实不变：行仍为租约内 processing、计数仍为 processing 1。
	st := mustClaimState(t, insp, "ir-1", "iro")
	if st.Status != event.ClaimProcessing || st.Attempts != 0 {
		t.Fatalf("state after queries = %+v, want processing/0 (read-only)", st)
	}
	c, err := insp.CountStates(t.Context(), typ, "iro")
	if err != nil {
		t.Fatalf("count states: %v", err)
	}
	if want := (event.StateCounts{Processing: 1}); c != want {
		t.Fatalf("counts after queries = %+v, want %+v (read-only)", c, want)
	}
}
