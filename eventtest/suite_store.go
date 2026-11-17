package eventtest

import (
	"testing"
	"time"

	"github.com/jninng/go_event"
)

// Store 组合对象契约用例（便利构造，非能力面）：钉住「三面同源、行为与
// 独立构造等价」。三面的完整语义已分别由 RunSuite / RunInspectorSuite /
// RunAdminReplaySuite 约束，本套件不重复——只验证同一组合对象的三面在
// 同一连接（或同一份内存状态）与命名空间下互通：Store.Driver 的落库
// 事实对 Store.Inspector 可见、Store.Admin 的重放对 Store.Driver 生效
// （重放行可被再次认领并完成）。

// RunStoreSuite 运行组合对象三面同源契约测试。d / insp / admin 须为同
// 一 Store 的三面（后端 NewStore 构造传入）；三面缺失沿 RunInspectorSuite
// / RunAdminReplaySuite 先例 Fatal 终止——提供组合对象的后端必须全过。
func RunStoreSuite(t *testing.T, d event.Driver, insp event.Inspector, admin event.Admin) {
	if d == nil {
		t.Fatalf("driver is nil: a store must hold all three surfaces " +
			"(driver / inspector / admin)")
	}
	if insp == nil {
		t.Fatalf("inspector is nil: a store must hold all three surfaces " +
			"(driver / inspector / admin)")
	}
	if admin == nil {
		t.Fatalf("admin is nil: a store must hold all three surfaces " +
			"(driver / inspector / admin)")
	}

	t.Run("StoreDriverClaim_StoreInspector_StateMatchesFacts", func(t *testing.T) { testStoreClaimFactsVisible(t, d, insp) })
	t.Run("StoreDriverTerminalFail_StoreInspector_FailedListCarriesSummary", func(t *testing.T) { testStoreFailedList(t, d, insp) })
	t.Run("StoreAdminReplay_StoreDriver_ReclaimToCompletion", func(t *testing.T) { testStoreReplayReclaim(t, d, insp, admin) })
}

// Store.Driver 认领的落库事实对 Store.Inspector 可见：状态与计数一致。
func testStoreClaimFactsVisible(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	appendEvent(t, d, "st-c1", typ, "1", nil)
	ls := claim(t, d, typ, "sc", time.Minute, baseNow)
	if len(ls) != 1 {
		t.Fatalf("claim: %+v", ls)
	}

	st := mustClaimState(t, insp, "st-c1", "sc")
	if st.Status != event.ClaimProcessing {
		t.Fatalf("status = %q, want processing", st.Status)
	}
	if st.Attempts != 0 {
		t.Fatalf("attempts = %d, want 0 (claim_count before any fail)", st.Attempts)
	}
}

// Store.Driver 的终态 Fail 进入 Store.Inspector 的 failed 列表：摘要与
// 终态时刻随行返回（timestamptz 微秒精度，双侧截断比较）。
func testStoreFailedList(t *testing.T, d event.Driver, insp event.Inspector) {
	typ := t.Name()
	appendEvent(t, d, "st-f1", typ, "1", nil)
	ls := terminalClaim(t, d, typ, "sf")
	failAt := baseNow.Add(30 * time.Second)
	terminalFail(t, d, ls, "store boom: order rejected", failAt)

	failed, err := insp.FailedEvents(t.Context(), event.FailedQuery{
		Type: typ, ConsumerType: "sf", Limit: 10,
	})
	if err != nil {
		t.Fatalf("failed events: %v", err)
	}
	if len(failed) != 1 {
		t.Fatalf("failed events len = %d, want 1", len(failed))
	}
	if failed[0].EventID != "st-f1" {
		t.Fatalf("event id = %q, want st-f1", failed[0].EventID)
	}
	if failed[0].LastError != "store boom: order rejected" {
		t.Fatalf("last error = %q, want the fail summary", failed[0].LastError)
	}
	if !failed[0].FailedAt.Truncate(time.Microsecond).Equal(failAt.Truncate(time.Microsecond)) {
		t.Fatalf("failed_at = %v, want %v", failed[0].FailedAt, failAt)
	}
}

// Store.Admin 的重放对 Store.Driver 生效：failed → 重放（Inspector 报
// retrying、计数归零）→ 再次认领 → 完成（Inspector 报 done）。
func testStoreReplayReclaim(t *testing.T, d event.Driver, insp event.Inspector, admin event.Admin) {
	typ := t.Name()
	appendEvent(t, d, "st-r1", typ, "1", nil)
	ls := terminalClaim(t, d, typ, "sr")
	terminalFail(t, d, ls, "store boom", baseNow)

	replayAt := baseNow.Add(time.Minute)
	if !replay(t, admin, "st-r1", "sr", replayAt) {
		t.Fatal("failed row must be replayable via store admin, got ok=false")
	}

	st := mustClaimState(t, insp, "st-r1", "sr")
	if st.Status != event.ClaimRetrying {
		t.Fatalf("status = %q, want retrying after replay", st.Status)
	}
	if st.Attempts != 0 {
		t.Fatalf("attempts = %d, want 0 (claim_count reset by replay)", st.Attempts)
	}

	reclaimed := claim(t, d, typ, "sr", time.Minute, replayAt)
	if len(reclaimed) != 1 {
		t.Fatalf("claim after replay: %+v", reclaimed)
	}
	if reclaimed[0].Attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (claim_count reset to 0 by replay)", reclaimed[0].Attempts)
	}
	if err := d.Complete(t.Context(), event.CompleteRequest{
		EventID: "st-r1", ConsumerType: "sr", LeaseToken: reclaimed[0].LeaseToken, Now: replayAt,
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	st = mustClaimState(t, insp, "st-r1", "sr")
	if st.Status != event.ClaimDone {
		t.Fatalf("status = %q, want done (replayed event consumed to completion)", st.Status)
	}
}
