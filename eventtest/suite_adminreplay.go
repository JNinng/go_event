package eventtest

import (
	"sync"
	"testing"
	"time"

	"github.com/jninng/go_event"
)

// Admin 管理面重放契约用例（design.md §5.3/§9.2、ADR 0010）：failed 终态
// 的手工重放由独立写面 Admin 承载（不进 Driver、不挂 Driver 类型）。
//   - 仅 failed 可重放：重放后可被再次认领并完成（端到端）；claim_count
//     归零（再 Claim 的 Attempts 语义合法）；再 Fail 到上限进入 failed 后
//     可再次重放（循环）。
//   - 非 failed 一律无效果且判别统一（ok=false）：done 不可逆、active
//     拒绝且行不变（原租约凭证仍有效）、retrying 拒绝且退避窗口不变、
//     未知事件 / 身份同判别；被重新认领后迟到的重放不污染新属主行。
//   - 重放落库事实（ADR 0009 口径）：last_error 保留、failed_at 清空；
//     经 ClaimStateReader 断言。
//   - 并发防护：单条条件更新（CAS，仅命中 status='failed'）——并发重放
//     恰好一次生效。
//
// 重放窗口 = 保留窗口：超窗行被清理后与「行不存在」同判别（清理资格已由
// RunRetentionSuite 契约约束，此处不重复）。

// RunAdminReplaySuite 运行管理面 Admin 重放契约测试。admin 须非空——
// 管理写路径不经 Driver 类型暴露，只能由后端 NewAdmin 构造获得（缺失即
// Fatal，沿 ClaimStateReader/RunInspectorSuite 先例：提供该能力的后端
// 必须全过本套件）。d 须实现 ClaimStateReader（重放事实断言的数据源）。
func RunAdminReplaySuite(t *testing.T, d event.Driver, admin event.Admin) {
	if admin == nil {
		t.Fatalf("admin is nil: the management write surface is a separate optional " +
			"capability constructed via the backend's NewAdmin, never on Driver (design.md §6)")
	}
	rd, ok := d.(ClaimStateReader)
	if !ok {
		t.Fatalf("driver %T must implement ClaimStateReader: the replay facts "+
			"(last_error preserved, failed_at cleared) are asserted against claim-row facts", d)
	}

	// ---- 重放后回到正常消费路径 ----
	t.Run("TerminalFailedRow_AdminReplay_ReclaimableAndCompletable", func(t *testing.T) { testAdminReplayEndToEnd(t, d, admin, rd) })
	t.Run("ReplayedRow_Claim_AttemptsRestartFromOne", func(t *testing.T) { testAdminReplayAttemptsRestart(t, d, admin) })
	t.Run("ReplayedRow_FailToLimit_TerminalAgainAndReplayable", func(t *testing.T) { testAdminReplayFailCycle(t, d, admin, rd) })

	// ---- 仅 failed 可重放：非 failed 行无效果且判别统一 ----
	t.Run("DoneRow_AdminReplay_NoEffectRowUnchanged", func(t *testing.T) { testAdminReplayDoneNoEffect(t, d, admin, rd) })
	t.Run("ActiveLeaseRow_AdminReplay_RejectedRowUnchanged", func(t *testing.T) { testAdminReplayActiveNoEffect(t, d, admin, rd) })
	t.Run("RetryingRow_AdminReplay_NoEffectBackoffUnchanged", func(t *testing.T) { testAdminReplayRetryingNoEffect(t, d, admin, rd) })
	t.Run("UnknownEventOrIdentity_AdminReplay_NoEffectSameDiscriminant", func(t *testing.T) { testAdminReplayUnknownNoEffect(t, d, admin) })
	t.Run("TakenOverRow_StaleReplay_NoEffectOnNewOwner", func(t *testing.T) { testAdminReplayStaleAfterTakeover(t, d, admin, rd) })

	// ---- 重放落库事实（ADR 0009：清 failed_at、保留 last_error）----
	t.Run("TerminalFailedRow_AdminReplay_KeepsLastErrorClearsFailedAt", func(t *testing.T) { testAdminReplayKeepsLastErrorClearsFailedAt(t, d, admin, rd) })

	// ---- 并发防护（CAS：仅命中 status='failed'）----
	t.Run("ConcurrentReplays_FailedRow_ExactlyOneTakesEffect", func(t *testing.T) { testAdminReplayConcurrentExactlyOne(t, d, admin) })
}

// replay 重放并断言无执行故障：执行故障不得折叠为「无效果」，立即失败于
// 断言。返回 ok（true = 行原为 failed 且已重放）。
func replay(t *testing.T, admin event.Admin, eventID, consumerType string, now time.Time) bool {
	t.Helper()
	ok, err := admin.ReplayFailed(t.Context(), eventID, consumerType, now)
	if err != nil {
		t.Fatalf("replay (%s, %s): %v", eventID, consumerType, err)
	}
	return ok
}

// adminFailedFixture 构造一条 failed 终态行（终态 Fail 携带错误摘要）。
func adminFailedFixture(t *testing.T, d event.Driver, id, typ, consumerType string) {
	t.Helper()
	appendEvent(t, d, id, typ, "1", nil)
	ls := terminalClaim(t, d, typ, consumerType)
	terminalFail(t, d, ls, "boom: order rejected", baseNow)
}

// 端到端：failed → 重放 → 再次认领 → 完成。重放后事件回到正常消费路径。
func testAdminReplayEndToEnd(t *testing.T, d event.Driver, admin event.Admin, rd ClaimStateReader) {
	typ := t.Name()
	adminFailedFixture(t, d, "ar-e1", typ, "are")

	replayAt := baseNow.Add(time.Minute)
	if !replay(t, admin, "ar-e1", "are", replayAt) {
		t.Fatal("failed row must be replayable, got ok=false")
	}
	ls := claim(t, d, typ, "are", time.Minute, replayAt)
	if len(ls) != 1 {
		t.Fatalf("claim after replay: %+v", ls)
	}
	if err := d.Complete(t.Context(), event.CompleteRequest{
		EventID: "ar-e1", ConsumerType: "are", LeaseToken: ls[0].LeaseToken, Now: replayAt,
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	st := readClaimState(t, rd, "ar-e1", "are")
	if st.Status != "done" {
		t.Fatalf("status = %q, want done (replayed event consumed to completion)", st.Status)
	}
}

// 重放归零重试计数的行为证据：重放后认领的 Attempts = claim_count + 1 = 1
// （若计数未归零，failed 行此前已计入 1，Attempts 至少为 2）。
func testAdminReplayAttemptsRestart(t *testing.T, d event.Driver, admin event.Admin) {
	typ := t.Name()
	adminFailedFixture(t, d, "ar-a1", typ, "ara")

	replayAt := baseNow.Add(time.Minute)
	if !replay(t, admin, "ar-a1", "ara", replayAt) {
		t.Fatal("failed row must be replayable, got ok=false")
	}
	ls := claim(t, d, typ, "ara", time.Minute, replayAt)
	if len(ls) != 1 {
		t.Fatalf("claim after replay: %+v", ls)
	}
	if ls[0].Attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (claim_count reset to 0 by replay)", ls[0].Attempts)
	}
}

// 循环：重放后再 Fail 到上限重新进入 failed，可再次重放、计数再次归零
// （RetryLimit 语义在重放后从合法范围重新开始）。
func testAdminReplayFailCycle(t *testing.T, d event.Driver, admin event.Admin, rd ClaimStateReader) {
	typ := t.Name()
	adminFailedFixture(t, d, "ar-c1", typ, "arc") // RetryLimit=1 终态，claim_count=1

	replayAt := baseNow.Add(time.Minute)
	if !replay(t, admin, "ar-c1", "arc", replayAt) {
		t.Fatal("failed row must be replayable, got ok=false")
	}

	// 重放后重试计数从 0 重新开始（RetryLimit=2）：两次 Fail 才回到终态。
	ls := claim(t, d, typ, "arc", time.Minute, replayAt)
	if len(ls) != 1 || ls[0].Attempts != 1 {
		t.Fatalf("claim after replay = %+v, want single lease with Attempts 1", ls)
	}
	if err := d.Fail(t.Context(), event.FailRequest{
		EventID: "ar-c1", ConsumerType: "arc", LeaseToken: ls[0].LeaseToken,
		Error: "boom again", RetryLimit: 2, Backoff: linearBackoffMs(), Now: replayAt,
	}); err != nil {
		t.Fatalf("fail 1: %v", err)
	}
	ls = claim(t, d, typ, "arc", time.Minute, replayAt.Add(time.Minute))
	if len(ls) != 1 || ls[0].Attempts != 2 {
		t.Fatalf("claim after first fail = %+v, want single lease with Attempts 2", ls)
	}
	if err := d.Fail(t.Context(), event.FailRequest{
		EventID: "ar-c1", ConsumerType: "arc", LeaseToken: ls[0].LeaseToken,
		Error: "boom again", RetryLimit: 2, Backoff: linearBackoffMs(), Now: replayAt.Add(time.Minute),
	}); err != nil {
		t.Fatalf("fail 2: %v", err)
	}
	st := readClaimState(t, rd, "ar-c1", "arc")
	if st.Status != "failed" {
		t.Fatalf("status = %q, want failed (count restarted from 0 reached limit again)", st.Status)
	}

	// 再次重放：循环可重复，计数再次归零。
	secondReplayAt := replayAt.Add(2 * time.Minute)
	if !replay(t, admin, "ar-c1", "arc", secondReplayAt) {
		t.Fatal("re-failed row must be replayable again, got ok=false")
	}
	ls = claim(t, d, typ, "arc", time.Minute, secondReplayAt)
	if len(ls) != 1 || ls[0].Attempts != 1 {
		t.Fatalf("claim after second replay = %+v, want single lease with Attempts 1", ls)
	}
}

// done 不可逆：重放 done 行无效果（ok=false，与其他非 failed 行判别
// 统一），行保持 done、不回到可认领路径。
func testAdminReplayDoneNoEffect(t *testing.T, d event.Driver, admin event.Admin, rd ClaimStateReader) {
	typ := t.Name()
	appendEvent(t, d, "ar-d1", typ, "1", nil)
	ls := terminalClaim(t, d, typ, "ard")
	if err := d.Complete(t.Context(), event.CompleteRequest{
		EventID: "ar-d1", ConsumerType: "ard", LeaseToken: ls.LeaseToken, Now: baseNow,
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	if replay(t, admin, "ar-d1", "ard", baseNow.Add(time.Minute)) {
		t.Fatal("done row must not be replayable, got ok=true")
	}
	st := readClaimState(t, rd, "ar-d1", "ard")
	if st.Status != "done" {
		t.Fatalf("status = %q, want done (irreversible)", st.Status)
	}
	if ls := claim(t, d, typ, "ard", time.Minute, baseNow.Add(time.Minute)); len(ls) != 0 {
		t.Fatalf("done row must stay unclaimable after replay, got %+v", ls)
	}
}

// active（租约活跃）行重放拒绝且行不变：租约期内（重放时刻早于
// lease_until）拒绝；租约届满时刻（lease_until <= now，认领接管判据已
// 满足）行仍为 active、尚未被接管，同样拒绝——拒绝只看 status，不区分
// 租约是否届满。行不变的行为证据：原租约凭证仍属主有效（Complete 照常
// 生效即为凭证未被重放破坏）。
func testAdminReplayActiveNoEffect(t *testing.T, d event.Driver, admin event.Admin, rd ClaimStateReader) {
	typ := t.Name()
	appendEvent(t, d, "ar-l1", typ, "1", nil)
	ls := terminalClaim(t, d, typ, "arl") // 租约 [baseNow, baseNow+1min)

	// 租约期内重放：拒绝且行不变。
	if replay(t, admin, "ar-l1", "arl", baseNow) {
		t.Fatal("active row must not be replayable within the lease, got ok=true")
	}
	st := readClaimState(t, rd, "ar-l1", "arl")
	if st.Status != "active" {
		t.Fatalf("status = %q, want active (replay must not touch active rows)", st.Status)
	}

	// 租约届满时刻重放：行仍为 active、尚未被接管，同样拒绝且行不变。
	if replay(t, admin, "ar-l1", "arl", baseNow.Add(time.Minute)) {
		t.Fatal("active row must not be replayable at lease expiry, got ok=true")
	}
	st = readClaimState(t, rd, "ar-l1", "arl")
	if st.Status != "active" {
		t.Fatalf("status = %q, want active at lease expiry (replay must not touch active rows)", st.Status)
	}

	// 行不变的行为证据：原租约凭证仍属主有效。
	if err := d.Complete(t.Context(), event.CompleteRequest{
		EventID: "ar-l1", ConsumerType: "arl", LeaseToken: ls.LeaseToken, Now: baseNow.Add(time.Minute),
	}); err != nil {
		t.Fatalf("complete with original token: %v", err)
	}
	st = readClaimState(t, rd, "ar-l1", "arl")
	if st.Status != "done" {
		t.Fatalf("status = %q, want done (original lease token must stay valid)", st.Status)
	}
}

// retrying 行重放拒绝且行不变：自动重试路径仍在正常运转，重放不重置
// 退避——退避窗口内不可认领（若重放误清 next_eligible，此处会发放租约）。
func testAdminReplayRetryingNoEffect(t *testing.T, d event.Driver, admin event.Admin, rd ClaimStateReader) {
	typ := t.Name()
	appendEvent(t, d, "ar-r1", typ, "1", nil)
	ls := terminalClaim(t, d, typ, "arr")
	// 非终态 Fail（RetryLimit=5）：retrying，退避 1 分钟后才能再认领。
	if err := d.Fail(t.Context(), event.FailRequest{
		EventID: "ar-r1", ConsumerType: "arr", LeaseToken: ls.LeaseToken,
		Error: "transient boom", RetryLimit: 5,
		Backoff: event.LinearBackoff(time.Minute, time.Hour), Now: baseNow,
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}

	if replay(t, admin, "ar-r1", "arr", baseNow.Add(30*time.Second)) {
		t.Fatal("retrying row must not be replayable, got ok=true")
	}
	st := readClaimState(t, rd, "ar-r1", "arr")
	if st.Status != "retrying" {
		t.Fatalf("status = %q, want retrying (replay must not touch retrying rows)", st.Status)
	}
	if got := claim(t, d, typ, "arr", time.Minute, baseNow.Add(30*time.Second)); len(got) != 0 {
		t.Fatalf("backoff window must be preserved after rejected replay, got %+v", got)
	}
	// 退避期满后照常可认领：拒绝重放不改变自动重试路径。
	if got := claim(t, d, typ, "arr", time.Minute, baseNow.Add(2*time.Minute)); len(got) != 1 {
		t.Fatalf("retrying row must stay claimable after backoff, got %+v", got)
	}
}

// 未知（事件 × 身份）重放：与 done / active 的拒绝同一判别（ok=false、
// 无错误）——未知事件、已存在事件 × 未认领身份均无效果。
func testAdminReplayUnknownNoEffect(t *testing.T, d event.Driver, admin event.Admin) {
	if replay(t, admin, "ar-none", "arn", baseNow) {
		t.Fatal("unknown event must replay nothing, got ok=true")
	}
	typ := t.Name()
	appendEvent(t, d, "ar-u1", typ, "1", nil)
	if replay(t, admin, "ar-u1", "arn", baseNow) {
		t.Fatal("unclaimed identity must replay nothing, got ok=true")
	}
}

// 被重新认领（接管）后迟到的重放请求不污染新属主行：重放生效后另一实例
// 已认领，随后的重放（基于重放时刻的 failed 事实）无效果、新属主凭证照
// 常完成。
func testAdminReplayStaleAfterTakeover(t *testing.T, d event.Driver, admin event.Admin, rd ClaimStateReader) {
	typ := t.Name()
	adminFailedFixture(t, d, "ar-s1", typ, "ars")

	replayAt := baseNow.Add(time.Minute)
	if !replay(t, admin, "ar-s1", "ars", replayAt) {
		t.Fatal("failed row must be replayable, got ok=false")
	}
	// 另一实例（同身份多实例）重新认领：行回到租约内 active。
	ls := claim(t, d, typ, "ars", time.Minute, replayAt)
	if len(ls) != 1 {
		t.Fatalf("claim after replay: %+v", ls)
	}
	// 迟到的重放：CAS 不命中（行已非 failed），新属主行不被改写。
	if replay(t, admin, "ar-s1", "ars", replayAt) {
		t.Fatal("stale replay on taken-over row must have no effect, got ok=true")
	}
	st := readClaimState(t, rd, "ar-s1", "ars")
	if st.Status != "active" {
		t.Fatalf("status = %q, want active (stale replay must not touch the new owner row)", st.Status)
	}
	if err := d.Complete(t.Context(), event.CompleteRequest{
		EventID: "ar-s1", ConsumerType: "ars", LeaseToken: ls[0].LeaseToken, Now: replayAt,
	}); err != nil {
		t.Fatalf("complete with new owner token: %v", err)
	}
	st = readClaimState(t, rd, "ar-s1", "ars")
	if st.Status != "done" {
		t.Fatalf("status = %q, want done (new owner lease token must stay valid)", st.Status)
	}
}

// 重放落库事实（ADR 0009 口径）：failed_at 清空、last_error 保留（重放
// 行与失败行经 failed_at 区分，摘要留作排障历史）。
func testAdminReplayKeepsLastErrorClearsFailedAt(t *testing.T, d event.Driver, admin event.Admin, rd ClaimStateReader) {
	typ := t.Name()
	adminFailedFixture(t, d, "ar-f1", typ, "arf")
	// 前提：终态事实已落库（摘要与终态时刻俱在）。
	before := readClaimState(t, rd, "ar-f1", "arf")
	if before.Status != "failed" || before.LastError != "boom: order rejected" || before.FailedAt.IsZero() {
		t.Fatalf("fixture state = %+v, want failed with summary and failed_at", before)
	}

	if !replay(t, admin, "ar-f1", "arf", baseNow.Add(time.Minute)) {
		t.Fatal("failed row must be replayable, got ok=false")
	}
	st := readClaimState(t, rd, "ar-f1", "arf")
	if st.Status != "retrying" {
		t.Fatalf("status = %q, want retrying", st.Status)
	}
	if st.LastError != "boom: order rejected" {
		t.Fatalf("last_error = %q, want preserved %q (ADR 0009)", st.LastError, "boom: order rejected")
	}
	if !st.FailedAt.IsZero() {
		t.Fatalf("failed_at = %v, want cleared", st.FailedAt)
	}
}

// 并发防护：并发重放同一 failed 行，单条条件更新（CAS，仅命中
// status='failed'）恰好一次生效，其余无效果；落库事实不受并发影响
// （重放后认领 Attempts = 1）。
func testAdminReplayConcurrentExactlyOne(t *testing.T, d event.Driver, admin event.Admin) {
	typ := t.Name()
	adminFailedFixture(t, d, "ar-p1", typ, "arp")

	const replays = 8
	var (
		mu   sync.Mutex
		wins int
		wg   sync.WaitGroup
	)
	start := make(chan struct{})
	replayAt := baseNow.Add(time.Minute)
	for range replays {
		wg.Go(func() {
			<-start
			ok, err := admin.ReplayFailed(t.Context(), "ar-p1", "arp", replayAt)
			if err != nil {
				t.Errorf("concurrent replay: %v", err)
				return
			}
			if ok {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		})
	}
	close(start)
	wg.Wait()

	if wins != 1 {
		t.Fatalf("concurrent replays = %d succeeded, want exactly 1 (single CAS on status='failed')", wins)
	}
	ls := claim(t, d, typ, "arp", time.Minute, replayAt)
	if len(ls) != 1 || ls[0].Attempts != 1 {
		t.Fatalf("claim after concurrent replays = %+v, want single lease with Attempts 1", ls)
	}
}
