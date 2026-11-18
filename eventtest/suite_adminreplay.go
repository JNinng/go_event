package eventtest

import (
	"context"
	"errors"
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
//   - 互斥（契约约束「所有写操作必须持相应锁」）：写路径持与迁移执行相同
//     的命名空间互斥锁——锁占用期间有界等待并返回 ErrAdminLockTimeout
//     （瞬时）、行不被改写，锁释放后重放生效。
//   - 操作来源记录（观测旁路）：每次重放发射 AdminReplayEvent（ok /
//     no_effect / lock_timeout 结果），事件 × 身份字段与调用一致。
//
// 重放窗口 = 保留窗口：超窗行被清理后与「行不存在」同判别（清理资格已由
// RunRetentionSuite 契约约束，此处不重复）。

// AdminExclusionLock 是互斥验证的注入接口（沿迁移套件 LockObserver 等可选
// 接口先例）：后端测试装配从外部占用与 Admin 写互斥的命名空间锁（与迁移
// 执行同一把锁——Postgres 后端为迁移 advisory 键，inmem 为进程内等价
// 信号量）。互斥是管理面契约约束，缺失即 Fatal。
type AdminExclusionLock interface {
	// HoldExclusionLock 占用互斥锁并返回释放函数（释放须幂等）。
	HoldExclusionLock(t *testing.T) (release func())
}

// AdminObserverSetter 是操作来源记录验证的注入接口：返回挂接观测旁路的
// 等价 Admin 实例（原实例不变更）。观测事件是管理面契约约束（记录操作
// 来源），缺失即 Fatal。
type AdminObserverSetter interface {
	WithReplayObserver(o event.Observer) event.Admin
}

// RunAdminReplaySuite 运行管理面 Admin 重放契约测试。admin 须非空——
// 管理写路径不经 Driver 类型暴露，只能由后端 NewAdmin 构造获得（缺失即
// Fatal，沿 ClaimStateReader/RunInspectorSuite 先例：提供该能力的后端
// 必须全过本套件）。d 须实现 ClaimStateReader（重放事实断言的数据源）；
// admin 须实现 AdminExclusionLock 与 AdminObserverSetter（互斥与操作来源
// 记录是管理面契约约束）。
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
	excl, ok := admin.(AdminExclusionLock)
	if !ok {
		t.Fatalf("admin %T must implement eventtest.AdminExclusionLock: mutual "+
			"exclusion with migration execution is a hard constraint of the "+
			"management write surface (all writes hold the corresponding lock)", admin)
	}
	obsSetter, ok := admin.(AdminObserverSetter)
	if !ok {
		t.Fatalf("admin %T must implement eventtest.AdminObserverSetter: recording "+
			"the operation source (typed Observer events) is a hard constraint of "+
			"the management write surface", admin)
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

	// ---- 互斥（与迁移执行共用命名空间锁）----
	t.Run("ExclusionLockHeld_AdminReplay_LockTimeoutRowUnchanged", func(t *testing.T) { testAdminReplayExclusionLockHeld(t, d, admin, rd, excl) })
	t.Run("ExclusionLockReleased_AdminReplay_ReplayTakesEffect", func(t *testing.T) { testAdminReplayExclusionLockReleased(t, d, admin, rd, excl) })

	// ---- 操作来源记录（类型化观测旁路）----
	t.Run("TerminalFailedRow_AdminReplay_EmitsReplayedThenNoEffect", func(t *testing.T) { testAdminReplayObservation(t, d, admin, obsSetter) })
	t.Run("ExclusionLockHeld_AdminReplay_ObservesLockTimeout", func(t *testing.T) { testAdminReplayObservesLockTimeout(t, d, admin, rd, excl, obsSetter) })

	// ---- 取消路径（canceled 结果枚举）----
	t.Run("CanceledContext_AdminReplay_CanceledResultRowUntouched", func(t *testing.T) { testAdminReplayCanceledContext(t, d, admin, rd, obsSetter) })
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

// ---- 互斥（与迁移执行共用命名空间锁）----

// warmAdmin 预热 Admin 实例的 ensure（首次操作按迁移模式处理持久结构）：
// 互斥断言针对管理写路径的锁等待，而非 ensure 内迁移的锁等待。对尚未
// 落行的键重放（无效果、无错误）即完成预热。
func warmAdmin(t *testing.T, admin event.Admin, eventID, consumerType string) {
	t.Helper()
	ok, err := admin.ReplayFailed(t.Context(), eventID, consumerType, baseNow)
	if ok || err != nil {
		t.Fatalf("warm-up replay (%s, %s) = (%v, %v), want (false, nil)",
			eventID, consumerType, ok, err)
	}
}

// 锁占用期间：重放有界等待后以 ErrAdminLockTimeout（瞬时）失败，行不被
// 改写——写操作持锁的互斥语义（Postgres 后端锁占用方为迁移执行，inmem
// 为进程内等价信号量，套件经 AdminExclusionLock 注入占用）。
func testAdminReplayExclusionLockHeld(t *testing.T, d event.Driver, admin event.Admin, rd ClaimStateReader, excl AdminExclusionLock) {
	typ := t.Name()
	warmAdmin(t, admin, "ar-x1", "arx")
	adminFailedFixture(t, d, "ar-x1", typ, "arx")

	release := excl.HoldExclusionLock(t)
	t.Cleanup(release)

	ok, err := admin.ReplayFailed(t.Context(), "ar-x1", "arx", baseNow.Add(time.Minute))
	if ok {
		t.Fatal("replay while exclusion lock held must not take effect, got ok=true")
	}
	if !errors.Is(err, event.ErrAdminLockTimeout) {
		t.Fatalf("replay while lock held = %v, want transient ErrAdminLockTimeout", err)
	}
	st := readClaimState(t, rd, "ar-x1", "arx")
	if st.Status != "failed" {
		t.Fatalf("status = %q, want failed (timed-out replay must not touch the row)", st.Status)
	}
}

// 锁释放后：同一 failed 行重放生效、回到可认领路径——互斥是串行化而非
// 拒绝，锁超时为瞬时错误。
func testAdminReplayExclusionLockReleased(t *testing.T, d event.Driver, admin event.Admin, rd ClaimStateReader, excl AdminExclusionLock) {
	typ := t.Name()
	warmAdmin(t, admin, "ar-y1", "ary")
	adminFailedFixture(t, d, "ar-y1", typ, "ary")

	release := excl.HoldExclusionLock(t)
	// 失败于断言时也必须释放锁（release 幂等）：inmem 信号量被占用会
	// 连锁阻塞后续子测试，Postgres 会话锁随连接滞留池中。
	t.Cleanup(release)
	if _, err := admin.ReplayFailed(t.Context(), "ar-y1", "ary", baseNow.Add(time.Minute)); !errors.Is(err, event.ErrAdminLockTimeout) {
		t.Fatalf("replay while lock held = %v, want ErrAdminLockTimeout", err)
	}
	release()

	replayAt := baseNow.Add(2 * time.Minute)
	if !replay(t, admin, "ar-y1", "ary", replayAt) {
		t.Fatal("failed row must be replayable after lock release, got ok=false")
	}
	st := readClaimState(t, rd, "ar-y1", "ary")
	if st.Status != "retrying" {
		t.Fatalf("status = %q, want retrying (replay takes effect after release)", st.Status)
	}
}

// ---- 操作来源记录（类型化观测旁路）----

// replayObserver 收集管理面重放观测事件（套件断言操作来源记录）。
type replayObserver struct {
	mu     sync.Mutex
	events []event.AdminReplayEvent
}

func (o *replayObserver) OnPublish(event.PublishEvent)      {}
func (o *replayObserver) OnConsume(event.ConsumeEvent)      {}
func (o *replayObserver) OnWake(event.WakeEvent)            {}
func (o *replayObserver) OnLeaseAbandoned(event.LeaseEvent) {}
func (o *replayObserver) OnRunnerStopped(event.StopEvent)   {}

func (o *replayObserver) OnAdminReplay(e event.AdminReplayEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, e)
}

func (o *replayObserver) collected() []event.AdminReplayEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]event.AdminReplayEvent(nil), o.events...)
}

// 成功路径的观测事件：重放生效发射 ok（事件 × 身份与调用一致），对同一
// 行的重复重放（已非 failed）发射 no_effect——结果枚举与返回值一致。
func testAdminReplayObservation(t *testing.T, d event.Driver, admin event.Admin, obsSetter AdminObserverSetter) {
	typ := t.Name()
	adminFailedFixture(t, d, "ar-o1", typ, "aro")

	obs := &replayObserver{}
	observed := obsSetter.WithReplayObserver(obs)

	replayAt := baseNow.Add(time.Minute)
	if !replay(t, observed, "ar-o1", "aro", replayAt) {
		t.Fatal("failed row must be replayable, got ok=false")
	}
	if replay(t, observed, "ar-o1", "aro", replayAt) {
		t.Fatal("already replayed row must have no effect, got ok=true")
	}

	events := obs.collected()
	if len(events) != 2 {
		t.Fatalf("observation events = %d, want 2 (replayed + no effect)", len(events))
	}
	first, second := events[0], events[1]
	if first.Result != event.AdminReplayOK || second.Result != event.AdminReplayNoEffect {
		t.Fatalf("results = [%s, %s], want [%s, %s]",
			first.Result, second.Result, event.AdminReplayOK, event.AdminReplayNoEffect)
	}
	if first.EventID != "ar-o1" || first.ConsumerType != "aro" ||
		second.EventID != "ar-o1" || second.ConsumerType != "aro" {
		t.Fatalf("event identities = (%s, %s) / (%s, %s), want (ar-o1, aro) twice",
			first.EventID, first.ConsumerType, second.EventID, second.ConsumerType)
	}
	if first.Err != nil || second.Err != nil {
		t.Fatalf("errs = %v / %v, want nil on ok and no_effect results", first.Err, second.Err)
	}
}

// 锁占用期间的观测事件：结果枚举为 lock_timeout、Err 为瞬时锁超时错误
// （失败结果携带错误，可诊断）。
func testAdminReplayObservesLockTimeout(t *testing.T, d event.Driver, admin event.Admin, rd ClaimStateReader, excl AdminExclusionLock, obsSetter AdminObserverSetter) {
	typ := t.Name()
	obs := &replayObserver{}
	observed := obsSetter.WithReplayObserver(obs)
	// 预热观测实例的 ensure（等价 Admin 为新实例）：锁占用期间 ensure 的
	// 迁移会先撞锁，预热后互斥断言落在管理写路径上。预热须先于 failed
	// 行装配——预热用同一键、行落库前尚不存在（无效果）。
	warmAdmin(t, observed, "ar-t1", "art")
	adminFailedFixture(t, d, "ar-t1", typ, "art")

	release := excl.HoldExclusionLock(t)
	t.Cleanup(release)

	_, err := observed.ReplayFailed(t.Context(), "ar-t1", "art", baseNow.Add(time.Minute))
	if !errors.Is(err, event.ErrAdminLockTimeout) {
		t.Fatalf("replay while lock held = %v, want ErrAdminLockTimeout", err)
	}

	events := obs.collected()
	// 预热重放（无效果）与锁超时重放各发一次事件：取结果为 lock_timeout
	// 的事件断言（恰一次）。
	var timedOut []event.AdminReplayEvent
	for _, e := range events {
		if e.Result == event.AdminReplayLockTimeout {
			timedOut = append(timedOut, e)
		}
	}
	if len(timedOut) != 1 {
		t.Fatalf("lock_timeout observation events = %d (all: %+v), want 1", len(timedOut), events)
	}
	ev := timedOut[0]
	if ev.EventID != "ar-t1" || ev.ConsumerType != "art" {
		t.Fatalf("identity = (%s, %s), want (ar-t1, art)", ev.EventID, ev.ConsumerType)
	}
	if !errors.Is(ev.Err, event.ErrAdminLockTimeout) {
		t.Fatalf("event err = %v, want ErrAdminLockTimeout", ev.Err)
	}
}

// ---- 取消路径（canceled 结果枚举）----

// 调用方取消：不发起写、判别为 canceled（异常路径显式断言）。canceled
// 不记错误日志的口径由后端 slog 集成用例覆盖（套件无 slog 面），此处
// 断言返回值与观测枚举。
func testAdminReplayCanceledContext(t *testing.T, d event.Driver, admin event.Admin, rd ClaimStateReader, obsSetter AdminObserverSetter) {
	typ := t.Name()
	obs := &replayObserver{}
	observed := obsSetter.WithReplayObserver(obs)
	warmAdmin(t, observed, "ar-nx", "arnx")
	adminFailedFixture(t, d, "ar-nx", typ, "arnx")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ok, err := observed.ReplayFailed(ctx, "ar-nx", "arnx", baseNow.Add(time.Minute))
	if ok {
		t.Fatal("canceled replay must not take effect, got ok=true")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled replay err = %v, want context.Canceled", err)
	}
	st := readClaimState(t, rd, "ar-nx", "arnx")
	if st.Status != "failed" {
		t.Fatalf("status = %q, want failed (canceled replay must not touch the row)", st.Status)
	}

	var canceled []event.AdminReplayEvent
	for _, e := range obs.collected() {
		if e.Result == event.AdminReplayCanceled {
			canceled = append(canceled, e)
		}
	}
	if len(canceled) != 1 {
		t.Fatalf("canceled observation events = %d (all: %+v), want 1", len(canceled), obs.collected())
	}
	if canceled[0].EventID != "ar-nx" || canceled[0].ConsumerType != "arnx" {
		t.Fatalf("identity = (%s, %s), want (ar-nx, arnx)", canceled[0].EventID, canceled[0].ConsumerType)
	}
	if !errors.Is(canceled[0].Err, context.Canceled) {
		t.Fatalf("event err = %v, want context.Canceled", canceled[0].Err)
	}
}
